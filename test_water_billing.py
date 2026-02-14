'''
Unit tests for water billing notebook.

Tests execute the notebook cells programmatically and validate the output.
'''

import json
import os
import shutil
import tempfile
import uuid
from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from spark_utils import create_spark_session


@pytest.fixture(scope='module')
def spark():
    '''Create a Spark session for testing.'''
    session = create_spark_session(
        app_name='WaterBillingTest',
        log_level='ERROR',
        shuffle_partitions=4,
    )
    yield session
    session.stop()


@pytest.fixture
def test_dir():
    '''Create a temporary directory for test data.'''
    dir_path = tempfile.mkdtemp(prefix='water_billing_test_')
    yield dir_path
    shutil.rmtree(dir_path, ignore_errors=True)


def create_test_data(data_dir: str, records: list[dict]):
    '''
    Create test input data files in Hive-style partitioned directories.
    
    Args:
        data_dir: Base directory for test data
        records: List of records with ingestion_hour for partitioning
    
    Directory format: data_dir/ingestion_hour=yyyy-MM-dd-HH/
    '''
    # Group records by ingestion hour
    partitions = {}
    for record in records:
        ingestion_hour = record.pop('ingestion_hour', record['recording_time'][:13])
        dt = datetime.fromisoformat(ingestion_hour.replace('-', '-').replace('T', ' ')[:13] + ':00:00')
        # Use Hive-style partition format: ingestion_hour=yyyy-MM-dd-HH
        partition_key = f"ingestion_hour={dt.strftime('%Y-%m-%d-%H')}"
        
        if partition_key not in partitions:
            partitions[partition_key] = []
        partitions[partition_key].append(record)
    
    # Write each partition
    for partition_key, partition_records in partitions.items():
        partition_dir = os.path.join(data_dir, partition_key)
        os.makedirs(partition_dir, exist_ok=True)
        
        filename = f'data_{uuid.uuid4().hex[:8]}.json'
        filepath = os.path.join(partition_dir, filename)
        
        with open(filepath, 'w') as f:
            for record in partition_records:
                f.write(json.dumps(record) + '\n')


def load_notebook(notebook_path: str) -> list[dict]:
    '''Load notebook and return cells.'''
    with open(notebook_path, 'r') as f:
        notebook = json.load(f)
    return notebook['cells']


def execute_notebook(
    spark: SparkSession,
    notebook_path: str,
    params: dict,
    verbose: bool = False,
) -> dict:
    '''
    Execute notebook cells with custom parameters.
    
    Args:
        spark: Spark session to use (passed to notebook via spark parameter)
        notebook_path: Path to the notebook file
        params: Dictionary of parameter overrides (INPUT_PATH, OUTPUT_PATH, etc.)
        verbose: If False, display-only cells will skip their output
        
    Returns:
        Dictionary with execution context (locals)
    '''
    cells = load_notebook(notebook_path)
    
    # Build execution context with spark session
    exec_globals = {}
    exec_locals = {}
    
    code_cell_index = 0
    for cell in cells:
        if cell['cell_type'] != 'code':
            continue
        
        source = ''.join(cell['source'])
        
        # After first code cell (parameters), inject parameter overrides
        if code_cell_index == 0:
            # Execute the parameters cell first
            try:
                exec(source, exec_globals, exec_locals)
                exec_globals.update(exec_locals)
            except Exception as e:
                raise RuntimeError(f'Error executing parameters cell: {e}\nSource:\n{source}')
            
            # Now override with test parameters
            override_code = f'spark = spark\nVERBOSE = {verbose}\n'
            for param_name, param_value in params.items():
                override_code += f"{param_name} = '{param_value}'\n"
            
            # Execute overrides with spark session in context
            exec_globals['spark'] = spark
            exec(override_code, exec_globals, exec_locals)
            exec_globals.update(exec_locals)
            
            code_cell_index += 1
            continue
        
        try:
            exec(source, exec_globals, exec_locals)
            # Update globals with locals for next cell
            exec_globals.update(exec_locals)
        except Exception as e:
            raise RuntimeError(f'Error executing cell {code_cell_index}: {e}\nSource:\n{source}')
        
        code_cell_index += 1
    
    return exec_locals


class TestWaterBillingNotebook:
    '''Tests for water billing notebook.'''
    
    NOTEBOOK_PATH = 'water_billing.ipynb'
    
    def _get_base_params(self, test_dir, window_start, window_end):
        '''Get base parameters for notebook execution with tumbling window settings.'''
        return {
            'INPUT_PATH': os.path.join(test_dir, 'input'),
            'HOURLY_OUTPUT_PATH': os.path.join(test_dir, 'hourly'),
            'CUMULATIVE_OUTPUT_PATH': os.path.join(test_dir, 'cumulative'),
            'CHECKPOINT_PATH': os.path.join(test_dir, 'checkpoint'),
            'DEDUPLICATED_RAW_PATH': os.path.join(test_dir, 'deduplicated_raw'),
            'WINDOW_START_TIME': window_start.isoformat(),
            'WINDOW_END_TIME': window_end.isoformat(),
        }
    
    def test_basic_aggregation(self, spark, test_dir):
        '''Test basic hourly aggregation for a single customer.'''
        # Create simple test data: 3 records for same customer in same hour
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        records = [
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 1.5,
                'record_id': 'rec_001',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=15)).isoformat(),
                'usage_gallons': 2.0,
                'record_id': 'rec_002',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=30)).isoformat(),
                'usage_gallons': 1.0,
                'record_id': 'rec_003',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        # Set window to cover the test data (7 days before to 1 hour after)
        window_start = base_time - timedelta(days=7)
        window_end = base_time + timedelta(hours=1)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        # Execute notebook
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        # Verify hourly output
        hourly_df = spark.read.format('delta').load(params['HOURLY_OUTPUT_PATH'])
        assert hourly_df.count() == 1
        
        row = hourly_df.collect()[0]
        assert row['customer_id'] == 'customer_001'
        assert row['total_usage_gallons'] == pytest.approx(4.5, rel=1e-6)
        
        # Verify cumulative output
        cumulative_df = spark.read.format('delta').load(params['CUMULATIVE_OUTPUT_PATH'])
        assert cumulative_df.count() == 1
        
        cum_row = cumulative_df.collect()[0]
        assert cum_row['cumulative_usage_gallons'] == pytest.approx(4.5, rel=1e-6)
    
    def test_deduplication(self, spark, test_dir):
        '''Test that duplicate records are properly deduplicated.'''
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        
        # Create data with duplicate record_id
        records = [
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 2.0,
                'record_id': 'duplicate_rec',  # Same record_id
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 2.0,
                'record_id': 'duplicate_rec',  # Same record_id - duplicate
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=20)).isoformat(),
                'usage_gallons': 1.0,
                'record_id': 'unique_rec',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        window_start = base_time - timedelta(days=7)
        window_end = base_time + timedelta(hours=1)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        # Should have 2.0 + 1.0 = 3.0 (not 2.0 + 2.0 + 1.0 = 5.0)
        hourly_df = spark.read.format('delta').load(params['HOURLY_OUTPUT_PATH'])
        row = hourly_df.collect()[0]
        assert row['total_usage_gallons'] == pytest.approx(3.0, rel=1e-6)
    
    def test_multiple_customers(self, spark, test_dir):
        '''Test aggregation for multiple customers.'''
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        
        records = [
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 3.0,
                'record_id': 'rec_001',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            {
                'meter_id': 'meter_002',
                'customer_id': 'customer_002',
                'recording_time': (base_time + timedelta(minutes=10)).isoformat(),
                'usage_gallons': 5.0,
                'record_id': 'rec_002',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        window_start = base_time - timedelta(days=7)
        window_end = base_time + timedelta(hours=1)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        hourly_df = spark.read.format('delta').load(params['HOURLY_OUTPUT_PATH'])
        assert hourly_df.count() == 2
        
        # Check each customer
        c1 = hourly_df.filter(F.col('customer_id') == 'customer_001').collect()[0]
        assert c1['total_usage_gallons'] == pytest.approx(3.0, rel=1e-6)
        
        c2 = hourly_df.filter(F.col('customer_id') == 'customer_002').collect()[0]
        assert c2['total_usage_gallons'] == pytest.approx(5.0, rel=1e-6)
    
    def test_cumulative_across_hours(self, spark, test_dir):
        '''Test cumulative usage calculation across multiple hours.'''
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        
        # Create data spanning 3 hours
        records = [
            # Hour 10
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 1.0,
                'record_id': 'rec_001',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            # Hour 11
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(hours=1, minutes=5)).isoformat(),
                'usage_gallons': 2.0,
                'record_id': 'rec_002',
                'ingestion_hour': (base_time + timedelta(hours=1)).strftime('%Y-%m-%dT%H'),
            },
            # Hour 12
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(hours=2, minutes=5)).isoformat(),
                'usage_gallons': 3.0,
                'record_id': 'rec_003',
                'ingestion_hour': (base_time + timedelta(hours=2)).strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        window_start = base_time - timedelta(days=7)
        window_end = base_time + timedelta(hours=3)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        # Verify cumulative sums
        cumulative_df = spark.read.format('delta').load(params['CUMULATIVE_OUTPUT_PATH'])
        cumulative_df = cumulative_df.orderBy('recording_hour').collect()
        
        assert len(cumulative_df) == 3
        
        # Hour 10: cumulative = 1.0
        assert cumulative_df[0]['cumulative_usage_gallons'] == pytest.approx(1.0, rel=1e-6)
        
        # Hour 11: cumulative = 1.0 + 2.0 = 3.0
        assert cumulative_df[1]['cumulative_usage_gallons'] == pytest.approx(3.0, rel=1e-6)
        
        # Hour 12: cumulative = 1.0 + 2.0 + 3.0 = 6.0
        assert cumulative_df[2]['cumulative_usage_gallons'] == pytest.approx(6.0, rel=1e-6)
    
    def test_cumulative_resets_each_month(self, spark, test_dir):
        '''Test that cumulative usage resets at the start of each month.'''
        # End of January and start of February
        jan_time = datetime(2026, 1, 31, 23, 0, 0)
        feb_time = datetime(2026, 2, 1, 0, 0, 0)
        
        records = [
            # January 31, 23:00
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (jan_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 10.0,
                'record_id': 'rec_jan',
                'ingestion_hour': jan_time.strftime('%Y-%m-%dT%H'),
            },
            # February 1, 00:00
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (feb_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 5.0,
                'record_id': 'rec_feb',
                'ingestion_hour': feb_time.strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        window_start = jan_time - timedelta(days=7)
        window_end = feb_time + timedelta(hours=1)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        cumulative_df = spark.read.format('delta').load(params['CUMULATIVE_OUTPUT_PATH'])
        cumulative_df = cumulative_df.orderBy('recording_hour').collect()
        
        assert len(cumulative_df) == 2
        
        # January: cumulative = 10.0
        assert cumulative_df[0]['cumulative_usage_gallons'] == pytest.approx(10.0, rel=1e-6)
        
        # February: cumulative resets, = 5.0 (not 15.0)
        assert cumulative_df[1]['cumulative_usage_gallons'] == pytest.approx(5.0, rel=1e-6)
    
    def test_idempotent_rerun(self, spark, test_dir):
        '''Test that running the notebook twice produces the same result.'''
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        
        records = [
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 2.5,
                'record_id': 'rec_001',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        window_start = base_time - timedelta(days=7)
        window_end = base_time + timedelta(hours=1)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        # Run notebook twice
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        # Should still have only 1 record
        hourly_df = spark.read.format('delta').load(params['HOURLY_OUTPUT_PATH'])
        assert hourly_df.count() == 1
        
        row = hourly_df.collect()[0]
        assert row['total_usage_gallons'] == pytest.approx(2.5, rel=1e-6)
    
    def test_multiple_meters_per_customer(self, spark, test_dir):
        '''Test that multiple meters for the same customer are aggregated together.'''
        base_time = datetime(2026, 1, 15, 10, 0, 0)
        
        # Same customer, two different meters
        records = [
            {
                'meter_id': 'meter_001',
                'customer_id': 'customer_001',
                'recording_time': (base_time + timedelta(minutes=5)).isoformat(),
                'usage_gallons': 3.0,
                'record_id': 'rec_001',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
            {
                'meter_id': 'meter_002',  # Different meter
                'customer_id': 'customer_001',  # Same customer
                'recording_time': (base_time + timedelta(minutes=10)).isoformat(),
                'usage_gallons': 4.0,
                'record_id': 'rec_002',
                'ingestion_hour': base_time.strftime('%Y-%m-%dT%H'),
            },
        ]
        
        input_dir = os.path.join(test_dir, 'input')
        create_test_data(input_dir, records)
        
        window_start = base_time - timedelta(days=7)
        window_end = base_time + timedelta(hours=1)
        params = self._get_base_params(test_dir, window_start, window_end)
        
        execute_notebook(spark, self.NOTEBOOK_PATH, params)
        
        # Should have one aggregated record per customer per hour
        hourly_df = spark.read.format('delta').load(params['HOURLY_OUTPUT_PATH'])
        assert hourly_df.count() == 1
        
        row = hourly_df.collect()[0]
        assert row['customer_id'] == 'customer_001'
        # 3.0 + 4.0 = 7.0
        assert row['total_usage_gallons'] == pytest.approx(7.0, rel=1e-6)
