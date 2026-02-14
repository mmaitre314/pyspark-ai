#!/usr/bin/env python3
'''
Synthetic data generator for water utility meter usage records.

Generates JSONL files organized in hourly partitions based on ingestion time,
simulating real-world data arrival patterns including:
- Records arriving with slight delays
- Out-of-order records
- Duplicate records (for retry scenarios)
- Late-arriving records (up to 7 days late due to connectivity issues)
'''

import argparse
import json
import os
import random
import uuid
from datetime import datetime, timedelta


def parse_hour(hour_str: str) -> datetime:
    '''Parse hour string in format YYYY-MM-DD-HH to datetime.'''
    return datetime.strptime(hour_str, '%Y-%m-%d-%H')


def format_hour_path(dt: datetime) -> str:
    '''Format datetime to Hive-style hourly partition path.'''
    return f"ingestion_hour={dt.strftime('%Y-%m-%d-%H')}"


def generate_record(
    meter_id: str,
    customer_id: str,
    recording_time: datetime,
    record_id: str = None,
) -> dict:
    '''Generate a single usage record.'''
    return {
        'meter_id': meter_id,
        'customer_id': customer_id,
        'recording_time': recording_time.isoformat(),
        'usage_gallons': round(random.uniform(0.1, 5.0), 2),
        'record_id': record_id or str(uuid.uuid4()),
    }


def generate_data(
    output_dir: str,
    start_hour: datetime,
    end_hour: datetime,
    num_customers: int = 10,
    meters_per_customer: int = 2,
    records_per_meter_per_hour: int = 12,  # ~5 min intervals
    duplicate_rate: float = 0.02,
    late_arrival_rate: float = 0.05,
    max_late_hours: int = 168,  # 7 days
):
    '''
    Generate synthetic meter usage data.
    
    Args:
        output_dir: Base directory for output data
        start_hour: Start hour for data generation
        end_hour: End hour for data generation (exclusive)
        num_customers: Number of customers to generate
        meters_per_customer: Number of meters per customer
        records_per_meter_per_hour: Records per meter per hour
        duplicate_rate: Rate of duplicate records
        late_arrival_rate: Rate of late-arriving records
        max_late_hours: Maximum hours a record can be late
    '''
    # Generate customer and meter IDs
    customers = [f'customer_{i:04d}' for i in range(num_customers)]
    meters = {}
    for customer in customers:
        meters[customer] = [
            f'meter_{customer}_{j:02d}' 
            for j in range(meters_per_customer)
        ]
    
    # Collect records by ingestion hour partition
    partitions = {}
    
    current_hour = start_hour
    while current_hour < end_hour:
        for customer in customers:
            for meter_id in meters[customer]:
                # Generate records for this hour
                for i in range(records_per_meter_per_hour):
                    # Recording time is within this hour
                    recording_time = current_hour + timedelta(
                        minutes=random.randint(0, 59),
                        seconds=random.randint(0, 59),
                    )
                    
                    record = generate_record(meter_id, customer, recording_time)
                    
                    # Determine ingestion time (usually same hour, but can be late)
                    if random.random() < late_arrival_rate:
                        # Late arrival: ingestion time is hours after recording
                        delay_hours = random.randint(1, max_late_hours)
                        ingestion_hour = current_hour + timedelta(hours=delay_hours)
                    else:
                        # Normal: ingested in same hour or next hour
                        delay_minutes = random.randint(0, 10)
                        ingestion_hour = recording_time + timedelta(minutes=delay_minutes)
                        ingestion_hour = ingestion_hour.replace(
                            minute=0, second=0, microsecond=0
                        )
                    
                    # Only write if ingestion hour falls within our range
                    # (late arrivals may fall outside)
                    partition_key = format_hour_path(ingestion_hour)
                    if partition_key not in partitions:
                        partitions[partition_key] = []
                    partitions[partition_key].append(record)
                    
                    # Add duplicate with small probability
                    if random.random() < duplicate_rate:
                        # Duplicate arrives within an hour (may cross partition)
                        dup_delay = timedelta(minutes=random.randint(1, 60))
                        dup_ingestion = ingestion_hour + dup_delay
                        dup_partition = format_hour_path(dup_ingestion)
                        if dup_partition not in partitions:
                            partitions[dup_partition] = []
                        # Same record_id for deduplication
                        partitions[dup_partition].append(record.copy())
        
        current_hour += timedelta(hours=1)
    
    # Write records to files
    file_id = uuid.uuid4().hex[:8]
    for partition_key, records in partitions.items():
        partition_dir = os.path.join(output_dir, partition_key)
        os.makedirs(partition_dir, exist_ok=True)
        
        # Use unique filename to avoid overwriting
        filename = f'data_{file_id}_{uuid.uuid4().hex[:8]}.json'
        filepath = os.path.join(partition_dir, filename)
        
        # Shuffle records to simulate out-of-order arrival
        random.shuffle(records)
        
        with open(filepath, 'w') as f:
            for record in records:
                f.write(json.dumps(record) + '\n')
        
        print(f'Wrote {len(records)} records to {filepath}')


def main():
    parser = argparse.ArgumentParser(
        description='Generate synthetic water meter usage data'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='data/input/raw',
        help='Output directory for generated data',
    )
    parser.add_argument(
        '--start-hour',
        type=str,
        required=True,
        help='Start hour in format YYYY-MM-DD-HH',
    )
    parser.add_argument(
        '--end-hour',
        type=str,
        required=True,
        help='End hour in format YYYY-MM-DD-HH (exclusive)',
    )
    parser.add_argument(
        '--num-customers',
        type=int,
        default=10,
        help='Number of customers to generate',
    )
    parser.add_argument(
        '--meters-per-customer',
        type=int,
        default=2,
        help='Number of meters per customer',
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help='Random seed for reproducibility',
    )
    
    args = parser.parse_args()
    
    if args.seed is not None:
        random.seed(args.seed)
    
    start_hour = parse_hour(args.start_hour)
    end_hour = parse_hour(args.end_hour)
    
    if end_hour <= start_hour:
        raise ValueError('end-hour must be after start-hour')
    
    generate_data(
        output_dir=args.output_dir,
        start_hour=start_hour,
        end_hour=end_hour,
        num_customers=args.num_customers,
        meters_per_customer=args.meters_per_customer,
    )
    
    print(f'Data generation complete for {start_hour} to {end_hour}')


if __name__ == '__main__':
    main()
