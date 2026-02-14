'''
Shared Spark utilities for water billing notebook and tests.
'''

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = 'WaterBilling',
    master: str = 'local[*]',
    log_level: str = 'WARN',
    shuffle_partitions: int | None = None,
) -> SparkSession:
    '''
    Create a configured Spark session with Delta Lake support.
    
    Args:
        app_name: Name for the Spark application
        master: Spark master URL (default: local[*])
        log_level: Log level for Spark (default: WARN)
        shuffle_partitions: Number of shuffle partitions (default: None, uses Spark default)
        
    Returns:
        Configured SparkSession with Delta Lake extensions
    '''
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config('spark.jars.packages', 'io.delta:delta-spark_2.13:4.0.1')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    )
    
    if shuffle_partitions is not None:
        builder = builder.config('spark.sql.shuffle.partitions', str(shuffle_partitions))
    
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel(log_level)
    
    return session
