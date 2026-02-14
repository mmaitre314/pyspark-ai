'''
Schema definitions for water utility billing system.
'''

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DoubleType,
)

# Input schema: Water meter usage records
# - meter_id: Unique identifier for the meter device
# - customer_id: Customer associated with the meter
# - recording_time: Timestamp when usage was recorded at the meter
# - usage_gallons: Amount of water used in gallons
# - record_id: Unique identifier for deduplication
USAGE_INPUT_SCHEMA = StructType([
    StructField('meter_id', StringType(), False),
    StructField('customer_id', StringType(), False),
    StructField('recording_time', TimestampType(), False),
    StructField('usage_gallons', DoubleType(), False),
    StructField('record_id', StringType(), False),
])

# Output schema: Hourly aggregated usage per customer
# - customer_id: Customer identifier
# - recording_hour: The hour for which usage is aggregated (formatted as yyyy-MM-dd-HH)
# - total_usage_gallons: Total water usage for that hour
HOURLY_USAGE_SCHEMA = StructType([
    StructField('customer_id', StringType(), False),
    StructField('recording_hour', StringType(), False),
    StructField('total_usage_gallons', DoubleType(), False),
])

# Output schema: Cumulative monthly usage per customer
# - customer_id: Customer identifier
# - recording_hour: The hour up to which usage is accumulated (formatted as yyyy-MM-dd-HH)
# - cumulative_usage_gallons: Running total of usage for the month up to this hour
CUMULATIVE_USAGE_SCHEMA = StructType([
    StructField('customer_id', StringType(), False),
    StructField('recording_hour', StringType(), False),
    StructField('cumulative_usage_gallons', DoubleType(), False),
])
