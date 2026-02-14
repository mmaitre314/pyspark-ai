# Goal

Create a PySpark notebook for a water utility company to aggregate water usage reported by meter devices installed at customer locations and generate monthly bills.

# Context

## Input data

Each water meter records every few minutes the amount of water used during that period. Those records are sent to a server and stored in a usage table. This JSONL table is an append-only log of records partitioned hourly based on record ingestion time. Records may take a few minutes to be received, can be received out of order, and a small percentage of records may be duplicated during transmission. Duplicated records are due to short-term retries and are assumed to be less than an hour apart from each other, but may cross time-partition boundaries. If meters lose connectivity (due to disconnected network, ingestion server outage, etc.), meters record usage locally for up-to 7 days and transmit those records when connectivity is restored. One customer can have multiple meters.

## Output data

The notebook outputs two Delta tables, one storing usage per hour per customer and a second storing cumulative hourly usage over the month. The tables are partitioned hourly based on usage recording time.

## Data processing

The PySpark notebook will be deployed and scheduled to run hourly. It is production code that needs to be written carefully and without taking shortcuts. Customers will complain if they receive incorrect bills. Once deployed, the notebook will process a large amount of data so it needs to be optimized for performance and reliability. The notebook may get rerun upon failures or if data errors are discovered, so it needs to be idempotent.

The notebook needs to convert data partitioned by usage ingestion time (wall-clock time at ingestion server) into data partitioned by usage recording time (wall-clock time at meter device). This will be achieved as follows:
- Take a new input hour.
- Use incremental batch processing, i.e. Spark Structured Streaming with a `availableNow` trigger, with a 7-day watermark to deduplicate the records and aggregate them in hourly partitions based on recording time. Use a Delta table to merge in an idempotent way. This produces the output table with customer hourly usage.
- Compute a running total over the month. This produces the output table with customer total hourly usage up-to a given hour. The final hour of the month is the customer bill. If it improves performance, consider computing the totals at a given hour by adding the customer hourly usage at that hour to the totals at the previous hour (except for the first hour of the month where there is no previous hour). Take watermark into consideration.

# Implementation

## Implementation plan

1. Inspect the definition of the Dev Container you are running in to get a sense of available tools, environment, etc.
1. Define the schemas for the input and output tables of the notebook
1. Create a Python script which generates synthetic data for the notebook. Organize the data in hourly folders to simulate the real system. The script should take as command-line parameters the start hour and end hour to generate data for. The script should add new files to hourly folder without deleting existing ones (effectively simulating data trickling in to a time partition). The file names should not collide so they do not overwrite each other.
1. Write the notebook and validate it using the synthetic data.
1. Write unit tests for the notebook. Use pytest, load the notebook, execute its cells, and validate the output.

## Implementation details

- Add Delta support when initializing the Spark session
    ```python
    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder
        .master('local[*]')
        .config('spark.jars.packages', 'io.delta:delta-spark_2.13:4.0.1')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .getOrCreate()
    )
    ```
- Ensure that notebook parameters are in the first code cell of the notebook.
- To execute notebooks in pytest tests, load the .ipynb notebook file as JSON and execute the cell code by looping through `$.cells[*].source`. Replace notebook parameters in the first code cell of the notebook so that each test writes output data into its own folder.
- Use single quotes for Python strings.
