from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, current_timestamp
import re
import time

spark = SparkSession.builder \
    .appName("IngestionFramework") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    

#equivalent to json file. the only non modular part
DATASET_CONFIGS = {
    "weather": {
        "format": "csv",
        "path": "weather.csv",
        "primary_keys": ["year", "month", "day", "hour"],
        "timestamp_cols": {}, # If weather just has year/month/day/hour integers
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_trips_01": {
        "format": "parquet",
        "path": "yellow_tripdata_2024-01.parquet",
        "primary_keys": [], # None defined natively
        "timestamp_cols": {"tpep_pickup_datetime": "yyyy-MM-dd HH:mm:ss", 
                           "tpep_dropoff_datetime": "yyyy-MM-dd HH:mm:ss"},
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_trips_02": {
        "format": "parquet",
        "path": "yellow_tripdata_2024-02.parquet",
        "primary_keys": [], # None defined natively
        "timestamp_cols": {"tpep_pickup_datetime": "yyyy-MM-dd HH:mm:ss", 
                           "tpep_dropoff_datetime": "yyyy-MM-dd HH:mm:ss"},
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_trips_03": {
        "format": "parquet",
        "path": "yellow_tripdata_2024-03.parquet",
        "primary_keys": [], # None defined natively
        "timestamp_cols": {"tpep_pickup_datetime": "yyyy-MM-dd HH:mm:ss", 
                           "tpep_dropoff_datetime": "yyyy-MM-dd HH:mm:ss"},
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_zone_lookup": {
        "format": "csv",
        "path": "taxi_zone_lookup.csv",
        "primary_keys": ["location_id"],
        "timestamp_cols": {}, # None defined
        "options": {"header": "true", "inferSchema": "true"} }
}

# Functions used for normalizing and fixing data.
def to_snake_case(name):
    """CamelCase / mixed case -> snake_case, handles acronym runs like 'PULocationID'."""
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace(" ", "_").replace("-", "_").lower()


def standardize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, to_snake_case(c))
    return df

#timestamps are transformed into TimestampType
def normalize_timestamps(df, timestamp_cols):
    """Casts defined columns to timestamp"""
    for col_name, fmt in timestamp_cols.items():
        # Ensure the col_name here matches the standardized snake_case version
        snake_col_name = col_name.lower()
        df = df.withColumn(snake_col_name, to_timestamp(col(snake_col_name), fmt))

    
    return df

#Removes duplicates according to PK (or full row duplicates)
def perform_data_quality_checks(df, primary_keys):
    """Filters out invalid data based on composite primary keys"""
    initial_count = df.count()

    if primary_keys:
        # 1. Drop row if ANY part of the composite primary key is missing (Null)
        df = df.dropna(subset=primary_keys)

        # 2. Drop duplicates based strictly on the combination of the primary keys
        df = df.dropDuplicates(subset=primary_keys)
    else:
        # If no primary key exists (like Taxi Trips), just drop exact full-row duplicates
        df = df.dropDuplicates()

    final_count = df.count()
    rejected_count = initial_count - final_count

    return df, initial_count, rejected_count


def schema_hash(df):
    """A simple stand-in for a schema version: hash of sorted (column, type) pairs."""
    return hash(tuple(sorted(df.dtypes)))
 

#  Main Ingestion Execution
def ingest_dataset(dataset_name, config):
    start_time = time.time()

    # A. Load according to file type (csv, parquet)
    df = spark.read.format(config["format"]).options(**config["options"]).load(config["path"])

    # B. Standardize (snake case) & Transform (timestamps)
    df = standardize_columns(df)
    df = normalize_timestamps(df, config["timestamp_cols"])
    

    # (Optional) Apply dataset-specific transformation rules here using an IF statement

    # C. Quality Checks
    # Standardize PK names to match the DataFrame
    standard_pks = [c.lower() for c in config["primary_keys"]]
    df, initial_cnt, rejected_cnt = perform_data_quality_checks(df, standard_pks)

    # D. Write to Delta
    output_path = f"/content/delta/{dataset_name}"
    df.write.format("delta").mode("overwrite").save(output_path)

    execution_time = time.time() - start_time

    # E. Metadata Generation
    metadata = {
        "dataset": dataset_name,
        "processed_records": initial_cnt,
        "rejected_records": rejected_cnt,
        "final_records": (initial_cnt - rejected_cnt),
        "execution_time_seconds": round(execution_time, 2),
        "schema_version": schema_hash(df)
    }
    return metadata

# 4. Run the framework
ingestion_logs = []
for name, conf in DATASET_CONFIGS.items():
    print(f"Ingesting {name}...")
    log = ingest_dataset(name, conf)
    ingestion_logs.append(log)

print(*ingestion_logs, sep="\n")

