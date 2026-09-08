from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lower, current_timestamp, concat, lpad, lit, when, trim
from pyspark.sql.types import StringType, BooleanType
import re
import time

spark = SparkSession.builder \
    .appName("IngestionFramework") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

    

# equivalent to a JSON config file — the only non-modular part.
# Each entry may include the following dataset-specific keys:
#
#   timestamp_cols      : dict of col_name -> format_string
#                         Columns cast to TimestampType (after snake_case rename).
#
#   assembled_timestamp : dict with keys year_col, month_col, day_col,
#                         and optionally hour_col, minute_col, second_col,
#                         plus output_col for the new timestamp column name.
#                         Used when date parts live in separate integer columns.
#
#   date_time_pairs     : list of {date_col, time_col, output_col, format}
#                         Used when date and time live in separate string columns.
#
#   bool_cols           : list of column names to cast to BooleanType.
#                         Understands Y/N, yes/no, true/false, 1/0.
DATASET_CONFIGS = {
    "weather": {
        "format": "csv",
        "path": "weather.csv",
        "primary_keys": ["year", "month", "day", "hour"],
        "timestamp_cols": {},
        # year/month/day/hour integer columns → single timestamp column
        "assembled_timestamp": {
            "year_col":   "year",
            "month_col":  "month",
            "day_col":    "day",
            "hour_col":   "hour",
            "output_col": "timestamp"
        },
        "date_time_pairs": [],
        "bool_cols": [],
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_trips_01": {
        "format": "parquet",
        "path": "yellow_tripdata_2024-01.parquet",
        "primary_keys": [],
        "timestamp_cols": {
            "tpep_pickup_datetime":  "yyyy-MM-dd HH:mm:ss",
            "tpep_dropoff_datetime": "yyyy-MM-dd HH:mm:ss"
        },
        "assembled_timestamp": None,
        "date_time_pairs": [],
        # store_and_fwd_flag is "Y"/"N" in trip data
        "bool_cols": ["store_and_fwd_flag"],
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_trips_02": {
        "format": "parquet",
        "path": "yellow_tripdata_2024-02.parquet",
        "primary_keys": [],
        "timestamp_cols": {
            "tpep_pickup_datetime":  "yyyy-MM-dd HH:mm:ss",
            "tpep_dropoff_datetime": "yyyy-MM-dd HH:mm:ss"
        },
        "assembled_timestamp": None,
        "date_time_pairs": [],
        "bool_cols": ["store_and_fwd_flag"],
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_trips_03": {
        "format": "parquet",
        "path": "yellow_tripdata_2024-03.parquet",
        "primary_keys": [],
        "timestamp_cols": {
            "tpep_pickup_datetime":  "yyyy-MM-dd HH:mm:ss",
            "tpep_dropoff_datetime": "yyyy-MM-dd HH:mm:ss"
        },
        "assembled_timestamp": None,
        "date_time_pairs": [],
        "bool_cols": ["store_and_fwd_flag"],
        "options": {"header": "true", "inferSchema": "true"}
    },
    "taxi_zone_lookup": {
        "format": "csv",
        "path": "taxi_zone_lookup.csv",
        "primary_keys": ["location_id"],
        "timestamp_cols": {},
        "assembled_timestamp": None,
        "date_time_pairs": [],
        "bool_cols": [],
        "options": {"header": "true", "inferSchema": "true"}
    }
}

# ---------------------------------------------------------------------------
# Helper – column name normalisation
# ---------------------------------------------------------------------------
def to_snake_case(name):
    """CamelCase / mixed case → snake_case, handles acronym runs like 'PULocationID'."""
    s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
    s2 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1)
    return s2.replace(" ", "_").replace("-", "_").lower()


def standardize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, to_snake_case(c))
    return df


# ---------------------------------------------------------------------------
# Timestamp normalisation
# ---------------------------------------------------------------------------
def normalize_timestamps(df, timestamp_cols):
    """Cast explicitly listed columns to TimestampType using the provided format."""
    for col_name, fmt in timestamp_cols.items():
        snake = to_snake_case(col_name)
        # If the column is already a TimestampType (e.g. from Parquet), keep it;
        # otherwise parse using the supplied format string.
        col_dtype = dict(df.dtypes).get(snake)
        if col_dtype == "timestamp":
            pass  # already correct
        else:
            df = df.withColumn(snake, to_timestamp(col(snake), fmt))
    return df


def assemble_timestamp_from_parts(df, config):
    """
    Build a single TimestampType column from separate integer or string
    year / month / day / [hour / minute / second] columns.

    The source columns are NOT dropped so that they can still serve as
    primary-key fields if needed.
    """
    if not config:
        return df

    year   = config["year_col"]
    month  = config["month_col"]
    day    = config["day_col"]
    hour   = config.get("hour_col")
    minute = config.get("minute_col")
    second = config.get("second_col")
    out    = config["output_col"]

    # Build a string like "2024-01-01 00:00:00" then parse it
    time_part = concat(
        lpad(col(hour).cast("string"),   2, "0") if hour   else lit("00"), lit(":"),
        lpad(col(minute).cast("string"), 2, "0") if minute else lit("00"), lit(":"),
        lpad(col(second).cast("string"), 2, "0") if second else lit("00"),
    )
    date_part = concat(
        col(year).cast("string"),  lit("-"),
        lpad(col(month).cast("string"), 2, "0"), lit("-"),
        lpad(col(day).cast("string"),   2, "0"),
    )
    datetime_str = concat(date_part, lit(" "), time_part)
    df = df.withColumn(out, to_timestamp(datetime_str, "yyyy-MM-dd HH:mm:ss"))
    return df


def normalize_date_time_pairs(df, pairs):
    """
    Combine separate date-string and time-string columns into a single
    TimestampType column.  The source columns are retained as-is.
    """
    for pair in pairs:
        date_col   = pair["date_col"]
        time_col   = pair["time_col"]
        output_col = pair["output_col"]
        fmt        = pair["format"]
        datetime_str = concat(col(date_col), lit(" "), col(time_col))
        df = df.withColumn(output_col, to_timestamp(datetime_str, fmt))
    return df


# ---------------------------------------------------------------------------
# Common data type normalisation
# ---------------------------------------------------------------------------
def normalize_string_columns(df):
    """
    Generic pass over every StringType column:
      - Strip leading/trailing whitespace (trim)
      - Replace empty strings with NULL so they are consistently absent
    """
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            c = field.name
            df = df.withColumn(c, when(trim(col(c)) == "", None).otherwise(trim(col(c))))
    return df


def normalize_boolean_columns(df, bool_cols):
    """
    Cast dataset-specific boolean-like columns to proper BooleanType.
    Recognised truthy values  : "y", "yes", "true", "1"
    Recognised falsy values   : "n", "no",  "false", "0"
    Anything else             : NULL
    """
    truthy = {"y", "yes", "true", "1"}
    falsy  = {"n", "no",  "false", "0"}

    for c in bool_cols:
        if c not in df.columns:
            continue
        lowered = lower(trim(col(c).cast("string")))
        df = df.withColumn(
            c,
            when(lowered.isin(*truthy), lit(True))
            .when(lowered.isin(*falsy),  lit(False))
            .otherwise(None)
            .cast(BooleanType())
        )
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
 

# ---------------------------------------------------------------------------
# Main ingestion function
# ---------------------------------------------------------------------------
def ingest_dataset(dataset_name, config):
    start_time = time.time()

    # A. Load according to file type
    df = spark.read.format(config["format"]).options(**config["options"]).load(config["path"])

    # B. Standardise column names → snake_case
    df = standardize_columns(df)

    # C. Normalise common data types (generic, dataset-agnostic)
    df = normalize_string_columns(df)

    # D. Normalise timestamps
    #    D1. Columns already containing datetime strings in a known format
    df = normalize_timestamps(df, config.get("timestamp_cols", {}))
    #    D2. Timestamp assembled from separate year/month/day/hour columns
    df = assemble_timestamp_from_parts(df, config.get("assembled_timestamp"))
    #    D3. Timestamp assembled from separate date-string + time-string columns
    df = normalize_date_time_pairs(df, config.get("date_time_pairs", []))

    # E. Dataset-specific type normalisations declared in the config
    df = normalize_boolean_columns(df, config.get("bool_cols", []))

    # F. Quality checks
    standard_pks = [to_snake_case(c) for c in config["primary_keys"]]
    df, initial_cnt, rejected_cnt = perform_data_quality_checks(df, standard_pks)

    # G. Write to Delta
    output_path = f"/content/delta/{dataset_name}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"/content/delta/{dataset_name}")
    

    execution_time = time.time() - start_time

    # H. Metadata
    metadata = {
        "dataset":                dataset_name,
        "processed_records":      initial_cnt,
        "rejected_records":       rejected_cnt,
        "final_records":          initial_cnt - rejected_cnt,
        "execution_time_seconds": round(execution_time, 2),
        "schema_version":         schema_hash(df),
    }
    return metadata

# ---------------------------------------------------------------------------
# Run the framework
# ---------------------------------------------------------------------------
ingestion_logs = []
for name, conf in DATASET_CONFIGS.items():
    print(f"Ingesting {name}...")
    log = ingest_dataset(name, conf)
    ingestion_logs.append(log)

print(*ingestion_logs, sep="\n")
