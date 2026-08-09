from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    current_timestamp,
    trim,
    upper
)

from delta import configure_spark_with_delta_pip


# ============================================================
# SPARK
# ============================================================

builder = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .master("local[*]")

    # Delta Lake
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )

    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)


spark = (
    configure_spark_with_delta_pip(builder)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# PATHS
# ============================================================

BRONZE_PATH = "../bronze_delta"

SILVER_PATH = "../silver_delta"

SILVER_CHECKPOINT = "../checkpoint/silver_orders"


# ============================================================
# READ BRONZE DELTA AS STREAM
# ============================================================

bronze_df = (
    spark.readStream
    .format("delta")

    # Start from Bronze Delta version 0
    .option(
        "startingVersion",
        0
    )

    .load(BRONZE_PATH)
)


# ============================================================
# SILVER TRANSFORMATIONS
# ============================================================

silver_df = (

    bronze_df

    # --------------------------------------------------------
    # Convert event timestamp
    # --------------------------------------------------------

    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_timestamp")
        )
    )

    # --------------------------------------------------------
    # Remove invalid prices
    # --------------------------------------------------------

    .filter(
        col("price") > 0
    )

    # --------------------------------------------------------
    # Remove invalid payment values
    # --------------------------------------------------------

    .filter(
        col("payment_value") > 0
    )

    # --------------------------------------------------------
    # Remove rows with missing important columns
    # --------------------------------------------------------

    .dropna(
        subset=[
            "order_id",
            "customer_id",
            "customer_state",
            "payment_type"
        ]
    )

    # --------------------------------------------------------
    # Remove unwanted spaces
    # --------------------------------------------------------

    .withColumn(
        "customer_city",
        trim(
            col("customer_city")
        )
    )

    # --------------------------------------------------------
    # Standardize state names
    # --------------------------------------------------------

    .withColumn(
        "customer_state",
        upper(
            col("customer_state")
        )
    )

    # --------------------------------------------------------
    # Create event date
    # --------------------------------------------------------

    .withColumn(
        "event_date",
        to_date(
            col("event_timestamp")
        )
    )

    # --------------------------------------------------------
    # Create event hour
    # --------------------------------------------------------

    .withColumn(
        "event_hour",
        hour(
            col("event_timestamp")
        )
    )

    # --------------------------------------------------------
    # Processing timestamp
    # --------------------------------------------------------

    .withColumn(
        "processing_timestamp",
        current_timestamp()
    )

    # --------------------------------------------------------
    # Streaming deduplication
    #
    # Keep state for 1 day to identify duplicate orders.
    # --------------------------------------------------------

    .withWatermark(
        "event_timestamp",
        "1 day"
    )

    .dropDuplicates(
        ["order_id"]
    )
)


# ============================================================
# WRITE SILVER DELTA
# ============================================================

print("=" * 60)
print("STARTING BRONZE → SILVER STREAM")
print("=" * 60)

print("Bronze Path   :", BRONZE_PATH)

print("Silver Path   :", SILVER_PATH)

print("Checkpoint     :", SILVER_CHECKPOINT)

print("=" * 60)


query = (
    silver_df

    .writeStream

    .format("delta")

    # New records are appended to Silver
    .outputMode("append")

    # Silver Delta location
    .option(
        "path",
        SILVER_PATH
    )

    # VERY IMPORTANT:
    # Spark remembers processed Bronze Delta commits here
    .option(
        "checkpointLocation",
        SILVER_CHECKPOINT
    )

    # Same partition strategy as Bronze
    .partitionBy(
        "event_date",
        "event_hour"
    )

    .start()
)


# ============================================================
# WAIT FOR STREAM
# ============================================================

query.awaitTermination()