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

    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )

    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# READ BRONZE DELTA
# ============================================================

bronze_df = (
    spark.read
    .format("delta")
    .load("../bronze_delta")
)


print("=" * 60)
print("BRONZE DATA")
print("=" * 60)

print("Rows:", bronze_df.count())
print("Columns:", len(bronze_df.columns))

bronze_df.printSchema()


# ============================================================
# SILVER TRANSFORMATIONS
# ============================================================

silver_df = (

    bronze_df

    # --------------------------------------------------------
    # Convert string timestamp → timestamp
    # --------------------------------------------------------

    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    )

    # --------------------------------------------------------
    # Remove duplicate orders
    # --------------------------------------------------------

    .dropDuplicates(["order_id"])

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
        trim(col("customer_city"))
    )

    # --------------------------------------------------------
    # Standardize state names
    # --------------------------------------------------------

    .withColumn(
        "customer_state",
        upper(col("customer_state"))
    )

    # --------------------------------------------------------
    # Create business columns
    # --------------------------------------------------------

    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    )

    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )

    # --------------------------------------------------------
    # ETL processing timestamp
    # --------------------------------------------------------

    .withColumn(
        "processing_timestamp",
        current_timestamp()
    )
)


# ============================================================
# WRITE SILVER DELTA
# ============================================================

print("=" * 60)
print("Writing Silver Delta Layer...")
print("=" * 60)

(
    silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy(
        "event_date",
        "event_hour"
    )
    .save("../silver_delta")
)


print("=" * 60)
print("SILVER DELTA WRITTEN SUCCESSFULLY")
print("=" * 60)

print("Silver Rows:", silver_df.count())

silver_df.printSchema()

silver_df.show(10, truncate=False)


spark.stop()