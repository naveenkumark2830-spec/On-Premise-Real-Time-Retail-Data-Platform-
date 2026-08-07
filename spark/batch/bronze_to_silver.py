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
#from pyspark.sql.function import *

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

bronze_df = spark.read.parquet("../bronze")

# T1



silver_df = (

    bronze_df

    # Convert string to timestamp
    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    )

    # Remove duplicate orders
    .dropDuplicates(["order_id"])

    # Remove invalid prices
    .filter(col("price") > 0)

    # Remove invalid payment values
    .filter(col("payment_value") > 0)

    # Remove rows with missing important columns
    .dropna(
        subset=[
            "order_id",
            "customer_id",
            "customer_state",
            "payment_type"
        ]
    )

    # Remove unwanted spaces
    .withColumn(
        "customer_city",
        trim(col("customer_city"))
    )

    # Standardize state names
    .withColumn(
        "customer_state",
        upper(col("customer_state"))
    )

    # Create business columns
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    )

    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )

    # ETL processing timestamp
    .withColumn(
        "processing_timestamp",
        current_timestamp()
    )

)

print("=" * 60)
print("Writing Silver Layer...")
print("=" * 60)

(
    silver_df.write
    .mode("overwrite")
    .parquet("../silver")
)

print("Silver Layer Written Successfully!")