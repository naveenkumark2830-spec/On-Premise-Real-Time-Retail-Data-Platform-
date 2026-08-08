import json
import os

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


BRONZE_PATH = "../bronze"
SILVER_PATH = "../silver"
METADATA_PATH = "../metadata/silver_processed.json"


spark = (
    SparkSession.builder
    .appName("IncrementalBronzeToSilver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def load_processed_partitions():

    if not os.path.exists(METADATA_PATH):
        return set()

    with open(METADATA_PATH, "r") as file:
        data = json.load(file)

    return set(data.get("processed_partitions", []))


def save_processed_partitions(processed_partitions):

    os.makedirs(
        os.path.dirname(METADATA_PATH),
        exist_ok=True
    )

    with open(METADATA_PATH, "w") as file:

        json.dump(
            {
                "processed_partitions":
                    sorted(processed_partitions)
            },
            file,
            indent=4
        )


processed_partitions = load_processed_partitions()


bronze_df = spark.read.parquet(BRONZE_PATH)


available_partitions = (

    bronze_df

    .select(
        "event_date",
        "event_hour"
    )

    .distinct()

    .collect()
)


new_partitions = []


for row in available_partitions:

    partition_key = (
        f"{row['event_date']}/"
        f"event_hour={row['event_hour']}"
    )

    if partition_key not in processed_partitions:

        new_partitions.append(
            (
                row["event_date"],
                row["event_hour"],
                partition_key
            )
        )


print("=" * 60)
print("INCREMENTAL SILVER PROCESSING")
print("=" * 60)

print("Already Processed:", len(processed_partitions))
print("Available:", len(available_partitions))
print("New:", len(new_partitions))


if not new_partitions:

    print("\nNo new partitions found.")
    spark.stop()
    exit(0)


# Build filter for only new partitions

partition_filter = None

for event_date, event_hour, _ in new_partitions:

    condition = (

        (col("event_date") == event_date)
        &
        (col("event_hour") == event_hour)

    )

    if partition_filter is None:

        partition_filter = condition

    else:

        partition_filter = (
            partition_filter | condition
        )


new_bronze_df = bronze_df.filter(
    partition_filter
)


silver_df = (

    new_bronze_df

    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_timestamp")
        )
    )

    .dropDuplicates(
        ["order_id"]
    )

    .filter(
        col("price") > 0
    )

    .filter(
        col("payment_value") > 0
    )

    .dropna(
        subset=[
            "order_id",
            "customer_id",
            "customer_state",
            "payment_type"
        ]
    )

    .withColumn(
        "customer_city",
        trim(col("customer_city"))
    )

    .withColumn(
        "customer_state",
        upper(col("customer_state"))
    )

    .withColumn(
        "event_date",
        to_date(
            col("event_timestamp")
        )
    )

    .withColumn(
        "event_hour",
        hour(
            col("event_timestamp")
        )
    )

    .withColumn(
        "processing_timestamp",
        current_timestamp()
    )

)


print("\nRecords Being Processed:")

print(
    silver_df.count()
)


(
    silver_df.write
    .mode("append")
    .partitionBy(
        "event_date",
        "event_hour"
    )
    .parquet(SILVER_PATH)
)


for _, _, partition_key in new_partitions:

    processed_partitions.add(
        partition_key
    )


save_processed_partitions(
    processed_partitions
)


print("\nSilver processing completed.")

print(
    "Processed partitions:",
    len(new_partitions)
)


spark.stop()