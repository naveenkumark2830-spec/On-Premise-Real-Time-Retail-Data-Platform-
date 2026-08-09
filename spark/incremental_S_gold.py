from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    sum,
    avg,
    round,
    max
)
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
import os


# ============================================================
# SPARK
# ============================================================

builder = (
    SparkSession.builder
    .appName("IncrementalGold")
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
# PATHS
# ============================================================

SILVER_PATH = "../silver_delta"
GOLD_PATH = "../gold/daily_sales"
CHECKPOINT_FILE = "../checkpoint/gold_silver_version.txt"


# ============================================================
# FIND SILVER VERSION
# ============================================================

silver_table = DeltaTable.forPath(
        spark,
        SILVER_PATH
        )

silver_history = silver_table.history() 

latest_version = (
    silver_history
    .agg(max("version"))
    .collect()[0][0]
)

print("Latest Silver Version:", latest_version)


# ============================================================
# FIND LAST PROCESSED VERSION
# ============================================================

if os.path.exists(CHECKPOINT_FILE):

    with open(CHECKPOINT_FILE, "r") as f:
        last_version = int(f.read().strip())

else:

    last_version = -1


print("Last Processed Version:", last_version)


# ============================================================
# NOTHING NEW?
# ============================================================

if latest_version <= last_version:

    print("No new Silver data.")

    spark.stop()

    exit()


# ============================================================
# READ NEW SILVER VERSION(S)
# ============================================================

new_silver = (
    spark.read
    .format("delta")
    .option(
        "versionAsOf",
        latest_version
    )
    .load(SILVER_PATH)
)


# For our first implementation, filter using processing timestamp
# based on the previous processed version.

if last_version >= 0:

    previous_silver = (
        spark.read
        .format("delta")
        .option(
            "versionAsOf",
            last_version
        )
        .load(SILVER_PATH)
    )

    previous_ids = (
        previous_silver
        .select("order_id")
        .distinct()
    )

    new_silver = (
        new_silver
        .join(
            previous_ids,
            on="order_id",
            how="left_anti"
        )
    )


print("New Silver Rows:", new_silver.count())


# ============================================================
# NEW DAILY AGGREGATES
# ============================================================

new_daily = (
    new_silver
    .groupBy("event_date")
    .agg(
        countDistinct("order_id").alias("new_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("new_revenue"),

        round(
            sum("freight_value"),
            2
        ).alias("new_freight")
    )
)


new_daily.show()


# ============================================================
# CREATE GOLD IF IT DOESN'T EXIST
# ============================================================

if not DeltaTable.isDeltaTable(
    spark,
    GOLD_PATH
):

    (
        new_daily
        .withColumnRenamed(
            "new_orders",
            "total_orders"
        )
        .withColumnRenamed(
            "new_revenue",
            "total_revenue"
        )
        .withColumnRenamed(
            "new_freight",
            "total_freight"
        )
        .write
        .format("delta")
        .mode("overwrite")
        .save(GOLD_PATH)
    )

else:

    gold_table = DeltaTable.forPath(
        spark,
        GOLD_PATH
    )

    # --------------------------------------------------------
    # MERGE
    # --------------------------------------------------------

    (
        gold_table.alias("gold")
        .merge(
            new_daily.alias("new"),
            "gold.event_date = new.event_date"
        )

        .whenMatchedUpdate(
            set={
                "total_orders":
                    col("gold.total_orders")
                    + col("new.new_orders"),

                "total_revenue":
                    col("gold.total_revenue")
                    + col("new.new_revenue"),

                "total_freight":
                    col("gold.total_freight")
                    + col("new.new_freight")
            }
        )

        .whenNotMatchedInsert(
            values={
                "event_date":
                    col("new.event_date"),

                "total_orders":
                    col("new.new_orders"),

                "total_revenue":
                    col("new.new_revenue"),

                "total_freight":
                    col("new.new_freight")
            }
        )

        .execute()
    )


# ============================================================
# SAVE CHECKPOINT
# ============================================================

with open(CHECKPOINT_FILE, "w") as f:
    f.write(str(latest_version))


print("=" * 60)
print("INCREMENTAL GOLD COMPLETED")
print("=" * 60)


spark.stop()
