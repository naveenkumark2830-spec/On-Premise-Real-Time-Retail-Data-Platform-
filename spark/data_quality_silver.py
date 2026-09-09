from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col


builder = (
    SparkSession.builder
    .appName("GoldDataQuality")
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


silver = (
    spark.read
    .format("delta")
    .load("../silver_delta")
)


# ============================================================
# CHECK 1
# ============================================================

null_orders = silver.filter(
    col("order_id").isNull()
).count()


# ============================================================
# CHECK 2
# ============================================================

invalid_prices = silver.filter(
    col("price") <= 0
).count()


# ============================================================
# CHECK 3
# ============================================================

duplicate_orders = (
    silver
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)


print("=" * 60)
print("DATA QUALITY")
print("=" * 60)

print("Null order IDs :", null_orders)

print("Invalid prices :", invalid_prices)

print("Duplicate IDs  :", duplicate_orders)


if (
    null_orders > 0
    or invalid_prices > 0
    or duplicate_orders > 0
):

    raise Exception(
        "DATA QUALITY CHECK FAILED"
    )


print("DATA QUALITY PASSED")


spark.stop()