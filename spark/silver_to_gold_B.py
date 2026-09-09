from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    countDistinct,
    count,
    sum,
    avg,
    round
)

from delta import configure_spark_with_delta_pip


# ============================================================
# SPARK
# ============================================================

builder = (
    SparkSession.builder
    .appName("SilverToGold")
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

spark = (
    configure_spark_with_delta_pip(builder)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# PATHS
# ============================================================

BRONZE_PATH = "/home/naveen/PowerBI/bronze_delta"

SILVER_PATH = "/home/naveen/PowerBI/silver_delta"

SILVER_CHECKPOINT = "/home/naveen/PowerBI/checkpoint/silver_orders"

GOLD_PATH = "/home/naveen/PowerBI/gold"


# ============================================================
# READ SILVER
# ============================================================

silver_df = (
    spark.read
    .format("delta")
    .load(SILVER_PATH)
)

print("=" * 70)
print("SILVER → GOLD")
print("=" * 70)

print("Silver Rows:", silver_df.count())

silver_df.printSchema()


# ============================================================
# 1. DAILY SALES MART
# ============================================================

gold_daily_sales = (
    silver_df
    .groupBy("event_date")
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("payment_value"),
            2
        ).alias("avg_order_value"),

        round(
            sum("freight_value"),
            2
        ).alias("total_freight")
    )
    .orderBy("event_date")
)


# ============================================================
# 2. SALES BY STATE MART
# ============================================================

gold_sales_by_state = (
    silver_df
    .groupBy("customer_state")
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("payment_value"),
            2
        ).alias("avg_order_value"),

        round(
            sum("freight_value"),
            2
        ).alias("total_freight")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)


# ============================================================
# 3. PAYMENT METHOD MART
# ============================================================

gold_payment_method = (
    silver_df
    .groupBy("payment_type")
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("payment_value"),
            2
        ).alias("avg_order_value")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)


# ============================================================
# 4. ORDER STATUS MART
# ============================================================

gold_order_status = (
    silver_df
    .groupBy("order_status")
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("payment_value"),
            2
        ).alias("avg_order_value")
    )
    .orderBy(
        col("total_orders").desc()
    )
)


# ============================================================
# 5. PRODUCT SALES MART
# ============================================================

gold_product_sales = (
    silver_df
    .groupBy("product_id")
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("price"),
            2
        ).alias("avg_product_price"),

        round(
            sum("freight_value"),
            2
        ).alias("total_freight")
    )
    .orderBy(
        col("total_revenue").desc()
    )
)


# ============================================================
# 6. HOURLY SALES MART
# ============================================================

gold_hourly_sales = (
    silver_df
    .groupBy(
        "event_date",
        "event_hour"
    )
    .agg(
        countDistinct("order_id").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("payment_value"),
            2
        ).alias("avg_order_value")
    )
    .orderBy(
        "event_date",
        "event_hour"
    )
)


# ============================================================
# WRITE GOLD MARTS
# ============================================================

print("=" * 70)
print("WRITING GOLD DATA MARTS")
print("=" * 70)


# ------------------------------------------------------------
# DAILY SALES
# ------------------------------------------------------------

(
    gold_daily_sales
    .write
    .format("delta")
    .mode("overwrite")
    .save(
        f"{GOLD_PATH}/daily_sales"
    )
)


# ------------------------------------------------------------
# SALES BY STATE
# ------------------------------------------------------------

(
    gold_sales_by_state
    .write
    .format("delta")
    .mode("overwrite")
    .save(
        f"{GOLD_PATH}/sales_by_state"
    )
)


# ------------------------------------------------------------
# PAYMENT METHOD
# ------------------------------------------------------------

(
    gold_payment_method
    .write
    .format("delta")
    .mode("overwrite")
    .save(
        f"{GOLD_PATH}/payment_method"
    )
)


# ------------------------------------------------------------
# ORDER STATUS
# ------------------------------------------------------------

(
    gold_order_status
    .write
    .format("delta")
    .mode("overwrite")
    .save(
        f"{GOLD_PATH}/order_status"
    )
)


# ------------------------------------------------------------
# PRODUCT SALES
# ------------------------------------------------------------

(
    gold_product_sales
    .write
    .format("delta")
    .mode("overwrite")
    .save(
        f"{GOLD_PATH}/product_sales"
    )
)


# ------------------------------------------------------------
# HOURLY SALES
# ------------------------------------------------------------

(
    gold_hourly_sales
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy(
        "event_date"
    )
    .save(
        f"{GOLD_PATH}/hourly_sales"
    )
)


# ============================================================
# DISPLAY RESULTS
# ============================================================

print("=" * 70)
print("GOLD DATA MARTS CREATED")
print("=" * 70)


print("\n1. DAILY SALES")
gold_daily_sales.show(
    20,
    truncate=False
)


print("\n2. SALES BY STATE")
gold_sales_by_state.show(
    20,
    truncate=False
)


print("\n3. PAYMENT METHOD")
gold_payment_method.show(
    20,
    truncate=False
)


print("\n4. ORDER STATUS")
gold_order_status.show(
    20,
    truncate=False
)


print("\n5. PRODUCT SALES - TOP 20")
gold_product_sales.show(
    20,
    truncate=False
)


print("\n6. HOURLY SALES")
gold_hourly_sales.show(
    50,
    truncate=False
)


# ============================================================
# STOP SPARK
# ============================================================

spark.stop()