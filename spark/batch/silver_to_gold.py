from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    countDistinct,
    round
)
from pyspark.sql.functions import *

spark = (
    SparkSession.builder
    .appName("SilverToGold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

silver_df = spark.read.parquet("/home/naveen/PowerBI/silver")


#KPI

kpi_summary = (

    silver_df

    .agg(

        count("*").alias("total_orders"),

        countDistinct("customer_id").alias("total_customers"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        round(
            avg("payment_value"),
            2
        ).alias("average_order_value"),

        round(
            avg("freight_value"),
            2
        ).alias("average_freight")

    )

)

(

    kpi_summary.write

    .mode("overwrite")

    .parquet("/home/naveen/PowerBI/gold/kpi_summary")

)


# Sales_BY_State

sales_by_state = (

    silver_df

    .groupBy("customer_state")

    .agg(

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        count("*").alias("total_orders")

    )

    .orderBy(
        col("total_revenue").desc()
    )

)

(
    sales_by_state.write
    .mode("overwrite")
    .parquet("/home/naveen/PowerBI/gold/sales_by_state")
)

# Payment 
sales_by_payment = (

    silver_df

    .groupBy("payment_type")

    .agg(

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue"),

        count("*").alias("total_orders")

    )

    .orderBy(
        col("total_revenue").desc()
    )

)

(
    sales_by_payment.write
    .mode("overwrite")
    .parquet("/home/naveen/PowerBI/gold/sales_by_payment")
)

# Orders by Hour

orders_by_hour = (

    silver_df

    .groupBy("event_hour")

    .agg(

        count("*").alias("total_orders"),

        round(
            sum("payment_value"),
            2
        ).alias("total_revenue")

    )

    .orderBy("event_hour")

)

(
    orders_by_hour.write
    .mode("overwrite")
    .parquet("/home/naveen/PowerBI/gold/orders_by_hour")
)

#Top Products

top_products = (

    silver_df

    .groupBy("product_id")

    .agg(

        count("*").alias("orders"),

        round(
            sum("payment_value"),
            2
        ).alias("revenue")

    )

    .orderBy(
        col("revenue").desc()
    )

)


(
    top_products.write
    .mode("overwrite")
    .parquet("/home/naveen/PowerBI/gold/top_products")
)

# Order Status Summary

order_status_summary = (

    silver_df

    .groupBy("order_status")

    .agg(

        count("*").alias("total_orders")

    )

)

(
    order_status_summary.write
    .mode("overwrite")
    .parquet("/home/naveen/PowerBI/gold/order_status_summary")
)

# Verifying

print("="*60)
print("EXECUTIVE KPI")
print("="*60)

kpi_summary.show(truncate=False)

print("="*60)
print("SALES BY STATE")
print("="*60)

sales_by_state.show(truncate=False)



print("="*60)
print("SALES BY PAYMENT")
print("="*60)

sales_by_payment.show(truncate=False)

print("="*60)
print("ORDERS BY HOUR")
print("="*60)
orders_by_hour.show(truncate=False)


print("="*60)
print("TOP PRODUCTS")
print("="*60)
top_products.show(10, truncate=False)

print("="*60)
print("TOP PRODUCTS")
print("="*60)
top_products.show(10, truncate=False)

print("="*60)
print("ORDER STATUS SUMMARY")
print("="*60)
order_status_summary.show(truncate=False)