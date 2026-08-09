from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    to_date,
    hour
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

from delta import configure_spark_with_delta_pip

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("KafkaOrdersBronzeDelta")
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


spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0"
    ]
).getOrCreate()


spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# ORDER SCHEMA
# ============================================================

order_schema = StructType([

    StructField(
        "order_id",
        StringType(),
        True
    ),

    StructField(
        "customer_id",
        StringType(),
        True
    ),

    StructField(
        "customer_city",
        StringType(),
        True
    ),

    StructField(
        "customer_state",
        StringType(),
        True
    ),

    StructField(
        "product_id",
        StringType(),
        True
    ),

    StructField(
        "seller_id",
        StringType(),
        True
    ),

    StructField(
        "price",
        DoubleType(),
        True
    ),

    StructField(
        "freight_value",
        DoubleType(),
        True
    ),

    StructField(
        "payment_type",
        StringType(),
        True
    ),

    StructField(
        "payment_value",
        DoubleType(),
        True
    ),

    StructField(
        "order_status",
        StringType(),
        True
    ),

    StructField(
        "event_timestamp",
        StringType(),
        True
    )
])


# ============================================================
# READ FROM KAFKA
# ============================================================

df = (

    spark.readStream

    .format("kafka")

    .option(
        "kafka.bootstrap.servers",
        "localhost:9092"
    )

    .option(
        "subscribe",
        "orders"
    )

    .option(
        "startingOffsets",
        "earliest"
    )

    .load()
)


# ============================================================
# JSON PARSING / SCHEMA VALIDATION
# ============================================================

parsed_df = (

    df.select(

        from_json(
            col("value").cast("string"),
            order_schema
        ).alias("order")

    )

)


final_df = parsed_df.select(
    "order.*"
)


# ============================================================
# BRONZE PREPARATION
# ============================================================

bronze_df = (

    final_df

    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_timestamp")
        )
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

)


# ============================================================
# WRITE BRONZE DELTA
# ============================================================

query = (

    bronze_df

    .writeStream

    .format("delta")

    .outputMode("append")

    .option(
        "path",
        "/home/naveen/PowerBI/bronze_delta"
    )

    .option(
        "checkpointLocation",
        "/home/naveen/PowerBI/checkpoint/bronze_orders_delta"
    )

    .partitionBy(
        "event_date",
        "event_hour"
    )

    .start()

)


query.awaitTermination()