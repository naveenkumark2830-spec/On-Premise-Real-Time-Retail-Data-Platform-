from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
)
from pyspark.sql.functions import (
    to_date,
    hour,
    col
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)


spark = (
    SparkSession.builder
    .appName("KafkaOrdersConsumer")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0-preview5"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("freight_value", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("payment_value", DoubleType(), True),
    StructField("order_status", StringType(), True),
    StructField("event_timestamp", StringType(), True)
])

df = (
    spark.readStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "localhost:9092")
         .option("subscribe", "orders")
         .option("startingOffsets", "latest")
         .load()
)

parsed_df = (
    df.select(
        from_json(
            col("value").cast("string"),
            order_schema
        ).alias("order")
    )
)

final_df = parsed_df.select("order.*")

bronze_df = (

    final_df

    .withColumn(
        "event_timestamp",
        to_timestamp(col("event_timestamp"))
    )

    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    )

    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )

)


query = (

    bronze_df

    .writeStream

    .format("parquet")

    .outputMode("append")

    .option(
        "path",
        "/home/naveen/PowerBI/bronze"
    )

    .option(
        "checkpointLocation",
        "/home/naveen/PowerBI/checkpoint/bronze_orders"
    )

    .partitionBy(
        "event_date",
        "event_hour"
    )

    .trigger(processingTime="5 seconds")

    .start()

)

query.awaitTermination()