from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("DeltaTest")
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
    builder
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


data = [
    ("ORD001", "SP", 100.0),
    ("ORD002", "RJ", 200.0),
    ("ORD003", "MG", 150.0)
]


df = spark.createDataFrame(
    data,
    ["order_id", "state", "amount"]
)


(
    df.write
    .format("delta")
    .mode("overwrite")
    .save("../delta_test")
)


print("=" * 60)
print("DELTA WRITE SUCCESSFUL")
print("=" * 60)


delta_df = (
    spark.read
    .format("delta")
    .load("../delta_test")
)


delta_df.show()


print("=" * 60)
print("DELTA SCHEMA")
print("=" * 60)

delta_df.printSchema()


spark.stop()