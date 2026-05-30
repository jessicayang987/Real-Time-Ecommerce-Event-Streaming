# Databricks notebook source
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ---- Schema for the raw JSON in bronze.body ----
order_schema = StructType([
    StructField("order_id",         StringType()),
    StructField("user_id",          StringType()),
    StructField("product_id",       StringType()),
    StructField("product_category", StringType()),
    StructField("price",            DoubleType()),
    StructField("quantity",         IntegerType()),
    StructField("country",          StringType()),
    StructField("timestamp",        StringType()),
])

# COMMAND ----------

# ---- Read stream from Bronze Delta Table ----
silver_stream = (
    spark.readStream
    .table("eh_streaming.oms.bronze_orders")
    .select(
        from_json(col("body"), order_schema).alias("d"),
        col("kafka_ts"),
    )
    .select("d.*", "kafka_ts")
    .withColumn("event_ts", to_timestamp("timestamp"))
    .withColumn("revenue", col("price") * col("quantity"))
    .filter(
        col("order_id").isNotNull() &
        (col("price") > 0) &
        (col("quantity") > 0)
    )
    .drop("timestamp")
)

silver_stream.printSchema()

# COMMAND ----------

# ---- Write to Silver Delta table ----
(silver_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/eh_streaming/oms/checkpoints/silver")
    .trigger(processingTime="15 seconds")
    .toTable("eh_streaming.oms.silver_orders"))

# COMMAND ----------

# ---- Validate ----
print("Silver count:", spark.read.table("eh_streaming.oms.silver_orders").count())
display(spark.read.table("eh_streaming.oms.silver_orders").limit(5))
