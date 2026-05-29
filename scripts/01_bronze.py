# Databricks notebook source
# ---- Config ----
NAMESPACE = "event-hubs-stream-lab"
TOPIC = "orders"


dbutils.widgets.text(
    "event_hubs_conn_str",
    "",
    "Event Hubs connection string"
)

CONN_STR = dbutils.widgets.get("event_hubs_conn_str")

EH_SASL = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
    f'required username="$ConnectionString" password="{CONN_STR}";'
)


# COMMAND ----------

# ---- Read stream from Event Hubs Kafka endpoint ----
bronze_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", f"{NAMESPACE}.servicebus.windows.net:9093")
    .option("subscribe", TOPIC)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "30000")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(value AS STRING) AS body", "topic", "partition", "offset", "timestamp AS kafka_ts")
)

# COMMAND ----------

# ---- Write to Bronze Delta table ----
(
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/eh_streaming/oms/checkpoints/bronze")
    .trigger(processingTime = "10 seconds")
    .toTable("eh_streaming.oms.bronze_orders")
 
 )

# COMMAND ----------

# ---- Validate ----
spark.read.table("eh_streaming.oms.bronze_orders").count()

# COMMAND ----------

spark.read.table("eh_streaming.oms.bronze_orders").limit(10).display()