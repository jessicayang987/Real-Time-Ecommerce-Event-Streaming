# Databricks notebook source
from pyspark.sql.functions import col, window, sum as _sum, count, approx_count_distinct

# ---- Windowed aggregation with watermark ----
gold_stream = (spark.readStream
    .table("eh_streaming.oms.silver_orders")
    .withWatermark("event_ts", "10 minutes")              # tolerate up to 10 min of late events
    .groupBy(
        window(col("event_ts"), "1 minute"),              # tumbling 1-minute windows on event time
        col("product_category"),
        col("country"),
    )
    .agg(
        _sum("revenue").alias("revenue"),
        count("order_id").alias("order_count"),
        approx_count_distinct("user_id").alias("unique_users"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "product_category",
        "country",
        "revenue",
        "order_count",
        "unique_users",
    )
)

gold_stream.printSchema()

# COMMAND ----------

# ---- Write to Gold Delta table ----
(gold_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/eh_streaming/oms/checkpoints/gold")
    .trigger(processingTime="30 seconds")
    .toTable("eh_streaming.oms.gold_revenue_by_cat_country_1m"))

# COMMAND ----------

# ---- Validate ----
print("Gold count:", spark.read.table("eh_streaming.oms.gold_revenue_by_cat_country_1m").count())
display(
    spark.read.table("eh_streaming.oms.gold_revenue_by_cat_country_1m")
        .orderBy("window_start", "product_category", "country")
        .limit(20)
)

# COMMAND ----------

silver_totals = spark.sql("""
    SELECT SUM(revenue) AS total_revenue, COUNT(*) AS total_orders
    FROM eh_streaming.oms.silver_orders
""")

gold_totals = spark.sql("""
    SELECT SUM(revenue) AS total_revenue, SUM(order_count) AS total_orders
    FROM eh_streaming.oms.gold_revenue_by_cat_country_1m
""")

display(silver_totals)
display(gold_totals)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT window_start, product_category, SUM(revenue) AS revenue
# MAGIC FROM eh_streaming.oms.gold_revenue_by_cat_country_1m
# MAGIC GROUP BY window_start, product_category
# MAGIC ORDER BY window_start

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping {q.name} ({q.id})")
    q.stop()
print("All streams stopped.")