# Databricks notebook source
# Databricks notebook: 01_generate_bronze

from pyspark.sql import functions as F
import uuid

# Widgets (safe for manual + jobs)
try:
    dbutils.widgets.get("rows")
except Exception:
    dbutils.widgets.text("rows", "1000000")

rows = int(dbutils.widgets.get("rows"))
batch_id = str(uuid.uuid4())
source = "synthetic_ecomm"

event_names = ["page_view","product_view","add_to_cart","checkout_started","purchase"]
platforms = ["web","ios","android"]
devices = ["mobile","desktop","tablet"]
currencies = ["CAD","USD"]

idx_event = ((F.col("id") % F.lit(len(event_names))) + F.lit(1)).cast("int")
idx_platform = ((F.col("id") % F.lit(len(platforms))) + F.lit(1)).cast("int")
idx_device = ((F.col("id") % F.lit(len(devices))) + F.lit(1)).cast("int")
idx_currency = ((F.col("id") % F.lit(len(currencies))) + F.lit(1)).cast("int")

df = (spark.range(rows)
  .withColumn("event_id", F.expr("uuid()"))
  .withColumn("event_ts", (F.current_timestamp().cast("long") - (F.col("id") % F.lit(86400))).cast("timestamp"))
  .withColumn("event_date", F.to_date("event_ts"))
  .withColumn("event_name", F.element_at(F.array(*[F.lit(x) for x in event_names]), idx_event))
  .withColumn("user_id", F.concat(F.lit("u_"), (F.col("id") % 500000).cast("string")))
  .withColumn("session_id", F.concat(F.lit("s_"), (F.col("id") % 200000).cast("string")))
  .withColumn("platform", F.element_at(F.array(*[F.lit(x) for x in platforms]), idx_platform))
  .withColumn("device_type", F.element_at(F.array(*[F.lit(x) for x in devices]), idx_device))
  .withColumn("country", F.lit("CA"))
  .withColumn("region", F.lit("ON"))
  .withColumn("city", F.lit("Toronto"))
  .withColumn("utm_source", F.lit("google"))
  .withColumn("utm_medium", F.lit("cpc"))
  .withColumn("utm_campaign", F.concat(F.lit("winter_"), (F.col("id") % 5).cast("string")))
  .withColumn("product_id", F.when(F.col("event_name").isin("product_view","add_to_cart","purchase"),
                                  F.concat(F.lit("p_"), (F.col("id") % 10000).cast("string"))))
  .withColumn("category", F.when(F.col("product_id").isNotNull(),
                                 F.concat(F.lit("cat_"), (F.col("id") % 50).cast("string"))))
  .withColumn("quantity", F.when(F.col("event_name").isin("add_to_cart","purchase"),
                                 (F.col("id") % 3 + 1).cast("int")))
  .withColumn("price", F.when(F.col("event_name").isin("add_to_cart","purchase"),
                              (F.col("id") % 20000) / 100.0))
  .withColumn("currency", F.when(F.col("price").isNotNull(),
                                 F.element_at(F.array(*[F.lit(x) for x in currencies]), idx_currency)))
  .withColumn("order_id", F.when(F.col("event_name")=="purchase",
                                 F.concat(F.lit("o_"), (F.col("id") % 300000).cast("string"))))
)

raw = (df.select(
    F.to_json(F.struct(*[F.col(c) for c in df.columns if c != "id"])).alias("raw_json"),
    F.current_timestamp().alias("ingest_ts"),
    F.lit(source).alias("source"),
    F.lit(batch_id).alias("ingest_batch_id")
))

raw.write.format("delta").mode("append").saveAsTable("ecomm_lakehouse.bronze_events_raw")
print("batch_id:", batch_id)

# COMMAND ----------

print("rows =", dbutils.widgets.get("rows"))
