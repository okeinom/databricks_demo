# Databricks notebook source
# 02_bronze_to_silver
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

schema = StructType([
  StructField("event_id", StringType()),
  StructField("event_ts", TimestampType()),
  StructField("event_date", DateType()),
  StructField("event_name", StringType()),
  StructField("user_id", StringType()),
  StructField("session_id", StringType()),
  StructField("device_type", StringType()),
  StructField("platform", StringType()),
  StructField("country", StringType()),
  StructField("region", StringType()),
  StructField("city", StringType()),
  StructField("utm_source", StringType()),
  StructField("utm_medium", StringType()),
  StructField("utm_campaign", StringType()),
  StructField("product_id", StringType()),
  StructField("category", StringType()),
  StructField("price", DoubleType()),
  StructField("quantity", IntegerType()),
  StructField("currency", StringType()),
  StructField("order_id", StringType()),
])

bronze = spark.table("ecomm_lakehouse.bronze_events_raw")

parsed = (bronze
  .withColumn("j", F.from_json("raw_json", schema))
  .select(
    "raw_json","ingest_ts","ingest_batch_id",
    F.col("j.*")
  )
)

valid_event = (
  F.col("event_id").isNotNull() &
  F.col("event_ts").isNotNull() &
  F.col("event_name").isin("page_view","product_view","add_to_cart","checkout_started","purchase") &
  F.col("user_id").isNotNull() &
  (F.col("price").isNull() | (F.col("price") >= 0)) &
  (F.col("quantity").isNull() | (F.col("quantity") >= 1))
)

good = parsed.where(valid_event)
bad  = parsed.where(~valid_event).select(
  "raw_json",
  F.lit("failed_basic_validation").alias("reason"),
  "ingest_ts",
  "ingest_batch_id"
)

# quarantine
bad.write.format("delta").mode("append").saveAsTable("ecomm_lakehouse.silver_events_quarantine")

# Dedup
w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").desc())
deduped = (good
  .withColumn("rn", F.row_number().over(w))
  .where(F.col("rn")==1)
  .drop("rn")
)

# ✅ Make schema match the existing Delta table exactly
silver_cols = [
  "event_id","event_ts","event_date","event_name","user_id","session_id",
  "device_type","platform","country","region","city",
  "utm_source","utm_medium","utm_campaign",
  "product_id","category","price","quantity","currency","order_id",
  "ingest_ts","ingest_batch_id"
]

silver_df = (deduped
  .withColumn("price", F.col("price").cast("decimal(18,2)"))
  .withColumn("quantity", F.col("quantity").cast("int"))
  .select(*silver_cols)
)

silver_df.write.format("delta").mode("append").saveAsTable("ecomm_lakehouse.silver_events_clean")
