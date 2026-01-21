# Databricks notebook source
# 03_silver_to_gold
from pyspark.sql import functions as F

silver = spark.table("ecomm_lakehouse.silver_events_clean")

# Fact purchases
purchases = (silver
  .where(F.col("event_name")=="purchase")
  .select(
    "order_id","event_id","event_ts","event_date","user_id","session_id",
    "product_id","category","quantity","price","currency",
    "utm_source","utm_medium","utm_campaign",
    "country","region","city","platform","device_type"
  )
)

purchases.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("ecomm_lakehouse.gold_fact_purchases")

# Funnel by day
funnel = (silver
  .groupBy("event_date")
  .agg(
    F.countDistinct(F.when(F.col("event_name")=="page_view", F.col("session_id"))).alias("sessions_with_page_view"),
    F.countDistinct(F.when(F.col("event_name")=="product_view", F.col("session_id"))).alias("sessions_with_product_view"),
    F.countDistinct(F.when(F.col("event_name")=="add_to_cart", F.col("session_id"))).alias("sessions_with_add_to_cart"),
    F.countDistinct(F.when(F.col("event_name")=="checkout_started", F.col("session_id"))).alias("sessions_with_checkout"),
    F.countDistinct(F.when(F.col("event_name")=="purchase", F.col("session_id"))).alias("sessions_with_purchase"),
  )
)

funnel.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("ecomm_lakehouse.gold_mart_funnel_daily")
