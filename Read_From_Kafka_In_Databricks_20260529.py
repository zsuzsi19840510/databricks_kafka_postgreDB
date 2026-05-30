# Databricks notebook source
# MAGIC %md
# MAGIC # Lépések

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Kafka kapcsolat

# COMMAND ----------

bootstrap_servers = "<HOST>:<IP>"
topic = "orders"

api_key = "<API_KEY>"
api_secret = "<API_SECRET>"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Stream olvasás Kafka-ból

# COMMAND ----------

raw_df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";'
        )
        .option("startingOffsets", "earliest")
        .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. JSON parse

# COMMAND ----------

from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import *

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("status", StringType()),
    StructField("created_at", StringType()),
    StructField("event_time", StringType()),
])

orders_df = (
    raw_df
    .selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS json_value",
        "timestamp AS kafka_timestamp"
    )
    .select(
        "kafka_key",
        "kafka_timestamp",
        from_json(col("json_value"), schema).alias("data")
    )
    .select("kafka_key", "kafka_timestamp", "data.*")
    .withColumn("created_at_ts", to_timestamp("created_at"))
    .withColumn("event_time_ts", to_timestamp("event_time"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Catalog és Schema létrehozása

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG kafka_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS kafka_demo.kafka_demo_schema;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS kafka_demo.kafka_demo_schema.streaming_volume;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG kafka_demo;
# MAGIC USE SCHEMA kafka_demo_schema;
# MAGIC SHOW VOLUMES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Delta táblába írás
# MAGIC Ez nem örökké futó stream, hanem: beolvassa az aktuálisan elérhető Kafka üzeneteket, Delta táblába írja, majd megáll.
# MAGIC
# MAGIC Ha új Kafka üzenetek jönnek, ezt a cellát újra kell futtatni:

# COMMAND ----------

query = (
    orders_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/Volumes/kafka_demo/kafka_demo_schema/streaming_volume/orders_bronze_checkpoint"
        )
        .trigger(availableNow=True)
        .toTable("kafka_demo.kafka_demo_schema.bronze_orders")
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ellenőrzés SQL-ben
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM kafka_demo.kafka_demo_schema.bronze_orders
# MAGIC ORDER BY event_time_ts DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Aggregáció:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   status,
# MAGIC   COUNT(*) AS order_count,
# MAGIC   ROUND(SUM(amount), 2) AS total_amount
# MAGIC FROM kafka_demo.kafka_demo_schema.bronze_orders
# MAGIC GROUP BY status
# MAGIC ORDER BY status;