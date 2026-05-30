```python
# Databricks notebook source
```

# Lépések


## 1. Kafka kapcsolat

```python


bootstrap_servers = "<HOST>:<IP>"
topic = "orders"

api_key = "<API_KEY>"
api_secret = "<API_SECRET>"
```

## 2. Stream olvasás Kafka-ból

```python


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
```

## 3. JSON parse

```python


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
```

## 4. Catalog és Schema létrehozása

```python
%sql
CREATE CATALOG kafka_demo;
```



```python
%sql
CREATE SCHEMA IF NOT EXISTS kafka_demo.kafka_demo_schema;

```



```python
%sql
CREATE VOLUME IF NOT EXISTS kafka_demo.kafka_demo_schema.streaming_volume;

```



```python
%sql

USE CATALOG kafka_demo;
USE SCHEMA kafka_demo_schema;
SHOW VOLUMES;

```





## 5. Delta táblába írás

Ez nem örökké futó stream, hanem: beolvassa az aktuálisan elérhető Kafka üzeneteket, Delta táblába írja, majd megáll.

Megj.: Free Edition nem támogatja a folyamatos streaming triggert, ezért használom a Delta írásnál a availableNow=True triggert.

Ha új Kafka üzenetek jönnek, ezt a cellát újra kell futtatni:

```python


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
```

## 6. Ellenőrzés SQL-ben

```python
%sql

SELECT *
FROM kafka_demo.kafka_demo_schema.bronze_orders
ORDER BY event_time_ts DESC;
```



### Aggregáció:

```python
%sql

SELECT
  status,
  COUNT(*) AS order_count,
  ROUND(SUM(amount), 2) AS total_amount
FROM kafka_demo.kafka_demo_schema.bronze_orders
GROUP BY status
ORDER BY status;

```

