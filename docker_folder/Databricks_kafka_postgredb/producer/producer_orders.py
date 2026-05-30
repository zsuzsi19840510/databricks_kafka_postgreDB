import json
import os
import random
import time
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Producer


pg = psycopg2.connect(
    host=os.environ["POSTGRES_HOST"],
    port=os.environ["POSTGRES_PORT"],
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
)
pg.autocommit = True

producer = Producer({
    "bootstrap.servers": os.environ["CONFLUENT_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["CONFLUENT_API_KEY"],
    "sasl.password": os.environ["CONFLUENT_API_SECRET"],
})

topic = os.environ["KAFKA_TOPIC"]


def init_db():
    with pg.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                customer_id INT,
                product_id INT,
                amount NUMERIC(10,2),
                currency VARCHAR(3),
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)


def delivery_report(err, msg):
    if err:
        print(f"Kafka error: {err}", flush=True)
    else:
        print(f"Sent to Kafka: {msg.value().decode('utf-8')}", flush=True)


init_db()

while True:
    customer_id = random.randint(1, 20)
    product_id = random.randint(1000, 1010)
    amount = round(random.uniform(5, 250), 2)
    status = random.choice(["created", "paid", "cancelled"])

    with pg.cursor() as cur:
        cur.execute("""
            INSERT INTO orders (customer_id, product_id, amount, currency, status)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, created_at;
        """, (customer_id, product_id, amount, "EUR", status))

        order_id, created_at = cur.fetchone()

    event = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "amount": float(amount),
        "currency": "EUR",
        "status": status,
        "created_at": created_at.isoformat(),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    producer.produce(
        topic,
        key=str(order_id),
        value=json.dumps(event),
        callback=delivery_report
    )

    producer.poll(0)
    producer.flush()

    time.sleep(5)