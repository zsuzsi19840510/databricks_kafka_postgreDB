# Kafka beállítása

## Login

https://login.confluent.io/

## 1. Environment létrehozása

A Confluent Cloudban:

**Environments → Create environment**

Név: `databricks-demo`

![Environment](images/image_1.png)

---

## 2. Kafka Cluster létrehozása

**Create cluster**

- Basic
- vagy Free Trial

Ajánlott régió: `AWS eu-central-1 (Frankfurt)`

![Cluster](images/image_2.png)

---

## 3. Topic létrehozása

**Topics → Create Topic**

Név: `orders`

Partíció: `1`

![Topic](images/image_3.png)
![Topic details](images/image_4.png)

---

## 4. API Key generálása

**API Keys → Create Key**

- Global access
- vagy cluster API key

Kapott adatok:

- API_KEY
- API_SECRET

> A secretet azonnal mentsd el!

![API key](images/image_5.png)
![API key list](images/image_6.png)
![API key create](images/image_7.png)
![API key created](images/image_8.png)

---

## 5. Bootstrap Server kimásolása

Példa:

`pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092`

---

## 6. .env kitöltése

```env
CONFLUENT_BOOTSTRAP_SERVERS=...
CONFLUENT_API_KEY=...
CONFLUENT_API_SECRET=...
KAFKA_TOPIC=orders
```

![ENV](images/image_9.png)

---

## 7. Docker PostgreDB indítása

```bash
docker compose up --build
```

Leállítás:

```bash
docker compose down
```

Törlés volume-okkal:

```bash
docker compose down -v
```

![Docker](images/image_10.png)

---

## 8. Ellenőrzés a Confluent UI-ban

**Topics → orders → Messages**

![Confluent](images/image_11.png)
![Messages](images/image_12.png)
