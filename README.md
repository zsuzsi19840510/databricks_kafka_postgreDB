# Databricks Kafka PostgreSQL Demo

## 📌 Projekt célja

Ez a projekt egy end-to-end adatfeldolgozási pipeline bemutatása PostgreSQL, Apache Kafka és Databricks használatával.

A rendszer célja a PostgreSQL adatbázisban tárolt rendelési adatok továbbítása Kafka segítségével, majd azok feldolgozása és elemzése Databricks környezetben Spark SQL használatával.

A projekt jól szemlélteti egy modern Data Engineering architektúra alapvető elemeit:

* adatforrás kezelése PostgreSQL-ben
* eseményalapú adatfolyam Kafka segítségével
* adatok beolvasása Databricks környezetbe
* Bronze réteg kialakítása
* aggregációk és riportkészítés Spark SQL használatával

---

## 🏗️ Architektúra

```text
PostgreSQL
     │
     ▼
Apache Kafka
     │
     ▼
Databricks
     │
     ▼
Bronze Layer
     │
     ▼
Aggregációk / Riportok
```

---

## 🛠️ Használt technológiák

* PostgreSQL
* Apache Kafka
* Databricks
* Apache Spark SQL
* Delta Lake

---

## 📂 Adatforrás

A projekt rendelési adatokat dolgoz fel.

Példa mezők:

| Oszlop          | Leírás                  |
| --------------- | ----------------------- |
| order_id        | Rendelés azonosító      |
| status          | Rendelés státusza       |
| amount          | Rendelés összege        |
| kafka_timestamp | Kafka esemény időbélyeg |

---

## 📊 Aggregáció

```sql
SELECT
  status,
  COUNT(*) AS order_count,
  ROUND(SUM(amount), 2) AS total_amount
FROM kafka_demo.kafka_demo_schema.bronze_orders
GROUP BY status
ORDER BY status;
```

### Eredmény

A lekérdezés meghatározza:

* a rendelések számát státuszonként
* a rendelések összértékét

---

## 🚀 Lehetséges továbbfejlesztések

* Structured Streaming használata
* Silver és Gold rétegek kialakítása
* Delta Live Tables
* Databricks Workflows automatizálás
* Dashboard készítés Databricks SQL segítségével
* CDC integráció PostgreSQL és Kafka között
* Adatminőségi validációk

---

## 📚 Tanulási célok

A projekt gyakorlati példát mutat:

* Kafka alapú adatfeldolgozásra
* Spark SQL aggregációkra
* Databricks használatára
* modern Data Engineering architektúrákra
* PostgreSQL és Kafka integrációra

---

## 👩‍💻 Készítette

Zsuzsanna Ujvári

GitHub:
https://github.com/zsuzsi19840510
