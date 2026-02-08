# Module 3 – BigQuery & External Tables

**Homework Submission**

> Course: Data Engineering Zoomcamp
> Module: 3
> Date: `08/02/2026`

---

## 1. Project Overview

This homework explores working with external and materialized tables in Google BigQuery using NYC Yellow Taxi trip data from January to June 2024.

The main goal is to compare query performance and estimated bytes processed when using different table designs, including external tables, non-partitioned tables, and optimized tables.

---

## 2. Dataset & Environment Setup

* Dataset: NYC Yellow Taxi Trip Records (Jan–Jun 2024)
* Format: Parquet
* Storage: Google Cloud Storage (GCS)
* Data Warehouse: Google BigQuery

### 2.1 Data Upload

The Parquet files were uploaded manually to Google Cloud Storage.

**Bucket name:**

```
data_warehouse_2026_02_earth
```

Files uploaded:

```
yellow_tripdata_2024-01.parquet
yellow_tripdata_2024-02.parquet
yellow_tripdata_2024-03.parquet
yellow_tripdata_2024-04.parquet
yellow_tripdata_2024-05.parquet
yellow_tripdata_2024-06.parquet
```

<img width="1318" height="322" alt="image" src="https://github.com/user-attachments/assets/ecbadc44-99f6-4dce-8d86-0f7c91338cfd" />

---

## 3. BigQuery Table Creation

A dataset named `ny_taxi_2024` was created in the project:

```
<CENSORED_PROJECT_ID>
```

Two tables were created: an external table and a materialized table.

---

### 3.1 External Table

The external table references Parquet files stored in GCS.

```sql
CREATE OR REPLACE EXTERNAL TABLE
  `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://data_warehouse_2026_02_earth/yellow_tripdata_2024-*.parquet'
  ]
);
```

Preview query:

```sql
SELECT *
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`
LIMIT 10;
```

<img width="1086" height="193" alt="image" src="https://github.com/user-attachments/assets/4601f7a9-72fc-477e-ad26-c23b6c90daf0" />

---

### 3.2 Materialized Table (Non-Partitioned)

A regular BigQuery table was created from the external table.

```sql
CREATE OR REPLACE TABLE
  `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned` AS
SELECT *
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`;
```

Row count check:

```sql
SELECT COUNT(*)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
```

<img width="1105" height="536" alt="image" src="https://github.com/user-attachments/assets/df86ff08-4f26-4a23-b514-c341377a1ba4" />
<img width="150" height="48" alt="image" src="https://github.com/user-attachments/assets/2bc17727-6959-4e42-8610-0a46a2ea2b88" />

---

## 4. Record Count Validation

A full record count was performed on the materialized table.

```sql
SELECT COUNT(*) AS total_records
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
```

**Result:**

```
20332093
```

<img width="154" height="48" alt="image" src="https://github.com/user-attachments/assets/7bf9c9e0-4f24-4d90-bb80-9732f7800d23" />

---

## 5. Distinct Pickup Locations

The number of distinct pickup locations (`PULocationID`) was calculated on both tables.

External table:

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`;
```
<img width="436" height="50" alt="image" src="https://github.com/user-attachments/assets/51e05de2-75ba-4aae-bc43-4647bffe0023" />

Materialized table:

```sql
SELECT COUNT(DISTINCT PULocationID)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
```
<img width="461" height="65" alt="image" src="https://github.com/user-attachments/assets/aa24fd8f-c22c-46a6-b36c-f880927f1c48" />

### Observations

| Table Type         | Estimated Bytes Read |
| ------------------ | -------------------- |
| External Table     | 0 B                  |
| Materialized Table | 155.12 MB            |

Notes:

* The external table returned 0 B because BigQuery was able to use Parquet metadata and optimization to avoid scanning the full files.
* The materialized table scanned stored data in BigQuery, resulting in higher bytes processed.
* Column pruning and file-level statistics helped reduce unnecessary data reads.

---

## 6. Column Selection Comparison

Two queries were executed on the materialized table.

Only `PULocationID`:

```sql
SELECT PULocationID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
```
<img width="476" height="88" alt="image" src="https://github.com/user-attachments/assets/217a1cb3-2f76-4405-a532-074c222792af" />

`PULocationID` and `DOLocationID`:

```sql
SELECT PULocationID, DOLocationID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
```
<img width="471" height="66" alt="image" src="https://github.com/user-attachments/assets/ba88ed0c-3203-47fa-b37b-ce8b803d963d" />

### Explanation

Selecting more columns increases bytes processed because BigQuery must read more data from storage. When fewer columns are selected, only the required columns are scanned, reducing I/O and query cost.

---

## 7. Zero Fare Analysis

Trips with zero fare were identified.

```sql
SELECT COUNT(*) AS zero_fare_trips
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;
```

**Number of records:**

```
8333
```

<img width="148" height="49" alt="image" src="https://github.com/user-attachments/assets/acd3a32c-eff7-4d61-a93d-b4cfd1164d88" />

---

## 8. Optimized Table Design

An optimized table was created using partitioning and clustering.

**Strategy Used:**

* Partitioned by: `DATE(tpep_pickup_datetime)`
* Clustered by: `VendorID`

```sql
CREATE OR REPLACE TABLE
  `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`;
```

<img width="1108" height="531" alt="image" src="https://github.com/user-attachments/assets/80fa8e6c-89cd-4a2d-a692-0c583b9d36fb" />

---

## 9. VendorID Query with Date Filter

Distinct `VendorID` values were retrieved for March 2024.

Non-partitioned table:

```sql
SELECT DISTINCT VendorID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime)
BETWEEN '2024-03-01' AND '2024-03-15';
```
<img width="419" height="82" alt="image" src="https://github.com/user-attachments/assets/146c1eed-3c82-4943-b89e-22fa1875b41b" />

Optimized table:

```sql
SELECT DISTINCT VendorID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime)
BETWEEN '2024-03-01' AND '2024-03-15';
```
<img width="447" height="63" alt="image" src="https://github.com/user-attachments/assets/895bf51b-e2dc-45e4-a3a8-96a8e007f1b2" />

### Bytes Processed Comparison

| Table Type            | Estimated Bytes |
| --------------------- | --------------- |
| Non-partitioned table | 310.24 MB        |
| Partitioned table     | 26.85 MB        |

Notes:

* Partitioning restricts scanning to only the required date partitions.
* Clustering groups similar VendorID values together, improving filtering efficiency.

---

## 10. External Table Storage Location

The external table does not store data inside BigQuery.

**Actual storage location:**

```
Google Cloud Storage bucket:
gs://data_warehouse_2026_02_earth/
```

---

## 11. Clustering Best Practice

Statement:

> It is best practice in BigQuery to always cluster your data.

**Answer:**

```
False
```

Reasoning:

> Clustering is useful only when queries frequently filter by clustered columns. Unnecessary clustering may not improve performance and can increase costs.

---

## 12. Bonus: Full Table Scan Observation

A full table scan was performed.

```sql
SELECT COUNT(*)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
```
<img width="417" height="60" alt="image" src="https://github.com/user-attachments/assets/4359689d-d0d6-4cea-9d4f-0bd6fc9a3480" />

**Estimated bytes processed:**

```
0 B
```

Explanation:

> BigQuery returned 0 B processed because it used table metadata to retrieve the total row count without scanning the underlying data files. For simple COUNT(*) queries without filters, BigQuery can avoid a full table scan.

---

## 13. Final Notes

* All SQL queries are included in this repository.
* Results may vary depending on execution time and region.
