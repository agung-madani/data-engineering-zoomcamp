# Module 4 - Analytics Engineering

**Homework Submission**

> Course: Data Engineering Zoomcamp
> Module: 4
> Date: `16/02/2026`

---

# 1. Project Overview

This project uses dbt to transform NYC Taxi data (Green, Yellow, and FHV) stored in Google Cloud Storage and BigQuery.

Data flow:

1. Raw `.csv.gz` files uploaded to Google Cloud Storage
2. Raw tables created in BigQuery
3. dbt staging models transform raw data
4. Intermediate models union and clean data
5. Mart models create fact and dimension tables for analytics

Final analytical models:

* fct_trips
* dim_zones
* fct_monthly_zone_revenue

---

# 2. Environment Setup

## 2.1 Google Cloud Setup

Created BigQuery datasets in region:

asia-southeast2

Datasets:

* nytaxi
* taxi_rides_ny_prod

---

## 2.2 Data Upload to GCS

Uploaded datasets:

* Green taxi 2019–2020
* Yellow taxi 2019–2020
* FHV 2019

Upload method (streaming directly to GCS):

<img width="320" height="164" alt="image" src="https://github.com/user-attachments/assets/91997cf3-0405-40bb-a31b-24c91b559853" />

---

## 2.3 Raw Tables Creation in BigQuery

Created raw tables from GCS files:

* green_tripdata
* yellow_tripdata
* fhv_tripdata

Settings:

* Format: CSV
* Schema: Auto-detect
* Dataset: taxi_rides_ny_prod

<img width="361" height="154" alt="image" src="https://github.com/user-attachments/assets/85c72343-976e-4d61-b692-937fbff5cdaa" />

---

# 4. dbt Execution

Executed:

```bash
dbt build --target prod
```

This command:

* Builds all models
* Runs tests
* Materializes staging as views
* Materializes intermediate and marts as tables

Generated models:

* stg_green_tripdata
* stg_yellow_tripdata
* int_trips_unioned
* fct_trips
* dim_zones
* fct_monthly_zone_revenue

<img width="1287" height="662" alt="image" src="https://github.com/user-attachments/assets/59bdca65-04f7-428b-b558-1809007a52b2" />
<img width="949" height="495" alt="image" src="https://github.com/user-attachments/assets/57cdd671-e150-43b9-8c6e-a9ed2b9bc669" />

---

# 5. Homework Questions and Answers

## Question 1 — dbt Lineage

Command:

```bash
dbt run --select int_trips_unioned
```
<img width="665" height="402" alt="image" src="https://github.com/user-attachments/assets/ae52c44a-f9c5-4351-b99c-ef9a02094fc8" />

Answer:

The following models are built:

* stg_green_tripdata
* stg_yellow_tripdata
* int_trips_unioned

Reason: dbt builds upstream dependencies automatically.

Correct answer:
stg_green_tripdata, stg_yellow_tripdata, and int_trips_unioned (upstream dependencies)

---

## Question 2 — dbt Tests

Scenario:
A new payment_type value (6) appears in source data, but accepted_values test only allows [1,2,3,4,5].

Command:

```bash
dbt test --select fct_trips
```

Answer:

dbt fails the test and returns a non-zero exit code.

Correct answer:
dbt will fail the test, returning a non-zero exit code.
The accepted_values test generates a validation query that checks whether payment_type contains values outside [1,2,3,4,5]. If a new value (e.g., 6) appears, the test query returns rows, causing dbt test to fail with a non-zero exit code.

---

## Question 3 — Count of fct_monthly_zone_revenue

Query:

```sql
SELECT COUNT(*)
FROM `aerobic-copilot-484810-i9.taxi_rides_ny_prod.fct_monthly_zone_revenue`;
```
<img width="499" height="198" alt="image" src="https://github.com/user-attachments/assets/9185ba63-b324-450f-8bfb-978e36d8eb02" />

Answer:
12184

---

## Question 4 — Best Performing Zone (Green Taxis, 2020)

Query:

```sql
SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_revenue
FROM `aerobic-copilot-484810-i9.taxi_rides_ny_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY pickup_zone
ORDER BY total_revenue DESC
LIMIT 1;
```
<img width="491" height="287" alt="image" src="https://github.com/user-attachments/assets/53068a28-0998-4fc9-8bec-036c8e8f72db" />

Answer:
East Harlem North

---

## Question 5 — Green Taxi Trip Counts (October 2019)

Query:

```sql
SELECT SUM(total_monthly_trips)
FROM `aerobic-copilot-484810-i9.taxi_rides_ny_prod.fct_monthly_zone_revenue`
WHERE service_type = 'Green'
  AND revenue_month = '2019-10-01';
```
<img width="495" height="257" alt="image" src="https://github.com/user-attachments/assets/ae646116-636c-4aca-83d5-032d4b575978" />

Answer:
384,624

---

## Question 6 — FHV Staging Model

Created model:

models/staging/stg_fhv_tripdata.sql

```sql
with source as (

    select * 
    from {{ source('raw', 'fhv_tripdata') }}

),

renamed as (

    select
        dispatching_base_num,

        cast(pickup_datetime as timestamp) as pickup_datetime,
        cast(dropoff_datetime as timestamp) as dropoff_datetime,

        cast(PUlocationID as integer) as pickup_location_id,
        cast(DOlocationID as integer) as dropoff_location_id,

        cast(SR_Flag as string) as sr_flag,
        cast(Affiliated_base_number as string) as affiliated_base_number

    from source
    where dispatching_base_num is not null

)

select * from renamed

{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01'
  and pickup_datetime < '2019-02-01'
{% endif %}

```

Add FHV to sources.yml

```
      - name: fhv_tripdata
        description: Raw FHV trip records
        loaded_at_field: pickup_datetime
        columns:
          - name: dispatching_base_num
          - name: pickup_datetime
          - name: dropoff_datetime
          - name: PUlocationID
          - name: DOlocationID
          - name: SR_Flag
          - name: Affiliated_base_number
```

After running:

```bash
dbt build --target prod
```

Record count query:

```sql
SELECT COUNT(*)
FROM `aerobic-copilot-484810-i9.taxi_rides_ny_prod.stg_fhv_tripdata`;
```
<img width="461" height="216" alt="image" src="https://github.com/user-attachments/assets/0d4c7030-5356-41d9-b009-d7cd2645a465" />

Answer:
43,244,693

---

# 6. Key Technical Notes

* BigQuery dataset region must match dbt job location.
* The homework requires running with --target prod.
* dbt automatically builds upstream dependencies.
* Generic tests enforce data quality constraints.
* Staging models standardize naming and filter invalid records.
