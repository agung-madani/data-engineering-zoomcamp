-- Create dataset for NYC Taxi 2024 data (if it does not already exist)
CREATE SCHEMA IF NOT EXISTS `<CENSORED_PROJECT_ID>.ny_taxi_2024`;


-- Create external table pointing to Parquet files in GCS
-- This table does not store data in BigQuery, it reads directly from GCS
CREATE OR REPLACE EXTERNAL TABLE
  `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = [
    'gs://data_warehouse_2026_02_earth/yellow_tripdata_2024-*.parquet'
  ]
);


-- Preview first 10 rows from the external table
-- Used to verify that data is loaded correctly
SELECT *
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`
LIMIT 10;


-- Create a non-partitioned materialized table from the external table
-- This copies data into BigQuery storage
CREATE OR REPLACE TABLE
  `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned` AS
SELECT *
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`;


-- Count total number of records in the materialized table
-- Used to validate successful data loading
SELECT COUNT(*)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;


-- Get total number of records with alias for reporting
SELECT COUNT(*) AS total_records
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;


-- Count distinct pickup locations from external table
-- Used to compare performance with materialized table
SELECT COUNT(DISTINCT PULocationID)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`;


-- Count distinct pickup locations from materialized table
SELECT COUNT(DISTINCT PULocationID)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;


-- Select only PULocationID column
-- Used to analyze column pruning behavior
SELECT PULocationID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;


-- Select both pickup and dropoff location columns
-- Used to compare bytes processed with more columns
SELECT PULocationID, DOLocationID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;


-- Count trips with zero fare amount
-- Used for data quality analysis
SELECT COUNT(*) AS zero_fare_trips
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`
WHERE fare_amount = 0;


-- Create optimized table with partitioning and clustering
-- Partitioned by pickup date and clustered by VendorID
CREATE OR REPLACE TABLE
  `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.external_yellow_tripdata`;


-- Get distinct VendorID values for March 2024 (non-partitioned table)
-- Used to measure full scan behavior
SELECT DISTINCT VendorID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime)
BETWEEN '2024-03-01' AND '2024-03-15';


-- Get distinct VendorID values for March 2024 (optimized table)
-- Used to compare with non-partitioned table
SELECT DISTINCT VendorID
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime)
BETWEEN '2024-03-01' AND '2024-03-15';


-- Run COUNT(*) without filters
-- Demonstrates metadata optimization (may return 0 B processed)
SELECT COUNT(*)
FROM `<CENSORED_PROJECT_ID>.ny_taxi_2024.yellow_tripdata_non_partitioned`;
