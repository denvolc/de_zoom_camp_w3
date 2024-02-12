--Creating external table from parquet files on GCS

CREATE OR REPLACE EXTERNAL TABLE spry-abacus-411519.nytaxi.external_green_tripdata
OPTIONS (
  format=PARQUET,
  uris=['gs://ny_gr_taxi_data_bucket/nyc_taxi_data/*']
);
--CREATE BigQuery table

CREATE OR REPLACE TABLE nytaxi.green_tripdata
AS SELECT * FROM spry-abacus-411519.nytaxi.external_green_tripdata;


--Quering external table data
--Q1
--Count of records for the 2022 Green Taxi Data
SELECT COUNT(1) FROM spry-abacus-411519.nytaxi.external_green_tripdata;

--Q2
--Count the distinct number of PULocationIDs
SELECT COUNT(DISTINCT PULocationID) FROM spry-abacus-411519.nytaxi.external_green_tripdata;
SELECT COUNT(DISTINCT PULocationID) FROM nytaxi.green_tripdata;

--Q3
--Records that have fair_amount of 0
SELECT COUNT(fare_amount) FROM nytaxi.green_tripdata WHERE fare_amount=0;

--Q4
--Creating partition clustered dataset
CREATE OR REPLACE TABLE nytaxi.clustered_green_tripdata
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID
AS (SELECT * FROM nytaxi.green_tripdata);

--Q5
--Distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022
SELECT DISTINCT PULocationID FROM nytaxi.green_tripdata
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';
SELECT DISTINCT PULocationID FROM nytaxi.clustered_green_tripdata
WHERE lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

