HW

**Q1**
``` 
SELECT COUNT(*) as record_count
FROM `de-zoomcamp-shane.trips_data_all.green_trips_2022`
``` 

**Q2**
``` 
SELECT COUNT(*) as record_count
FROM `de-zoomcamp-shane.trips_data_all.green_trips_2022`
WHERE fare_amount = 0
``` 


**Q3**
``` 
SELECT COUNT(distinct PULocationID) as PULocationIDs_count
FROM `de-zoomcamp-shane.trips_data_all.green_trips_2022` --0 MB

SELECT COUNT(distinct PULocationID) as PULocationIDs_count
FROM `de-zoomcamp-shane.trips_data_all.green_tripdata_materialized_2022` --6.41 MB
```
**Q4**

``` 
CREATE OR REPLACE TABLE
`de-zoomcamp-shane.trips_data_all.green_tripdata_materialized_partitioned_2022`
PARTITION BY
lpep_pickup_datetime
AS SELECT * EXCEPT(lpep_pickup_datetime), CAST(lpep_pickup_datetime AS DATE) as lpep_pickup_datetime FROM `de-zoomcamp-shane.trips_data_all.green_tripdata_materialized_2022`
``` 

**Q5**
``` 
SELECT COUNT(distinct PULocationID) as PULocationIDs_count
FROM `de-zoomcamp-shane.trips_data_all.green_tripdata_materialized_2022` --4.14 MB
WHERE CAST(lpep_pickup_datetime AS DATE) BETWEEN "2022-06-01" and "2022-06-30"

SELECT COUNT(distinct PULocationID) as PULocationIDs_count
FROM `de-zoomcamp-shane.trips_data_all.green_tripdata_materialized_partitioned_2022` --12.82 MB MB
WHERE CAST(lpep_pickup_datetime AS DATE) BETWEEN "2022-06-01" and "2022-06-30"
``` 
