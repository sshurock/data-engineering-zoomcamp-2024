**Q1**

**What happens when we execute dbt build --vars '{'is_test_run':'true'}'"**

It applies a limit 100 only to our staging models


**Q2**

**What is the code that our CI job will run? Where is this code coming from?**


The code from a development branch requesting a merge to main

**Q3**
**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**

``` 
SELECT count(*) record_count
FROM `de-zoomcamp-shane.dbt_sshurock.fhv_fact_trips`
WHERE EXTRACT(year from pickup_datetime) = 2019
``` 

22998722

**Q4**
**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table and the fact_trips tile as seen in the videos?**



``` 
SELECT service_type, COUNT(tripid) as ride_count
FROM `de-zoomcamp-shane.dbt_sshurock.fact_trips`
WHERE pickup_datetime BETWEEN "2019-07-01" AND "2019-07-31"
GROUP BY 1
ORDER BY 2 DESC
``` 

Yellow

DBT: https://github.com/sshurock/taxi_ride_ny_de_zoomcamp
