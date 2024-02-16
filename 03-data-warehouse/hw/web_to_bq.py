import io
import os
import requests
import pandas as pd
from google.cloud import storage
import google.auth


"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

    ## Sample URL: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet

# services = ['fhv','green','yellow']
init_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", INSERT_GCS_LOCATION)
credentials = google.auth.default()

def write_bq(df: pd.DataFrame, year: int, service: str)-> None:
    """Write DataFrame to BiqQuery"""

    df.to_gbq(
        # destination_table=f"dezoomcamp.rides_{service}_{year}",
        destination_table=f"trips_data_all.{service}_tripdata_materialized_{year}",
        project_id=INSERT_GCS_PROJECT,
        chunksize=500_000,
        if_exists="append",
    );

def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        request_url = f"{init_url}{file_name}"
        print(request_url)
        r = requests.get(request_url)
        open(file_name, 'wb').write(r.content)
        print(f"Local: {file_name}")

        # read it back into a parquet file
        df = pd.read_parquet(file_name, engine='pyarrow')

        # upload it to gcs 
        write_bq(df, year, service)
        print(f"BQ: {service}/{file_name}")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
web_to_gcs('2022', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')

