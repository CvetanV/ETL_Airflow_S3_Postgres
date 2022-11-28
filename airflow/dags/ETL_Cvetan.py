from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
from sqlalchemy import create_engine
from datetime import date, datetime
from datetime import timedelta
from airflow.models import DAG
import pandas as pd
import requests
import json

# Set DAG parameters/variables
today = date.today()
yesterday = today - timedelta(days = 1)
now = datetime.now()
processed_date = now.strftime("%Y-%m-%d")

psql = PostgresHook('docplanner_dwh')
engine = create_engine('postgresql://postgres:postgres@postgres:5432/')
s3 = S3Hook('docplanner_aws')
s3_raw_bucket = "raw"
s3_refined_bucket = "refined"

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='ETL_workflow',
    default_args=args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=60),
    tags=['ETL_workflow_tag']
)

# Fuction to create S3 buckets if they don't exist
def create_s3_buckets():
    if not s3.check_for_bucket(s3_raw_bucket):
        s3.create_bucket("raw")
    else:
        print("Raw bucket already created!")

    if not s3.check_for_bucket(s3_refined_bucket):
        s3.create_bucket("refined")
    else:
        print("Refined bucket already created!")

# Function to create postgres tables if they don't exist
def create_postgres_tables():
    # psql.run("""DROP TABLE public.datacategories""")
    # psql.run("""DROP TABLE public.datasets""")
    # psql.run("""DROP TABLE public.datatypes""")
    # psql.run("""DROP TABLE public.locationcategories""")
    # psql.run("""DROP TABLE public.locations""")
    # psql.run("""DROP TABLE public.stations""")
    # psql.run("""DROP TABLE public.data""")

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.datacategories
    (
       name           varchar,
       id             varchar,
       processed_date date
    );
    """, autocommit=True
    )

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.datasets
    (
       uid            varchar,
       mindate        date,
       maxdate        date,
       name           varchar,
       datacoverage   varchar,
       id             varchar,
       processed_date date
    );
    """, autocommit=True
    )

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.datatypes
    (
       index          varchar,
       mindate        date,
       maxdate        date,
       name           varchar,
       datacoverage   varchar,
       id             varchar,
       processed_date date
    );
    """, autocommit=True
    )

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.locationcategories
    (
       name           varchar,
       id             varchar,
       processed_date date
    );
    """, autocommit=True
    )

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.locations
    (
       mindate           date,
       maxdate           date,
       name              varchar,
       datacoverage      varchar,
       id                varchar,
       processed_date    date
    );
    """, autocommit=True
    )

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.stations
    (
       elevation         varchar,
       mindate           date,
       maxdate           date,
       latitude          varchar,
       name              varchar,
       datacoverage      varchar,
       id                varchar,
       elevationUnit     varchar,
       longitude         varchar,
       processed_date    date
    );
    """, autocommit=True
    )

    psql.run("""
    CREATE TABLE IF NOT EXISTS  public.data
    (
       index          varchar,
       date           date,
       datatype       varchar,
       station        varchar,
       attributes      varchar,
       value          integer,
       processed_date date
    );
    """, autocommit=True
    )

# Functions that call the APIs, collect only the "results" and store the data as json in a folder
def api_ingest_datacategories():
    # API request
    response_datacategories = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datacategories?datasetid=GHCND",
                                           headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    # Locate the results in the API response
    response_datacategories = response_datacategories.json()['results']
    # Create a dictionary that will be saved as json
    json_datacategories = json.dumps(response_datacategories)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_datacategories, f"{yesterday}/datacategories.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File datacategories.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

def api_ingest_datasets():
    response_datasets = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/datasets?datasetid=GHCND",
                                     headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    response_datasets = response_datasets.json()['results']
    json_datasets = json.dumps(response_datasets)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_datasets, f"{yesterday}/datasets.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File datasets.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

def api_ingest_datatypes():
    response_datatypes = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/datatypes?datasetid=GHCND&startdate={yesterday}&enddate={yesterday}",
                                      headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    response_datatypes = response_datatypes.json()['results']
    json_datatypes = json.dumps(response_datatypes)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_datatypes, f"{yesterday}/datatypes.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File datatypes.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

def api_ingest_locationcat():
    response_locationcat = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/locationcategories?datasetid=GHCND",
                                        headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    response_locationcat = response_locationcat.json()['results']
    json_locationcat = json.dumps(response_locationcat)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_locationcat, f"{yesterday}/locationcategories.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File locationcategories.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

def api_ingest_locations():
    response_locations = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/locations?datasetid=GHCND",
                                      headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    response_locations = response_locations.json()['results']
    json_locations = json.dumps(response_locations)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_locations, f"{yesterday}/locations.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File locations.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

def api_ingest_stations():
    response_stations = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/stations?datasetid=GHCND",
                                     headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    response_stations = response_stations.json()['results']
    json_stations = json.dumps(response_stations)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_stations, f"{yesterday}/stations.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File stations.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

def api_ingest_data():
    response_data = requests.get(f"https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&startdate={yesterday}&enddate={yesterday}",
                                 headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    response_data = response_data.json()['results']
    json_data = json.dumps(response_data)

    print("Saving raw file to S3")
    if not s3.check_for_bucket(s3_raw_bucket):
        print("Bucket is not preset")
    else:
        s3.load_string(json_data, f"{yesterday}/data.json", bucket_name=s3_raw_bucket, replace=True)
        print(f"File data.json for date {yesterday} has been written to the bucket {s3_raw_bucket}")

# Functions to refine the RAW data and store it in a postgres table and in s3 bucket
## Here I save it as json file because the saving of a parquet file was not working for me (I have limited knowledge in
## Airflow but with a bit more practice will be able to do everything that I need) and I normaly use
## parquet format for storing the refined data
def refine_datacategories():
    # Read the RAW file from the RAW S3 bucket
    loaded_datacategories = s3.read_key(f'{yesterday}/datacategories.json', bucket_name=s3_raw_bucket)
    # Load the data into a pandas dataframe
    df_datacategories = pd.read_json(loaded_datacategories)
    # Transformation: add a column that shows when the processing of the data has been done
    df_datacategories['processed_date'] = processed_date
    print(df_datacategories)

    # Saving the refined data in the S3 bucket as file
    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_datacategories, default=pd.DataFrame.to_dict)
        s3.load_string(data, f"GHCND_Model/datacategories/datacategories_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File datacategories_{today}.json has been written to the bucket {s3_refined_bucket}")

    # Saving the refined data in a postgres table
    print("Start Insert into postgres table")
    df_datacategories.to_sql('datacategories', engine, if_exists='replace')
    print("Inserting into postgres table done")

def refine_datasets():
    loaded_datasets = s3.read_key(f'{yesterday}/datasets.json', bucket_name=s3_raw_bucket)
    df_datasets = pd.read_json(loaded_datasets)
    df_datasets['processed_date'] = processed_date
    print(df_datasets)

    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_datasets, default=pd.DataFrame.to_dict)
        s3.load_string(data, f"GHCND_Model/datasets/datasets_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File datasets_{today}.json has been written to the bucket {s3_refined_bucket}")

    print("Start Insert into postgres table")
    df_datasets.to_sql('datasets', engine, if_exists='replace')
    print("Inserting into postgres table done")

def refine_datatypes():
    loaded_datatypes = s3.read_key(f'{yesterday}/datatypes.json', bucket_name=s3_raw_bucket)
    df_datatypes = pd.read_json(loaded_datatypes)
    df_datatypes['processed_date'] = processed_date
    print(df_datatypes)

    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_datatypes, default=pd.DataFrame.to_dict)
        s3.load_string(data, f"GHCND_Model/datatypes/datatypes_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File datatypes_{today}.json has been written to the bucket {s3_refined_bucket}")

    print("Start Insert into postgres table")
    df_datatypes.to_sql('datatypes', engine, if_exists='append')
    print("Inserting into postgres table done")

def refine_locationcategories():
    loaded_locationcat = s3.read_key(f'{yesterday}/locationcategories.json', bucket_name=s3_raw_bucket)
    df_locationcategories = pd.read_json(loaded_locationcat)
    df_locationcategories['processed_date'] = processed_date
    print(df_locationcategories)

    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_locationcategories, default=pd.DataFrame.to_dict)
        s3.load_string(data, f"GHCND_Model/locationcategories/locationcategories_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File locationcategories_{today}.json has been written to the bucket {s3_refined_bucket}")

    print("Start Insert into postgres table")
    df_locationcategories.to_sql('locationcategories', engine, if_exists='replace')
    print("Inserting into postgres table done")

def refine_locations():
    loaded_locations = s3.read_key(f'{yesterday}/locations.json', bucket_name=s3_raw_bucket)
    df_locations = pd.read_json(loaded_locations)
    df_locations['processed_date'] = processed_date
    print(df_locations)

    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_locations, default=pd.DataFrame.to_dict)
        s3.load_string(data, f"GHCND_Model/locations/locations_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File locations_{today}.json has been written to the bucket {s3_refined_bucket}")

    print("Start Insert into postgres table")
    df_locations.to_sql('locations', engine, if_exists='replace')
    print("Inserting into postgres table done")

def refine_stations():
    loaded_stations = s3.read_key(f'{yesterday}/stations.json', bucket_name=s3_raw_bucket)
    df_stations = pd.read_json(loaded_stations)
    df_stations['processed_date'] = processed_date
    print(df_stations)

    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_stations, default=pd.DataFrame.to_dict)
        s3.load_string(data, f"GHCND_Model/stations/stations_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File stations_{today}.json has been written to the bucket {s3_refined_bucket}")

    print("Start Insert into postgres table")
    df_stations.to_sql('stations', engine, if_exists='replace')
    print("Inserting into postgres table done")

def refine_data():
    loaded_data = s3.read_key(f'{yesterday}/data.json', bucket_name=s3_raw_bucket)
    df_data = pd.read_json(loaded_data)
    df_data['processed_date'] = processed_date
    print(df_data)

    print("Saving refined data to S3 refined layer")
    if not s3.check_for_bucket(s3_refined_bucket):
        print("Bucket is not preset")
    else:
        data = json.dumps(df_data, default = lambda df_data: json.loads(df_data.to_json()))
        s3.load_string(data, f"GHCND_Model/data/data_{today}.json", bucket_name=s3_refined_bucket,
                       replace=True)
        print(f"File data_{today}.json has been written to the bucket {s3_refined_bucket}")

    print("Start Insert into postgres table")
    df_data.to_sql('data', engine, if_exists='append')
    print("Inserting into postgres table done")

# A simple function that checks if the data has been inserted in the postgres tables
def check_processed_time():
    datacategories = psql.get_records(f"""SELECT * from public.datacategories where processed_date = '{processed_date}'""")
    datasets = psql.get_records(f"""SELECT * from public.datasets where processed_date = '{processed_date}'""")
    datatypes = psql.get_records(f"""SELECT * from public.datatypes where processed_date = '{processed_date}'""")
    locationcategories = psql.get_records(f"""SELECT * from public.locationcategories where processed_date = '{processed_date}'""")
    locations = psql.get_records(f"""SELECT * from public.locations where processed_date = '{processed_date}'""")
    stations = psql.get_records(f"""SELECT * from public.stations where processed_date = '{processed_date}'""")
    data = psql.get_records(f"""SELECT * from public.data where processed_date = '{processed_date}'""")

    print(datacategories)
    print(datasets)
    print(datatypes)
    print(locationcategories)
    print(locations)
    print(stations)
    print(data)

    if len(datacategories) > 0:
        print(f"Ingestion in table datacategories for date {yesterday} is done!")

    if len(datasets) > 0:
        print(f"Ingestion in table datasets for date {yesterday} is done!")

    if len(datatypes) > 0:
        print(f"Ingestion in table datatypes for date {yesterday} is done!")

    if len(locationcategories) > 0:
        print(f"Ingestion in table locationcategories for date {yesterday} is done!")

    if len(locations) > 0:
        print(f"Ingestion in table locations for date {yesterday} is done!")

    if len(stations) > 0:
        print(f"Ingestion in table stations for date {yesterday} is done!")

    if len(data) > 0:
        print(f"Ingestion in table data for date {yesterday} is done!")

# Calling the functions
# Actions create S3 bucket and Postgres tables
create_s3_buckets = PythonOperator(
    task_id = "create_s3_buckets",
    dag=dag,
    python_callable=create_s3_buckets
)

create_postgres_tables = PythonOperator(
    task_id='create_postgres_tables',
    dag=dag,
    python_callable=create_postgres_tables
)

# Actions API ingestion
api_ingest_datacategories = PythonOperator(
    task_id='api_ingest_datacategories',
    dag=dag,
    python_callable=api_ingest_datacategories
)

api_ingest_datasets = PythonOperator(
    task_id='api_ingest_datasets',
    dag=dag,
    python_callable=api_ingest_datasets
)

api_ingest_datatypes = PythonOperator(
    task_id='api_ingest_datatypes',
    dag=dag,
    python_callable=api_ingest_datatypes
)

api_ingest_locationcat = PythonOperator(
    task_id='api_ingest_locationcat',
    dag=dag,
    python_callable=api_ingest_locationcat
)

api_ingest_locations = PythonOperator(
    task_id='api_ingest_locations',
    dag=dag,
    python_callable=api_ingest_locations
)

api_ingest_stations = PythonOperator(
    task_id='api_ingest_stations',
    dag=dag,
    python_callable=api_ingest_stations
)

api_ingest_data = PythonOperator(
    task_id='api_ingest_data',
    dag=dag,
    python_callable=api_ingest_data
)

# Actions refinement of raw data
refine_datacategories = PythonOperator(
    task_id='refine_datacategories',
    dag=dag,
    python_callable=refine_datacategories
)

refine_datasets = PythonOperator(
    task_id='refine_datasets',
    dag=dag,
    python_callable=refine_datasets
)

refine_datatypes = PythonOperator(
    task_id='refine_datatypes',
    dag=dag,
    python_callable=refine_datatypes
)

refine_locationcategories = PythonOperator(
    task_id='refine_locationcategories',
    dag=dag,
    python_callable=refine_locationcategories
)

refine_locations = PythonOperator(
    task_id='refine_locations',
    dag=dag,
    python_callable=refine_locations
)

refine_stations = PythonOperator(
    task_id='refine_stations',
    dag=dag,
    python_callable=refine_stations
)

refine_data = PythonOperator(
    task_id='refine_data',
    dag=dag,
    python_callable=refine_data
)

# Activity check if last ingest is available in pg table
check_processed_time = PythonOperator(
    task_id='check_processed_time',
    dag=dag,
    python_callable=check_processed_time
)

# Orchestration of actions in DAG
create_s3_buckets >> create_postgres_tables >>[api_ingest_datacategories,
                                               api_ingest_datasets,
                                               api_ingest_datatypes,
                                               api_ingest_locationcat,
                                               api_ingest_locations,
                                               api_ingest_stations,
                                               api_ingest_data]

api_ingest_datacategories >> refine_datacategories
api_ingest_datasets >> refine_datasets
api_ingest_datatypes >> refine_datatypes
api_ingest_locationcat >> refine_locationcategories
api_ingest_locations >> refine_locations
api_ingest_stations >> refine_stations
api_ingest_data >> refine_data

[refine_datacategories,
 refine_datasets,
 refine_datatypes,
 refine_locationcategories,
 refine_locations,
 refine_stations,
 refine_data] >> check_processed_time

if __name__ == "__main__":
    dag.cli()
