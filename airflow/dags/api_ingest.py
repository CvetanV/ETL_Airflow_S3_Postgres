from builtins import range
from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.S3_hook import S3Hook
import json
import requests
import pandas as pd
from pandas import json_normalize


args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    dag_id='api_ingest',
    default_args=args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    tags=['api_ingest_tag']
)


def api_ingest():
    response_categories = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datacategories?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    print(response_categories.status_code)
    response_categories = response_categories.json()
    print(response_categories)
    json_categories = json.dumps(response_categories)
    print(type(json_categories))


    # Transformations
    # df = pd.read_json('response_categories_results.json')
    df_api_dcat = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datacategories?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    df_json_dcat = df_api_dcat.json()
    print(df_json_dcat['results'])
    print(type(df_json_dcat['results']))
    print(type(df_json_dcat['results'][0]))

    df_dcat = pd.DataFrame(df_json_dcat['results'])
    print(df_dcat)
    print(df_dcat.columns)

    response_datasets = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datasets?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    print(response_datasets.status_code)
    response_datasets = response_datasets.json()['results']
    print(response_datasets)
    json_datasets = json.dumps(response_datasets)
    print(type(json_datasets))


    # Transformations
    # df = pd.read_json('response_categories_results.json')
    df_api_datasets = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datasets?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    df_json_datasets = df_api_datasets.json()
    print(df_json_datasets['results'])
    print(type(df_json_datasets['results']))
    print(type(df_json_datasets['results'][0]))

    df_datasets = pd.DataFrame(df_json_datasets['results'])
    print(df_datasets)
    print(df_datasets.columns)


    response_datatypes = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datatypes?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    print(response_datatypes.status_code)
    response_datatypes = response_datatypes.json()['results']
    print(response_datatypes)

    # Transformations
    # df = pd.read_json('response_categories_results.json')
    df_api_datatypes = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/datatypes?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    df_json_datatypes = df_api_datatypes.json()
    print(df_json_datatypes['results'])
    print(type(df_json_datatypes['results']))
    print(type(df_json_datatypes['results'][0]))

    df_datatypes = pd.DataFrame(df_json_datatypes['results'])
    print(df_datatypes)
    print(df_datatypes.columns)

    #
    # response_locationcat = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/locationcategories?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    # print(response_locationcat.status_code)
    # response_locationcat = response_locationcat.json()['results']
    # print(response_locationcat)
    #
    # response_locations = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/locations?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    # print(response_locations.status_code)
    # response_locations = response_locations.json()['results']
    # print(response_locations)
    #
    # response_stations = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/stations?datasetid=GHCND", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    # print(response_stations.status_code)
    # response_stations = response_stations.json()['results']
    # print(response_stations)
    #
    # response_data = requests.get("https://www.ncei.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&startdate=2022-11-01&enddate=2022-11-10", headers={'token':'BhTGUauitLtwSQTusVntADPdmDELhTNC'})
    # print(response_data.status_code)
    # response_data = response_data.json()['results']
    # print(response_data)


run_api_ingest = PythonOperator(
    task_id='api_ingest',
    dag=dag,
    python_callable=api_ingest
)


if __name__ == "__main__":
    dag.cli()