# Import libraries

## Import package for fetching data
import psycopg2 as db

## Import library for data transformation
import datetime as dt
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

## Import library for posting data to elasticsearch
from elasticsearch import Elasticsearch

# --------------------------------------------------------- #

# Fetch from Postgre SQL

def fetch():
    ## Connect to PostgreSQL
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow' port='5432'"
    conn=db.connect(conn_string)

    ### Fetch all data from table_m3
    df=pd.read_sql("select * from table_m3", conn)
    df.to_csv('/opt/airflow/dags/P2M3_amri_hakim_data_raw.csv', index=False)
    print("-------Data Saved------")

    ### Close connection
    conn.close()

# --------------------------------------------------------- #

# Clean Data

## Cleaning data
def clean():
    df=pd.read_csv('/opt/airflow/dags/P2M3_amri_hakim_data_raw.csv')
    df.columns=[x.lower() for x in df.columns]
    df['orderdate'] = pd.to_datetime(df['orderdate'], format='%d/%m/%Y')
    print(df.dtypes)
    df.reset_index(drop=True, inplace=True)
    space = df.columns.str.isspace().sum()
    dupl = df.duplicated().sum()
    nan = df.isna().sum()
    print("Space on column:", space)
    print("Duplicate counts:", dupl)
    print("Missing values:", nan)
    df.to_csv('/opt/airflow/dags/P2M3_amri_hakim_data_clean.csv', index=False)

# --------------------------------------------------------- #

# Post to Elasticsearch

## CSV to JSON and posting to Elasticsearch
def csv_json_es():
    es = Elasticsearch("http://elasticsearch:9200") 
    df=pd.read_csv('/opt/airflow/dags/P2M3_amri_hakim_data_clean.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="frompostgresql", doc_type="doc", body=doc)
        print(res)

## Default arguments for DAG and set the time
default_args = {
    'owner': 'amri',
    'start_date': dt.datetime(2024, 4, 28, 15, 5, 0) - dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=3),
}

## DAG Initialization
with DAG('DataPipeline',
         default_args=default_args,
         schedule_interval='30 * * * *', # Every hour, on 30th mins
         ) as dag:

    ### Fetch data
    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch)
    
    ### Cleaning data
    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean)
    
    ### CSV convert to JSON and posting to Elasticsearch
    csv_json_es_data = PythonOperator(
        task_id='csv_json_es_data',
        python_callable=csv_json_es
    )

    ## Dependencies settings
    fetch_data >> clean_data >> csv_json_es_data