"""
MoMA Load Staging Data DAG
Loads all data into staging schema
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Import functions
from scripts.load_staging_data import load_artworks_to_staging, load_artists_to_staging
from scripts.load_wikidata_artists import fetch_and_load_wikidata
from scripts.load_geographic_data import fetch_and_load_geographic_data  
from scripts.load_economic_data import fetch_and_load_economic_data

default_args = {
    'owner': 'moma_de',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'moma_load_staging_complete',
    default_args=default_args,
    description='Load all data to staging schema',
    schedule=None,
    catchup=False,
    tags=['moma', 'staging', 'load'],
)

# Task 1: Load CSVs (parallel)
load_artworks = PythonOperator(
    task_id='load_artworks_csv',
    python_callable=load_artworks_to_staging,
    dag=dag,
)

load_artists = PythonOperator(
    task_id='load_artists_csv',
    python_callable=load_artists_to_staging,
    dag=dag,
)

# Task 2: Load external data (parallel)
load_wikidata = PythonOperator(
    task_id='load_wikidata',
    python_callable=fetch_and_load_wikidata,
    dag=dag,
)

load_geographic = PythonOperator(
    task_id='load_geographic',
    python_callable=fetch_and_load_geographic_data,
    dag=dag,
)

load_economic = PythonOperator(
    task_id='load_economic',
    python_callable=fetch_and_load_economic_data,
    dag=dag,
)

# Dependencies
load_artworks >> [load_wikidata, load_geographic, load_economic]
load_artists >> [load_wikidata, load_geographic, load_economic]