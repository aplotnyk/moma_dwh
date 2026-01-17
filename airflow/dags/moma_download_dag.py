"""
MoMA Data Download DAG
Downloads Artists.csv and Artworks.csv from GitHub
Runs only when manually triggered
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project scripts to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Import download functions
from scripts.download_data import download_artworks, download_artists

# Default arguments
default_args = {
    'owner': 'moma_de',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,  # Retry up to 3 times if download fails
    'retry_delay': timedelta(seconds=30),
}

# Define the DAG
dag = DAG(
    'moma_download_github_data',
    default_args=default_args,
    description='Download MoMA CSV files from GitHub (manual trigger only)',
    schedule=None,  # None = manual trigger only
    catchup=False,
    tags=['moma', 'github', 'download', 'csv'],
)

# Task 1: Download Artworks CSV
download_artworks_task = PythonOperator(
    task_id='download_artworks_csv',
    python_callable=download_artworks,
    dag=dag,
)

# Task 2: Download Artists CSV  
download_artists_task = PythonOperator(
    task_id='download_artists_csv',
    python_callable=download_artists,
    dag=dag,
)

# To run tasks in sequence:
# download_artworks_task >> download_artists_task
