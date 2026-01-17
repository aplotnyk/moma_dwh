"""
MoMA Dimensional Data Loading DAG
Orchestrates the transformation and loading of data into the transformed schema
with parallel execution for artworks and artists transformations
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator 
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Import functions
from scripts.transform_artworks import run_transform_artworks
from scripts.transform_artists import run_transform_artists
from scripts.transform_geo_economic import run_transform_geo, run_transform_economic
from scripts.build_bridge_add_artist_stats import run_build_bridge_add_stats
from scripts.transform_verification_lib import verify_staging_data, verify_transformations

default_args = {
    'owner': 'moma_de',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moma_transformed_load',
    default_args=default_args,
    description='Load cleaned data into transformed schema',
    schedule=None,
    catchup=False,
    tags=['moma', 'dimensional', 'transformation'],
)

# Define tasks

verify_staging = PythonOperator(
    task_id='verify_staging_data',
    python_callable=verify_staging_data,
    dag=dag,
)

# Parallel transformations (independent of each other)
transform_artworks_task = PythonOperator(
    task_id='transform_artworks',
    python_callable=run_transform_artworks,
    dag=dag,
)

transform_artists_task = PythonOperator(
    task_id='transform_artists',
    python_callable=run_transform_artists,
    dag=dag,
)

# Wait for both transformations to complete before proceeding
sync_transformations = EmptyOperator(
    task_id='sync_after_parallel_transforms',
    dag=dag,
)

# Geography and economic data (depends on artists data for counts)
transform_geo_task = PythonOperator(
    task_id='transform_geo',
    python_callable=run_transform_geo,
    dag=dag,
)

# Economic data
transform_economic_task = PythonOperator(
    task_id='transform_economic',
    python_callable=run_transform_economic,
    dag=dag,
)

# Bridge table and artist stats (requires both artworks and artists)
build_bridge_task = PythonOperator(
    task_id='build_bridge_add_artist_stats',
    python_callable=run_build_bridge_add_stats,
    dag=dag,
)

verify_final = PythonOperator(
    task_id='verify_transformations',
    python_callable=verify_transformations,
    dag=dag,
)

# Define task dependencies

# Verify Staging -> Parallel Transforms (Artworks and Artists)
verify_staging >> [transform_artworks_task, transform_artists_task]

# Both parallel transforms -> Sync point
[transform_artworks_task, transform_artists_task] >> sync_transformations

# Sync -> Geo/Economic (needs artists for counts) and Bridge (needs both)
sync_transformations >> [transform_economic_task, transform_geo_task, build_bridge_task]

# Both Geo/Economic and Bridge -> Verify
[transform_economic_task, transform_geo_task, build_bridge_task] >> verify_final