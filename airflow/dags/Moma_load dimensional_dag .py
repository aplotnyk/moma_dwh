"""
MoMA Dimensional Schema Loading DAG
Orchestrates the loading of dimension and fact tables from transformed layer
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

# Import functions from facts_dimensions script
from scripts.facts_dimensions import (
    run_load_dim_date,
    run_load_dim_geography,
    run_load_dim_artist,
    run_load_dim_artwork,
    run_load_fact_artwork_acquisitions,
    run_load_fact_artist_summary,
    run_verify_dimensional
)

default_args = {
    'owner': 'moma_de',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moma_dimensional_load',
    default_args=default_args,
    description='Load dimension and fact tables into dimensional schema',
    schedule=None,
    catchup=False,
    tags=['moma', 'dimensional', 'star-schema', 'facts', 'dimensions'],
)

# Define tasks

# ============================================================
# DIMENSION TABLES (can run in parallel after start)
# ============================================================

load_dim_date_task = PythonOperator(
    task_id='load_dim_date',
    python_callable=run_load_dim_date,
    dag=dag,
)

load_dim_geography_task = PythonOperator(
    task_id='load_dim_geography',
    python_callable=run_load_dim_geography,
    dag=dag,
)

load_dim_artist_task = PythonOperator(
    task_id='load_dim_artist',
    python_callable=run_load_dim_artist,
    dag=dag,
)

load_dim_artwork_task = PythonOperator(
    task_id='load_dim_artwork',
    python_callable=run_load_dim_artwork,
    dag=dag,
)

# Sync point after all dimensions loaded
sync_dimensions = EmptyOperator(
    task_id='sync_after_dimensions',
    dag=dag,
)

# ============================================================
# FACT TABLES (requires dimensions to be loaded first)
# ============================================================

load_fact_acquisitions_task = PythonOperator(
    task_id='load_fact_artwork_acquisitions',
    python_callable=run_load_fact_artwork_acquisitions,
    dag=dag,
)

load_fact_summary_task = PythonOperator(
    task_id='load_fact_artist_summary',
    python_callable=run_load_fact_artist_summary,
    dag=dag,
)

# Sync point after all facts loaded
sync_facts = EmptyOperator(
    task_id='sync_after_facts',
    dag=dag,
)

# ============================================================
# VERIFICATION
# ============================================================

verify_dimensional_task = PythonOperator(
    task_id='verify_dimensional_load',
    python_callable=run_verify_dimensional,
    dag=dag,
)

# ============================================================
# TASK DEPENDENCIES
# ============================================================

# All Dimensions -> Sync Point
[
    load_dim_date_task,
    load_dim_geography_task,
    load_dim_artist_task,
    load_dim_artwork_task
] >> sync_dimensions

# Sync -> Both Facts (parallel, both need dimensions)
sync_dimensions >> [
    load_fact_acquisitions_task,
    load_fact_summary_task
]

# Both Facts -> Sync Point
[
    load_fact_acquisitions_task,
    load_fact_summary_task
] >> sync_facts

# Sync -> Verify
sync_facts >> verify_dimensional_task
