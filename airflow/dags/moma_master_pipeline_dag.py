"""
MoMA Master Pipeline Orchestrator
Triggers all MoMA DAGs in the correct sequence
"""

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add project scripts to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

default_args = {
    'owner': 'moma_de',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moma_master_pipeline',
    default_args=default_args,
    description='Master orchestrator for entire MoMA data pipeline',
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=['moma', 'master', 'orchestrator', 'pipeline'],
)

# ============================================================
# PHASE 1: Setu(parallel)
# ============================================================

trigger_download = TriggerDagRunOperator(
    task_id='trigger_download_data',
    trigger_dag_id='moma_download_github_data',
    wait_for_completion=True,  # Waits for DAG to finish before proceeding
    poke_interval=30,  # Checks dag status every 30 seconds
    dag=dag,
)

trigger_create_schemas = TriggerDagRunOperator(
    task_id='trigger_create_schemas',
    trigger_dag_id='moma_create_db_schemas',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# Sync point after phase 1
sync_setup = EmptyOperator(
    task_id='sync_after_setup',
    dag=dag,
)

# ============================================================
# PHASE 2: Load Staging
# ============================================================

trigger_load_staging = TriggerDagRunOperator(
    task_id='trigger_load_staging',
    trigger_dag_id='moma_load_staging_complete',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# ============================================================
# PHASE 3: Transform Data
# ============================================================

trigger_load_transformed = TriggerDagRunOperator(
    task_id='trigger_load_transformed',
    trigger_dag_id='moma_transformed_load',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# ============================================================
# PHASE 4: Load Dimensional
# ============================================================

trigger_load_dimensional = TriggerDagRunOperator(
    task_id='trigger_load_dimensional',
    trigger_dag_id='moma_dimensional_load',
    wait_for_completion=True,
    poke_interval=30,
    dag=dag,
)

# ============================================================
# TASK DEPENDENCIES
# ============================================================

# Phase 1: Download and Create Schemas (parallel)
[trigger_download, trigger_create_schemas] >> sync_setup

# Phase 2: Load Staging (after setup complete)
sync_setup >> trigger_load_staging

# Phase 3: Transform (after staging loaded)
trigger_load_staging >> trigger_load_transformed

# Phase 4: Load Dimensional (after transform complete)
trigger_load_transformed >> trigger_load_dimensional