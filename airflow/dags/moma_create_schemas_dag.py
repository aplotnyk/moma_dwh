"""
MoMA Schema Creation DAG
Creates staging, transformed and dimensional schemas
Runs only when manually triggered
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

# Add project scripts to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

# Import create schemas functions
from scripts.execute_sql import create_all_schemas, create_staging_schema, create_transformed_schema, create_dimensional_schema

default_args = {
    'owner': 'moma_de',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
}

dag = DAG(
    dag_id='moma_create_db_schemas',
    default_args=default_args,
    description='Create MoMA database schemas (manual run)',
    schedule=None,          # manual trigger only
    catchup=False,
    tags=['moma', 'database', 'schema'],
)

# # --- Tasks ---
# create_staging = PythonOperator(
#     task_id='create_staging_schema',
#     python_callable=create_staging_schema,
#     dag=dag,
# )

# create_transformed = PythonOperator(
#     task_id='create_transformed_schema',
#     python_callable=create_transformed_schema,
#     dag=dag,
# )

# create_dimensional = PythonOperator(
#     task_id='create_dimensional_schema',
#     python_callable=create_dimensional_schema,
#     dag=dag,
# )

# OR: combined function
create_all = PythonOperator(
    task_id='create_all_schemas_in_order',
    python_callable=create_all_schemas,
    dag=dag,
)

# Option 1: run all tasks in order
# create_staging >> create_transformed >> create_dimensional
# Option 2: comment above line & enable this instead if you only want 1 task
create_all
