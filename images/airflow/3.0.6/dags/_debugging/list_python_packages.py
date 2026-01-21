from datetime import datetime

from airflow.models import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='__debug_list_python_packages',
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False
) as dag:
    list_python_packages_operator: BashOperator = BashOperator(
        task_id='list_python_packages',
        bash_command='python3 -m pip list'
    )