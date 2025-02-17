from airflow import DAG
from airflow.providers.sql.operators.sql_execute_query import SQLExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 17),
}

dag = DAG(
    'sql_execute_query_operator_example',
    default_args=default_args,
    schedule_interval=None,
)

# Define SQL query to execute
sql_query = "SELECT COUNT(*) FROM edw.dim_account;"

# Example for using Trino
trino_task = SQLExecuteQueryOperator(
    task_id='run_trino_sql',
    sql=sql_query,
    conn_id='ayadwh_trino',  # Your Trino connection ID in Airflow UI
    autocommit=True,
    dag=dag,
)

# Define task dependencies
trino_task
