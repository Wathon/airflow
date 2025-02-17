import sys
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pyarrow as pa
import pynessie
import logging

# Import custom utilities
from utils.nessie_branch_manager import NessieBranchManager
from utils.iceberg_table_manager import IcebergTableManager

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Exporter function
def populate_table_with_arrow(**context):
    # """
    # Exports data to Iceberg/Nessie with Arrow schema definitions.
    # """
    # table_schemas = context['ti'].xcom_pull(task_ids='get_table_columns')
    namespace = 'ods'
    bucket_name = 'warehouse'
    data_layer = 'main'

    logger.info("Initializing Nessie and Iceberg table manager.")
    branch_manager = NessieBranchManager()
    table_manager = IcebergTableManager()

    bronze_branch = branch_manager.create_branch(data_layer)

    for table_name, columns in table_schemas.items():
        fields = []
        for column in columns:
            column_name = column['COLUMN_NAME']
            sql_type = column['DATA_TYPE']
            arrow_type = table_manager.sql_to_arrow_type(sql_type)
            fields.append(pa.field(column_name, arrow_type))

        arrow_schema = pa.schema(fields)
        main_catalog = table_manager.initialize_rest_catalog(bronze_branch)
        table_manager.create_namespace_if_not_exists(main_catalog, namespace)

        logger.info(f"Creating Iceberg table for: {table_name}")
        table_manager.create_iceberg_table(
            main_catalog,
            namespace,
            table_name,
            arrow_schema,
            f"s3://{bucket_name}/{namespace}"
        )

        branch_name = branch_manager.generate_custom_branch_name(table_name, namespace)
        new_branch = branch_manager.create_branch(branch_name, bronze_branch)
        branch_manager.merge_branch(from_branch=bronze_branch, to_branch=new_branch)
        branch_manager.delete_branch(new_branch)

# Define the DAG
with DAG(
    dag_id='synapse_to_iceberg_pipeline',
    default_args={
        'owner': 'data_team',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline to extract data from Synapse and load into Iceberg',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task to fetch table names
    get_table_names_task = SQLExecuteQueryOperator(
        task_id='get_table_names',
        conn_id='mssql_synapse',  # Connection ID configured in Airflow
        sql=f"""
        SELECT TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'ods'
        AND TABLE_NAME = 'fbe_bank_guarantee'
        """,
        do_xcom_push=True,  # Push results to XCom
    )

    # Task to fetch table columns
    get_table_columns_task = SQLExecuteQueryOperator(
        task_id='get_table_columns',
        conn_id='mssql_synapse',  # Connection ID configured in Airflow
        sql=f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'ods'
          AND TABLE_NAME = 'fbe_bank_guarantee'
        """,
        do_xcom_push=True,  # Push results to XCom
    )

    # Export data task
    export_data_task = PythonOperator(
        task_id='populate_table_with_arrow',
        python_callable=populate_table_with_arrow,
        provide_context=True,  # Enable context passing
    )

    get_table_names_task >> get_table_columns_task >> export_data_task