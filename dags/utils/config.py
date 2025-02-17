import os
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

def get_synapse_conn():
    """Fetch Synapse connection details from Airflow Connections."""
    conn = BaseHook.get_connection("mssql_synapse")
    return {
        "server": conn.host,
        "database": conn.schema,
        "username": conn.login,
        "password": conn.password,
    }

def get_iceberg_config():
    """Fetch Iceberg storage configuration from Airflow Variables."""
    return {
        "s3_bucket": Variable.get("ICEBERG_S3_BUCKET"),
        "s3_path": Variable.get("ICEBERG_S3_PATH"),
        "warehouse": Variable.get("ICEBERG_WAREHOUSE"),
    }