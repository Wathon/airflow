# dags/utils/synapse_client.py
import pyodbc
from dags.utils.config import get_synapse_conn, logger

def query_synapse(query):
    """Execute a query on Microsoft Synapse and return results."""
    conn_config = get_synapse_conn()
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn_config['server']};"
        f"DATABASE={conn_config['database']};"
        f"UID={conn_config['username']};"
        f"PWD={conn_config['password']};"
    )
    
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error(f"Synapse query failed: {str(e)}")
        raise