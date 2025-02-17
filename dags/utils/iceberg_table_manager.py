from pyiceberg.catalog import load_catalog
import pyarrow as pa
import polars as pl
import os


class IcebergTableManager:
    def __init__(self):

        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY")


    def create_namespace_if_not_exists(self, catalog, namespace):
        try:
            # Check if the namespace exists
            existing_namespaces = [ns[0] for ns in catalog.list_namespaces()]
            if namespace not in existing_namespaces:
                print(f"Namespace '{namespace}' does not exist. Creating it.")
                catalog.create_namespace(namespace)
                print(f"Namespace '{namespace}' created successfully.")
            else:
                print(f"Namespace '{namespace}' already exists.")
            return namespace
        except Exception as e:
            print(f"Failed to create or list namespace: {e}")
            raise

    def initialize_rest_catalog(self, branch_name):
        """Initialize the PyIceberg REST catalog to communicate with Nessie."""
        try:
            if not all([self.minio_endpoint, self.minio_access_key, self.minio_secret_key]):
                raise ValueError("MinIO environment variables are not properly set.")

            catalog = load_catalog(
                name="nessie",
                type="rest",
                uri=f"http://nessie:19120/iceberg/{branch_name}",  # REST endpoint for Nessie
                **{
                    "s3.endpoint": f"http://{self.minio_endpoint}",
                    "s3.access-key-id": self.minio_access_key,
                    "s3.secret-access-key": self.minio_secret_key,
                    "nessie.default-branch.name": 'main',
                    "s3.warehouse.location": "s3://warehouse/ayadedicateddwh/"
                }
            )
            print(f"Catalog initialized successfully for branch: {branch_name}")
            return catalog
        except Exception as e:
            print(f"Error initializing catalog for branch '{branch_name}': {str(e)}")
            raise


    def create_iceberg_table(self, catalog, namespace, table_name, schema, location):
        """Create an Iceberg table using the REST catalog."""
        try:
            tables = catalog.list_tables(namespace)
            if table_name not in [t[1] for t in tables]:
                print(f"Creating table '{table_name}' at location '{location}'.")
                catalog.create_table(
                    identifier=(namespace, table_name),
                    schema=schema,
                    location=location
                )
                print(f"Created Iceberg table: '{table_name}' successfully.")
            else:
                print(f"Iceberg table '{table_name}' already exists.")
        except Exception as e:
            print(f"Failed to create or list table '{table_name}': {e}")
            raise
    
    def drop_iceberg_table(self, catalog, namespace, table_name):
        """
        Drop an Iceberg table from the specified namespace in the catalog.

        Args:
            catalog: The Iceberg catalog instance.
            namespace: The namespace of the table.
            table_name: The name of the table to drop.

        Returns:
            bool: True if the table was successfully dropped, False otherwise.
        """
        try:
            # Check if the table exists
            tables = catalog.list_tables(namespace)
            if table_name in [t[1] for t in tables]:
                print(f"Dropping table '{table_name}' in namespace '{namespace}'.")
                catalog.drop_table((namespace, table_name))
                print(f"Table '{table_name}' successfully dropped.")
                return True
            else:
                print(f"Table '{table_name}' does not exist in namespace '{namespace}'.")
                return False
        except Exception as e:
            print(f"Failed to drop table '{table_name}' in namespace '{namespace}': {e}")
            raise
            
    # Function to convert Polars schema to PyArrow schema
    def polars_to_pyarrow_schema(self, polars_schema):
        schema_map = {
            pl.Utf8: pa.string(),
            pl.Int32: pa.int32(),
            pl.Int64: pa.int64(),
            pl.Float32: pa.float32(),
            pl.Float64: pa.float64(),
            pl.Boolean: pa.bool_(),
            pl.Datetime: pa.timestamp('us'),
            pl.UInt32: pa.uint32(),
            pl.UInt64: pa.uint64(),
            pl.UInt8: pa.uint8(),  # You can add more mappings if needed
            pl.Date: pa.date32(),
            pl.Time: pa.time32('ms')
        }


        fields = []
        for col, dtype in polars_schema.items():
            if dtype in schema_map:
                fields.append(pa.field(col, schema_map[dtype]))
            else:
                fields.append(pa.field(col, pa.string()))  # Default to string if type not found

        return pa.schema(fields)

    # Corrected function to convert Polars DataFrame to PyArrow Table with schema
    def polars_to_arrow_with_schema(self, df, arrow_schema):
        """Convert Polars DataFrame to PyArrow Table using a given schema."""
        
        # Create an empty dictionary to store Arrow columns
        arrow_columns = {}
        
        # Convert each column in Polars DataFrame to an Arrow Array
        for col in df.columns:
            polars_series = df[col]
            
            # Extract the data type from the schema for the current column
            arrow_dtype = arrow_schema.field_by_name(col).type
            
            # Convert Polars column to PyArrow array using the extracted data type
            arrow_columns[col] = pa.array(polars_series.to_list(), type=arrow_dtype)
        
        # Build Arrow Table using the Arrow columns
        arrow_table = pa.Table.from_pydict(arrow_columns, schema=arrow_schema)
        
        return arrow_table

    def sql_to_arrow_type(self, sql_type: str) -> pa.DataType:
        """
        Map an MSSQL data type to an Arrow data type.

        Args:
            sql_type (str): The SQL data type as a string.

        Returns:
            pa.DataType: The corresponding Arrow data type.
        """
        sql_type_mapping = {
            'int': pa.int32(),
            'bigint': pa.int64(),
            'smallint': pa.int16(),
            'tinyint': pa.uint8(),
            'bit': pa.bool_(),
            'float': pa.float64(),
            'real': pa.float32(),
            'decimal': pa.decimal128(38, 10),  # Adjust precision and scale as needed
            'numeric': pa.decimal128(38, 10),
            'char': pa.string(),
            'varchar': pa.string(),
            'text': pa.string(),
            'nchar': pa.string(),
            'nvarchar': pa.string(),
            'ntext': pa.string(),
            'date': pa.date32(),
            'datetime': pa.timestamp('us'),  # Changed 'ns' to 'us'
            'datetime2': pa.timestamp('us'),  # Changed 'ns' to 'us'
            'smalldatetime': pa.timestamp('us'),  # Changed 's' to 'us'
            'time': pa.time64('us'),  # Changed 'ns' to 'us'
            'uniqueidentifier': pa.string(),
        }
        sql_type_lower = sql_type.lower()
        arrow_type = sql_type_mapping.get(sql_type_lower)
        if not arrow_type:
            raise ValueError(f"Unsupported SQL data type: {sql_type}")
        return arrow_type

    def _convert_iceberg_to_arrow_type(self, iceberg_type):
        """
        Convert Iceberg types to PyArrow types.

        Args:
            iceberg_type: The Iceberg type to convert (string or object).

        Returns:
            pyarrow.DataType: The corresponding PyArrow type.
        """
        import re

        type_mapping = {
            "long": pa.int64(),
            "int": pa.int32(),
            "float": pa.float32(),
            "double": pa.float64(),
            "string": pa.string(),
            "boolean": pa.bool_(),
            "timestamp": pa.timestamp('ms'),
            "date": pa.date32(),
        }

        # Ensure iceberg_type is a string for pattern matching
        iceberg_type_str = str(iceberg_type)

        # Check if the type is a decimal type
        decimal_match = re.match(r"decimal\((\d+),\s*(\d+)\)", iceberg_type_str)
        if decimal_match:
            precision = int(decimal_match.group(1))
            scale = int(decimal_match.group(2))
            return pa.decimal128(precision, scale)

        # Return the mapped type or raise an error if unsupported
        arrow_type = type_mapping.get(iceberg_type_str)
        if not arrow_type:
            raise ValueError(f"Unsupported Iceberg type: {iceberg_type_str}")
        return arrow_type

    def transform_arrow_table(self, arrow_table, iceberg_schema):
        """
        Transforms the schema of the Arrow table to match the Iceberg schema.

        Args:
            arrow_table: The input PyArrow table.
            iceberg_schema: The schema of the Iceberg table.

        Returns:
            pyarrow.Table: The transformed Arrow table.
        """
        field_map = {field.name: field.field_type for field in iceberg_schema.fields}
        arrays = []

        for column in arrow_table.schema.names:
            if column in field_map:
                target_type = self._convert_iceberg_to_arrow_type(field_map[column])

                if target_type is None:
                    raise ValueError(f"Unsupported conversion for column '{column}' with type {field_map[column]}")

                current_type = arrow_table[column].type

                # Handle timestamp[ns] to timestamp[ms] conversion explicitly
                if current_type == pa.timestamp('ns') and target_type == pa.timestamp('ms'):
                    # Truncate to milliseconds before casting
                    normalized_column = arrow_table[column].cast(pa.timestamp('ms'), safe=False)
                    arrays.append(normalized_column)
                else:
                    # Default casting for other types
                    arrays.append(arrow_table[column].cast(target_type))
            else:
                raise ValueError(f"Column '{column}' does not exist in the Iceberg schema")

        # Create a new table with the transformed schema
        transformed_schema = pa.schema(
            [pa.field(name, self._convert_iceberg_to_arrow_type(field_map[name]))
            for name in arrow_table.schema.names]
        )
        return pa.Table.from_arrays(arrays, schema=transformed_schema)


