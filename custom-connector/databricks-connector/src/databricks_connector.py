# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType  # Import necessary types
from src.constants import EntryType
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.constants import JDBC_JAR
from databricks import sql


def _get_columns(schema_name: str, object_type: str) -> str:
    """Returns list of columns a tables or view"""
    if not isinstance(object_type, list) or not all(isinstance(ot, str) for ot in object_type):
        raise ValueError("object_types must be a list of strings.")

    quoted_object_types = ", ".join(f"'{ot}'" for ot in object_type)

    return (f"SELECT c.table_name, c.column_name,  "
            f"c.data_type, c.is_nullable "
            f"FROM system.information_schema.columns c "
            f"JOIN system.information_schema.tables t ON  "
            f"c.table_catalog = t.table_catalog "
            f"AND c.table_schema = t.table_schema "
            f"AND c.table_name = t.table_name "
            f"WHERE c.table_schema = '{schema_name}' "
            f"AND t.table_type IN ({quoted_object_types})")


class DatabricksConnector:
    """Reads metadata from a Databricks Unity Catalog and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Get jar file, allowing override for local jar file (different version / name)
        # jar_path = getJarPath(config,[DATABRICKS_SPARK_JAR,JDBC_JAR])
        # jar_path = getJarPath(config,[JDBC_JAR])
        # print(jar_path)
        # Check jar files exist. Throws exception if not found
        # jarsExist = fileExists(jar_path)

        self._spark = SparkSession.builder.appName("DatabricksUnityCatalogIngestor") \
            .config("spark.log.level", "INFO") \
            .getOrCreate()

        self._host = config['workspace_url']  # TODO: @sunnyar - have this in config
        self._http_path = config['http_path']  # TODO: @sunnyar - have this in config
        self._token = config['token']  # TODO: @sunnyar - have this in config

        self._connection = sql.connect(
            server_hostname=self._host,
            http_path=self._http_path,
            access_token=self._token,
            _tls_no_verify=True
        )

        self._cursor = self._connection.cursor()

    def _execute(self, query: str) -> list:
        _cursor = self._cursor

        _cursor.execute(query)
        columns = [desc[0] for desc in _cursor.description]
        print(columns)
        schema_fields = [StructField(col_name, StringType(), True) for col_name in columns]

        explicit_schema = StructType(schema_fields)
        result = _cursor.fetchall()
        return self._spark.createDataFrame(result, schema=explicit_schema)

    def get_metastore_catalogs(self):
        query = "SELECT catalog_name FROM system.information_schema.catalogs where catalog_name not in ('system', " \
                "'workspace')"
        return self._execute(query)

    def get_db_schemas(self, catalog_name: str) -> DataFrame:
        query = f"SELECT schema_name FROM system.information_schema.schemata where catalog_name = '{catalog_name}' " \
                f"and schema_name not in ('default', 'information_schema')"
        return self._execute(query)

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        short_type = entry_type.name  # table or view, or the title of enum value
        if short_type == "TABLE":
            object_type = ["MANAGED" , "EXTERNAL"]
        else:
            object_type = ["VIEW"]
        query = _get_columns(schema_name, object_type)
        return self._execute(query)

    def get_models(self, schema_name: str) -> DataFrame:
        query = f"""
            SELECT model_name, version, creation_timestamp, created_by 
            FROM system.information_schema.model_versions 
            WHERE schema_name = '{schema_name}'
            AND schema_name NOT IN ('default', 'information_schema')
        """
        return self._execute(query)

    def get_functions(self, schema_name: str) -> DataFrame:
        query = f"""
            SELECT function_name, function_language, is_deterministic, data_type 
            FROM '{catalog_name}'.information_schema.functions 
            WHERE schema_name = '{schema_name}'
            AND schema_name NOT IN ('default', 'information_schema')
        """
        return self._execute(query)

    def get_volumes(self, schema_name: str) -> DataFrame:
        query = f"""
            SELECT volume_catalog, volume_schema, volume_name, comment 
            FROM system.information_schema.volumes 
            WHERE volume_schema = '{schema_name}'
            AND volume_schema NOT IN ('default', 'information_schema')
        """
        return self._execute(query)
