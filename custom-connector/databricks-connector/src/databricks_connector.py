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
# from src.constants import EntryType
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.constants import JDBC_JAR

class DatabricksConnector:
    """Reads metadata from a Databricks Unity Catalog and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Get jar file, allowing override for local jar file (different version / name)
        # jar_path = getJarPath(config,[DATABRICKS_SPARK_JAR,JDBC_JAR])
        jar_path = getJarPath(config,[JDBC_JAR])
        # Check jar files exist. Throws exception if not found
        jarsExist = fileExists(jar_path)

        self._spark = SparkSession.builder.appName("DatabricksUnityCatalogIngestor") \
            .config("spark.jars",jar_path) \
            .config("spark.log.level", "ERROR") \
            .getOrCreate()

        # self._url = f"{config['account']}.snowflakecomputing.com"

        self._host = config['host'] # TODO: @sunnyar - have this in config
        self._http_path = config['http_path'] # TODO: @sunnyar - have this in config
        self._token = config['token'] # TODO: @sunnyar - have this in config

        # Construct the JDBC URL for Databricks SQL Endpoint
        # AuthMech=2 for Personal Access Token authentication
        self._jdbc_url = (
            f"jdbc:databricks://{self._host}"
            f"?AuthMech=3&transportMode=http&httpPath={self._http_path}"
            f";SSL=1;UID=token;PWD={self._token}"
        )

        self._dbxConnectOptions = {
            "url": self._jdbc_url,
            "driver": "com.databricks.client.jdbc.Driver" # Standard Databricks JDBC driver
        }

    def _execute(self, query: str) -> DataFrame:
        _dbxConnectOptions = self._dbxConnectOptions

        return self._spark.read.format("jdbc") \
            .options(**self._dbxConnectOptions) \
            .option("query", query) \
            .load()

    # TODO: sunnyar - update this query to get the list of schemas
    def get_db_schemas(self) -> DataFrame:
        query = f"""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name != 'INFORMATION_SCHEMA'
        """
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Returns list of columns a tables or view"""
        return (f"SELECT c.table_name, c.column_name,  "
                f"c.data_type, c.is_nullable "
                f"FROM information_schema.columns c "
                f"JOIN information_schema.tables t ON  "
                f"c.table_catalog = t.table_catalog "
                f"AND c.table_schema = t.table_schema "
                f"AND c.table_name = t.table_name "
                f"WHERE c.table_schema = '{schema_name}' "
                f"AND t.table_type = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        short_type = entry_type.name  # table or view, or the title of enum value
        if ( short_type == "TABLE" ):
            object_type = "BASE TABLE"
        else:
            object_type = "VIEW"
        query = self._get_columns(schema_name, object_type)
        return self._execute(query)