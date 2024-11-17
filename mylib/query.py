"""
Query the database
"""

import os
from pyspark.sql import SparkSession

def query(sql_query):
    """
    Runs a SQL query using Spark SQL. Supports a mock mode for CI/CD testing.

    Parameters:
        sql_query (str): The SQL query to execute.

    Returns:
        list: Query results as a list of rows, or a mocked response in CI/CD.
    """
    # Check if mock mode is enabled (e.g., in CI/CD)
    if os.getenv("MOCK_ENV") == "true":
        print(f"Mock executing query: {sql_query}")
        return [{"result": "mocked result"}]

    # Initialize Spark session for real query execution
    spark = SparkSession.builder.appName("DatabricksQueryRunner").getOrCreate()

    try:
        # Execute the query
        result = spark.sql(sql_query).collect()
        print(f"Query executed successfully:\n{sql_query}")
        for row in result:
            print(row)
        return result
    except Exception as e:
        print(f"An error occurred while executing the query: {e}")
        return None
