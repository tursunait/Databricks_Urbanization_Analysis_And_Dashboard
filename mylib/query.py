"""
Query the database
"""

from pyspark.sql import SparkSession

def query(sql_query):
    """
    Runs a SQL query using Spark SQL.

    Parameters:
        sql_query (str): The SQL query to execute.

    Returns:
        list: Query results as a list of rows, or None if an error occurs.
    """
    # Initialize Spark session
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
