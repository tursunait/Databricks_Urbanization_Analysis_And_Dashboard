"""Query the database"""

import importlib.util
from pyspark.sql import SparkSession

# Helper function to check if `dbutils` is available
def get_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        print("dbutils is not available in this environment.")
        return None


# Define a global variable for the log file
LOG_FILE = "dbfs:/tmp/query_log.md"


def log_query(dbutils, query, result="none"):
    """Adds to a query markdown file."""
    try:
        # Check if the log file exists
        existing_content = ""
        log_dir = "/".join(LOG_FILE.split("/")[:-1])
        if dbutils.fs.ls(log_dir):
            try:
                existing_content = dbutils.fs.head(LOG_FILE)
            except Exception:
                pass  # Log file does not exist yet

        # Append the new query and result to the log
        new_content = f"```sql\n{query}\n```\n\n```response from Databricks\n{result}\n```\n\n"
        dbutils.fs.put(LOG_FILE, existing_content + new_content, overwrite=True)
    except Exception as e:
        print(f"Error writing to log: {e}")


def general_query(query):
    """Runs a query a user inputs using Spark SQL."""
    # Initialize Spark session
    spark = SparkSession.builder.appName("DatabricksQueryRunner").getOrCreate()
    dbutils = get_dbutils(spark)

    if not dbutils:
        raise EnvironmentError("dbutils is required but not available in this environment.")

    try:
        # Execute the query
        result = spark.sql(query).collect()

        # Format result as a string
        result_str = "\n".join([str(row) for row in result])

        # Log the query and result
        log_query(dbutils, query, result_str)

        print(f"Query executed successfully:\n{query}")
        return result

    except Exception as e:
        # Log the query and error
        error_message = f"An error occurred: {e}"
        log_query(dbutils, query, error_message)
        print(error_message)
        return None


if __name__ == "__main__":
    # Example usage
    sample_query = "SHOW TABLES"
    general_query(sample_query)
