"""Query the database"""
import importlib.util
from pyspark.sql import SparkSession

# Check if dbutils is available
if importlib.util.find_spec("pyspark.dbutils"):
    pass
else:
    from mocks import dbutils  # Use mock for local testing

# Define a global variable for the log file
LOG_FILE = "dbfs:/tmp/query_log.md"


def log_query(query, result="none"):
    """Adds to a query markdown file."""
    try:
        # Read existing log content if the file exists
        existing_content = ""
        try:
            existing_content = dbutils.fs.head(LOG_FILE)
        except Exception as e:
            print(f"Log file not found. A new log will be created: {e}")

        # Append the new query and result to the log
        new_content = f"```sql\n{query}\n```\n\n```response from Databricks\n{result}\n```\n\n"
        dbutils.fs.put(LOG_FILE, existing_content + new_content, overwrite=True)
    except Exception as e:
        print(f"Error writing to log: {e}")


def general_query(query):
    """Runs a query a user inputs using Spark SQL."""
    # Initialize Spark session
    spark = SparkSession.builder.appName("DatabricksQueryRunner").getOrCreate()

    try:
        # Execute the query
        result = spark.sql(query).collect()

        # Format result as a string
        result_str = "\n".join([str(row) for row in result])

        # Log the query and result
        log_query(query, result_str)

        print(f"Query executed successfully:\n{query}")
        return result

    except Exception as e:
        # Log the query and error
        error_message = f"An error occurred: {e}"
        log_query(query, error_message)
        print(error_message)
        return None


if __name__ == "__main__":
    # Example usage
    sample_query = "SHOW TABLES"
    general_query(sample_query)
