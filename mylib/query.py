"""Query the database"""

import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Define a global variable for the log file
LOG_FILE = "/dbfs/tmp/query_log.md"


def log_query(query, result="none"):
    """Adds to a query markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")
        file.write(f"```response from Databricks\n{result}\n```\n\n")


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

