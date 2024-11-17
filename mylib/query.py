"""
Query and visualize the urbanization table
"""

import os
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql.functions import col


def query_table(
    table_name="urbanization_data_tt284",
    database="default",
    limit=1000,
    mock_mode=False
):
    """
    Queries the specified table and returns the results.

    Args:
        table_name (str): The name of the table to query.
        database (str): The database containing the table.
        limit (int): The number of rows to fetch.
        mock_mode (bool): If True, returns a mocked response for testing.

    Returns:
        DataFrame: Spark DataFrame with the query results.
    """
    if mock_mode or os.getenv("MOCK_ENV") == "true":
        print(f"Mock query executed: SELECT * FROM {database}.{table_name} LIMIT {limit}")
        return [{"result": f"mocked data from {table_name}"}]

    # Initialize Spark session
    spark = SparkSession.builder.appName("DatabricksQueryUrbanization").enableHiveSupport().getOrCreate()

    try:
        # Use the specified database
        print(f"Switching to database: {database}")
        spark.sql(f"USE {database}")

        # Define and execute the query
        sql_query = f"SELECT * FROM {table_name} LIMIT {limit}"
        print(f"Executing query: {sql_query}")
        df = spark.sql(sql_query)

        print("Query executed successfully.")
        return df

    except Exception as e:
        print(f"An error occurred while querying the table: {e}")
        return None


def visualize_population_vs_urbanindex(df):
    """
    Creates a scatter plot of population vs. urban index.

    Args:
        df (DataFrame): Spark DataFrame containing the data.
    """
    print("Creating scatter plot: Population vs Urban Index...")
    # Convert to Pandas for Matplotlib compatibility
    pandas_df = df.select("population", "urbanindex").toPandas()

    # Plot
    plt.figure(figsize=(10, 6))
    plt.scatter(pandas_df["population"], pandas_df["urbanindex"], alpha=0.5, c="blue", edgecolors="k")
    plt.title("Population vs Urban Index")
    plt.xlabel("Population")
    plt.ylabel("Urban Index")
    plt.grid(True)
    plt.show()


def visualize_urbanindex_distribution(df):
    """
    Creates a histogram and density plot of the urban index distribution.

    Args:
        df (DataFrame): Spark DataFrame containing the data.
    """
    print("Creating histogram: Urban Index Distribution...")
    # Convert to Pandas for Matplotlib/Seaborn compatibility
    pandas_df = df.select("urbanindex").toPandas()

    # Plot
    plt.figure(figsize=(12, 6))
    sns.histplot(pandas_df["urbanindex"], kde=True, bins=30, color="purple", edgecolor="black")
    plt.title("Urban Index Distribution")
    plt.xlabel("Urban Index")
    plt.ylabel("Frequency")
    plt.grid(True)
    plt.show()


def viz_main():
    """
    Main function for querying and visualizing urbanization data.
    """
    # Define table and database
    table_name = "urbanization_data_tt284"
    database = "default"

    # Query the data
    print("Querying the urbanization table...")
    df = query_table(table_name=table_name, database=database, limit=1000)

    if df is not None:
        # Generate visualizations
        visualize_population_vs_urbanindex(df)
        visualize_urbanindex_distribution(df)
    else:
        print("Query execution failed. Visualizations cannot be generated.")


if __name__ == "__main__":
    viz_main()
