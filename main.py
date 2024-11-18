from mylib.extract import extract
from mylib.transform_load import transform, load
from mylib.query import query_table, visualize_population_vs_urbanindex, visualize_urbanindex_distribution
from pyspark.sql import SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    try:
        # Start a Spark session
        spark = SparkSession.builder.appName("UrbanizationDataPipeline").getOrCreate()

        logging.info("Spark session started.")

        # Step 1: Extract the CSV file
        extract_path = extract()  # Should return '/dbfs/tmp/urbanization_census_tract.csv'

        # Adjust the path for Spark to use DBFS format
        spark_extract_path = extract_path.replace('/dbfs', 'dbfs:')

        # Step 2: Transform the data
        transformed_df = transform(spark_extract_path, spark)

        # Step 3: Load the data into storage (e.g., Parquet)
        load_path = "output/urbanization_census_tract.parquet"
        load(transformed_df, output_path=load_path, file_format="parquet")

        # Step 4: Query the table
        table_name = "urbanization_data"
        database = "urbanization_db"
        logging.info("Querying the table for visualization...")
        queried_df = query_table(table_name=table_name, database=database, limit=1000)

        # Generate visualizations if data exists
        if queried_df is not None:
            logging.info("Generating visualizations...")
            visualize_population_vs_urbanindex(queried_df)
            visualize_urbanindex_distribution(queried_df)
        else:
            logging.warning("No data to visualize.")
    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)
    finally:
        # Stop the Spark session
        spark.stop()
        logging.info("Spark session stopped.")

# Run main function if this is the main script
if __name__ == "__main__":
    main()
