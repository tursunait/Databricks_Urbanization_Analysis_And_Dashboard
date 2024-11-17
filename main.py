"""
Main entry point for the Urbanization ETL pipeline.
"""
import os
from mylib.extract import extract, load_csv, save_random_sample
from mylib.transform_load import load_data as transform_and_load
from mylib.query import query_table, visualize_population_vs_urbanindex, visualize_urbanindex_distribution


def main():
    """
    Main execution pipeline for the Urbanization ETL process.
    """
    # Print the current working directory for debugging purposes
    current_directory = os.getcwd()
    print(f"Current Directory: {current_directory}")
    
    # Step 1: Extract the dataset
    print("Starting extraction process...")
    dataset_url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv"
    csv_file_path = "/dbfs/tmp/urbanization_census_tract.csv"
    file_path = extract(url=dataset_url, file_path=csv_file_path)
    
    # Step 2: Load the dataset into a Spark DataFrame
    print("Loading the dataset into a DataFrame...")
    df = load_csv(file_path)
    
    # Step 3: Save a random sample of the DataFrame
    print("Saving a random sample of the data...")
    output_dir = "dbfs:/tmp/urbanization_census_subset/"
    save_random_sample(df, output_dir, sample_fraction=0.1)
    
    # Step 4: Transform and Load the dataset into Databricks
    print("Transforming and loading the data into Databricks...")
    table_name = "urbanization_data_tt284"
    transform_and_load(dataset_path=csv_file_path, table=table_name)
    
    # Step 5: Query and visualize the data
    print("Querying and visualizing the data...")
    query_df = query_table(table_name=table_name, limit=1000)
    if query_df is not None:
        visualize_population_vs_urbanindex(query_df)
        visualize_urbanindex_distribution(query_df)
    else:
        print("Query execution failed. Visualizations cannot be generated.")

    print("ETL pipeline executed successfully!")


if __name__ == "__main__":
    main()
