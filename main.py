"""
Main CLI or app entry point
"""

from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query
import os

def main():
    """
    Main execution pipeline.
    Executes the extract, load, query steps sequentially.
    """
    # Print current working directory for debugging
    current_directory = os.getcwd()
    print(f"Current Directory: {current_directory}")
    
    # Step 1: Extraction
    print("Starting extraction process...")
    extract()
    
    # Step 2: Transformation and Load
    print("Starting transformation and loading process...")
    load()
    
    # Step 3: Query Execution
    print("Starting query execution process...")
    sample_query = "SHOW TABLES"  # Replace with your desired query
    result = query(sample_query)
    if result:
        print("Query executed successfully.")
    else:
        print("Query execution failed.")

if __name__ == "__main__":
    main()
