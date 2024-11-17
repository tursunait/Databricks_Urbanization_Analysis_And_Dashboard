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
    Executes the extract, load, query, and visualization steps sequentially.
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
    
    # Step 3: Query Transformation
    print("Starting query transformation process...")
    query()
    
    # Step 4: Visualization
    #print("Starting visualization process...")
   # viz()

if __name__ == "__main__":
    main()
