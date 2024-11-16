"""
ETL-Query script
"""
import argparse
import sys
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import general_query


def handle_arguments(args):
    """Handles actions based on initial calls"""
    parser = argparse.ArgumentParser(description="ETL-Query script for Databricks")

    # Define the action argument
    parser.add_argument(
        "action",
        choices=[
            "extract",
            "transform_load",
            "general_query",
        ],
        help="Specify the action to perform: extract, transform_load, or general_query.",
    )

    # Parse only the action first
    args = parser.parse_args(args[:1])

    # Add specific arguments based on the action
    if args.action == "general_query":
        parser.add_argument("query", help="SQL query to run on Databricks.")

    # Parse all arguments after adding action-specific options
    full_args = parser.parse_args(sys.argv[1:])
    return full_args


def main():
    args = handle_arguments(sys.argv[1:])

    # Perform the specified action
    if args.action == "extract":
        print("Starting extraction...")
        extract()
    elif args.action == "transform_load":
        print("Starting transform and load...")
        load()
    elif args.action == "general_query":
        print(f"Executing query: {args.query}")
        result = general_query(args.query)
        print(f"Query result: {result}")
    else:
        print(f"Unknown action: {args.action}")
        sys.exit(1)


if __name__ == "__main__":
    main()
