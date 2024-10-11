"""
ETL-Query script
"""

import argparse
import sys
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import general_query


# Handle arguments function
def handle_arguments(args):
    """Handles actions based on initial calls"""
    parser = argparse.ArgumentParser(description="ETL-Query script")

    # Define the action argument
    parser.add_argument(
        "action",
        choices=[
            "extract",
            "transform_load",
            "general_query",
            "read_data",
        ],
    )
    # Parse only the action first
    args = parser.parse_args(args[:1])
    print(f"Action: {args.action}")

    # Define specific arguments for each action
    if args.action == "general_query":
        parser.add_argument("query")

    # Parse again to get all the necessary arguments
    full_args = parser.parse_args(sys.argv[1:])
    return full_args


def main():
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        extract()
    elif args.action == "transform_load":
        load()
    elif args.action == "general_query":
        general_query(args.query)
    else:
        print(f"Unknown action: {args.action}")


if __name__ == "__main__":
    main()
