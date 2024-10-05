"""
ETL-Query script
"""

import argparse
import sys
from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import general_query, create_rec, update_rec, delete_rec, read_data


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
            "update_rec",
            "delete_rec",
            "create_rec",
            "general_query",
            "read_data",
        ],
    )
    # Parse only the action first
    args = parser.parse_args(args[:1])
    print(f"Action: {args.action}")

    # Define specific arguments for each action
    if args.action == "extract":
        parser.add_argument(
            "--url",
            default="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
        )
        parser.add_argument("--file_path", default="data/urbanization.csv")

    if args.action == "transform_load":
        parser.add_argument("--dataset", default="data/urbanization.csv")

    if args.action == "create_rec":
        parser.add_argument("statefips", type=int)
        parser.add_argument("state")
        parser.add_argument("gisjoin")
        parser.add_argument("lat_tract", type=float)
        parser.add_argument("long_tract", type=float)
        parser.add_argument("population", type=int)
        parser.add_argument("adj_radiuspop_5", type=float)
        parser.add_argument("urbanindex", type=float)

    if args.action == "update_rec":
        parser.add_argument("statefips", type=int)
        parser.add_argument("state")
        parser.add_argument("gisjoin")
        parser.add_argument("lat_tract", type=float)
        parser.add_argument("long_tract", type=float)
        parser.add_argument("population", type=int)
        parser.add_argument("adj_radiuspop_5", type=float)
        parser.add_argument("urbanindex", type=float)

    if args.action == "delete_rec":
        parser.add_argument("gisjoin")

    if args.action == "general_query":
        parser.add_argument("query")

    # Parse again to get all the necessary arguments
    full_args = parser.parse_args(sys.argv[1:])
    return full_args


def main():
    args = handle_arguments(sys.argv[1:])

    if args.action == "extract":
        extract(args.url, args.file_path)
    elif args.action == "transform_load":
        load(args.dataset)
    elif args.action == "create_rec":
        create_rec(
            args.statefips,
            args.state,
            args.gisjoin,
            args.lat_tract,
            args.long_tract,
            args.population,
            args.adj_radiuspop_5,
            args.urbanindex,
        )
    elif args.action == "update_rec":
        update_rec(
            args.statefips,
            args.state,
            args.gisjoin,
            args.lat_tract,
            args.long_tract,
            args.population,
            args.adj_radiuspop_5,
            args.urbanindex,
        )
    elif args.action == "delete_rec":
        delete_rec(args.gisjoin)
    elif args.action == "general_query":
        general_query(args.query)
    elif args.action == "read_data":
        data = read_data()
        print(data)


if __name__ == "__main__":
    main()
