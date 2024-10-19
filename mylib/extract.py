"""
Extract a dataset 
urbanization dataset
"""

import requests
import pandas as pd
import os


def extract(
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    url2="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-state.csv",
    file_path="data/urbanization.csv",
    file_path2="data/urbanization_state.csv",
    directory="data",
):
    """Extract a url to a file path"""
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    with requests.get(url2) as r:
        with open(file_path2, "wb") as f:
            f.write(r.content)
    df = pd.read_csv(file_path2)

    df_subset = df.head(121)

    df_subset.to_csv(file_path2, index=False)
    return file_path, file_path2


if __name__ == "__main__":
    extract()
