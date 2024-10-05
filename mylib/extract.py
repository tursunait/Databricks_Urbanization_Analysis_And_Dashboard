"""
Extract a dataset 
urbanization dataset
"""

import requests


def extract(
    url="https://github.com/fivethirtyeight/data/raw/refs/heads/master/urbanization-index/urbanization-census-tract.csv",
    file_path="data/urbanization.csv",
):
    """ "Extract a url to a file path"""
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path


if __name__ == "__main__":
    extract()
