import glob
import hashlib
import os

import pandas as pd
import pyarrow.parquet as pq
import urllib3
import sqlalchemy
from typing import Tuple, List

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def make_hash_key(df_row) -> str:
    # convert row to string
    row_str = df_row.to_string()

    # make hash
    hash_key = hashlib.md5(row_str.encode()).hexdigest()

    return hash_key


def get_parquet_total_row_count(parquet_files_path: str) -> int:
    parquet_files = [
        os.path.join(parquet_files_path, file)
        for file in os.listdir(parquet_files_path)
        if file.endswith(".parquet")
    ]

    total_rows = 0

    # Loop through each parquet file and get number of rows from metadata
    for file in parquet_files:
        parquet_file = pq.ParquetFile(file)
        total_rows += parquet_file.metadata.num_rows

    return total_rows


def get_filtered_mintel_row(
    parquet_files_path: str, filters: List[List[Tuple]]
) -> pd.DataFrame:
    mintel_files = glob.glob(f"{parquet_files_path}/**/*.parquet", recursive=True)

    for file in mintel_files:
        df = pd.read_parquet(file, filters=filters).drop_duplicates()
        for index, row in df.iterrows():
            yield row


def get_filtered_parquet_row_count(
    parquet_files_path: str, filters: List[List[Tuple]]
) -> int:
    total = 0

    mintel_files = glob.glob(f"{parquet_files_path}/**/*.parquet", recursive=True)

    for file in mintel_files:
        df = pd.read_parquet(
            file, filters=filters, columns=[x[0] for x in filters[0]]
        )
        total += df.shape[0]

    return total


def get_mintel_row(parquet_files_path: str):
    files = [f for f in os.listdir(parquet_files_path) if f.endswith(".parquet")]

    for file in files:
        filepath = os.path.join(parquet_files_path, file)
        parquet_file = pq.ParquetFile(filepath)

        # Get the number of row groups
        num_row_groups = parquet_file.num_row_groups

        # Go through each row group
        for i in range(num_row_groups):
            table = parquet_file.read_row_group(i)
            df = table.to_pandas()

            # Yield each row of the DataFrame.
            for index, row in df.iterrows():
                yield row


def load_mintel_into_sql(sql_connection_string: str) -> None:
    engine = sqlalchemy.create_engine(sql_connection_string)


if __name__ == "__main__":
    print(get_parquet_total_row_count("../data/mintel_source"))
