import pandas as pd
import requests
import hashlib


def make_hash_key(df_row) -> str:
    # convert row to string
    row_str = df_row.to_string()

    # make hash
    hash_key = hashlib.md5(row_str.encode()).hexdigest()

    return hash_key
