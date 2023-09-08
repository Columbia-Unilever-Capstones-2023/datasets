from src.utils import make_hash_key
import pandas as pd


def test_hash_key():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
    assert make_hash_key(df.iloc[0]) != make_hash_key(df.iloc[1])
    assert make_hash_key(df.iloc[0]) == make_hash_key(df.iloc[0])
    assert make_hash_key(df.iloc[1]) == make_hash_key(df.iloc[1])

