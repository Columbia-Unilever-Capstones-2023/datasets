import hashlib
import os
import pyarrow.parquet as pq
from tqdm import tqdm
import requests
from time import sleep
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def make_hash_key(df_row) -> str:
    # convert row to string
    row_str = df_row.to_string()

    # make hash
    hash_key = hashlib.md5(row_str.encode()).hexdigest()

    return hash_key


def get_mintel_row(parquet_files_path):
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


def save_image(image_url: str, image_filename: str, save_path: str) -> None:
    r = requests.get(image_url, allow_redirects=True, verify=False)

    open(f"{save_path}/{image_filename}.png", "wb").write(r.content)

    return


if __name__ == "__main__":
    for row in tqdm(get_mintel_row("../data/mintel_source")):
        hash_key = make_hash_key(row)
        all_images = row["ProductAllImagesLinksText"].strip().split()
        for idx, url in enumerate(all_images):
            save_image(
                image_url=url,
                image_filename=f"{hash_key}_{idx}",
                save_path="../data/images",
            )
            # sleep(1)
