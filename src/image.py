import requests
import fnmatch
import os
from tqdm import tqdm
from tabular import get_mintel_row, make_hash_key, get_parquet_total_row_count


def _image_already_downloaded(images_dir: str, hash_key: str) -> bool:
    for entry in os.scandir(images_dir):
        if entry.is_file() and fnmatch.fnmatch(entry.name, f"{hash_key}*"):
            return True
    return False


def save_image(image_url: str, image_filename: str, save_path: str) -> None:
    r = requests.get(image_url, allow_redirects=True, verify=False)

    open(f"{save_path}/{image_filename}.png", "wb").write(r.content)

    return


if __name__ == "__main__":
    for row in tqdm(
        get_mintel_row("../data/mintel_source"),
        total=get_parquet_total_row_count("../data/mintel_source"),
    ):
        hash_key = make_hash_key(row)

        if not _image_already_downloaded(
            images_dir="../data/images", hash_key=hash_key
        ):
            all_images = row["ProductAllImagesLinksText"].strip().split()
            for idx, url in enumerate(all_images):
                save_image(
                    image_url=url,
                    image_filename=f"{hash_key}_{idx}",
                    save_path="../data/images",
                )
