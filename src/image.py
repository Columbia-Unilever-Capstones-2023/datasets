import requests
import fnmatch
import os


def _image_already_downloaded(images_dir: str, hash_key: str) -> bool:
    for entry in os.scandir(images_dir):
        if entry.is_file() and fnmatch.fnmatch(entry.name, f'{hash_key}*'):
            return True
    return False


def save_image(image_url: str, image_filename: str, save_path: str) -> None:
    r = requests.get(image_url, allow_redirects=True, verify=False)

    open(f"{save_path}/{image_filename}.png", "wb").write(r.content)

    return
