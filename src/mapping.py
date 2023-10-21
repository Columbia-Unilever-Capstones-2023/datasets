from typing import List
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from tqdm import tqdm


def make_mapping(from_list:List[str], to_list:List[str], confidence_threshold:float=0.8) -> dict:
    """
    Creates a mapping from one list to another using fuzzywuzzy
    """
    mapping = {}
    for from_item in tqdm(from_list):
        best_match = process.extractOne(from_item, to_list)
        mapping[from_item] = best_match[0]
    return mapping

