import pandas as pd
import numpy as np
import sys

def one_hot_encode(categories):
    unique_categories = sorted(set(categories))
    category_map = {cat: i for i, cat in enumerate(unique_categories)}
    one_hot = np.zeros((len(categories), len(unique_categories)), dtype=int)
    for i, cat in enumerate(categories):
        one_hot[i, category_map[cat]] = 1
    return one_hot

if __name__ == "__main__":
    input_categories = sys.argv[1:]
    encoded = one_hot_encode(input_categories)
    print(encoded)