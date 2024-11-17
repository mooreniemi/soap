import pandas as pd
import numpy as np
import sys

def min_max_scale(values):
    """
    Perform min-max scaling on a list of numerical values.
    Scales values to the range [0, 1].
    """
    min_val = min(values)
    max_val = max(values)
    if min_val == max_val:
        return [0.5] * len(values)  # Avoid division by zero; scale to midpoint
    return [(val - min_val) / (max_val - min_val) for val in values]

if __name__ == "__main__":
    task = sys.argv[1]
    input_values = sys.argv[2:]

    if task == "scale":
        # Convert input values to floats for scaling
        input_values = [float(x) for x in input_values]
        scaled = min_max_scale(input_values)
        print("Scaled values:", scaled)
    else:
        print("Unknown task. Use 'scale' for min-max scaling.")