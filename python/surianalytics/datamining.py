"""
Helpers for datamining tasks
"""
import pandas as pd


def min_max_scaling(c: pd.Series) -> pd.Series:
    min = c.min()
    max = c.max()
    return c.apply(lambda x: (x - min) / (max - min))
