"""
Data Utility Functions

    This module provides functions for transforming data
    across the different medallion levels.

Configurations: None

Functions:
    - flatten_annotation_list(lst)
    - sanitize_bq_name(name: str)
    - select_existing_columns(df: pd.DataFrame, cols: list[str])
    
Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 10-15-2025
Date Updated: 
Modified By:  
"""

import re
import pandas as pd

def flatten_annotation_list(lst):
    """
    Flatten a list of dicts with 'key' and 'value' into a flat dict.
    If 'value' is array-like, take the first element.

    Args:
        - lst (list): List of dictionaries.

    Returns:
        - out (dict): Flattened dictionary.
    """
    out = {}
    for item in lst:
        if isinstance(item, dict) and "key" in item and "value" in item:
            val = item["value"]
            out[item["key"]] = val[0] if hasattr(val, "__getitem__") else val
    return out

def sanitize_bq_name(name: str):
    """
    Ensure column and table names are BigQuery-friendly.
    
    Args:
        - name (str): BigQuery table/column name.

    Returns:
        - re.sub (str): Cleaned name.
    """
    return re.sub(r"[^0-9a-zA-Z]+", "_", name)

def select_existing_columns(df: pd.DataFrame, cols: list[str]):
    """
    Gets list of columns that exist in df (preserves order).

    Args:
        - df (pandas.DataFrame): Table containing data.
        - cols (list): List of potential column names.

    Returns:
        - list: List of column names.
    """
    return [c for c in cols if c in df.columns]
