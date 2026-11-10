#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare Released Rows

    This module contains helper functions that compare released
    Record Set rows in GOLD and compares them to the carried-over
    Record Set rows in SILVER. It compares cell values and counts 
    rows that match or were modified. 

Configuration: None

Function:

    - normalize_value(value)
    - compare_released_rows(df, gold_df, component, latest_folder)
"""

from collections import Counter
import re
import pandas as pd
import numbers

#####################################################
#                 HELPER FUNCTIONS
#####################################################

def normalize_value(value):
    """
    Normalize BQ cell values (e.g. empty lists to None)

    Args:
        value (any): A value retrieved from a BigQuery cell.

    Returns:
        any: The normalized value. Empty collections, missing values,
             and blank strings are converted to None; non-empty
             collections are converted to tuples; and whole-number
             numeric values are converted to integers.
    """

    # Handle empty lists and other collections
    if isinstance(value, (list, tuple, set)):
        if len(value) == 0:
            return None
        return tuple(normalize_value(v) for v in value)

    # Handle dictionaries
    if isinstance(value, dict):
        if len(value) == 0:
            return None
        return tuple(sorted(
            (k, normalize_value(v)) for k, v in value.items()
        ))

    # Handle missing values
    if value is None or value is pd.NA:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    # Handle strings and empty list representations
    if isinstance(value, str):
        value = value.strip()

        if value in ("", "[]"):
            return None

        return value

    # Normalize numeric values (E.g., 25 and 25.0 should match)
    if isinstance(value, numbers.Number) and not isinstance(value, bool):
        if float(value).is_integer():
            return int(value)

    return value

def compare_released_rows(df, gold_df, component, latest_folder):
    """
    Compares released record set rows in the GOLD layer tables
    to its counterparts in the SILVER layer, and generates a tables
    counting rows that have been modified vs stayed the same by 
    HTAN Center and assay component.

    Args:
        - df (pd.Dataframe): Component-level metadata table - SILVER layer
        - gold_df (pd.Dataframe): Component-level metadata table - GOLD layer
        - component (str): The HTAN assay type (e.g. Biospecimen)
        - latest_folder (str): Name of the Synapse folder containing the record set
        of the most recent data release.

    Returns:
        - diff_stats (pd.Dataframe): Dataframe containing release row
        comparison counts.
    """

    diff_stats = pd.DataFrame()

    silver_centers = df['HTAN_Center'].unique().tolist()

    # Exclude columns not in the data model
    released_cols = [
        col for col in gold_df.columns
        if str(col).isupper()
        and "AGE_IS_OBFUSCATED" not in col
        and "AGE_IN_MONTHS_" not in col
        and col in df.columns
    ]

    # Get the version of the 
    folder_version = re.search(r'v\d+', latest_folder).group()

    # Get the name of the most recent release folder
    release_folders = gold_df["Status_Folder_Name"].dropna().unique()

    if len(release_folders) == 0:
        return pd.DataFrame()

    latest_release = max(
        release_folders,
        key=lambda x: int(re.search(r'v(\d+)', x).group(1))
    )

    # Do the comparison for each center
    for center in silver_centers:

        silver_center = df[df["HTAN_Center"] == center].copy()

        gold_center = gold_df[
            (gold_df["HTAN_Center"] == center) &
            (gold_df["Status_Folder_Name"] == latest_release)
        ].copy()

        # Normalize values and create comparable row tuples
        silver_rows = [
            tuple(normalize_value(v) for v in row)
            for row in silver_center[released_cols].itertuples(
                index=False, name=None
            )
        ]

        gold_rows = [
            tuple(normalize_value(v) for v in row)
            for row in gold_center[released_cols].itertuples(
                index=False, name=None
            )
        ]

        # Count occurrences of each row combination
        silver_counts = Counter(silver_rows)
        gold_counts = Counter(gold_rows)

        # Calculate matching, new, and modified/removed rows
        unchanged_rows = sum(
            min(gold_counts[row], silver_counts[row])
            for row in gold_counts
        )

        new_rows = sum(
            (silver_counts - gold_counts).values()
        )

        removed_rows = sum(
            (gold_counts - silver_counts).values()
        )

        # Format table
        center_stats = {
            "HTAN_Center": center,
            "Component": component,
            "Gold_Total": len(gold_center),
            f"{folder_version}_Total": len(silver_center),
            "Net_Diff": len(silver_center) - len(gold_center),
            f"New_In_{folder_version}": int(new_rows),
            "Modified_Or_Removed_Rows": int(removed_rows),
            "Carried_Over": int(unchanged_rows)
        }

        diff_stats = pd.concat(
            [diff_stats, pd.DataFrame([center_stats])],
            ignore_index=True
        )

    return diff_stats
