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

# RAW TO BRONZE
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

# BRONZE TO SILVER
def merge_error_data(manifest_data, error_data, column_name):
    """
    Helper function to merge error data into the manifest.

    Args:
        - manifest_data (pandas.Dataframe): Dataframe containing manifest
            data for Synapse project.
        - error_data (pandas.Dataframe): Dataframe containing validation
            error notices.
        - column_name (string): Name of specific error code.

    Returns:
        - pandas.Dataframe: Merged dataframe containing all manifest info
            and combined error notices.
    """
    error_df = pd.DataFrame(error_data.items(), columns=['entityId', column_name])

    return pd.merge(manifest_data, error_df, on='entityId', how='left')

def combine_all_errors(manifest_data, error_df, columns):
    """
    Helper function to summarize manifest data.

    Args:
        - manifest_data (pandas.Dataframe): Dataframe containing manifest
            data for Synapse project.
        - columns (list): List of relevant manifest column names

    Returns:
        - pandas.Dataframe: Filtered manifest dataframe.
    """
    # pd.concat([silver_manifests_all_errors, manifest_data_all_errors], ignore_index=True)
    return pd.concat([error_df, manifest_data[columns]], ignore_index=True)

def get_exclusion_list(client, HTAN_BQ_PROJECT, dataset):
    """
    Retrieves metadata for files and projects that must be excluded from
    the data release.

    Args:
        - client (BigQuery instance): BigQuery client object.
        - HTAN_BQ_PROJECT (string): BigQuery project name.
        - dataset (string): BigQuery dataset name.

    Returns:
        - exclusion_list (pandas.DataFrame): Files intended for exclusion
            from release--and associated metadata. 
    """

    exclusion_log = client.query(
        f"""SELECT *
            FROM `{HTAN_BQ_PROJECT}.{dataset}.exclusion_list`"""
    ).result().to_dataframe()

    exclusion_list = exclusion_log[exclusion_log['Status'] == "EXCLUDE"]

    return exclusion_list

def get_parent_ids(meta_map):
    """
    Creates a table containing primary and parent IDs from all manifests.

    Args:
        - meta_map (dict): A dictionary that organizes metadata tables by
            component.

    Returns:
        - id_list (pandas.Dataframe): Dataframe linking the primary-parent
            file ID relationship.
    """

    # Define primary and parent columns
    primary_cols = ['HTAN_Data_File_ID', 'HTAN_Biospecimen_ID']
    parent_cols = ['HTAN_Parent_Data_File_ID', 'HTAN_Parent_Biospecimen_ID', 'HTAN_Parent_ID']
    all_cols = primary_cols + parent_cols + ['entityId', 'Component']

    id_list = pd.DataFrame(columns=all_cols)
    for component, data in meta_map.items():
        id_list = pd.concat([id_list, data], axis=0).reset_index(drop=True)[all_cols]

    id_list['primaryId'] = id_list[primary_cols].values.tolist()
    id_list['parentId'] = id_list[parent_cols].values.tolist()

    # Get one row per ID
    id_list = id_list.explode('primaryId').explode('parentId')
    id_list = id_list[~id_list['parentId'].str.contains('Not', na=False)]
    id_list = id_list.applymap(lambda x: x.strip() if isinstance(x, str) else x).drop_duplicates()

    return id_list

def map_metadata(client, HTAN_BQ_PROJECT, MEDALLION_LAYER):
    """
    Build and return a dictionary (meta_map) that groups metadata
    tables by their component value BigQuery tables.

    Args:
        - client (BigQuery instance): BigQuery client object

    Returns:
        - meta_map (dict): A dictionary that organizes metadata tables by
            component.
    """

    meta_map = {}
    tables = client.list_tables(f'{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}')
    for table in tables:
        if table.table_id == "bronze_Manifests":
            continue
        current_table = table.table_id
        manifest_data = client.query(
            f"""SELECT *
                FROM `{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{current_table}`""") \
        .result().to_dataframe()
        try:
            component = manifest_data['Component'][0]
        except KeyError:
            print(f"Component not found for manifest {current_table}")
            continue

        # Create metadata map by merging manifests by component
        if component in meta_map:
            meta_map[component] = pd.concat([meta_map[component],
                                  manifest_data]).reset_index(drop=True)
        else:
            meta_map[component] = manifest_data

    return meta_map
