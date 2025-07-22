#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Synapse to Raw Level

    This module loads and prepares metadata from Synapse for integration into the
    Medallion Architecture's raw data layer.

Configurations: None

Functions:
    - process_synapse_table(syn, table_id: str, query_filter: str,
                            column_mapping: dict, output_columns: list)
    - main()
    
Author:       Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>
Date Created: 01-17-2025
Date Updated: 07-16-2025
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""

import pandas as pd
from workflow_functions.client_load import (
    load_bq,
    init_bq_client,
    init_synapse_client)

# Constants
BQ_PROJECT = 'htan-dcc'
BQ_DATASET = 'htan_medallion_raw'
SYNAPSE_TABLE_ID = "syn20446927"

def process_synapse_table(syn, table_id: str, query_filter: str,
                          column_mapping: dict, output_columns: list):
    """
    Processes a Synapse table by querying, filtering, and transforming the data.

    Args:
        - syn (Synapse instance): Synapse client object .
        - table_id (string): BigQuery table name. 
        - query_filter (string): WHERE clause of SQL query.
        - column_mapping (dict): Dict of updated column names for a pandas DataFrame.
        - output_columns (list): List of specific column names to be included in the
            returned pandas DataFrame.

    Returns:
        - table_data_latest (pandas.Dataframe): Merged dataframe containing all manifest info
            and combined error notices.
    """
    # Query data using the Synapse Client
    table_data = syn.tableQuery(f"SELECT * FROM {table_id} {query_filter}").asDataFrame()
    table_data['modifiedDate'] = pd.to_datetime(table_data['modifiedOn'], unit='ms').dt.date
    table_data['createdDate'] = pd.to_datetime(table_data['createdOn'], unit='ms').dt.date

    # Get latest version by grouping on 'id' and finding the max 'modifiedOn'
    latest_dates = table_data.groupby('id')['modifiedOn'].max().reset_index()
    table_data_latest = pd.merge(table_data, latest_dates, on="id", how="inner")

    # Clean final DataFrame
    table_data_latest = table_data_latest.drop_duplicates(['parentId'])
    table_data_latest = table_data_latest[output_columns].reset_index(drop=True)
    table_data_latest.rename(columns=column_mapping, inplace=True)

    return table_data_latest

def main():
    """
    Main function to process and load Synapse data into BigQuery.
    """
    # Initialize clients
    syn = init_synapse_client()
    client = init_bq_client()

    # Process Synapse Manifests
    manifest_query_filter = "WHERE name LIKE 'synapse_storage_manifest%'"
    manifest_column_mapping = {'id': 'manifestEntityId', 'name': 'manifestFileName'}
    manifest_output_columns = [
        'id', 'currentVersion', 'parentId', 'projectId', 'name', 'Component',
        'modifiedDate', 'createdDate', 'dataFileMD5Hex', 'path'
    ]
    manifest_data = process_synapse_table(
        syn, SYNAPSE_TABLE_ID,
        manifest_query_filter,
        manifest_column_mapping,
        manifest_output_columns
    )
    load_bq(client, BQ_PROJECT, BQ_DATASET,
            'raw_synapse_latest_manifests',
            manifest_data)

    # Process Synapse File View
    fileview_query_filter = """
        WHERE type = 'file'
        AND name NOT LIKE 'synapse_storage_manifest%'
        AND projectId NOT IN ('syn21989705',
                              'syn20977135',
                              'syn20687304',
                              'syn32596076',
                              'syn52929270')
    """
    fileview_column_mapping = {'id': 'fileEntityId', 'name': 'dataFileName'}
    fileview_output_columns = ['id', 'currentVersion',
                               'parentId', 'projectId',
                               'name', 'Component',
                               'modifiedDate', 'createdDate',
                               'dataFileMD5Hex', 'dataFileSizeBytes',
                               'path']
    fileview_data = process_synapse_table(syn,
                                          SYNAPSE_TABLE_ID,
                                          fileview_query_filter,
                                          fileview_column_mapping,
                                          fileview_output_columns)
    load_bq(client, BQ_PROJECT, BQ_DATASET, 'raw_synapse_latest_fileview', fileview_data)

if __name__ == "__main__":
    main()
