#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 17 14:44:14 2025
@author: Dar'ya Pozhidayeva
"""
import pandas as pd
import synapseclient
from google.cloud import bigquery
from workflow_functions.bq_load import load_bq

# ================================ #
# Constants
# ================================ #
BQ_PROJECT = 'htan-dcc'
BQ_DATASET = 'htan_medallion_raw'
SYNAPSE_TABLE_ID = "syn20446927"

# ================================ #
# BigQuery Loading Functions
# ================================ #
def init_bq_client():
    """
    Initializes the BigQuery client.
    """
    return bigquery.Client()

# ================================ #
# Synapse Functions
# ================================ #
def init_synapse_client(auth_token: str):
    """
    Initializes the Synapse client with the provided authentication token.
    """
    syn = synapseclient.Synapse()
    try:
       syn.login()
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print("Please fill in 'username' and 'password'/'api_key' values in .synapseConfig.")
    except synapseclient.core.exceptions.SynapseAuthenticationError:
        print("Please make sure the credentials in the .synapseConfig file are correct.")
    return syn


def process_synapse_table(syn, table_id: str, query_filter: str, column_mapping: dict, output_columns: list):
    """
    Processes a Synapse table by querying, filtering, and transforming the data.
    """
    # Query table
    table_data = syn.tableQuery(f"SELECT * FROM {table_id} {query_filter}").asDataFrame()
    
    # Add readable date columns
    table_data['modifiedDate'] = pd.to_datetime(table_data['modifiedOn'], unit='ms').dt.date
    table_data['createdDate'] = pd.to_datetime(table_data['createdOn'], unit='ms').dt.date
    
    # Get latest version by grouping on 'id' and finding the max 'modifiedOn'
    latest_dates = table_data.groupby('id')['modifiedOn'].max().reset_index()
    table_data_latest = pd.merge(table_data, latest_dates, on="id", how="inner")
    
    # Drop duplicates and filter necessary columns
    table_data_latest = table_data_latest.drop_duplicates(['parentId'])
    table_data_latest = table_data_latest[output_columns].reset_index(drop=True)
    
    # Rename columns as needed
    table_data_latest.rename(columns=column_mapping, inplace=True)
    return table_data_latest

# ================================ #
# Main Processing and Loading Logic
# ================================ #
def main():
    """
    Main function to process and load Synapse data into BigQuery.
    """
    # Initialize clients
    syn = init_synapse_client(auth_token='my_token')
    client = init_bq_client()
    
    # ------------------------ #
    # Process Synapse Manifests
    # ------------------------ #
    manifest_query_filter = "WHERE name LIKE 'synapse_storage_manifest%'"
    manifest_column_mapping = {'id': 'manifestEntityId', 'name': 'manifestFileName'}
    manifest_output_columns = [
        'id', 'currentVersion', 'parentId', 'projectId', 'name', 'Component',
        'modifiedDate', 'createdDate', 'dataFileMD5Hex', 'path'
    ]
    manifest_data = process_synapse_table(
        syn, SYNAPSE_TABLE_ID, manifest_query_filter, manifest_column_mapping, manifest_output_columns
    )
    load_bq(client, BQ_PROJECT, BQ_DATASET, 'raw_synapse_latest_manifests', manifest_data)
    
    # ------------------------ #
    # Process Synapse File View
    # ------------------------ #
    fileview_query_filter = """
        WHERE type = 'file'
        AND name NOT LIKE 'synapse_storage_manifest%'
        AND projectId NOT IN ('syn21989705','syn20977135','syn20687304','syn32596076','syn52929270')
    """
    fileview_column_mapping = {'id': 'fileEntityId', 'name': 'dataFileName'}
    fileview_output_columns = [
        'id', 'currentVersion', 'parentId', 'projectId', 'name', 'Component',
        'modifiedDate', 'createdDate', 'dataFileMD5Hex', 'dataFileSizeBytes', 'path'
    ]
    fileview_data = process_synapse_table(
        syn, SYNAPSE_TABLE_ID, fileview_query_filter, fileview_column_mapping, fileview_output_columns
    )
    load_bq(client, BQ_PROJECT, BQ_DATASET, 'raw_synapse_latest_fileview', fileview_data)

# ================================ #
# Entry Point
# ================================ #
if __name__ == "__main__":
    main()
