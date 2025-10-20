#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Bronze to Silver Level

    This module processes Synapse project metadata that has been evaluated
    at the bronze level as part of a medallion architecture. It calls a
    series of validation checks to determine whether each project meets
    the criteria for promotion to silver. Projects that pass validation
    have their corresponding metadata recorded in a BigQuery table.

Configurations:

    This module reads configuration settings from
    'configs/config.yaml' and 'configs/config.json'
    for Synapse and BigQuery data retrieval.

Functions:
    - merge_error_data(manifest_data, error_data, column_name)
    - combine_all_errors(manifest_data, error_df, columns)
    - get_parent_ids(meta_map)
    - get_exclusion_list()
    - map_metadata(client)
    - main()
    
Author:       Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>
Date Created: 12-17-2024
Date Updated: 07-16-2025
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""

import os
import json
import pandas as pd
import yaml
from workflow_functions.bq_validation import (
    htan_id_unique,
    htan_id_regex,
    basename_regex,
    adjacent_bios,
    unique_bios,
    unique_demographics,
    parents_exist,
    get_channel_files)
from workflow_functions.client_load import (
    load_bq,
    init_synapse_client,
    init_bq_client)

# ----------------------------------------
#        MODULE-LEVEL CONFIGURATION
# ----------------------------------------


with open('./configs/config.yaml', 'r') as file:
    config_yaml = yaml.safe_load(file)

with open('./configs/config.json', 'r') as file:
    config_json = json.load(file)

# Set environment variables
center_map = os.environ.get('HTAN_CENTERS_MAP')
htan_centers = config_json['centers']

# Get descriptions for additional BigQuery columns
attributes = os.environ.get('ATTRIBUTE_DESCRIPTIONS')
add_descriptions = config_json['descriptions']

# Identify file-based components
file_components = os.environ.get('ASSAYS')
assays = config_json['assays']

center_map = config_yaml['centers']
clinical = config_yaml['clinical_attributes']
biospecimen = config_yaml['biospecimen_attributes']
assay_files = config_yaml['files']

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

    #return manifest_data[columns]

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

def get_exclusion_list():
    """
    Retrieves metadata for files and projects that must be excluded from
    the data release.
    """

    SHEET_ID = '1tUOd0kiQfW-cjnTbX24Tso5Gnq42k7sKQZLCLFLxBCA'
    SHEET_NAME = 'current'
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
    exclude = pd.read_csv(url)
    exclusion_list = exclude[['file id', 'manifest id', 'exclusion reason']]

    return exclusion_list

def map_metadata(client):
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
    tables = client.list_tables('htan-dcc.htan_medallion_bronze')
    for table in tables:
        if table.table_id == "bronze_Manifests":
            continue
        current_table = table.table_id
        manifest_data = client.query(f"""SELECT * FROM `htan-dcc.htan_medallion_bronze.{current_table}`""").result().to_dataframe()
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

def main():
    """
    Main function to process and load Synapse data into BigQuery.
    """

    # Initialize clients
    syn = init_synapse_client()
    client = init_bq_client()
    id_provenance_bronze = client.query(
        """SELECT *
        FROM `htan-dcc.htan_medallion_bronze.bronze_INDEXING_TABLE_Upstream_IDs`""") \
        .result().to_dataframe()

    # Get manifest and file exclusion list
    exclusion_list = get_exclusion_list()
    load_bq(client,
            'htan-dcc',
            'htan_medallion_silver',
            'silver_INDEXING_TABLE_ManualExclusionList',
            exclusion_list)

    meta_map = map_metadata(client)
    parent_ids = get_parent_ids(meta_map)
    error_unique_demo = unique_demographics(meta_map, id_provenance_bronze)
    error_unique_bios = unique_bios(meta_map, id_provenance_bronze)
    error_adj_bios = adjacent_bios(meta_map, id_provenance_bronze)

    silver_manifests_all_errors = pd.DataFrame()

    tables = client.list_tables('htan-dcc.htan_medallion_bronze')
    for table in tables:
        if "INDEXING_TABLE" in str(table.table_id):
            continue

        current_table = table.table_id

        # Load manifest data from BigQuery
        manifest_data = client.query(
            f"SELECT * FROM `htan-dcc.htan_medallion_bronze.{current_table}`"
        ).result().to_dataframe()

        # Merge all error data
        manifest_data['BQ_Hash'] = manifest_data['BQ_Hash'].astype(str)

        if table.table_id in {'bronze_METADATA_TABLE_CDSSequencingTemplate'}:
            continue

        if "HTAN_Data_File_ID" in manifest_data.columns:
            manifest_data = merge_error_data(manifest_data,
                                             error_unique_demo,
                                             'Error_Not_Unique_Demo')
            manifest_data = merge_error_data(manifest_data,
                                             error_unique_bios,
                                             'Error_Not_Unique_Bios')
            manifest_data = merge_error_data(manifest_data,
                                             error_adj_bios,
                                             'Error_Adjacent_Bios')

            manifest_data = merge_error_data(manifest_data,
                                             htan_id_unique(manifest_data),
                                             'Error_Not_Unique_HTAN_ID')
            manifest_data = merge_error_data(manifest_data,
                                             htan_id_regex(manifest_data),
                                             'Error_Ending_Not_Conform_HTAN_Standard')
            manifest_data = merge_error_data(manifest_data,
                                             basename_regex(manifest_data),
                                             'Error_Basename_Not_Conform_HTAN_Standard')
            manifest_data = merge_error_data(manifest_data,
                                             parents_exist(manifest_data, parent_ids),
                                             'Error_Parent_Not_Found')

            # Manual Exclusion List
            manifest_data = pd.merge(manifest_data, exclusion_list,
                                     left_on='entityId', right_on='file_id',
                                     how='left')
            manifest_data = pd.merge(manifest_data, exclusion_list,
                                     left_on='Manifest_Id', right_on='manifest_id',
                                     how='left')
            manifest_data = manifest_data.drop(['file_id_x',
                                                'manifest_id_x',
                                                'file_id_y',
                                                'manifest_id_y'],
                                                axis=1)
            manifest_data.rename(columns={'exclusion_reason_x': 'File_Removal_Reason',
                                          'exclusion_reason_y': 'Manifest_Removal_Reason'},
                                          inplace=True)

            # Summary for file errors
            silver_manifests_all_errors = combine_all_errors(
                manifest_data,
                silver_manifests_all_errors,
                ['Filename', 'entityId',
                 'Manifest_Id', 'Component',
                 'Manifest_Version', 'HTAN_Center',
                 'Error_Not_Unique_Demo', 'Error_Not_Unique_Bios',
                 'Error_Adjacent_Bios', 'Error_Not_Unique_HTAN_ID',
                 'Error_Ending_Not_Conform_HTAN_Standard',
                 'Error_Basename_Not_Conform_HTAN_Standard',
                 'Error_Parent_Not_Found', 'File_Removal_Reason',
                 'Manifest_Removal_Reason', 'md5', 'BQ_Hash'])

        # Imaging-level 2-specific logic
        if (manifest_data['Component'] == 'ImagingLevel2').all():
            channel = get_channel_files(syn, manifest_data[:5], 
                                        meta_map['ImagingLevel2'],
                                        center_map)
            manifest_data = merge_error_data(manifest_data,
                                             channel[1],
                                             'Error_Channel_Metadata_Not_Found')

            # Summary for imaging errors
            silver_manifests_all_errors = combine_all_errors(
                        manifest_data,
                        silver_manifests_all_errors,
                        ['Filename', 'entityId',
                         'Channel_Metadata_Filename',
                         'Manifest_Id', 'Component', 
                         'Manifest_Version', 'HTAN_Center',
                         'Error_Channel_Metadata_Not_Found',
                         'md5', 'BQ_Hash'])

        # Load table to BigQuery
        load_bq(client, 'htan-dcc', 'htan_medallion_silver',
                f"silver_METADATA_TABLE_{current_table.split('_', 4)[3]}",
                manifest_data)

    silver_manifests_all_errors_drop = silver_manifests_all_errors.dropna(
        subset=['Filename',
                'BQ_Hash',
                'entityId',
                'Manifest_Id',
                'Component',
                'Channel_Metadata_Filename'], how='all')
    first_cols = ['entityId', 'Manifest_Id', 'Manifest_Version',
                  'Component', 'HTAN_Center' , 'md5', 'Filename',
                  'Channel_Metadata_Filename', 'BQ_Hash']
    other_cols = [col for col in silver_manifests_all_errors_drop.columns \
                  if col not in first_cols]
    silver_manifests_all_errors_drop = silver_manifests_all_errors_drop \
                                       .reindex(columns=first_cols + other_cols)
    silver_manifests_all_errors_drop['BQ_Hash'] = silver_manifests_all_errors_drop['BQ_Hash'] \
                                                  .astype(str)
    load_bq(client, 'htan-dcc', 'htan_medallion_silver',
            'silver_INDEXING_TABLE_All_Error_Tagged_Files',
            silver_manifests_all_errors_drop)

    # Summarize Errors
    num_rows_manifests = silver_manifests_all_errors_drop \
                         .groupby(['Manifest_Id','Component'])['BQ_Hash'] \
                         .count().reset_index(name='Count')
    error_counts_grouped = silver_manifests_all_errors_drop \
                            .groupby(['Manifest_Id', 'Component', 'HTAN_Center'])[
                            ['Error_Not_Unique_Demo', 
                            'Error_Not_Unique_Bios', 
                            'Error_Adjacent_Bios', 
                            'Error_Not_Unique_HTAN_ID',
                            'Error_Ending_Not_Conform_HTAN_Standard', 
                            'Error_Basename_Not_Conform_HTAN_Standard', 
                            'Error_Parent_Not_Found',
                            'File_Removal_Reason', 
                            'Manifest_Removal_Reason', 
                            'Error_Channel_Metadata_Not_Found']].count()
    error_counts_grouped['Total'] = error_counts_grouped.sum(axis=1)
    Manifests_grouped_error_counts = pd.merge(error_counts_grouped,
                                              num_rows_manifests,
                                              on='Manifest_Id', how='left')
    bronze_manifests = client.query(
        """SELECT *
        FROM `htan-dcc.htan_medallion_bronze.bronze_INDEXING_TABLE_Manifests`""") \
        .result().to_dataframe()
    bronze_manifests.rename(columns={'manifestEntityId': 'Manifest_Id'}, inplace=True)
    bronze_manifests = bronze_manifests[['Manifest_Id',
                                         'path',
                                         'modifiedDate',
                                         'createdDate',
                                         'dataFileMD5Hex']]
    Manifests_grouped_error_counts = pd.merge(Manifests_grouped_error_counts,
                                              bronze_manifests,
                                              on='Manifest_Id',
                                              how='left')
    Manifests_grouped_error_counts.rename(columns={'Total': 'Number of Errors', 
                                                   'Count': 'Number of Rows'}, inplace=True)
    load_bq(client, 'htan-dcc',
            'htan_medallion_silver',
            'silver_INDEXING_TABLE_All_Errors_Grouped_Counts',
            Manifests_grouped_error_counts)

    # Make table with only errors
    error_only_data = Manifests_grouped_error_counts[Manifests_grouped_error_counts['Number_of_Errors'] != 0]
    load_bq(client, 'htan-dcc',
            'htan_medallion_silver',
            'silver_INDEXING_TABLE_All_All_Errors_Across_Manifests',
            error_only_data)

    # Not Released Errors
    Released = client.query(
        """SELECT *
        FROM `htan-dcc.released.entities`""").result().to_dataframe()
    Not_Released_Errors_All = silver_manifests_all_errors_drop[~silver_manifests_all_errors_drop["entityId"].isin(Released["entityId"])]
    load_bq(client, 'htan-dcc',
            'htan_medallion_silver',
            'silver_INDEXING_TABLE_All_Tested_Manifests_Not_Released',
            Not_Released_Errors_All)

    Released_Manifests = client.query(
        """SELECT *
        FROM `htan-dcc.released.metadata`""").result().to_dataframe()
    Not_Released_Errors_Manifests_grouped_error_counts = error_only_data[~error_only_data["Manifest_Id"].isin(Released_Manifests["Manifest_Id"])]
    load_bq(client, 'htan-dcc',
            'htan_medallion_silver',
            'silver_INDEXING_TABLE_All_Errors_Grouped_Counts_Not_Released',
            Not_Released_Errors_Manifests_grouped_error_counts)


if __name__ == "__main__":
    main()
