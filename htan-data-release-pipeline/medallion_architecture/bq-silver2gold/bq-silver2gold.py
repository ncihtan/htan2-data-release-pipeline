#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Silver to Gold Level

    This module processes Synapse project metadata that has been evaluated
    at the silver level as part of a medallion architecture. It calls a
    series of validation checks to determine whether each project meets
    the criteria for promotion to gold. Projects with NO ERRORS
    have their corresponding metadata recorded in a BigQuery table.

Configurations: None

Functions:
    - ping_graphql_api(query, variables=None)
    - fetch_graphql_data()
    - main()
    
Author:       Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>
Date Created: 01-21-2025
Date Updated: 07-17-2025
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""

import pandas as pd
from workflow_functions.client_load import (
    load_bq,
    init_bq_client)
import requests

# ----------------------------------------
#        MODULE-LEVEL CONSTANTS
# ----------------------------------------
API_URL = "https://general.datacommons.cancer.gov/v1/graphql/"
GRAPHQL_BATCH_SIZE = 10000

def ping_graphql_api(query, variables=None):
    """
    Send a query to the GraphQL API and return the response.

    Args:
        - query (string): Query formatted for the GraphQL API.
        - variables (dict): Variables to be passed in the query.

    Returns:
        - response (json): GraphQL response as a json file.
    """
    try:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        if e.response is not None:
            print("Response content:", e.response.text)
        return None

def fetch_graphql_data():
    """
    Query the GraphQL API to exhaust all rows and return the concatenated DataFrame.

    Returns:
        - cds_all_data (pandas.DataFrame): All responses from the GraphQL query converted
            to an aggregated DataFrame.
    """
    cds_all_data = pd.DataFrame()
    offset_value = 0

    query = """
    query fileOverview($studies: [String], $first: Int, $offset: Int) {
      fileOverview(
        studies: $studies,
        first: $first,
        offset: $offset
      ) {
        study_acronym
        phs_accession
        subject_id
        sample_id
        experimental_strategy
        sex
        analyte_type
        is_tumor
        file_name
        file_type
        file_size
        file_id
        md5sum
        study_data_type
        library_strategy
        image_modality
        __typename
      }
    }
    """

    while True:
        variables = {
            "studies": [
                "Human Tumor Atlas Network (HTAN) primary sequencing data",
                "Human Tumor Atlas Network (HTAN) imaging data"
            ],
            "first": GRAPHQL_BATCH_SIZE,
            "offset": offset_value
        }

        response = ping_graphql_api(query, variables)
        if not response:
            print("No response; exiting loop.")
            break

        files = response.get("data", {}).get("fileOverview", [])
        if not files:
            print("No more files found; done.")
            break

        print(f"Fetched {len(files)} rows at offset {offset_value}")
        batch_df = pd.DataFrame(files)
        cds_all_data = pd.concat([cds_all_data, batch_df], ignore_index=True)

        offset_value += GRAPHQL_BATCH_SIZE

    return cds_all_data

def main():
    """
    Validates Silver level and General Commons file candidates for promotion to Gold.

    """
    # Instantiate google bigquery client
    client = init_bq_client()

    # Load previously released data
    previously_Released = client.query("""
                            SELECT * FROM `htan-dcc.released.entities`
                            """).result().to_dataframe()
    load_bq(client,
            'htan-dcc',
            'htan_medallion_gold',
            'gold_INDEXING_TABLE_Legacy_Released_File_Metadata',
            previously_Released)

    exclusion_list = client.query("""
        SELECT * 
        FROM `htan-dcc.htan_medallion_silver.silver_INDEXING_TABLE_ManualExclusionList`
        """).result().to_dataframe()

    # Fetch data from the GraphQL API
    print("Fetching data from GraphQL API...")
    cds_all_data = fetch_graphql_data()
    sub_cds_files = cds_all_data[["md5sum", "file_id"]].rename(columns={"md5sum": "md5"})

    tables = client.list_tables('htan-dcc.htan_medallion_silver')
    for table in tables:
        if "INDEXING_TABLE" in str(table.table_id):
            continue
        current_table = table.table_id

        # Load manifest data from BigQuery
        manifest_data = client.query(f"""
                                     SELECT * FROM `htan-dcc.htan_medallion_silver.{current_table}`
                                     """).result().to_dataframe()

        # Remove .bai files
        if "Filename" in manifest_data.columns:
            manifest_data = manifest_data[~manifest_data["Filename"].str.contains(".bai")]

        # Add GC metadata data to silver tables
        if "HTAN_Data_File_ID" in manifest_data.columns:
            manifest_data = pd.merge(manifest_data,
                                     sub_cds_files,
                                     on="md5",
                                     how="left").rename(columns={"file_id": "drs_uri"})

        # Remove columns that contain error information
        target_columns = [
            'Error_Not_Unique_Demo',
            'Error_Not_Unique_Bios',
            'Error_Adjacent_Bios',
            'Error_Not_Unique_HTAN_ID',
            'Error_Ending_Not_Conform_HTAN_Standard',
            'Error_Basename_Not_Conform_HTAN_Standard',
            'Error_Parent_Not_Found',
            'File_Removal_Reason',
            'Manifest_Removal_Reason'
        ]

        existing_columns = [col for col in target_columns if col in manifest_data.columns]

        if existing_columns:
            if (manifest_data['Component'] == 'ImagingLevel2').all() and len(manifest_data) != 0:
                manifest_data = manifest_data[manifest_data['Channel_Metadata_Filename'].isna()]
                manifest_data = manifest_data.drop('Channel_Metadata_Filename', axis=1)

                For_Release = previously_Released[['entityId',
                                                   'Data_Release',
                                                   "CDS_Release",
                                                   'IDC_Release']]
                manifest_data = pd.merge(manifest_data,
                                         For_Release,
                                         on='entityId',
                                         how='left')

            mask = manifest_data[existing_columns].isna().all(axis=1)
            manifest_data = manifest_data[mask]
            manifest_data.drop(columns=existing_columns, inplace=True)

        # Add previously released files to ready-to-release files
        htan_release_info = previously_Released[['entityId',
                                                 'Data_Release',
                                                 'CDS_Release']]
        manifest_data = pd.merge(manifest_data,
                                 htan_release_info,
                                 left_on='entityId',
                                 right_on='entityId',
                                 how='left')

        # Load updated gold tables to BigQuery
        gold_key = f"gold_METADATA_TABLE_{current_table.split('_', 4)[3]}"
        load_bq(client, 'htan-dcc',
                'htan_medallion_gold', gold_key,
                manifest_data)

    # Make the release shortlist and overviews
    all_tagged = client.query("""
        SELECT * FROM `htan-dcc.htan_medallion_silver.silver_INDEXING_TABLE_All_Error_Tagged_Files`
        WHERE coalesce(
                    Error_Not_Unique_Demo,
                    Error_Not_Unique_Bios,
                    Error_Adjacent_Bios,
                    Error_Not_Unique_HTAN_ID,
                    Error_Ending_Not_Conform_HTAN_Standard,
                    Error_Basename_Not_Conform_HTAN_Standard,
                    Error_Parent_Not_Found,
                    File_Removal_Reason,
                    Manifest_Removal_Reason) IS NULL
        AND Filename NOT LIKE "%.bai%"
        """).result().to_dataframe()

    # Filter out to-be-released files tagged on the exclusion list
    Not_Released_no_error = all_tagged[~all_tagged["entityId"] \
                                       .isin(previously_Released["entityId"])]
    Not_Released_no_error = Not_Released_no_error[~Not_Released_no_error["entityId"] \
                                                  .isin(exclusion_list["file_id"])]
    Not_Released_no_error = Not_Released_no_error[~Not_Released_no_error["Manifest_Id"] \
                                                  .isin(exclusion_list["manifest_id"])]

    # Load final release candidates to BigQuery
    print("Loading release candidates...")
    load_bq(client,
            'htan-dcc',
            'htan_medallion_gold',
            'gold_INDEXING_TABLE_Current_Release_Candidates',
            Not_Released_no_error)
    release_table_grouped = Not_Released_no_error \
        .groupby(['Manifest_Id','Component', 'HTAN_Center'])[['entityId']] \
        .count().reset_index().rename(columns={"entityId": "Number_of_Files"})
    load_bq(client,
            'htan-dcc',
            'htan_medallion_gold',
            'gold_INDEXING_TABLE_Current_Release_Candidates_Grouped_Counts',
            release_table_grouped)

if __name__ == "__main__":
    main()
