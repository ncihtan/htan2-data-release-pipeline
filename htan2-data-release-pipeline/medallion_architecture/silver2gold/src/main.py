#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Silver to Gold
    The module is responsible for the SILVER to GOLD transition of
    the medallion architecture. It applies filtering logic on 
    all HTAN2 metadata, and generates release candidate 
    tables for both Files and Record Sets. 

Configurations: None

Functions:
    - query_bigquery_table(client, project_id, dataset_id, table_id)
    - print_sub_section(title)
    
Author: Dar'ya Pozhidayeva, Yamina Katariya
Updated: 07/1/2026
"""
import pandas as pd
from client_load import (
    load_bq,
    init_bq_client
)
#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################
PROJECT = "htan2-dcc"
RAW_DATASET = "htan2_synapse_raw"
BRONZE_DATASET = "htan2_medallion_bronze"
SILVER_DATASET = "htan2_medallion_silver"
GOLD_DATASET = "htan2_medallion_gold"
DM_DATASET = "htan2_data_model_cache"
#####################################################
#                 HELPER FUNCTIONS
#####################################################

def query_bigquery_table(client, project_id, dataset_id, table_id):
    """
    Get an entire table from BigQuery as a Pandas DataFrame.

    Args:
        - client (BigQuery instance): A BigQuery client object.
        - project_id (str): BigQuery project name.
        - dataset_id (str): BigQuery dataset name.
        - table_id (str): BigQuery table name.
    
    Returns:
        - (pandas.DataFrame): The BigQuery table as a dataframe.
    """
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    return client.query(query).to_dataframe()

def print_sub_section(title):
    """
    Print subsection headers.

    Args:
        - title (string): The title to be printed.
    """
    border = "=" * (len(title) + 8)
    print(f"\n{border}\n>>> {title.upper()} <<<\n{border}\n")

#####################################################
#                   MAIN 
#####################################################=
def main():
    """
    Entry point into the GOLD layer.
    """
    # Initialize BQ Client
    client = init_bq_client()
    
    print_sub_section("PULLING THE EXCLUSION LIST FOR POST-RELEASE EXCLUSIONS")
    #---------------------------------------------------------------------------------
    exclusion_information_query = f"""
        SELECT *
        FROM `{PROJECT}.{RAW_DATASET}.raw_INDEXING_TABLE_Exclusion_List_Form_Results`
    """
    print(exclusion_information_query)

    exclusion_files = client.query(exclusion_information_query).to_dataframe()
    
    exclusion_files = exclusion_files.loc[exclusion_files['Status'] == "EXCLUDE"]
    
    print_sub_section("PULLING FILE VALIDATION RESULTS IN SILVER LAYER")
    #---------------------------------------------------------------------------------
    file_validation_information = f"""
        SELECT *
        FROM `{PROJECT}.{SILVER_DATASET}.silver_INDEXING_TABLE_All_Files_Passed_Validation`
    """
    print(file_validation_information)

    validated_files = client.query(file_validation_information).to_dataframe()
    
    record_validation_information = f"""
        SELECT *
        FROM `{PROJECT}.{SILVER_DATASET}.silver_INDEXING_TABLE_All_Records_Passed_Validation`
    """
    print(file_validation_information)
    
    validated_records = client.query(record_validation_information).to_dataframe()

    print_sub_section("CREATING STAGING DATASETS FOR RELEASE")
    #---------------------------------------------------------------------------------
    #Confirmed Centers for Release Table
    url = "https://docs.google.com/spreadsheets/d/1wQ5XZ9uYtAzKKe3cANam8UBdOofuFaAEod-AWDdWK8Q/export?format=csv&gid=0" # Skip the header lines in the doc.
    confirmed_center_list = pd.read_csv(url, skiprows=4)
    #Load to BQ
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_STAGING_FOR_RELASE_INDEXING_TABLE_Confirmed_Centers_in_Data_Releases",
        confirmed_center_list)

    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = [
        table.table_id
        for table in bronze_tables
        if table.table_id.startswith("bronze_METADATA_TABLE_All")]
    
    collected_files_list = []
    collected_records_list = []
    
    for table_id in bronze_metadata:
        print(table_id)                
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]
        
        df = None
        
        if metadata_type == "Files":
            df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
            #File entityid remains the same regardless of location
            df = df[df['File_EntityId'].isin(validated_files['File_EntityId'])]
            df = df[df['Status_Folder_Name'].str.contains('ingest|staging')]
            df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]

            if not df.empty:
                file_slice = df[['Filename','File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component']].copy()
                collected_files_list.append(file_slice)
        
        if metadata_type == "Records":
            df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
            df = df[df['BQ_Hash_Record_ID'].isin(validated_records['BQ_Hash_Record_ID'])]
            df = df[df['Status_Folder_Name'].str.contains('ingest|staging')]
            df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]
            
            if not df.empty:
                record_slice = df[['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_Center', 'Component', 'Status_Folder_Name']].copy()
                collected_records_list.append(record_slice)
        

    if collected_files_list:
        staged_files_df = pd.concat(collected_files_list, ignore_index=True)
    else:
        staged_files_df = pd.DataFrame(columns=['Filename','File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component'])
    
    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release",
            staged_files_df
        )
    
    staged_summary_count_files = staged_files_df.groupby(['Component', 'HTAN_Center', 'Status_Folder_Name']).size().reset_index(name='Number_of_Files')
    
    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release_Counts",
            staged_summary_count_files
        )
    
    
    if collected_records_list:
        staged_records_df = pd.concat(collected_records_list, ignore_index=True)
    else:
        staged_records_df = pd.DataFrame(columns=['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_Center', 'Component', 'Status_Folder_Name'])

    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release",
            staged_records_df
        )
    
    
    staged_summary_count_records = staged_records_df.groupby(['Component', 'HTAN_Center', 'Status_Folder_Name']).size().reset_index(name='Number_Rows_in_RecordSet')
    
    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release_Counts",
            staged_summary_count_records
        )
    
    
    print_sub_section("GENERATING CURRENTLY RELEASED FILES")
    #---------------------------------------------------------------------------------    
    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = [
        table.table_id
        for table in bronze_tables
        if table.table_id.startswith("bronze_METADATA_TABLE_All_")]
    
    released_entities = []
    released_records = []
    
    #For Dropping the validation columns
    validation_columns = ['Validation', 'Error', 'Violations', 'Valid', 'Validated']
    for table_id in bronze_metadata:
        print("Filtering Table To Released Files: "  +  table_id)                
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]
        
        df = None
        
        if metadata_type == "Files":
            df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
            df = df[df['Status_Folder_Name'].str.contains('release')]
            cols_to_drop = [col for col in df.columns if any(val_column in col for val_column in validation_columns)]
            df = df.drop(columns=cols_to_drop)
            df = df[~df['File_EntityId'].isin(exclusion_files['File_EntityId'])]

        
            if not df.empty:
                file_slice = df[['File_EntityId', 'BQ_Hash_ID']].copy()
                released_entities.append(file_slice)
                
        if metadata_type == "Records":
            df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
            df = df[df['Status_Folder_Name'].str.contains('release')]
            cols_to_drop = [col for col in df.columns if any(val_column in col for val_column in validation_columns)]
            df = df.drop(columns=cols_to_drop)
            if 'HTAN_PARTICIPANT_ID' in df.columns:
                df = df[~df['HTAN_PARTICIPANT_ID'].isin(exclusion_files['HTAN_PARTICIPANT_ID'])]
            if 'HTAN_BIOSPECIMEN_ID' in df.columns:
                df = df[~df['HTAN_BIOSPECIMEN_ID'].isin(exclusion_files['HTAN_ORIGINATING_BIOSPECIMEN_ID'])]
                        
            if not df.empty:
                record_slice = df[['Record_EntityId', 'BQ_Hash_Record_ID']].copy()
                released_records.append(record_slice)
        
        if df is not None:
            table_name = f"gold_RELEASED_METADATA_TABLE_All_{metadata_type}_{component}"
            load_bq(
                client,
                PROJECT,
                GOLD_DATASET,
                table_name,
                df
            )
    
    #Final Files DataFrame
    if released_entities:
        current_released_entities = pd.concat(released_entities, ignore_index=True)
    else:
        current_released_entities = pd.DataFrame(columns=['File_EntityId', 'BQ_Hash_ID'])
    
    #Final Records DataFrame
    if released_records:
        current_released_records = pd.concat(released_records, ignore_index=True)
    else:
        current_released_records = pd.DataFrame(columns=['Record_EntityId', 'BQ_Hash_Record_ID'])

    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_RELEASED_INDEXING_TABLE_Released_Entities",
            current_released_entities
        )
    
    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows",
            current_released_records
        )
    
    
    print_sub_section("PULLING BRONZE PROVENANCE TABLE")
    #---------------------------------------------------------------------------------
    bronze_provenance_query = f"""
        SELECT *
        FROM `{PROJECT}.{BRONZE_DATASET}.bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance`
    """

    bronze_prov = client.query(bronze_provenance_query).to_dataframe()
    gold_prov = bronze_prov[bronze_prov['File_EntityId'].isin(current_released_entities['File_EntityId'])]
    
    load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            "gold_RELEASED_INDEXING_TABLE_All_Files_and_Records_ID_Provenance",
            gold_prov
        )

    #---------------------------------------------------------------------------------
    print_sub_section("FETCHING AND UPDATING LATEST DATA MODEL TABLE")

    # Get all data models from BQ
    data_models = list(client.list_tables(f"{PROJECT}.{DM_DATASET}"))
    dm_versions = [
        table.table_id
        for table in data_models
        if table.table_id.startswith("HTAN2_Data_Model_")
    ]

    if dm_versions:

        # Get most recent data model table
        latest_model_table = sorted(dm_versions, reverse=True)[0]
        bq_version = latest_model_table.split("HTAN2_Data_Model_")[-1]
        github_version = bq_version.replace("_", ".")

        # Get table and add schema version
        latest_model = query_bigquery_table(client,
                                            PROJECT,
                                            DM_DATASET,
                                            latest_model_table)
        latest_model["Schema_Version"] = github_version

        # Push data model dictionary to BQ GOLD layer
        load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            'gold_INDEXING_TABLE_Tabular_Data_Model',
            latest_model
        )
        


if __name__ == "__main__":
    main()
