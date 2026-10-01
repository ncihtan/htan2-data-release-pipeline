#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Silver to Gold
    The module is responsible for the SILVER to GOLD transition of
    the medallion architecture. It applies filtering logic on 
    all HTAN2 metadata, and generates release candidate 
    tables for both Files and Record Sets. 

Configurations: None
    
Author: Dar'ya Pozhidayeva, Yamina Katariya
Updated: 09/30/2026
"""
import pandas as pd
from client_load import load_bq, init_bq_client

# Import custom modularized help functions
from gold_helpers import (
    print_sub_section,
    query_bigquery_table,
    process_excluded_panels,
    apply_age_masking,
    derive_age_in_months
)

#####################################################
#             GLOBAL CONFIGURATIONS
#####################################################

# BigQuery Project & Dataset Identifiers
PROJECT = "htan2-dcc"
RAW_DATASET = "htan2_synapse_raw"
BRONZE_DATASET = "htan2_medallion_bronze"
SILVER_DATASET = "htan2_medallion_silver"
GOLD_DATASET = "htan2_medallion_gold"
DM_DATASET = "htan2_data_model_cache"

# External Data Sources
CONFIRMED_CENTERS_URL = "https://docs.google.com/spreadsheets/d/1wQ5XZ9uYtAzKKe3cANam8UBdOofuFaAEod-AWDdWK8Q/export?format=csv&gid=0"

# Business & Filtering Logic Constants
MAX_AGE_DAYS = int((90 * 365) - 1)  # AGE OBFUSCATION CUT OFF
VALIDATION_COLUMNS = ['Validation', 'Error', 'Violations', 'Valid', 'Validated']  # For dropping validation columns

#####################################################
#                   MAIN 
#####################################################
def main():
    """
    Entry point into the GOLD layer.
    """
    # Initialize BQ Client
    client = init_bq_client()
    
    print_sub_section("PULLING THE EXCLUSION LIST FOR POST-RELEASE EXCLUSIONS")
    #---------------------------------------------------------------------------------
    exclusion_files = query_bigquery_table(client, PROJECT, RAW_DATASET, "raw_INDEXING_TABLE_Exclusion_List_Form_Results")
    exclusion_files = exclusion_files.loc[exclusion_files['Status'] == "EXCLUDE"]
    
    print_sub_section("PULLING FILE VALIDATION RESULTS IN SILVER LAYER")
    #---------------------------------------------------------------------------------
    validated_files = query_bigquery_table(client, PROJECT, SILVER_DATASET, "silver_INDEXING_TABLE_All_Files_Passed_Validation")
    validated_records = query_bigquery_table(client, PROJECT, SILVER_DATASET, "silver_INDEXING_TABLE_All_Records_Passed_Validation")
    
    print_sub_section("PULLING SCHEMA INFORMATION FROM BRONZE")
    #---------------------------------------------------------------------------------
    bronze_file_schema = query_bigquery_table(client, PROJECT, BRONZE_DATASET, "bronze_INDEXING_TABLE_All_Files_With_Schema_Information")
    bronze_file_schema = bronze_file_schema[["File_EntityId", "Schema_Version"]].drop_duplicates()
    
    bronze_record_schema = query_bigquery_table(client, PROJECT, BRONZE_DATASET, "bronze_INDEXING_TABLE_All_RecordSets_With_Schema_Information")
    bronze_record_schema = bronze_record_schema[["Record_EntityId", "Schema_Version"]].drop_duplicates()

    print_sub_section("CREATING STAGING DATASETS FOR RELEASE")
    #---------------------------------------------------------------------------------
    # Confirmed Centers for Release Table (Skip the header lines in the doc)
    confirmed_center_list = pd.read_csv(CONFIRMED_CENTERS_URL, skiprows=4)
    
    # Load to BQ
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELASE_INDEXING_TABLE_Confirmed_Centers_in_Data_Releases", confirmed_center_list)

    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = [
        table.table_id for table in bronze_tables
        if table.table_id.startswith("bronze_METADATA_TABLE_All")
    ]
    
    collected_files_list = []
    collected_records_list = []
    
    for table_id in bronze_metadata:
        print(table_id)                
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]
        
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
        
        if metadata_type == "Files":
            # File entityid remains the same regardless of location
            df = df[df['File_EntityId'].isin(validated_files['File_EntityId'])]
            df = df[df['Status_Folder_Name'].str.contains('ingest|staging')]
            df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]

            if not df.empty:
                df = pd.merge(df, bronze_file_schema, on="File_EntityId")
                file_slice = df[['Filename','File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component', 'Schema_Version']].copy()
                collected_files_list.append(file_slice)
        
        elif metadata_type == "Records":
            df = df[df['BQ_Hash_Record_ID'].isin(validated_records['BQ_Hash_Record_ID'])]
            df = df[df['Status_Folder_Name'].str.contains('ingest|staging')]
            df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]
            
            if not df.empty:
                df = pd.merge(df, bronze_record_schema, on="Record_EntityId")
                record_slice = df[['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_Center', 'Component', 'Status_Folder_Name', 'Schema_Version']].copy()
                collected_records_list.append(record_slice)
        
    if collected_files_list:
        staged_files_df = pd.concat(collected_files_list, ignore_index=True)
    else:
        staged_files_df = pd.DataFrame(columns=['Filename','File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component'])
    
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release", staged_files_df)
    staged_summary_count_files = staged_files_df.groupby(['Component', 'HTAN_Center', 'Status_Folder_Name']).size().reset_index(name='Number_of_Files')
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release_Counts", staged_summary_count_files)
    
    if collected_records_list:
        staged_records_df = pd.concat(collected_records_list, ignore_index=True)
    else:
        staged_records_df = pd.DataFrame(columns=['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_Center', 'Component', 'Status_Folder_Name'])

    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release", staged_records_df)
    staged_summary_count_records = staged_records_df.groupby(['Component', 'HTAN_Center', 'Status_Folder_Name']).size().reset_index(name='Number_Rows_in_RecordSet')
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release_Counts", staged_summary_count_records)
    
    
    print_sub_section("GENERATING CURRENTLY RELEASED FILES")
    #---------------------------------------------------------------------------------    
    bronze_metadata_release = [t for t in bronze_metadata if t.startswith("bronze_METADATA_TABLE_All_")]
    
    released_entities = []
    released_records = []
    removed_files_exclude_list = []
    
    mm_panel_check_df = pd.DataFrame()
    spatial_panel_check_df = pd.DataFrame()
    mm_expected_panel_counts = pd.DataFrame()
    spatial_expected_panel_counts = pd.DataFrame()
    molecular_panel_check = pd.DataFrame(columns=['HTAN_DATA_FILE_ID'])
    
    for table_id in bronze_metadata_release:
        print("Filtering Table To Released Files: " + table_id)                
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]
        
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
        df = df[df['Status_Folder_Name'].str.contains('release')]
        
        # FILE METADATA PORTION
        if metadata_type == "Files":
            df = pd.merge(df, bronze_file_schema, on="File_EntityId")
            cols_to_drop = [col for col in df.columns if any(val_column in col for val_column in VALIDATION_COLUMNS)]
            df = df.drop(columns=cols_to_drop)
            
            if "MultiplexMicroscopyLevel2" in table_id:
                mm_expected_panel_counts = (df.groupby("HTAN_PANEL_ID")["File_EntityId"].nunique().reset_index(name="File_Count"))
            
            if "SpatialLevel3" in table_id:
                spatial_expected_panel_counts = (df.groupby("HTAN_PANEL_ID")["File_EntityId"].nunique().reset_index(name="File_Count"))
            
            # Save excluding files before filtering them out below.
            if not df.empty:
                remove_files = df[df['File_EntityId'].isin(exclusion_files['File_EntityId'])].copy()
                removed_files_exclude_list.append(remove_files)
            
            # Filter out files in the exclusion list (post-release)
            df = df[~df['File_EntityId'].isin(exclusion_files['File_EntityId'])]
    
            # For removal of files from the exclusion list-fetch the panel IDs here.
            for removal_df in removed_files_exclude_list:
                if removal_df.empty or 'Component' not in removal_df.columns:
                    continue
                    
                component_name = removal_df['Component'].iloc[0]
                
                if component_name == "MultiplexMicroscopyLevel2" and not mm_expected_panel_counts.empty:
                    mm_panel_check_df = process_excluded_panels(removal_df, mm_expected_panel_counts)
                    
                elif component_name == "SpatialLevel3" and not spatial_expected_panel_counts.empty:
                    spatial_panel_check_df = process_excluded_panels(removal_df, spatial_expected_panel_counts)
                    
                elif component_name == "MolecularAssignment":
                    if 'HTAN_DATA_FILE_ID' in removal_df.columns:
                        new_ids = removal_df[['HTAN_DATA_FILE_ID']].dropna().drop_duplicates()
                        molecular_panel_check = pd.concat([molecular_panel_check, new_ids], ignore_index=True).drop_duplicates()
                
            if not df.empty:
                file_slice = df[['File_EntityId', 'BQ_Hash_ID']].copy()
                released_entities.append(file_slice)
    
        # RECORD METADATA PORTION
        elif metadata_type == "Records":
            df = pd.merge(df, bronze_record_schema, on="Record_EntityId")
            cols_to_drop = [col for col in df.columns if any(val_column in col for val_column in VALIDATION_COLUMNS)]
            df = df.drop(columns=cols_to_drop)

            if 'HTAN_PARTICIPANT_ID' in df.columns:
                df = df[~df['HTAN_PARTICIPANT_ID'].isin(exclusion_files['HTAN_PARTICIPANT_ID'])]
            if 'HTAN_BIOSPECIMEN_ID' in df.columns:
                df = df[~df['HTAN_BIOSPECIMEN_ID'].isin(exclusion_files['HTAN_ORIGINATING_BIOSPECIMEN_ID'])]
            
            # Filter panels from record sets if they belong to excluded files.    
            if "ChannelMetadata" in table_id and not mm_panel_check_df.empty:
                remove_panels = mm_panel_check_df.loc[mm_panel_check_df['Fully_Excluded'] == True]
                df = df[~df['HTAN_PANEL_ID'].isin(remove_panels['HTAN_PANEL_ID'])]
            if "SpatialLevel3" in table_id and not spatial_panel_check_df.empty:
                remove_panels = spatial_panel_check_df.loc[spatial_panel_check_df['Fully_Excluded'] == True]
                df = df[~df['HTAN_PANEL_ID'].isin(remove_panels['HTAN_PANEL_ID'])]
            if "MolecularAssignment" in table_id:
                df = df[~df['HTAN_DATA_FILE_ID'].isin(molecular_panel_check['HTAN_DATA_FILE_ID'])]
            
            if not df.empty:
                record_slice = df[['Record_EntityId', 'BQ_Hash_Record_ID']].copy()
                released_records.append(record_slice)
                
        # Common Tags & Derived fields application
        if not df.empty:
            # ADD RELEASE TAG - Extract the integer digit(s) following 'v' and before '_release' and format for release column
            extracted_version = df["Status_Folder_Name"].str.extract(r"v(\d+)_release", expand=False)
            df["Data_Release"] = "Release " + extracted_version + ".0"
            df["CRDC_Release"] = None # TEMPORARY CRDC TAG
            
            # Mask ages & derive age in months logic (imported from gold_helpers)
            df = apply_age_masking(df, client, PROJECT, SILVER_DATASET, table_id, MAX_AGE_DAYS)
            df = derive_age_in_months(df)
            
            # Push tables to BQ for Gold Layer.        
            table_name = f"gold_RELEASED_METADATA_TABLE_All_{metadata_type}_{component}"
            load_bq(client, PROJECT, GOLD_DATASET, table_name, df)
    
    # Final Files DataFrame
    if released_entities:
        current_released_entities = pd.concat(released_entities, ignore_index=True)
    else:
        current_released_entities = pd.DataFrame(columns=['File_EntityId', 'BQ_Hash_ID'])
    
    # Final Records DataFrame
    if released_records:
        current_released_records = pd.concat(released_records, ignore_index=True)
    else:
        current_released_records = pd.DataFrame(columns=['Record_EntityId', 'BQ_Hash_Record_ID'])

    load_bq(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_Released_Entities", current_released_entities)
    load_bq(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows", current_released_records)

    print_sub_section("PULLING BRONZE PROVENANCE TABLE")
    #---------------------------------------------------------------------------------
    bronze_prov = query_bigquery_table(client, PROJECT, BRONZE_DATASET, "bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance")
    
    gold_prov = bronze_prov[bronze_prov['File_EntityId'].isin(current_released_entities['File_EntityId'])]
    gold_prov = pd.merge(gold_prov, bronze_file_schema, on="File_EntityId")
    idp_extracted_version = gold_prov["Status_Folder_Name"].str.extract(r"v(\d+)_release", expand=False)
    gold_prov["Data_Release"] = "Release " + idp_extracted_version + ".0"
    
    # TEMPORARY ADDITION
    gold_prov["CRDC_Release"] = None

    load_bq(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_All_Files_and_Records_ID_Provenance", gold_prov)

    #---------------------------------------------------------------------------------
    print_sub_section("FETCHING AND UPDATING LATEST DATA MODEL TABLE")

    # Get all data models from BQ
    data_models = list(client.list_tables(f"{PROJECT}.{DM_DATASET}"))
    dm_versions = [
        table.table_id for table in data_models
        if table.table_id.startswith("HTAN2_Data_Model_")
    ]

    if dm_versions:
        # Get most recent data model table
        latest_model_table = sorted(dm_versions, reverse=True)[0]
        bq_version = latest_model_table.split("HTAN2_Data_Model_")[-1]
        github_version = bq_version.replace("_", ".")

        # Get table and add schema version
        latest_model = query_bigquery_table(client, PROJECT, DM_DATASET, latest_model_table)
        latest_model["Schema_Version"] = github_version

        # Push data model dictionary to BQ GOLD layer
        load_bq(client, PROJECT, GOLD_DATASET, 'gold_INDEXING_TABLE_Tabular_Data_Model', latest_model)

if __name__ == "__main__":
    main()