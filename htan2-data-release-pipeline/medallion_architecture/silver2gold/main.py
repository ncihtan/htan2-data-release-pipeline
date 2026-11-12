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
Updated: 11/12/2026
"""
import pandas as pd
from client_load import load_bq, init_bq_client
from google.cloud.exceptions import NotFound
# Import custom modularized help functions
from gold_helpers import (
    print_sub_section,
    query_bigquery_table,
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

# Age cutoffs and column cleaning
MAX_AGE_DAYS = int((90 * 365) - 1)  # AGE OBFUSCATION CUT OFF
VALIDATION_COLUMNS = ['Validation', 'Error', 'Violations', 'Valid', 'Validated']

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
    #Infer release status from record sets
    releases_list = bronze_record_schema[["Status_Folder_Name"]].drop_duplicates()
    bronze_record_schema = bronze_record_schema[["Record_EntityId", "Schema_Version"]].drop_duplicates()

    print_sub_section("PULLING BRONZE PROVENANCE TABLE")
    #---------------------------------------------------------------------------------
    bronze_prov = query_bigquery_table(client, PROJECT, BRONZE_DATASET, "bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance")    

    print_sub_section("FETCH CONFIRMED CENTERS FOR RELEASE")
    #---------------------------------------------------------------------------------
    # Confirmed Centers for Release Table (Skip the header lines in the doc)
    confirmed_center_list = pd.read_csv(CONFIRMED_CENTERS_URL, skiprows=4)
    # Load to BQ
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELASE_INDEXING_TABLE_Confirmed_Centers_in_Data_Releases", confirmed_center_list)
    
    print_sub_section("FETCH RELEASED FILES")
    #---------------------------------------------------------------------------------
    released_entities = query_bigquery_table(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_Released_Entities")
    released_records = query_bigquery_table(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows")

    print_sub_section("ESTABLISH LATEST RELEASE AND PIN READIED EXCLUSION FILES")
    #---------------------------------------------------------------------------------  
    #GET ALL RELEASES
    releases_list['Version_Number'] = pd.to_numeric(releases_list['Status_Folder_Name'].str.extract(r'v(\d+)', expand=False),errors='coerce').astype('Int64')
    #IDENTIFY LATEST RELEASE
    latest_release = releases_list["Version_Number"].max()
    version_list = [f"v{latest_release}_ingest", f"v{latest_release}_staging", f"v{latest_release}_release"]
    #SELECT COLUMNS FOR BQ TABLE
    columns = [
        "HTAN_Center",
        "File_EntityId",
        "File_Name",
        "HTAN_PARTICIPANT_ID",
        "HTAN_BIOSPECIMEN_ID",
        "Release_Version"
    ]
    target_table_name = "raw_INDEXING_TABLE_Exclusion_List_Files_Pinned_Version"
    #CHECK IF THE TABLE EXISTS
    try:
        
        release_tracker_existing = query_bigquery_table(client, PROJECT, RAW_DATASET, target_table_name)
        if not release_tracker_existing.empty and "File_EntityId" in release_tracker_existing.columns:
            release_tracker_existing["File_EntityId"] = release_tracker_existing["File_EntityId"].astype(str).str.strip()
    except NotFound:
        release_tracker_existing = pd.DataFrame(columns=columns)
        load_bq(client, PROJECT, RAW_DATASET, target_table_name, release_tracker_existing, write_disposition="WRITE_APPEND")
    
    #FILTER TO READY FOR RELEASE ONLY
    ready_release = exclusion_files.loc[exclusion_files['Status'] == "READY FOR RELEASE"].copy()
    ready_release = ready_release.rename(columns={"HTAN_ORIGINATING_BIOSPECIMEN_ID": "HTAN_BIOSPECIMEN_ID"})
    
    if not ready_release.empty and "File_EntityId" in ready_release.columns:
        ready_release["File_EntityId"] = ready_release["File_EntityId"].astype(str).str.strip()
        ready_release = ready_release.drop_duplicates(subset=["File_EntityId"])
    
        if not release_tracker_existing.empty and "File_EntityId" in release_tracker_existing.columns:
            existing_ids = set(release_tracker_existing["File_EntityId"].dropna())
            ready_release = ready_release[~ready_release["File_EntityId"].isin(existing_ids)]
    
        if not ready_release.empty:
            matching_cols = [c for c in columns if c in ready_release.columns and c != "Release_Version"]
            release_tracker = ready_release[matching_cols].copy()
    
            for col in columns:
                if col not in release_tracker.columns and col != "Release_Version":
                    release_tracker[col] = None
    
            release_tracker["Release_Version"] = [version_list] * len(release_tracker)
            release_tracker = release_tracker[columns]
    
            load_bq(
                client,
                PROJECT,
                RAW_DATASET,
                target_table_name,
                release_tracker
            )
    
            release_tracker_existing = pd.concat([release_tracker_existing, release_tracker], ignore_index=True)
            
    #ESTABLISH THE LISTS FROM THE RELEASE
    tracker_clean = release_tracker_existing.dropna(subset=["Release_Version"])
    #SUBSET THE SPECIFIC KEYS THAT ARE RELEVANT
    bio_map = (
        tracker_clean.dropna(subset=["HTAN_BIOSPECIMEN_ID"])
        .set_index("HTAN_BIOSPECIMEN_ID")["Release_Version"]
        .to_dict())
    part_map = (
        tracker_clean.dropna(subset=["HTAN_PARTICIPANT_ID"])
        .set_index("HTAN_PARTICIPANT_ID")["Release_Version"]
        .to_dict())
    file_map = (
        tracker_clean.dropna(subset=["File_EntityId"])
        .set_index("File_EntityId")["Release_Version"]
        .to_dict())
    print_sub_section("CREATING STAGING DATASETS FOR RELEASE")
    #---------------------------------------------------------------------------------
    #STAGED PANEL INFORMATION
    panel_records_list = []
    # SETUP TO LOOP THROUGH ALL BRONZE TABLES
    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = [table.table_id for table in bronze_tables if table.table_id.startswith("bronze_METADATA_TABLE_All")]
    collected_files_list = []
    collected_records_list = []
    #LOOP THROUGH ALL BRONZE TABLES TO INDEX BEFORE CREATING STAGING
    for table_id in bronze_metadata:
        print("Fetching: " + table_id)               
        metadata_type = table_id.split("_")[4]
        #QUERY FOR BRONZE TABLE
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
        #FILE TABLES
        if metadata_type == "Files":
            if not df.empty:
                #FILTER TO STAGING OR INGEST ONLY AND ALLOW OLDER FILES THAT ARE READIED FOR RELASE ON THE EXCLUSION LIST
                df = df[df['Status_Folder_Name'].str.contains('staging|ingest', na=False) | df['File_EntityId'].isin(file_map)]
                #FILTER OUT FILES THAT ARE ON THE EXCLUSION LIST
                df = df[~df['File_EntityId'].isin(exclusion_files['File_EntityId'])]
                #FILTER TO VALIDATED FILES (BYPASS LIST IN SILVER); FILES ARE CONSIDERED VALIDATED IF THEY ARE READIED ON THE EXCLUSION LIST            
                df = df[df['File_EntityId'].isin(validated_files['File_EntityId']) | df['File_EntityId'].isin(file_map)]
                #ALLOW ONLY CONFIRMED CENTER
                df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]
                #REMOVE RELEASED FILES
                df = df[~df['File_EntityId'].isin(released_entities['File_EntityId'])]
                #ADD ADDITIONAL SCHEMA INFORMATION
                df = pd.merge(df, bronze_file_schema, on="File_EntityId")
                #SAVE PANEL INFORMATION FROM STAGED FILES TO FILTER RECORDS
                if 'HTAN_PANEL_ID' in df.columns:
                    cols_to_extract = [col for col in ['HTAN_PANEL_ID', 'Component', 'File_EntityId'] if col in df.columns]
                    panel_records_list.append(df[cols_to_extract].copy())
                #SLICE AND COLLECT FILES AFTER FILTERING
                file_slice = df[['File_Name', 'File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component', 'Schema_Version', 'HTAN_DATA_FILE_ID'] + (['HTAN_PANEL_ID'] if 'HTAN_PANEL_ID' in df.columns else [])].copy()
                if 'HTAN_PANEL_ID' not in file_slice.columns:
                    file_slice['HTAN_PANEL_ID'] = None
                collected_files_list.append(file_slice)
        ############
        elif metadata_type == "Records":
                    if not df.empty:
                        #ALLOW BIOSPECIMEN AND PARTICIPANTS RESIDING ON THE EXCLUSION LIST PROVIDED THEY HAVE A CARRIED OVER, UPDATED RECORD IN THE CURRENT RELEASE
                        exclusion_mask = pd.Series(False, index=df.index)
                        #BIOSPECIMEN CHECK
                        if 'HTAN_BIOSPECIMEN_ID' in df.columns:
                            exclusion_mask = exclusion_mask | pd.Series(
                                [pd.notna(s) and str(s) in str(bio_map.get(b, '')) for b, s in zip(df['HTAN_BIOSPECIMEN_ID'], df['Status_Folder_Name'])],
                                index=df.index)
                        #PARTICIPANT CHECK
                        if 'HTAN_PARTICIPANT_ID' in df.columns:
                            exclusion_mask = exclusion_mask | pd.Series(
                                [pd.notna(s) and str(s) in str(part_map.get(p, '')) for p, s in zip(df['HTAN_PARTICIPANT_ID'], df['Status_Folder_Name'])],
                                index=df.index)
                        #FILTER TO STATUS ONLY INGEST AND STAGING (OR ALLOWED EXCLUSIONS)
                        status_keep_mask = df['Status_Folder_Name'].str.contains('ingest|staging', na=False) | exclusion_mask
                        df = df[status_keep_mask]
                        exclusion_mask = exclusion_mask[status_keep_mask]
                        #FILTER TO ONLY VALIDATED RECORDS AND HTAN_PANEL_ID (BYPASS LIST IN SILVER), OR ALLOWED EXCLUSIONS (THIS IS BECAUSE OLDER; NOW RELEASED ENTITIES NEED TO BE ALLOWED BECAUSE THEY ARE NOT IN THE VALIDATION LIST)
                        if 'HTAN_PANEL_ID' in df.columns and 'HTAN_PANEL_ID' in validated_records.columns:
                            valid_record_mask = df['HTAN_PANEL_ID'].isin(validated_records['HTAN_PANEL_ID'].dropna())
                        else:
                            valid_record_mask = df['BQ_Hash_Record_ID'].isin(validated_records['BQ_Hash_Record_ID'])
                        df = df[valid_record_mask | exclusion_mask]
                        # ALLOW ONLY CONFIRMED CENTER
                        df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]
                        # REMOVE RELEASED ROWS 
                        df = df[~df['BQ_Hash_Record_ID'].isin(released_records['BQ_Hash_Record_ID'])]
                        # ADD ADDITIONAL SCHEMA INFORMATION
                        df = pd.merge(df, bronze_record_schema, on="Record_EntityId")
                        # FILTER TO LATEST RELEASE
                        df = df[df['Status_Folder_Name'].str.contains(f'v{latest_release}', na=False)]
                        # SLICE AND COLLECT RECORDS AFTER FILTERING
                        target_cols = ['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_Center', 'Component', 'Status_Folder_Name', 'Schema_Version']
                        optional_cols = ['HTAN_PARTICIPANT_ID', 'HTAN_BIOSPECIMEN_ID', 'HTAN_PANEL_ID']
                        cols_to_keep = target_cols + [col for col in optional_cols if col in df.columns]
                        record_slice = df[cols_to_keep].copy()
                        collected_records_list.append(record_slice)
    
    #AFTER FILTERING AND COLLATING THE ABOVE, STAGE THE INFORMATION:
    #STAGE RELEASE FILES:
    if collected_files_list:
        staged_files_df = pd.concat(collected_files_list, ignore_index=True)
    else:
        staged_files_df = pd.DataFrame(columns=['File_Name','File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component'])
    #GET ALL THE PANEL INFORMATION FROM THE STAGED FILES IF THEY HAVE A PANEL ID:
    panel_df = pd.concat(panel_records_list, ignore_index=True) if panel_records_list else pd.DataFrame(columns=['HTAN_PANEL_ID', 'Component', 'File_EntityId'])
    #LOAD THE FILE TABLE INTO BQ (STAGING)
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release", staged_files_df)
    #LOAD THE COUNT FILE TABLE INTO BQ (STAGING)
    staged_summary_count_files = staged_files_df.groupby(['Component', 'HTAN_Center', 'Status_Folder_Name']).size().reset_index(name='Number_of_Files')
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release_Counts", staged_summary_count_files)
    #STAGE RECORDS
    #FILTER RECORDS BY WHAT IS AVAILABLE IN THE STAGED FILES - WE DO NOT WANT ANY RECORDS UNASSOCIATED WITH FILES
    filter_prov = bronze_prov[bronze_prov['File_EntityId'].isin(staged_files_df['File_EntityId'])]
    filter_prov = filter_prov.rename(columns={"HTAN_ORIGINATING_BIOSPECIMEN_ID": "HTAN_BIOSPECIMEN_ID"})
    #SUBSET VALID PARTICIPANTS
    valid_parts = set(filter_prov['HTAN_PARTICIPANT_ID'].dropna()) if 'HTAN_PARTICIPANT_ID' in filter_prov.columns else set()
    #SUBSET VALID BIOSPECIMEN
    valid_bios = set(filter_prov['HTAN_BIOSPECIMEN_ID'].dropna()) if 'HTAN_BIOSPECIMEN_ID' in filter_prov.columns else set()
    #FILTER TO ONLY THOSE ASSOCIATED WITH FILES
    for i, df in enumerate(collected_records_list):
        has_part_col = 'HTAN_PARTICIPANT_ID' in df.columns
        has_bio_col = 'HTAN_BIOSPECIMEN_ID' in df.columns
        if has_part_col and has_bio_col:
            # KEEP IF PARTICIPANT OR BIOSPECIMEN MATCH
            match = df['HTAN_PARTICIPANT_ID'].isin(valid_parts) | df['HTAN_BIOSPECIMEN_ID'].isin(valid_bios)
            collected_records_list[i] = df[match]
        elif has_part_col:
            collected_records_list[i] = df[df['HTAN_PARTICIPANT_ID'].isin(valid_parts)]
        elif has_bio_col:
            collected_records_list[i] = df[df['HTAN_BIOSPECIMEN_ID'].isin(valid_bios)]
    #COLLECT RECORDS AND STAGE THEM IN THE STAGING SET
    if collected_records_list:
        staged_records_df = pd.concat(collected_records_list, ignore_index=True)
    else:
        staged_records_df = pd.DataFrame(columns=['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_Center', 'Component', 'Status_Folder_Name'])
    #FILTER PANELS TO ONLY FILES THAT ARE MATCHED IN THE FILE SECTION ABOVE
    staged_records_df = staged_records_df[
        staged_records_df['HTAN_PANEL_ID'].isna() | 
        staged_records_df['HTAN_PANEL_ID'].isin(panel_df['HTAN_PANEL_ID'])]    
    #LOAD THE RECORDS INTO BQ
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release", staged_records_df)
    staged_summary_count_records = staged_records_df.groupby(['Component', 'HTAN_Center', 'Status_Folder_Name']).size().reset_index(name='Number_Rows_in_RecordSet')
    load_bq(client, PROJECT, GOLD_DATASET, "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release_Counts", staged_summary_count_records)
    

    print_sub_section("GENERATING CURRENTLY RELEASED FILES")
    #---------------------------------------------------------------------------------    
    bronze_metadata_release = [t for t in bronze_metadata if t.startswith("bronze_METADATA_TABLE_All_")]
    #ESTABLISH CONTAINERS FOR ENTITIES
    released_entities = []
    released_records = []
    panel_records_list = []
    rel_prov = pd.DataFrame()
    #LOOP THROUGH ALL THE BRONZE TABLES TO GENERATE THE GOLD TABLES BASED ON INDEXING
    for table_id in bronze_metadata_release:
        print("Filtering Table To Released Files: " + table_id)                
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]
        #FETCH THE RELEVANT TABLE IN THE LOOP
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
        # FILE METADATA PORTION
        if metadata_type == "Files":
            if not df.empty:
                #MATCH THE FILES TO READIED FILES 
                matches_file_map = pd.Series(
                    [pd.notna(s) and f"'{s}'" in str(file_map.get(f, '')) for f, s in zip(df['File_EntityId'], df['Status_Folder_Name'])],index=df.index)
                #CHECK IF THE FILES IS IN RELEASED FOLDERS AND IF NOT THAT IT MATCHES THE REDIED FILES AND RELEASE NUMBER
                df = df[df['Status_Folder_Name'].str.contains('release', na=False) | matches_file_map]
                #FILTER OUT FILES THAT ARE ON THE EXCLUSION LIST (AND EXCLUDE FILE_MAP ENTRIES THAT DON'T MATCH STATUS)
                not_excluded_file = ~df['File_EntityId'].isin(exclusion_files['File_EntityId'].dropna())
                not_in_file_map = ~df['File_EntityId'].isin(file_map)
                matches_file_map = matches_file_map.loc[df.index]
                df = df[(not_excluded_file & not_in_file_map) | matches_file_map]
                #ALLOW ONLY CONFIRMED CENTER
                df = df[df['HTAN_Center'].isin(confirmed_center_list['HTAN_Center'])]
                #ADD ADDITIONAL SCHEMA INFORMATION
                df = pd.merge(df, bronze_file_schema, on="File_EntityId")
                #SAVE PANEL INFORMATION FROM STAGED FILES TO FILTER RECORDS
                if 'HTAN_PANEL_ID' in df.columns:
                    cols_to_extract = [col for col in ['HTAN_PANEL_ID', 'Component', 'File_EntityId'] if col in df.columns]
                    panel_records_list.append(df[cols_to_extract].copy())
                #SLICE AND COLLECT FILES AFTER FILTERING
                file_slice = df[['File_Name', 'File_EntityId', 'HTAN_Center', 'Status_Folder_Name', 'BQ_Hash_ID', 'Component', 'Schema_Version', 'HTAN_DATA_FILE_ID'] + (['HTAN_PANEL_ID'] if 'HTAN_PANEL_ID' in df.columns else [])].copy()
                if 'HTAN_PANEL_ID' not in file_slice.columns:
                    file_slice['HTAN_PANEL_ID'] = None
                released_entities.append(file_slice)  
                #FILTER RECORDS BY WHAT IS AVAILABLE IN THE RELEASED FILES
                released_entities_df = pd.concat(released_entities, ignore_index=True)
                curr_rel_prov = bronze_prov[bronze_prov['File_EntityId'].isin(released_entities_df['File_EntityId'])]
                curr_rel_prov = curr_rel_prov.rename(columns={"HTAN_ORIGINATING_BIOSPECIMEN_ID": "HTAN_BIOSPECIMEN_ID"})
                rel_prov = pd.concat([rel_prov, curr_rel_prov], ignore_index=True).drop_duplicates()
        #RECORD METADATA PORTION
        elif metadata_type == "Records":
            if not df.empty:
                #ADD SOME ADDITIONAL INFO THAT WASN'T PRESENT BEFORE
                df = pd.merge(df, bronze_record_schema, on="Record_EntityId")
                #DROP ANY COLUMNS THAT REFER TO VALIDATION
                cols_to_drop = [col for col in df.columns if any(val_column in col for val_column in VALIDATION_COLUMNS)]
                df = df.drop(columns=cols_to_drop)
                #ALLOW BIOSPECIMEN AND PARTICIPANTS RESIDING ON THE EXCLUSION LIST PROVIDED THEY HAVE A CARRIED OVER, UPDATED RECORD IN THE CURRENT RELEASE
                #PARTICIPANT
                matches_part_map = pd.Series(
                    [pd.notna(s) and f"'{s}'" in str(part_map.get(p, '')) for p, s in zip(df['HTAN_PARTICIPANT_ID'], df['Status_Folder_Name'])],
                    index=df.index) if 'HTAN_PARTICIPANT_ID' in df.columns else pd.Series(False, index=df.index)
                #BIOSPECIMEN
                matches_bio_map = pd.Series(
                    [pd.notna(s) and f"'{s}'" in str(bio_map.get(b, '')) for b, s in zip(df['HTAN_BIOSPECIMEN_ID'], df['Status_Folder_Name'])],
                    index=df.index) if 'HTAN_BIOSPECIMEN_ID' in df.columns else pd.Series(False, index=df.index)
                #STATUS RELEASE OR MATCHES SOMETHING FROM THE READIED LIST
                df = df[df['Status_Folder_Name'].str.contains('release', na=False) | matches_part_map | matches_bio_map]
                #APPLY TO TABLES WITH INDEXED COLUMNS
                if 'HTAN_PARTICIPANT_ID' in df.columns:
                    not_excluded_part = ~df['HTAN_PARTICIPANT_ID'].isin(exclusion_files['HTAN_PARTICIPANT_ID'].dropna())
                    not_in_part_map = ~df['HTAN_PARTICIPANT_ID'].isin(part_map)
                    df = df[(not_excluded_part & not_in_part_map) | matches_part_map.loc[df.index]]
                if 'HTAN_BIOSPECIMEN_ID' in df.columns:
                    not_excluded_bio = ~df['HTAN_BIOSPECIMEN_ID'].isin(exclusion_files['HTAN_ORIGINATING_BIOSPECIMEN_ID'].dropna())
                    not_in_bio_map = ~df['HTAN_BIOSPECIMEN_ID'].isin(bio_map)
                    df = df[(not_excluded_bio & not_in_bio_map) | matches_bio_map.loc[df.index]]
                #CHECK THAT THE PANEL ID IS ASSOCIATED WITH THE FILES
                if 'HTAN_PANEL_ID' in df.columns:
                    valid_panel_ids = (
                        pd.concat(panel_records_list, ignore_index=True)['HTAN_PANEL_ID'].dropna()
                        if panel_records_list else pd.Series(dtype=object))
                    df = df[df['HTAN_PANEL_ID'].isin(valid_panel_ids)]              
                #SUBSET VALID PARTICIPANTS
                valid_parts = set(rel_prov['HTAN_PARTICIPANT_ID'].dropna()) if 'HTAN_PARTICIPANT_ID' in rel_prov.columns else set()
                #SUBSET VALID BIOSPECIMEN
                valid_bios = set(rel_prov['HTAN_BIOSPECIMEN_ID'].dropna()) if 'HTAN_BIOSPECIMEN_ID' in rel_prov.columns else set()
                #FILTER TO ONLY THOSE BIOSPECIMEN AND PARTICIPANTS ASSOCIATED WITH FILES
                has_part_col = 'HTAN_PARTICIPANT_ID' in df.columns
                has_bio_col = 'HTAN_BIOSPECIMEN_ID' in df.columns
                if has_part_col and has_bio_col:
                    # KEEP IF PARTICIPANT OR BIOSPECIMEN MATCH
                    match = df['HTAN_PARTICIPANT_ID'].isin(valid_parts) | df['HTAN_BIOSPECIMEN_ID'].isin(valid_bios)
                    df = df[match]
                elif has_part_col:
                    df = df[df['HTAN_PARTICIPANT_ID'].isin(valid_parts)]
                elif has_bio_col:
                    df = df[df['HTAN_BIOSPECIMEN_ID'].isin(valid_bios)]
                #ADD RELEASE TAG - Extract the integer digit(s) following 'v' and before '_release' and format for release column
                extracted_version = df["Status_Folder_Name"].str.extract(r"v(\d+)_release", expand=False)
                df["Data_Release"] = "Release " + extracted_version + ".0"
                df["CRDC_Release"] = None # TEMPORARY CRDC TAG
                # MASK AGES AND DERIVE AGES IN MONTHS (HELPER FUNCTION)
                df = apply_age_masking(df, client, PROJECT, SILVER_DATASET, table_id, MAX_AGE_DAYS)
                df = derive_age_in_months(df)
                #TAKE A SLICE OF THE CURRENT DF FOR THE RELEASED RECORDS
                target_cols = ['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_PARTICIPANT_ID', 'HTAN_BIOSPECIMEN_ID', "HTAN_PANEL_ID"]
                record_slice = df.reindex(columns=target_cols).copy()
                released_records.append(record_slice)
                #PUSH THE TABLE TO THE GOLD LAYER     
                table_name = f"gold_RELEASED_METADATA_TABLE_All_{metadata_type}_{component}"
                load_bq(client, PROJECT, GOLD_DATASET, table_name, df)
    #TRACKING TABLES FOR FILES
    if released_entities:
        current_released_entities = pd.concat(released_entities, ignore_index=True)
    else:
        current_released_entities = pd.DataFrame(columns=['File_EntityId', 'BQ_Hash_ID'])
    load_bq(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_Released_Entities", current_released_entities)
    #TRACKING TABLES FOR RECORDS
    if released_records:
        current_released_records = pd.concat(released_records, ignore_index=True)
    else:
        current_released_records = pd.DataFrame(columns=['Record_EntityId', 'BQ_Hash_Record_ID', 'HTAN_PARTICIPANT_ID', 'HTAN_BIOSPECIMEN_ID', 'HTAN_PANEL_ID'])
    load_bq(client, PROJECT, GOLD_DATASET, "gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows", current_released_records)

    print_sub_section("CONSTRUCT GOLD PROVENANCE TABLE")
    #---------------------------------------------------------------------------------    
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
