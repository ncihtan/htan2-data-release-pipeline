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
Updated: 11/13/2026
"""
import pandas as pd
from client_load import load_bq, init_bq_client

# Import help functions
from gold_helpers import (
    
    print_sub_section,
    query_bigquery_table,
    fetch_table,
    sync_pinned_release_versions,
    apply_age_masking,
    derive_age_in_months,
    match_status_in_map,
    extract_panel_and_file_slice,
    get_valid_provenance_ids,
    filter_records_by_provenance,
    concat_or_empty,
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

#RELEVANT TABLES
TABLE_IDS = [
"raw_INDEXING_TABLE_Exclusion_List_Form_Results",
"silver_INDEXING_TABLE_All_Files_Passed_Validation",
"silver_INDEXING_TABLE_All_Records_Passed_Validation",
"bronze_INDEXING_TABLE_All_Files_With_Schema_Information",
"bronze_INDEXING_TABLE_All_RecordSets_With_Schema_Information",
"bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance",
"gold_RELEASED_INDEXING_TABLE_Released_Entities",
"gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows"]

# External Data Sources
CONFIRMED_CENTERS_URL = "https://docs.google.com/spreadsheets/d/1wQ5XZ9uYtAzKKe3cANam8UBdOofuFaAEod-AWDdWK8Q/export?format=csv&gid=0"

# Business & Filtering Logic Constants
MAX_AGE_DAYS = int((90 * 365) - 1)  # AGE OBFUSCATION CUT OFF
VALIDATION_COLUMNS = ['Validation', 'Error', 'Violations', 'Valid', 'Validated'] #Remove Validation Columns

#####################################################
#                   MAIN 
#####################################################
def main():
    """
    Entry point into the GOLD layer.
    """
    # Initialize BQ Client
    client = init_bq_client()
    
    print_sub_section("PULLING RELEVANT TABLES (SEE TABLE IDS)")
    #---------------------------------------------------------------------------------
    tables = {
    table_id: fetch_table(
        client,
        PROJECT,
        RAW_DATASET,
        BRONZE_DATASET,
        SILVER_DATASET,
        GOLD_DATASET,
        table_id,
        dm_dataset=DM_DATASET,)
    for table_id in TABLE_IDS}
    
    print_sub_section("PROCESS AND FILTER TABLES AFTER PULLING")
    #---------------------------------------------------------------------------------
    #Tables for Exclusion
    exclusion_files = tables["raw_INDEXING_TABLE_Exclusion_List_Form_Results"].loc[lambda df: df["Status"] == "EXCLUDE"]
    ready_files = tables["raw_INDEXING_TABLE_Exclusion_List_Form_Results"].loc[lambda df: df["Status"] == "READY FOR RELEASE"]
    #Validation information from SILVER
    validated_files = tables["silver_INDEXING_TABLE_All_Files_Passed_Validation"]
    validated_records = tables["silver_INDEXING_TABLE_All_Records_Passed_Validation"]
    #Additional information from Bronze
    bronze_file_schema = tables["bronze_INDEXING_TABLE_All_Files_With_Schema_Information"][["File_EntityId", "Schema_Version"]].drop_duplicates()
    #Information from Raw
    raw_record_schema = tables["bronze_INDEXING_TABLE_All_RecordSets_With_Schema_Information"]
    releases_list = raw_record_schema[["Status_Folder_Name"]].drop_duplicates()
    bronze_record_schema = raw_record_schema[["Record_EntityId", "Schema_Version"]].drop_duplicates()
    #Bronze ID Provenance
    bronze_prov = tables["bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance"]
    #Existing released tables
    released_entities = tables["gold_RELEASED_INDEXING_TABLE_Released_Entities"]
    released_records = tables["gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows"]
    
    print_sub_section("FETCH CONFIRMED CENTERS FOR RELEASE")
    #---------------------------------------------------------------------------------
    # Confirmed Centers for Release Table (Skip the header lines in the doc)
    confirmed_center_list = pd.read_csv(CONFIRMED_CENTERS_URL, skiprows=4)
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_STAGING_FOR_RELASE_INDEXING_TABLE_Confirmed_Centers_in_Data_Releases",
        confirmed_center_list)

    print_sub_section("ESTABLISH LATEST RELEASE AND PIN READIED EXCLUSION FILES")
    #---------------------------------------------------------------------------------  
    latest_release, bio_map, part_map, file_map = sync_pinned_release_versions(
        client=client,
        project=PROJECT,
        raw_dataset=RAW_DATASET,
        releases_list=releases_list,
        ready_files=ready_files,
        load_bq_fn=load_bq)
    
    #List all bronze metadata tables once (IMPORTANT:RUN FILES BEFORE RECORD SETS TO POPULATE PANEL IDS)
    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = sorted(
        [t.table_id for t in bronze_tables if t.table_id.startswith("bronze_METADATA_TABLE_All_")],
        key=lambda t: (0 if t.split("_")[4] == "Files" else 1, t))
    
    # =================================================================================
    print_sub_section("CREATING STAGING DATASETS FOR RELEASE")
    # =================================================================================
    panel_records_list = []
    collected_files_list = []
    collected_records_list = []
    
    for table_id in bronze_metadata:
        print(f"Fetching: {table_id}")
        metadata_type = table_id.split("_")[4]
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
        
        if df.empty:
            continue
        
        #FILE PROCESSING
        if metadata_type == "Files":
            in_file_map = df["File_EntityId"].isin(file_map)
            df = df[df["Status_Folder_Name"].str.contains("staging|ingest", na=False) | in_file_map]
            df = df[~df["File_EntityId"].isin(exclusion_files["File_EntityId"])]
            df = df[df["File_EntityId"].isin(validated_files["File_EntityId"]) | in_file_map]
            df = df[df["HTAN_Center"].isin(confirmed_center_list["HTAN_Center"])]
            df = df[~df["File_EntityId"].isin(released_entities["File_EntityId"])]
            df = pd.merge(df, bronze_file_schema, on="File_EntityId")
    
            panel_slice, file_slice = extract_panel_and_file_slice(df)
            if panel_slice is not None:
                panel_records_list.append(panel_slice)
            collected_files_list.append(file_slice)
        #RECORD PROCESSING
        elif metadata_type == "Records":
            if not df.empty:
                exclusion_mask = (match_status_in_map(df, "HTAN_BIOSPECIMEN_ID", bio_map)| match_status_in_map(df, "HTAN_PARTICIPANT_ID", part_map))
                status_keep_mask = df["Status_Folder_Name"].str.contains("ingest|staging", na=False) | exclusion_mask
                df = df[status_keep_mask]
                exclusion_mask = exclusion_mask[status_keep_mask]
                #Filter to validated ids and validated panels
                if "HTAN_PANEL_ID" in df.columns and "HTAN_PANEL_ID" in validated_records.columns:
                    valid_record_mask = df["HTAN_PANEL_ID"].isin(validated_records["HTAN_PANEL_ID"].dropna())
                else:
                    valid_record_mask = df["BQ_Hash_Record_ID"].isin(validated_records["BQ_Hash_Record_ID"])
                df = df[valid_record_mask | exclusion_mask]
                #Check if confirmed center
                df = df[df["HTAN_Center"].isin(confirmed_center_list["HTAN_Center"])]
                #Check not released
                df = df[~df["BQ_Hash_Record_ID"].isin(released_records["BQ_Hash_Record_ID"])]
                #Add info
                df = pd.merge(df, bronze_record_schema, on="Record_EntityId")
                df = df[df["Status_Folder_Name"].str.contains(f"v{latest_release}", na=False)]
                #Slice
                target_cols = ["Record_EntityId", "BQ_Hash_Record_ID", "HTAN_Center", "Component", "Status_Folder_Name", "Schema_Version"]
                optional_cols = [c for c in ["HTAN_PARTICIPANT_ID", "HTAN_BIOSPECIMEN_ID", "HTAN_PANEL_ID"] if c in df.columns]
                collected_records_list.append(df[target_cols + optional_cols].copy())
    
    #Stage Files & Counts
    staged_files_df = concat_or_empty(collected_files_list,["File_Name", "File_EntityId", "HTAN_Center", "Status_Folder_Name", "BQ_Hash_ID", "Component"])
    panel_df = concat_or_empty(panel_records_list, ["HTAN_PANEL_ID", "Component", "File_EntityId"])
    
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release",
        staged_files_df)
    
    staged_summary_count_files = (
        staged_files_df.groupby(["Component", "HTAN_Center", "Status_Folder_Name"])
        .size()
        .reset_index(name="Number_of_Files"))
    
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_File_Staged_For_Current_Release_Counts",
        staged_summary_count_files)
    
    #Stage Records & Counts
    valid_parts, valid_bios = get_valid_provenance_ids(bronze_prov, staged_files_df["File_EntityId"])
    filtered_records = [filter_records_by_provenance(df, valid_parts, valid_bios) for df in collected_records_list]
    
    staged_records_df = concat_or_empty(filtered_records,["Record_EntityId", "BQ_Hash_Record_ID", "HTAN_Center", "Component", "Status_Folder_Name", "HTAN_PANEL_ID"])
    staged_records_df = staged_records_df[staged_records_df["HTAN_PANEL_ID"].isna() | staged_records_df["HTAN_PANEL_ID"].isin(panel_df["HTAN_PANEL_ID"])]
    
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release",
        staged_records_df)
    
    staged_summary_count_records = (
        staged_records_df.groupby(["Component", "HTAN_Center", "Status_Folder_Name"])
        .size()
        .reset_index(name="Number_Rows_in_RecordSet"))
    
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_STAGING_FOR_RELEASE_INDEXING_TABLE_All_Record_Rows_Staged_For_Current_Release_Counts",
        staged_summary_count_records)
    
    
    # =================================================================================
    print_sub_section("GENERATING CURRENTLY RELEASED FILES")
    # =================================================================================
    released_entities_list = []
    released_records_list = []
    panel_records_list = []
    valid_parts, valid_bios = set(), set()
    
    for table_id in bronze_metadata:
        print(f"Filtering Table To Released Files: {table_id}")
        _, _, _, _, metadata_type, component = table_id.split("_", 5)
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)
        
        if df.empty:
            continue
        #FILE PROCESSING
        if metadata_type == "Files":
            matches_file_map = match_status_in_map(df, "File_EntityId", file_map)
            df = df[df["Status_Folder_Name"].str.contains("release", na=False) | matches_file_map]
    
            not_excluded = ~df["File_EntityId"].isin(exclusion_files["File_EntityId"].dropna())
            not_in_map = ~df["File_EntityId"].isin(file_map)
            df = df[(not_excluded & not_in_map) | matches_file_map.loc[df.index]]
    
            df = df[df["HTAN_Center"].isin(confirmed_center_list["HTAN_Center"])]
            df = pd.merge(df, bronze_file_schema, on="File_EntityId")
    
            panel_slice, file_slice = extract_panel_and_file_slice(df)
            if panel_slice is not None:
                panel_records_list.append(panel_slice)
            released_entities_list.append(file_slice)
    
            # Update valid provenance IDs from all accumulated released files
            all_released_file_ids = pd.concat(released_entities_list, ignore_index=True)["File_EntityId"]
            valid_parts, valid_bios = get_valid_provenance_ids(bronze_prov, all_released_file_ids)
        #RECORD PROCESSING
        elif metadata_type == "Records":
            df = pd.merge(df, bronze_record_schema, on="Record_EntityId")
            cols_to_drop = [c for c in df.columns if any(v in c for v in VALIDATION_COLUMNS)]
            df = df.drop(columns=cols_to_drop)
    
            matches_part_map = match_status_in_map(df, "HTAN_PARTICIPANT_ID", part_map)
            matches_bio_map = match_status_in_map(df, "HTAN_BIOSPECIMEN_ID", bio_map)
            df = df[df["Status_Folder_Name"].str.contains("release", na=False) | matches_part_map | matches_bio_map]
    
            if "HTAN_PARTICIPANT_ID" in df.columns:
                not_excluded_part = ~df["HTAN_PARTICIPANT_ID"].isin(exclusion_files["HTAN_PARTICIPANT_ID"].dropna())
                not_in_part_map = ~df["HTAN_PARTICIPANT_ID"].isin(part_map)
                df = df[(not_excluded_part & not_in_part_map) | matches_part_map.loc[df.index]]
    
            if "HTAN_BIOSPECIMEN_ID" in df.columns:
                not_excluded_bio = ~df["HTAN_BIOSPECIMEN_ID"].isin(exclusion_files["HTAN_ORIGINATING_BIOSPECIMEN_ID"].dropna())
                not_in_bio_map = ~df["HTAN_BIOSPECIMEN_ID"].isin(bio_map)
                df = df[(not_excluded_bio & not_in_bio_map) | matches_bio_map.loc[df.index]]
    
            if "HTAN_PANEL_ID" in df.columns:
                valid_panel_ids = concat_or_empty(panel_records_list, ["HTAN_PANEL_ID"])["HTAN_PANEL_ID"].dropna()
                df = df[df["HTAN_PANEL_ID"].isin(valid_panel_ids)]
    
            df = filter_records_by_provenance(df, valid_parts, valid_bios)
    
            extracted_version = df["Status_Folder_Name"].str.extract(r"v(\d+)_release", expand=False)
            df["Data_Release"] = "Release " + extracted_version + ".0"
            df["CRDC_Release"] = None
    
            df = apply_age_masking(df, client, PROJECT, SILVER_DATASET, table_id, MAX_AGE_DAYS)
            df = derive_age_in_months(df)
    
            target_cols = ["Record_EntityId", "BQ_Hash_Record_ID", "HTAN_PARTICIPANT_ID", "HTAN_BIOSPECIMEN_ID", "HTAN_PANEL_ID"]
            released_records_list.append(df.reindex(columns=target_cols).copy())
    
            load_bq(
                client,
                PROJECT,
                GOLD_DATASET,
                f"gold_RELEASED_METADATA_TABLE_All_{metadata_type}_{component}",
                df)
    
    # Load Final Released Tracking Tables
    current_released_entities = concat_or_empty(released_entities_list, ["File_EntityId", "BQ_Hash_ID"])
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_RELEASED_INDEXING_TABLE_Released_Entities",
        current_released_entities)
    
    current_released_records = concat_or_empty(
        released_records_list,
        ["Record_EntityId", "BQ_Hash_Record_ID", "HTAN_PARTICIPANT_ID", "HTAN_BIOSPECIMEN_ID", "HTAN_PANEL_ID"])
    load_bq(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_RELEASED_INDEXING_TABLE_Released_RecordsetRows",
        current_released_records)

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
        if table.table_id.startswith("HTAN2_Data_Model_")]

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