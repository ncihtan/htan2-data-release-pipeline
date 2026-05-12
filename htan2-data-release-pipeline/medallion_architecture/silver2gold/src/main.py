#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Silver to Gold

Author: Dar'ya Pozhidayeva, Yamina Katariya
Updated: 05/06/2026
"""

from client_load import (
    load_bq,
    init_bq_client
)

#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################

PROJECT = "htan2-dcc"
RAW_DATASET = "htan2_synapse_raw"
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

def main():
    """
    Entry point into the GOLD layer.
    """
    # Initialize BQ Client
    client = init_bq_client()
    
    #---------------------------------------------------------------------------------
    file_validation_query = f"""
        SELECT *
        FROM `{PROJECT}.{SILVER_DATASET}.silver_INDEXING_TABLE_All_File_Errors`
    """
    passed_file_validation_table = client.query(file_validation_query).to_dataframe()

    record_validation_query = f"""
        SELECT *
        FROM `{PROJECT}.{SILVER_DATASET}.silver_INDEXING_TABLE_All_Record_Errors`
    """
    passed_record_validation_table = client.query(record_validation_query).to_dataframe()

    #---------------------------------------------------------------------------------

    #VALIDATION BYPASS TABLES
    bypass_file_query = f"""
        SELECT *
        FROM `{PROJECT}.{RAW_DATASET}.raw_INDEXING_TABLE_All_Bypass_Validation_Table`
        WHERE Type = "File"
        """
    bypass_files = client.query(bypass_file_query).to_dataframe()
    
    #VALIDATION BYPASS TABLES
    bypass_record_query = f"""
        SELECT *
        FROM `{PROJECT}.{RAW_DATASET}.raw_INDEXING_TABLE_All_Bypass_Validation_Table`
        WHERE Type = "Record"
        """
    bypass_records = client.query(bypass_record_query).to_dataframe()

    #---------------------------------------------------------------------------------
    release_staged_files = passed_file_validation_table[(passed_file_validation_table['Validation_Completion'] == '3/3') | 
                                                        (passed_file_validation_table['File_EntityId'].isin(bypass_files))]
    
    match_cols = [
        'Record_EntityId', 
        'HTAN_PARTICIPANT_ID', 
        'HTAN_BIOSPECIMEN_ID', 
        'HTAN_PANEL_ID']
    
    bypass_temp = bypass_records[match_cols].drop_duplicates()
    bypass_temp['is_bypass'] = True
    
    merged_table = passed_record_validation_table.merge(
        bypass_temp, 
        on=match_cols,
        how='left')
    
    release_staged_records = merged_table[
        (merged_table['Validation_Completion'] == '3/3') | 
        (merged_table['is_bypass'] == True)
    ].drop(columns=['is_bypass'])
    #---------------------------------------------------------------------------------
    #
    silver_tables = list(client.list_tables(f"{PROJECT}.{SILVER_DATASET}"))
    silver_metadata = [
        table.table_id
        for table in silver_tables
        if table.table_id.startswith("silver_METADATA_TABLE_All_")]
    
    for table_id in silver_metadata:                
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]
        
        if metadata_type == "Files":
            df = query_bigquery_table(client, PROJECT, SILVER_DATASET, table_id)
            
            df = df[df['File_EntityId'].isin(release_staged_files['File_EntityId'])]
        
        if metadata_type == "Records":
            df = query_bigquery_table(client, PROJECT, SILVER_DATASET, table_id)
            #Subset record entity first
            df = df[df['Record_EntityId'].isin(release_staged_records['Record_EntityId'])]
            #define which elements to match on based on the record type:
            if component == "Biospecimen":
                df = df[df['HTAN_BIOSPECIMEN_ID'].isin(release_staged_records['HTAN_BIOSPECIMEN_ID'])]  
                
            elif "Panel" in component or "Channel" in component:
                df = df[df['HTAN_PANEL_ID'].isin(release_staged_records['HTAN_PANEL_ID'])]  
                
            else:                         
                df = df[df['HTAN_PARTICIPANT_ID'].isin(release_staged_records['HTAN_PARTICIPANT_ID'])]
        
        #Remove Validation Columns After Filtering
        validation_columns = ['Validation', 'Error', 'Violations']
        cols_to_drop = [col for col in df.columns if any(val_column in col for val_column in validation_columns)]
        df = df.drop(columns=cols_to_drop)
        table_name = f"gold_METADATA_TABLE_All_{metadata_type}_{component}"
        
        load_bq(
                client,
                PROJECT,
                GOLD_DATASET,
                table_name,
                df
            )
        

        #File level validated files - push to BQ
    passed_file_validation_table
    cols_to_drop = [col for col in passed_file_validation_table.columns if any(val_column in col for val_column in validation_columns)]
    passed_file_validation_table = passed_file_validation_table.drop(columns=cols_to_drop)
    load_bq(
                client,
                PROJECT,
                GOLD_DATASET,
                "gold_INDEXING_TABLE_All_File_Validation_Passed",
                passed_file_validation_table
            )

        #Record level validated files - push to BQ
    passed_record_validation_table
    cols_to_drop = [col for col in passed_record_validation_table.columns if any(val_column in col for val_column in validation_columns)]
    passed_record_validation_table = passed_record_validation_table.drop(columns=cols_to_drop)
    load_bq(
                client,
                PROJECT,
                GOLD_DATASET,
                "gold_INDEXING_TABLE_All_Record_Validation_Passed",
                passed_record_validation_table
            )
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
