#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 11:27:05 2025
@author: Dar'ya Pozhidayeva
"""
#from workflow_functions.bq_load import load_bq as load_bq2
from google.cloud import bigquery
import pandas as pd
from workflow_functions.bq_load import load_bq as load_bq_no_schema
import requests


# Constants
API_URL = "https://dataservice.datacommons.cancer.gov/v1/graphql/"
GRAPHQL_BATCH_SIZE = 10000


# instantiate google bigquery client
client = bigquery.Client()

#Source of truth for releases for HTAN1 - best bet
previously_Released = client.query("""SELECT * FROM `htan-dcc.released.entities`""").result().to_dataframe()
load_bq_no_schema(client, 'htan-dcc', 'htan_medallion_gold', 'gold_INDEXING_TABLE_Legacy_Released_File_Metadata', previously_Released)

exclusion_list = client.query("""SELECT * FROM `htan-dcc.htan_medallion_silver.silver_INDEXING_TABLE_ManualExclusionList`""").result().to_dataframe()


# ================================ #
# GraphQL API Functions
# ================================ #
def ping_graphql_api(query):
    """
    Send a query to the GraphQL API and return the response.
    """
    try:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        response = requests.post(API_URL, json={"query": query}, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def fetch_graphql_data():
    """
    Query the GraphQL API to exhaust all rows and return the concatenated DataFrame.
    """
    cds_all_data = pd.DataFrame()
    offset_value = 0
    while True:
        # Construct GraphQL query
        query = f"""
        query fileOverview(
          $studies: [String] = ["Human Tumor Atlas Network (HTAN) primary sequencing data",
                                "Human Tumor Atlas Network (HTAN) imaging data"],
          $first: Int = {GRAPHQL_BATCH_SIZE},
          $offset: Int = {offset_value}
        ) {{
          fileOverview(
            studies: $studies
            first: $first
            offset: $offset
          ) {{
            study_acronym
            phs_accession
            subject_id
            sample_id
            experimental_strategy
            gender
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
          }}
        }}
        """
        response = ping_graphql_api(query)
        if not response or not response.get("data", {}).get("fileOverview", []):
            break  # Exit when no more data

        files = response["data"]["fileOverview"]
        all_files = pd.DataFrame(files)
        cds_all_data = pd.concat([cds_all_data, all_files], ignore_index=True)
        offset_value += GRAPHQL_BATCH_SIZE

    return cds_all_data

# ================================ #
def main():
    # Fetch data from the GraphQL API
    print("Fetching data from GraphQL API...")
    cds_all_data = fetch_graphql_data()    
    sub_cds_files = cds_all_data[["md5sum", "file_id"]].rename(columns={"md5sum": "md5"})
    
    tables = client.list_tables('htan-dcc.htan_medallion_silver')
    for table in tables:
        if "INDEXING_TABLE" in str(table.table_id):
            continue
        current_table = table.table_id
        print(current_table)
    
        # Load manifest data from BigQuery
        manifest_data = client.query(
            f"""SELECT * FROM `htan-dcc.htan_medallion_silver.{current_table}`
            """
        ).result().to_dataframe()
        #REMOVE BAI
        if "Filename" in manifest_data.columns:
            manifest_data = manifest_data[~manifest_data["Filename"].str.contains(".bai")]
        #ADD CDS:
        if "HTAN_Data_File_ID" in manifest_data.columns:
            manifest_data = pd.merge(manifest_data, sub_cds_files, on="md5", how="left").rename(columns={"file_id": "drs_uri"})
            #manifest_data = manifest_data[manifest_data["HTAN_Data_File_ID"].notna()].rename(columns={"file_id": "drs_uri"})
        
        
        #REMOVE ANYTHING WITH ERRORS
        # List of target columns to check
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
        
        # Check if any of the target columns exist in manifest_data
        existing_columns = [col for col in target_columns if col in manifest_data.columns]
        
        if existing_columns:
            if (manifest_data['Component'] == 'ImagingLevel2').all() and len(manifest_data) != 0:
                manifest_data = manifest_data[manifest_data['Channel_Metadata_Filename'].isna()]
                manifest_data = manifest_data.drop('Channel_Metadata_Filename', axis=1)
            
                For_Release = previously_Released[['entityId', 'Data_Release', "CDS_Release", 'IDC_Release']]
                manifest_data = pd.merge(manifest_data, For_Release, on='entityId', how='left')
            
            # Filter rows where all values in the existing columns are NA
            mask = manifest_data[existing_columns].isna().all(axis=1)
            manifest_data = manifest_data[mask]
        
            # Drop the existing columns
            manifest_data.drop(columns=existing_columns, inplace=True)
            
            
         #APPEND RELEASE STATUS:
        htan_release_info = previously_Released[['entityId', 'Data_Release', 'CDS_Release']]
        manifest_data = pd.merge(manifest_data, htan_release_info, left_on='entityId', right_on='entityId', how='left')
            
        # Load table to BigQuery
        gold_key = f"gold_METADATA_TABLE_{current_table.split('_', 4)[3]}"
        load_bq_no_schema(client, 'htan-dcc', 'htan_medallion_gold', gold_key, manifest_data)
        
  # ================================ #      
    #Make the release shortlist and overviews
    all_tagged = client.query("""SELECT * FROM `htan-dcc.htan_medallion_silver.silver_INDEXING_TABLE_All_Error_Tagged_Files`
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
                               
    
    Not_Released_no_error = all_tagged[~all_tagged["entityId"].isin(previously_Released["entityId"])]
    Not_Released_no_error = Not_Released_no_error[~Not_Released_no_error["entityId"].isin(exclusion_list["file_id"])]
    Not_Released_no_error = Not_Released_no_error[~Not_Released_no_error["Manifest_Id"].isin(exclusion_list["manifest_id"])]
    
    load_bq_no_schema(client, 'htan-dcc', 'htan_medallion_gold', 'gold_INDEXING_TABLE_Current_Release_Candidates', Not_Released_no_error)

# ================================ #        
    release_table_grouped = Not_Released_no_error.groupby(['Manifest_Id', 'Component', 'HTAN_Center'])[['entityId']].count().reset_index().rename(columns={"entityId": "Number_of_Files"})
    load_bq_no_schema(client, 'htan-dcc', 'htan_medallion_gold', 'gold_INDEXING_TABLE_Current_Release_Candidates_Grouped_Counts', release_table_grouped)

    
