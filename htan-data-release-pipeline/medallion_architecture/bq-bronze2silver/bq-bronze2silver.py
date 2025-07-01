#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec 17 12:00:42 2024
@author: darya
"""
import json
import os
import pandas as pd
import synapseclient
import yaml
from google.cloud import bigquery
from workflow_functions.file_validation import htan_id_unique
from workflow_functions.file_validation import htan_id_regex
from workflow_functions.file_validation import basename_regex
from workflow_functions.file_validation import adjacent_bios
from workflow_functions.file_validation import unique_bios
from workflow_functions.file_validation import unique_demographics
from workflow_functions.file_validation import parents_exist
from workflow_functions.file_validation import get_channel_files
from workflow_functions.bq_load import load_bq as load_bq2


#instantiate synapse client
syn = synapseclient.Synapse()

try:
       syn.login(authToken='eyJ0eXAiOiJKV1QiLCJraWQiOiJXN05OOldMSlQ6SjVSSzpMN1RMOlQ3TDc6M1ZYNjpKRU9VOjY0NFI6VTNJWDo1S1oyOjdaQ0s6RlBUSCIsImFsZyI6IlJTMjU2In0.eyJhY2Nlc3MiOnsic2NvcGUiOlsidmlldyIsImRvd25sb2FkIiwibW9kaWZ5Il0sIm9pZGNfY2xhaW1zIjp7fX0sInRva2VuX3R5cGUiOiJQRVJTT05BTF9BQ0NFU1NfVE9LRU4iLCJpc3MiOiJodHRwczovL3JlcG8tcHJvZC5wcm9kLnNhZ2ViYXNlLm9yZy9hdXRoL3YxIiwiYXVkIjoiMCIsIm5iZiI6MTcxNTYyNzQxNiwiaWF0IjoxNzE1NjI3NDE2LCJqdGkiOiI3OTg5Iiwic3ViIjoiMzQ0NzA0NiJ9.Rc_WX5DxIdEX5sM0NtZmzSw5O0lamYXXozKC36096ncf4yiPCrddIoNrpxgKDSyPjduIwLR8DMs7_sxzuLjTce2wIA4BW8GVMOiccp4wo0O7v30sGOiCwcW58okMqtjR-JtnROpJ7_4gRMRaKlfjYtZRSHB-OUHBlVCD8unPOYD84CEnwKSQ62OFtZYymT2R3NBTTQ0X3bOkIfCCqwKlrCDwcf_4pJ0bjTQWxlVVidjAzqf3LMg8WLnLLgVbI5qhyLax3JxSE5Z_XFWNK_DkTypZ3HXTScrWTL98WKUtxBMKnz_9Kvg45ayAtJi_hVKStZtt3Zxgxdaxv7hwSjmpuQ')
except synapseclient.core.exceptions.SynapseNoCredentialsError:
      print("Please fill in 'username' and 'password'/'api_key' values in .synapseConfig.")
except synapseclient.core.exceptions.SynapseAuthenticationError:
      print("Please make sure the credentials in the .synapseConfig file are correct.")

with open('./config.yaml', 'r') as file:
      config_yaml = yaml.safe_load(file)


with open('config.json', 'r') as file:
    config_json = json.load(file)
    
    
# environment variables
center_map = os.environ.get('HTAN_CENTERS_MAP')
htan_centers = config_json['centers']

# descriptions for additional BigQuery columns
attributes = os.environ.get('ATTRIBUTE_DESCRIPTIONS')
add_descriptions = config_json['descriptions']

# identify file-based components 
file_components = os.environ.get('ASSAYS')
assays = config_json['assays']

# instantiate google bigquery client
client = bigquery.Client()


center_map = config_yaml['centers']
clinical = config_yaml['clinical_attributes']
biospecimen = config_yaml['biospecimen_attributes']
assay_files = config_yaml['files']



exclusion_list = client.query("""SELECT * FROM `htan-dcc.htan_medallion_bronze.bronze_ManualExclusionList`""").result().to_dataframe()
bronze_manifests = client.query("""SELECT * FROM `htan-dcc.htan_medallion_bronze.bronze_Manifests`""").result().to_dataframe()
id_provenance_bronze = client.query("""SELECT * FROM `htan-dcc.id_provenance.upstream_ids`""").result().to_dataframe()

def GetParentIds(meta_map):
    """
    Create table containing primary and parent IDs from all manifests
    """
    
    primary_cols = [
        'HTAN_Data_File_ID',
        'HTAN_Biospecimen_ID'
    ]

    parent_cols = [
        'HTAN_Parent_Data_File_ID',
        'HTAN_Parent_Biospecimen_ID',
        'HTAN_Parent_ID'
    ]

    all_cols = primary_cols + parent_cols + ['entityId','Component']
    id_list = pd.DataFrame(columns=all_cols)
        
    for component in meta_map:
        data = meta_map[component]
        id_list = pd.concat(
            [id_list,data],axis=0
        ).reset_index(drop=True)[all_cols]
    
    id_list['primaryId'] = [[e for e in row if e==e] for row 
        in id_list[primary_cols].values.tolist()]
    id_list['parentId'] = [[e for e in row if e==e] for row 
        in id_list[parent_cols].values.tolist()]
        
    id_list = id_list[[
        'primaryId','parentId','entityId','Component']].explode(
        'primaryId').explode('parentId')
        
    id_list = id_list.assign(parentId = \
        id_list.parentId.str.split("[,;]")).explode('parentId')

    id_list = id_list[id_list['parentId'].str.contains('Not') == False]
    id_list = id_list.applymap(
        lambda x: x.strip() if isinstance(x, str) else x
    ).drop_duplicates()
        
    return id_list


meta_map = {}
tables = client.list_tables('htan-dcc.htan_medallion_bronze')
for table in tables:
    if table.table_id == "bronze_ManualExclusionList" or table.table_id == "bronze_Manifests":
        continue
    current_table = table.table_id
    manifest_data = client.query(f"""SELECT * FROM `htan-dcc.htan_medallion_bronze.{current_table}`""").result().to_dataframe()
    try:
        component = manifest_data['Component'][0]
    except:
        print("Component not found for manifest %s")
        continue
      
    # create metadata map by merging manifests by component
    if component in meta_map:
        meta_map[component] = pd.concat([meta_map[component],manifest_data]).reset_index(drop=True)
    else:
        meta_map[component] = manifest_data
    


parent_ids = GetParentIds(meta_map)
error_unique_demo = unique_demographics(meta_map, id_provenance_bronze)
error_unique_bios = unique_bios(meta_map, id_provenance_bronze)
error_adj_bios = adjacent_bios(meta_map, id_provenance_bronze)

tables = client.list_tables('htan-dcc.htan_medallion_bronze')
total_errors = pd.DataFrame()
silver_manifest_summary = pd.DataFrame()
for table in tables:
    if table.table_id == "bronze_ManualExclusionList" or table.table_id == "bronze_Manifests" or table.table_id == "bronze_CDSSequencingTemplate":
        continue
    current_table = table.table_id
    print(current_table)
    manifest_data = client.query(f"""SELECT * FROM `htan-dcc.htan_medallion_bronze.{current_table}`""").result().to_dataframe()
    
    #All entityids
    error_unique_demo_df = pd.DataFrame(error_unique_demo.items(), columns=['entityId', 'error_unique_demo'])
    manifest_data = pd.merge(manifest_data, error_unique_demo_df, left_on='entityId', right_on='entityId', how='left')
    #        
    error_unique_bios_df = pd.DataFrame(error_unique_bios.items(), columns=['entityId', 'error_unique_bios'])
    manifest_data = pd.merge(manifest_data, error_unique_bios_df, left_on='entityId', right_on='entityId', how='left')
    #
    error_adj_bios_df = pd.DataFrame(error_adj_bios.items(), columns=['entityId', 'error_adj_bios'])
    manifest_data = pd.merge(manifest_data, error_adj_bios_df, left_on='entityId', right_on='entityId', how='left')
    #
    manifest_data_for_summary = manifest_data[['entityId','Manifest_Id', 'Component', 'Manifest_Version', 'HTAN_Center', 'error_unique_demo','error_unique_bios', 'error_adj_bios']]
    silver_manifest_summary = pd.concat([silver_manifest_summary, manifest_data_for_summary], ignore_index=True)
    #Manual Exclusion List
    manifest_data = pd.merge(manifest_data, exclusion_list, left_on='entityId', right_on='file_id', how='left')
    manifest_data = pd.merge(manifest_data, exclusion_list, left_on='Manifest_Id', right_on='manifest_id', how='left')
    manifest_data_for_summary = manifest_data[['entityId','Manifest_Id', 'Component', 'Manifest_Version', 'HTAN_Center', 'exclusion_reason_x', 'exclusion_reason_y']]
    
    #ONLY FILEIDS
    if "HTAN_Data_File_ID" in manifest_data.columns:
        error_id_unique = htan_id_unique(manifest_data)
        error_id_unique_df = pd.DataFrame(error_id_unique.items(), columns=['entityId', 'id_unique_error'])
        manifest_data = pd.merge(manifest_data, error_id_unique_df, left_on='entityId', right_on='entityId', how='left')
        
        error_id_regex = htan_id_regex(manifest_data)
        error_id_regex_df = pd.DataFrame(error_id_regex.items(), columns=['entityId', 'htan_id_regex'])
        manifest_data = pd.merge(manifest_data, error_id_regex_df, left_on='entityId', right_on='entityId', how='left')
        
        error_basename_regex = basename_regex(manifest_data)
        error_basename_regex_df = pd.DataFrame(error_basename_regex.items(), columns=['entityId', 'basename_regex'])
        manifest_data = pd.merge(manifest_data, error_basename_regex_df, left_on='entityId', right_on='entityId', how='left')
        
        error_parents = parents_exist(manifest_data, parent_ids)
        error_parents_df = pd.DataFrame(error_parents.items(), columns=['entityId', 'parent_error'])
        manifest_data = pd.merge(manifest_data, error_parents_df, left_on='entityId', right_on='entityId', how='left')
        #
        manifest_data_for_summary = manifest_data[['Filename','entityId','Manifest_Id', 'Component', 'Manifest_Version', 'HTAN_Center', 'id_unique_error', 'htan_id_regex','basename_regex', 'parent_error', 'md5']]
        silver_manifest_summary = pd.concat([silver_manifest_summary, manifest_data_for_summary], ignore_index=True)
        #ONLY IMAGING
        if (manifest_data['Component'] == 'ImagingLevel2').all():
            channel_aux_files, e_missing_channel = get_channel_files(syn, manifest_data[:500], meta_map['ImagingLevel2'], center_map)
            error_channel = pd.DataFrame(e_missing_channel.items(), columns=['entityId','error_channel_metadata'])
            manifest_data = pd.merge(manifest_data, error_channel, left_on='entityId', right_on='entityId', how='left')
            #
            manifest_data_for_summary = manifest_data[['Filename','entityId','Channel_Metadata_Filename','Manifest_Id', 'Component', 'Manifest_Version', 'HTAN_Center','error_channel_metadata', 'md5']]
            silver_manifest_summary = pd.concat([silver_manifest_summary, manifest_data_for_summary], ignore_index=True)
            
    #REMOVE NAs from merge    
    manifest_data = manifest_data[manifest_data['entityId'].notna()]
    # load table to BigQuery
    silver_key = "silver_" + current_table.split('_', 1)[1]
    load_bq2(client,'htan-dcc', 'htan_medallion_silver', silver_key, manifest_data)
    silver_manifest_summary = pd.concat([silver_manifest_summary, manifest_data_for_summary], ignore_index=True)
    

silver_manifest_summary = silver_manifest_summary[silver_manifest_summary['entityId'].notna()]
silver_manifest_summary = silver_manifest_summary.drop_duplicates()       
#LOAD SILVER TABLE TO BQ
#silver_manifest_summary.columns = silver_manifest_summary.columns.str.replace('[^0-9a-zA-Z]+', '_', regex=True)
load_bq2(client,'htan-dcc', 'htan_medallion_silver', 'silver_Summary_Error_Table', silver_manifest_summary)        

num_rows_manifests = silver_manifest_summary.groupby(['Manifest_Id', 'Component'])['entityId'].count().reset_index(name='Count')
# Group by and count
error_counts_grouped = silver_manifest_summary.groupby(['Manifest_Id', 'Component', 'HTAN_Center'])[
    ['id_unique_error', 'htan_id_regex', 'basename_regex', 'parent_error', 
     'error_unique_demo', 'error_unique_bios', 'error_adj_bios', 
     'exclusion_reason_x', 'exclusion_reason_y', 'error_channel_metadata']
].count()

# Add a 'Total' column with the sum across rows
error_counts_grouped['Total'] = error_counts_grouped.sum(axis=1)

# Reset the index to remove row names and turn it into a regular column
error_counts_grouped = error_counts_grouped.reset_index()
load_bq2(client,'htan-dcc', 'htan_medallion_silver', 'silver_Summary_Error_Table_Counts_By_Manifest', error_counts_grouped)        

# Get manifest and file exclusion list
SHEET_ID = '1tUOd0kiQfW-cjnTbX24Tso5Gnq42k7sKQZLCLFLxBCA'
SHEET_NAME = 'current'
url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
exclude = pd.read_csv(url)
excluded_bq = exclude[['file id', 'manifest id', 'exclusion reason']]
load_bq2(client,'htan-dcc', 'htan_medallion_silver', 'silver_ManualExclusionList', excluded_bq)



#Temporary to keep track of currently released
Released = client.query("""SELECT * FROM `htan-dcc.released.entities`""").result().to_dataframe()
Error_Table = client.query("""SELECT * FROM `htan-dcc.htan_medallion_silver.silver_Summary_Error_Table`""").result().to_dataframe()
Exclusion_List = client.query("""SELECT * FROM `htan-dcc.htan_medallion_silver.silver_ManualExclusionList`""").result().to_dataframe()


#Remove everything that has already been released:
Not_Release_Error_Table = Error_Table[~Error_Table['entityId'].isin(Released['entityId'])]
Not_Release_Error_Table = Not_Release_Error_Table[~Not_Release_Error_Table['Filename'].str.contains('bai', case=False, na=False)]

Not_Release_Error_Table = Not_Release_Error_Table[~Not_Release_Error_Table['entityId'].isin(Exclusion_List['file_id'])]
Not_Release_Error_Table = Not_Release_Error_Table[~Not_Release_Error_Table['Manifest_Id'].isin(Exclusion_List['manifest_id'])]
Not_Release_Error_Table = Not_Release_Error_Table[Not_Release_Error_Table['Filename'].notna()]
Not_Release_Error_Table = Not_Release_Error_Table[~Not_Release_Error_Table['Component'].isin(['Demographics', 
                                                                                                       'Biospecimen', 
                                                                                                       'Exposure',
                                                                                                       'FollowUp',
                                                                                                       'Diagnosis',
                                                                                                       'FamilyHistory',
                                                                                                       'Therapy',
                                                                                                       'MolecularTest',
                                                                                                       'ClinicalDataTier2',
                                                                                                       'ClinicalDataTier3',
                                                                                                       'LungCancerTier3',
                                                                                                       'BreastCancerTier3',
                                                                                                       'CDSSequencingTemplate'])]

columns_to_check = ['id_unique_error', 'htan_id_regex', 'basename_regex', 'parent_error', 
 'error_unique_demo', 'error_unique_bios', 'error_adj_bios', 
 'exclusion_reason_x', 'exclusion_reason_y', 'error_channel_metadata']
filtered_Not_Release_Error_Table = Not_Release_Error_Table[Not_Release_Error_Table[columns_to_check].notnull().any(axis=1)]


filtered_Not_Release_No_Error_Table = Not_Release_Error_Table[~Not_Release_Error_Table[columns_to_check].notnull().any(axis=1)]
filtered_Not_Release_No_Error_Table = filtered_Not_Release_No_Error_Table[~filtered_Not_Release_No_Error_Table['entityId'].isin(filtered_Not_Release_Error_Table['entityId'])]
#filtered_Not_Release_No_Error_Table = filtered_Not_Release_No_Error_Table.drop_duplicates(['entityId'])

manifest_release_candidates = filtered_Not_Release_No_Error_Table.drop_duplicates(['Manifest_Id'])

