#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Raw to Bronze Provenance Table

Requires (env):
- GOOGLE_CLOUD_PROJECT (defaults to 'htan2-dcc')
- BQ_DATASET (defaults to 'htan2_synapse_bronze')

Authors: Dar'ya Pozhidayeva
Updated: 2026-03-24
"""

import pandas as pd
import re
import ast
import os
import logging
from client_load import (
    load_bq,
    init_bq_client,
)

def is_file_id(x):
    return pd.notna(x) and bool(re.search(r"_D\d+$", str(x)))

def is_biospecimen_id(x):
    return pd.notna(x) and bool(re.search(r"_B\d+$", str(x)))

def is_participant_id(x):
    return pd.notna(x) and bool(re.fullmatch(r"HTA\d+_\d+", str(x)))

def walk_all_Provenance_Paths(start_parent, parent_map, max_depth=20, seen=None):
    """
    Return all ancestry paths starting from one parent.
    A path ends when it reaches a biospecimen, participant, unknown type, null, or cycle.
    """
    if seen is None:
        seen = set()

    if pd.isna(start_parent):
        return [[]]

    current = str(start_parent).strip()

    if current in seen:
        return [[current, "[CYCLE]"]]

    if is_biospecimen_id(current) or is_participant_id(current):
        return [[current]]

    if not is_file_id(current):
        return [[current, "[UNKNOWN_PARENT_TYPE]"]]

    # file parent: branch to all its parents
    next_parents = parent_map.get(current, [])

    if not next_parents:
        return [[current, "[NO_PARENT_FOUND]"]]

    all_paths = []
    for p in next_parents:
        child_paths = walk_all_Provenance_Paths(
            p,
            parent_map=parent_map,
            max_depth=max_depth - 1,
            seen=seen | {current}
        )
        for cp in child_paths:
            all_paths.append([current] + cp)

    return all_paths if max_depth > 0 else [[current, "[MAX_DEPTH]"]]


# --------------------------------------------------------------------------------------
# Settings (env-overridable)
HTAN_BQ_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")
MEDALLION_LAYER = os.getenv("BQ_DATASET", "htan2_medallion_bronze")

# --------------------------------------------------------------------------------------
#Set Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)



#MAIN PROGRAM-------------------------------------------------------------------------------------
def main() -> None:
    #Instantiate the relevant clients
    client = init_bq_client()
    #Create bios table for participants, samples, and adjacent samples
    bios = client.query("""                                        
      WITH
      #Get all Biospecimen
      bios AS (
      SELECT
        HTAN_BIOSPECIMEN_ID,
        JSON_VALUE(HTAN_PARENT_ID) AS HTAN_PARENT_ID
      FROM `htan2-dcc.htan2_medallion_bronze.bronze_METADATA_TABLE_All_Records_Biospecimen`
      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(HTAN_PARENT_ID)) AS HTAN_PARENT_ID
      ),
      #Join the first time for the first generation
      anc1 AS (
        SELECT b1.HTAN_BIOSPECIMEN_ID, b1.HTAN_PARENT_ID, b2.HTAN_PARENT_ID AS gParent_ID
        FROM bios b1
        LEFT JOIN bios b2
        ON ( b1.HTAN_PARENT_ID=b2.HTAN_BIOSPECIMEN_ID )
      ),
      #Second generation
      anc2 AS (
        SELECT b1.HTAN_BIOSPECIMEN_ID, b1.HTAN_PARENT_ID, b1.gParent_ID, b2.HTAN_PARENT_ID AS ggParent_ID
        FROM anc1 b1
        LEFT JOIN bios b2
        ON ( b1.gParent_ID=b2.HTAN_BIOSPECIMEN_ID )
      ),
      #Third generation (Can add more as needed)
      anc3 AS (
        SELECT b1.HTAN_BIOSPECIMEN_ID, b1.HTAN_PARENT_ID, b1.gParent_ID, b1.ggParent_ID, b2.HTAN_PARENT_ID AS gggParent_ID
        FROM anc2 b1
        LEFT JOIN bios b2
        ON ( b1.ggParent_ID=b2.HTAN_BIOSPECIMEN_ID )  
      ),
      #Remove the Participant from the Biospecimen_Path Output
      rem AS (
        SELECT 
        IF(ARRAY_LENGTH(SPLIT(gggParent_ID,"_")) = 2, null, gggParent_ID ) AS gggParent_ID,
        IF(ARRAY_LENGTH(SPLIT(ggParent_ID,"_")) = 2, null, ggParent_ID ) AS ggParent_ID,
        IF(ARRAY_LENGTH(SPLIT(gParent_ID,"_")) = 2, null, gParent_ID ) AS gParent_ID,
        IF(ARRAY_LENGTH(SPLIT(HTAN_PARENT_ID,"_")) = 2, null, HTAN_PARENT_ID ) AS HTAN_PARENT_ID,
        HTAN_BIOSPECIMEN_ID
        FROM anc3
      ),
      #Compile the final table
      skipg AS (
        SELECT HTAN_BIOSPECIMEN_ID AS HTAN_ASSAYED_BIOSPECIMEN_ID, 
        COALESCE(gggParent_ID, ggParent_ID, gParent_ID, HTAN_PARENT_ID, HTAN_BIOSPECIMEN_ID) AS HTAN_ORIGINATING_BIOSPECIMEN_ID,
        REGEXP_EXTRACT(HTAN_BIOSPECIMEN_ID, r'(.+)_[^_]*$') AS HTAN_PARTICIPANT_ID,
        TRIM(REPLACE(RTRIM(CONCAT(
          IFNULL(ggParent_ID, ''),' ', IFNULL(gParent_ID, ''),' ', IFNULL(HTAN_PARENT_ID, ''),' ', HTAN_BIOSPECIMEN_ID)), ' ',','), ',') 
          AS BIOSPECIMEN_PATH
        FROM rem
      )
      SELECT * FROM skipg""").result().to_dataframe()
      
    table_list = client.list_tables("htan2-dcc.htan2_medallion_bronze")
    
    all_tables = client.query("""
    SELECT table_name,column_name FROM
    `htan2-dcc.htan2_medallion_bronze.INFORMATION_SCHEMA.COLUMNS`
    """).result().to_dataframe()
    
    f = pd.DataFrame()
    
    for t in table_list:
        tn = t.table_id
    
        if "METADATA_TABLE_All_Files" not in tn or "Panel" in tn:
            continue
        print( '' )
        print( ' Processing: ' + str(tn) )
        print( '' )
    
        cols = list(all_tables[all_tables['table_name'] == tn]['column_name'])
    
        common_cols = ['HTAN_DATA_FILE_ID', 'File_Name', 'File_EntityId', 
            'Component', 'HTAN_Center', "BQ_Hash_ID"]
    
        # Add 'HTAN_PARENT_ID' if present in cols
        if 'HTAN_PARENT_ID' in cols:
            common_cols.append('HTAN_PARENT_ID')
    
        # Join the columns into a string
        qco = ', '.join(common_cols)
    
        df = client.query("""
            SELECT %s FROM `%s.%s.%s`
            """ % (qco,t.project,t.dataset_id,t.table_id)).result().to_dataframe()
    
        f = pd.concat([f, df], ignore_index=True)
    
    
    # expand comma and semicolon separated parent lists into individual rows
    f["HTAN_PARENT_ID"] = f["HTAN_PARENT_ID"].apply(
        lambda x: ast.literal_eval(x) if pd.notna(x) else [])
    
    f = f.explode("HTAN_PARENT_ID")
    
    f = f.assign(HTAN_PARENT_ID = \
        f.HTAN_PARENT_ID.str.split("[,;]")).explode('HTAN_PARENT_ID')
    
    # trim whitespace
    f = f.applymap(lambda x: x.strip() if isinstance(x, str) else x).drop_duplicates()
    
    print('')
    print(' Walking parent file ancestry ')
    print('')
    
    parent_map = (
        f[["HTAN_DATA_FILE_ID", "HTAN_PARENT_ID"]]
        .dropna(subset=["HTAN_DATA_FILE_ID", "HTAN_PARENT_ID"])
        .drop_duplicates()
        .groupby("HTAN_DATA_FILE_ID")["HTAN_PARENT_ID"]
        .apply(list)
        .to_dict())
    
    
    # build all paths for each row
    f["Provenance_Paths"] = f["HTAN_PARENT_ID"].apply(
        lambda x: walk_all_Provenance_Paths(x, parent_map=parent_map, max_depth=10))
    
    # explode so each row is one provenance path
    f_paths = f.explode("Provenance_Paths").copy()
    
    # store string version
    f_paths["Full_Provenance_Chain"] = f_paths["Provenance_Paths"].apply(
        lambda x: " -> ".join(map(str, x)) if isinstance(x, list) else pd.NA)
    
    f_paths["Depth_Prov_Chain"] = f_paths["Provenance_Paths"].apply(
        lambda x: len(x) if isinstance(x, list) else pd.NA)
    
    # first biospecimen found in a path
    f_paths["HTAN_ASSAYED_BIOSPECIMEN_ID"] = f_paths["Provenance_Paths"].apply(
        lambda path: next((i for i in reversed(path) if is_biospecimen_id(i)), pd.NA))
    
    # join to bios table
    id_prov = (
        f_paths.drop(columns=["Provenance_Paths"])
        .merge(bios, on="HTAN_ASSAYED_BIOSPECIMEN_ID", how="left")
        .drop_duplicates())
    
    #Load BQ Table----------------------------------------------------------------------------------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance",
        id_prov)
  
if __name__ == "__main__":
    main()

