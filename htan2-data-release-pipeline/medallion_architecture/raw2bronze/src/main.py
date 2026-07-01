#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Raw to Bronze
Script pulls in all files with bound schemas from the raw layer in BQ.
Transforms metadata from files into stacked component assay tables with curator validation information.

Requires (env):
- GOOGLE_CLOUD_PROJECT (defaults to 'htan2-dcc')
- BQ_DATASET (defaults to 'htan2_synapse_bronze')

Authors: Dar'ya Pozhidayeva, Yamina Katariya
Updated: 06-30-2026
"""

import pandas as pd
import numpy as np
import hashlib
import base64
from collections import defaultdict
from datetime import datetime
from synapseclient.models import RecordSet
import tempfile
import yaml
from google.cloud import bigquery

import os
import logging
from client_load import (
    load_bq,
    init_bq_client,
    init_synapse_client,
)
# --------------------------------------------------------------------------------------
# Settings (env-overridable)
HTAN_BQ_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")
MEDALLION_LAYER = os.getenv("BQ_DATASET", "htan2_medallion_bronze")
# --------------------------------------------------------------------------------------
#Set Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)
# --------------------------------------------------------------------------------------
# Helper Functions
def mint_bq_hash(
    htan_id: str,
    synapse_id: str,
    namespace: str = "HTAN",
    version: str = "v1",
    length: int = 16):
    
    if htan_id is None or synapse_id is None:
        return None

    htan_id = str(htan_id).strip()
    synapse_id = str(synapse_id).strip()

    if (htan_id == "" or synapse_id == ""
        or htan_id.lower() == "nan" or synapse_id.lower() == "nan"):
        return None

    payload = f"{namespace}|{version}|{htan_id}|{synapse_id}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    token = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return token[:length]

def mint_record_id(
    row: pd.Series | dict,
    component: str,
    payload_fields_by_component: dict,
    namespace: str = "HTAN",
    version: str = "v1",
    length: int = 16) -> str | None:
    
    # 1. Fetch the baseline payload fields for this component
    payload_fields = payload_fields_by_component.get(component)
    
    if payload_fields is None:
        raise ValueError(f"No necessary fields defined for component: '{component}'")
        
    cleaned = []
    
    # 2. Process the standard payload fields
    for field in payload_fields:
        val = row.get(field)
        if val is None:
            return None
        val = str(val).strip()
        
        if val == "" or val.lower() == "nan":
            return None
            
        cleaned.append(val)
    
    # 3. Conditional addition: Inject row_index *only* for specific components
    if component in ("SpatialPanel", "ChannelMetadata"):
        row_idx = row.get("row_index")
        
        if row_idx is None or pd.isna(row_idx):
            return None
            
        cleaned.append(str(row_idx).strip())
        
    # 4. Generate the unique ID
    id_segment = "|".join(cleaned)
    payload = f"{namespace}|{version}|{id_segment}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    token = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    
    return token[:length]
# --------------------------------------------------------------------------------------
#MAIN
def main() -> None:
    # Instantiate clients
    syn = init_synapse_client()
    client = init_bq_client()

    #SET UP HASH REGISTRIES
    #FILE REGISTRY
    registry_table = "bronze_INDEXING_TABLE_BQ_Hash_File_ID_Registry"
    try:
        registry_df = client.query(f"""
            SELECT BQ_Hash_ID, HTAN_DATA_FILE_ID, Synapse_EntityId
            FROM `{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{registry_table}`
        """).to_dataframe()
        print(f"Loaded {len(registry_df)} existing IDs")
    except Exception:
        print("Registry not found — initializing new registry")
        registry_df = pd.DataFrame(columns=[
            "BQ_Hash_ID", "HTAN_DATA_FILE_ID", "Synapse_EntityId", "First_Seen", "Source_Component"
        ])

    #RECORD REGISTRY
    #Fetch config used for record hashes
    with open("record_fields_config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    PAYLOAD_FIELDS_BY_COMPONENT = config["fields_by_component"]
    record_registry_table = "bronze_INDEXING_TABLE_BQ_Hash_Record_ID_Registry"
    #Fetch all the unique columns from the config
    all_payload_fields = list(dict.fromkeys(
        field
        for fields in PAYLOAD_FIELDS_BY_COMPONENT.values()
        for field in fields
    ))
    #Set the columns
    registry_columns = [
        "BQ_Hash_Record_ID",
        *all_payload_fields,
        "First_Seen",
        "Source_Component",
    ]
    
    full_registry_schema = [
        bigquery.SchemaField(col, "STRING", mode="NULLABLE")
        for col in registry_columns
    ]
    table_ref = f"{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{record_registry_table}"
    
    try:
        client.get_table(table_ref)
        print("Registry table found")
    except Exception:
        print("Registry not found — initializing new registry")
        bq_table = bigquery.Table(table_ref, schema=full_registry_schema)
        client.create_table(bq_table)
    
    record_registry_df = client.query(f"""
        SELECT *
        FROM `{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{record_registry_table}`
    """).to_dataframe()
    print(f"Loaded {len(record_registry_df)} existing IDs")

    #Load up source tables-----------------------------
    all_file_annotations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Files_Annotation_Fileview_Source`
    """).result().to_dataframe()
    
    all_file_validations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Files_With_Validation_Status`
    """).result().to_dataframe()
    
    subset_file_validations = all_file_validations[["File_EntityId", "Is_Valid", "Validated_On", "Validation_Error_Message", "All_Validation_Error_Messages"]]

    all_record_annotations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_RecordSets_With_Validation_Status`
    """).result().to_dataframe()
    
    subset_record_validations = all_record_annotations[["HTAN_Center", "Folder_EntityId", "Record_EntityId", "Folder_Source_Path", "Status_Folder_Name", "Component", "Modified_On", "Version_Label", "Bound_Schema_Name", "Schema_Version"]]

    #File Metadata Processing
    component_dfs = defaultdict(list)

    for _, row in all_file_annotations.iterrows():
        annotation_view_id = row.get("Annotation_EntityId")
        component = row.get("Component")

        if pd.isna(annotation_view_id):
            continue

        try:
            df = syn.tableQuery(f"SELECT * FROM {annotation_view_id}").asDataFrame()
            df["HTAN_Center"] = row["HTAN_Center"]
            df["Folder_EntityId"] = row["Folder_EntityId"]
            df["Status_Folder_Name"] = row["Status_Folder_Name"]
            df["Component"] = component
            component_dfs[component].append(df)
        except Exception as e:
            print(f"Failed querying {annotation_view_id}: {e}")
    
    stacked_by_component = {
        component: pd.concat(dfs, ignore_index=True)
        for component, dfs in component_dfs.items()
    }
    #Adjust column names
    rename_map = {
        "id": "File_EntityId",
        "name": "File_Name",
        "parentId": "Parent_EntityId",
        "projectId": "Project_EntityId",
        "benefactorId": "Benefactor_EntityId",
        "description": "Description",
        "type": "Entity_Type",
        "path": "Path",
        "createdOn": "Created_On",
        "createdBy": "Created_By",
        "Modified_On": "Modified_On",
        "Modified_By": "Modified_By",
        "etag": "Etag",
        "currentVersion": "Current_Version",
        "dataFileHandleId": "File_Handle_Id",
        "dataFileSizeBytes": "File_Size_Bytes",
        "dataFileMD5Hex": "File_MD5",
        "dataFileConcreteType": "File_Handle_Type",
        "dataFileBucket": "S3_Bucket",
        "dataFileKey": "S3_Key",
        "HTAN_Center": "HTAN_Center",
        "source_fileview": "Source_Fileview",
        "schema_isValid": "Schema_Is_Valid",
        "schema_errors": "Schema_Validation_Errors"
    }
    #Loop through all tables and stack them by component into one table
    for component, df in stacked_by_component.items():
    
        # Remove splitting of scRNA-seq in components; variable name remains the same.
        split_components = [component]
    
        for comp in split_components:
            component_safe = comp.replace("-", "_").replace(" ", "_")
            table_name = f"bronze_METADATA_TABLE_All_Files_{component_safe}"

        print(f"Processing {component} ({len(df):,} rows)")
        df = df.rename(columns=rename_map)
        #Apply BQ hashing
        htan_cols = ["HTAN_DATA_FILE_ID"]
        htan_col = next((c for c in htan_cols if c in df.columns), None)

        if htan_col is None:
            pass
        else:
            df["HTAN_DATA_FILE_ID"] = df[htan_col].astype(str)
            df["Synapse_EntityId"] = df["File_EntityId"].astype(str)
            
            df = df.merge(
                registry_df,
                how="left",
                on=["HTAN_DATA_FILE_ID", "Synapse_EntityId"])
            
            has_htan = (
                df["HTAN_DATA_FILE_ID"].notna()
                & df["HTAN_DATA_FILE_ID"].astype(str).str.strip().ne("")
                & df["HTAN_DATA_FILE_ID"].astype(str).str.lower().ne("nan"))
            
            has_syn = (
                df["Synapse_EntityId"].notna()
                & df["Synapse_EntityId"].astype(str).str.strip().ne("")
                & df["Synapse_EntityId"].astype(str).str.lower().ne("nan"))
            
            needs_id = df["BQ_Hash_ID"].isna() & has_htan & has_syn

            df.loc[needs_id, "BQ_Hash_ID"] = df.loc[needs_id].apply(
                lambda r: mint_bq_hash(r["HTAN_DATA_FILE_ID"], r["Synapse_EntityId"]),
                axis=1
            )

            new_registry_rows = df.loc[needs_id, [
                "BQ_Hash_ID", "HTAN_DATA_FILE_ID", "Synapse_EntityId"
            ]].drop_duplicates().replace('nan', np.nan).dropna(subset=['BQ_Hash_ID'])

            if not new_registry_rows.empty:
                new_registry_rows["First_Seen"] = datetime.utcnow()
                new_registry_rows["Source_Component"] = component

                load_bq(
                    client,
                    HTAN_BQ_PROJECT,
                    MEDALLION_LAYER,
                    registry_table,
                    new_registry_rows,
                    write_mode="append"
                )

            df = df[["BQ_Hash_ID"] + [c for c in df.columns if c != "BQ_Hash_ID"]]
            df = df.merge(subset_file_validations, on = "File_EntityId", how="inner")
            #If all columns are NULL in the table; drop row.
            df = df.dropna(how="all")
            df = df.drop(columns=['Synapse_EntityId'])
        
            load_bq(
                client,
                HTAN_BQ_PROJECT,
                MEDALLION_LAYER,
                table_name,
                df
            )

    #Process records next in a similar manner using the API commands for Record sets
    component_dfs_records = defaultdict(list)

    for _, row in all_record_annotations.iterrows():
        record_view_id_raw = row.get("Record_EntityId")
        component = row.get("Component")
        
        if pd.isna(record_view_id_raw) or record_view_id_raw is None or str(record_view_id_raw).strip() == "None":
            continue
            
        record_view_id_raw = str(record_view_id_raw).strip()
        if "." in record_view_id_raw:
            record_view_id, version_str = record_view_id_raw.split(".", 1)
            version = int(version_str)
        else:
            record_view_id = record_view_id_raw
            version = None
        
        try:
            #Extract RecordSet metadata
            rs_meta = syn.get(record_view_id, version=version, downloadFile=False)
            file_handle_id = rs_meta.dataFileHandleId
            validation_fh_id = getattr(rs_meta, "validationFileHandleId", None)
            
            #Download the recordset
            download_info = syn._getFileHandleDownload(
                fileHandleId=file_handle_id,
                objectId=record_view_id,
                objectType="FileEntity"
            )
            
            download_url = download_info.get("preSignedURL")
            
            if not download_url:
                raise ValueError("Could not retrieve a valid records data download URL from Synapse.")
                
            #Load recordset
            data_df = pd.read_csv(download_url).reset_index(drop=True)
            #If all columns are NULL in the table; drop row.
            data_df = data_df.dropna(how="all")
            
            #Get validation metrics
            if not validation_fh_id:
                print(f"RecordSet {record_view_id_raw} has no validationFileHandleId")
                validation_df = pd.DataFrame(index=data_df.index)
                validation_df['is_valid'] = 'False'
                validation_df['validation_error_message'] = 'Files were not validated using the Curator'
                validation_df['all_validation_messages'] = 'Files are missing validationFileHandleId'
                validation_df = validation_df.reset_index().rename(columns={"index": "row_index"})

            else:
                # Download the validation CSV
                validation_info = syn._getFileHandleDownload(
                    fileHandleId=validation_fh_id,
                    objectId=record_view_id,
                    objectType="FileEntity"
                )
                
                #Pull out the local file path
                validation_path = validation_info['preSignedURL']
                
                if not validation_path:
                    raise ValueError(f"Could not determine validation file path from: {validation_info}")
                validation_df = pd.read_csv(validation_path)
            
            #Join data and validation logs by row number
            data_df = data_df.reset_index().rename(columns={"index": "row_index"})
            merged_df = data_df.merge(validation_df, on="row_index", how="left")
            
            merged_df = merged_df.rename(columns={
                'is_valid': 'Validation_Passed', 
                'validation_error_message': 'Validation_Error_Message',
                'all_validation_messages': 'All_Validation_Messages'
            })

            merged_df["HTAN_Center"] = row["HTAN_Center"]
            merged_df["Folder_EntityId"] = row["Folder_EntityId"]
            merged_df["Component"] = component
            merged_df["Record_EntityId"] = record_view_id_raw
            merged_df["Status_Folder_Name"] = row['Status_Folder_Name']
            component_dfs_records[component].append(merged_df)
            
        except Exception as e:
            print(f"Failed downloading {record_view_id_raw}: {e}")

    stacked_by_component_records = {
        component: pd.concat(dfs, ignore_index=True)
        for component, dfs in component_dfs_records.items()
    }
    
    #Log BQ hash values for records before pushing tables
    for component, df in stacked_by_component_records.items():
        component_safe = component.replace("-", "_").replace(" ", "_")
        
        table_name = f"bronze_METADATA_TABLE_All_Records_{component_safe}"
        
        payload_fields = PAYLOAD_FIELDS_BY_COMPONENT[component]
        
        merge_cols = [c for c in payload_fields if c in df.columns and c in record_registry_df.columns]
    
        registry_for_merge = record_registry_df.drop_duplicates(subset=merge_cols) if merge_cols else record_registry_df
    
        df = df.merge(registry_for_merge[merge_cols + ["BQ_Hash_Record_ID"]], how="left", on=merge_cols)
        needs_id = df["BQ_Hash_Record_ID"].isna()
    
        df.loc[needs_id, "BQ_Hash_Record_ID"] = df.loc[needs_id].apply(
                    lambda row: mint_record_id(
                        row={**row.to_dict(), "Record_EntityId": str(row["Record_EntityId"]).split('.')[0]}
                            if "Record_EntityId" in row and pd.notna(row["Record_EntityId"]) 
                            else row,
                        component=component,
                        payload_fields_by_component=PAYLOAD_FIELDS_BY_COMPONENT,
                    ),
                    axis=1,
                )
    
        registry_payload_cols = [
            c for c in payload_fields
            if c in df.columns and c not in ("Record_EntityId", "BQ_Hash_Record_ID")
        ]
    
        new_registry_rows = df.loc[needs_id, [
            "BQ_Hash_Record_ID",
            "Record_EntityId",
            *registry_payload_cols,
        ]].drop_duplicates(subset=["BQ_Hash_Record_ID"]).replace('nan', np.nan).dropna(subset=['BQ_Hash_Record_ID'])
    
        if not new_registry_rows.empty:
            new_registry_rows["First_Seen"] = datetime.utcnow()
            new_registry_rows["Source_Component"] = component
            new_registry_rows = new_registry_rows.drop(columns=["Component"], errors="ignore")
            load_bq(
                client,
                HTAN_BQ_PROJECT,
                MEDALLION_LAYER,
                record_registry_table,
                new_registry_rows,
                write_mode="append"
            )
            record_registry_df = pd.concat([record_registry_df, new_registry_rows], ignore_index=True)
    
        df = df.rename(columns=rename_map)
        df = df.drop(columns=['row_index'], errors='ignore')
        
        load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            table_name,
            df
        )

        
    #Migrate version of raw table to bronze for silver indexing
    load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            "bronze_INDEXING_TABLE_All_Files_With_Schema_Information",
            all_file_validations
        )
    
    load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            "bronze_INDEXING_TABLE_All_RecordSets_With_Schema_Information",
            subset_record_validations
        )

if __name__ == "__main__":
    main()
