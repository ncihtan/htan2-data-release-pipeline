#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Raw to Bronze

Requires (env):
- GOOGLE_CLOUD_PROJECT (defaults to 'htan2-dcc')
- BQ_DATASET (defaults to 'htan2_synapse_bronze')

Authors: Dar'ya Pozhidayeva
Updated: 2026-02-09
"""

import pandas as pd
import hashlib
import base64
from collections import defaultdict
from datetime import datetime
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
# Helpers
def mint_bq_hash(htan_id: str, synapse_id: str,
                 namespace: str = "HTAN", version: str = "v1",
                 length: int = 16) -> str:
    if not htan_id or not synapse_id:
        return None

    payload = f"{namespace}|{version}|{htan_id}|{synapse_id}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    token = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return token[:length]


def main() -> None:
    # Instantiate clients
    syn = init_synapse_client()
    client = init_bq_client()

    registry_table = "bronze_INDEXING_TABLE_BQ_Hash_File_ID_Registry"
    #Check if hash table exists-----------------------------
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

    #Load up source tables-----------------------------
    all_file_annotations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Files_Annotation_Fileview_Source`
    """).result().to_dataframe()

    all_record_annotations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Records_Annotation_Fileview_Source`
    """).result().to_dataframe()
    
    only_valid_files = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Files_With_Validation_Status`
        WHERE Is_Valid = "True"
    """).result().to_dataframe()

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
            df["Component"] = component
            component_dfs[component].append(df)
        except Exception as e:
            print(f"Failed querying {annotation_view_id}: {e}")
    
    stacked_by_component = {
        component: pd.concat(dfs, ignore_index=True)
        for component, dfs in component_dfs.items()
    }

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

    for component, df in stacked_by_component.items():
        component_safe = component.replace("-", "_").replace(" ", "_")
        table_name = f"bronze_METADATA_TABLE_All_Files_{component_safe}"

        print(f"Processing {component} ({len(df):,} rows)")
        df = df.rename(columns=rename_map)
        df = df[df["File_EntityId"].isin(only_valid_files["File_EntityId"])]

        htan_cols = ["HTAN_DATA_FILE_ID"]
        htan_col = next((c for c in htan_cols if c in df.columns), None)

        if htan_col is None:
            df["BQ_Hash_ID"] = None
        else:
            df["HTAN_DATA_FILE_ID"] = df[htan_col].astype(str)
            df["Synapse_EntityId"] = df["File_EntityId"].astype(str)

            df = df.merge(
                registry_df,
                how="left",
                on=["HTAN_DATA_FILE_ID", "Synapse_EntityId"]
            )

            needs_id = df["BQ_Hash_ID"].isna()

            df.loc[needs_id, "BQ_Hash_ID"] = df.loc[needs_id].apply(
                lambda r: mint_bq_hash(r["HTAN_DATA_FILE_ID"], r["Synapse_EntityId"]),
                axis=1
            )

            new_registry_rows = df.loc[needs_id, [
                "BQ_Hash_ID", "HTAN_DATA_FILE_ID", "Synapse_EntityId"
            ]].drop_duplicates()

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

        load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            table_name,
            df
        )

    #Do the same for Records----------------
    component_dfs_records = defaultdict(list)

    for _, row in all_record_annotations.iterrows():
        annotation_view_id = row.get("Annotation_EntityId")
        component = row.get("Component")

        if pd.isna(annotation_view_id):
            continue

        try:
            df = syn.tableQuery(f"SELECT * FROM {annotation_view_id}").asDataFrame()
            df["HTAN_Center"] = row["HTAN_Center"]
            df["Folder_EntityId"] = row["Folder_EntityId"]
            df["Component"] = component
            component_dfs_records[component].append(df)
        except Exception as e:
            print(f"Failed querying {annotation_view_id}: {e}")

    stacked_by_component_records = {
        component: pd.concat(dfs, ignore_index=True)
        for component, dfs in component_dfs_records.items()
    }

    for component, df in stacked_by_component_records.items():
        component_safe = component.replace("-", "_").replace(" ", "_")
        table_name = f"bronze_METADATA_TABLE_All_Records_{component_safe}"

        df = df.rename(columns=rename_map)

        load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            table_name,
            df
        )

    #Valid Files Table----------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "bronze_INDEXING_TABLE_Valid_Files_With_Schema_Information",
        only_valid_files
    )


if __name__ == "__main__":
    main()
