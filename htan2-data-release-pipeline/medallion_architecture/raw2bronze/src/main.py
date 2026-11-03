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
Updated: 11-02-2026
"""

import os
import logging
import hashlib
import base64
from datetime import datetime
from collections import defaultdict
import yaml
import numpy as np
import pandas as pd
from google.cloud import bigquery

from client_load import (
    load_bq,
    init_bq_client,
    init_synapse_client,
)

# Settings (env-overridable)
HTAN_BQ_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")
MEDALLION_LAYER = os.getenv("BQ_DATASET", "htan2_medallion_bronze")

# Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------------------
# Helper Functions
# --------------------------------------------------------------------------------------
def mint_bq_hash(
    htan_id: str,
    synapse_id: str,
    namespace: str = "HTAN",
    version: str = "v1",
    length: int = 16,
) -> str | None:
    if htan_id is None or synapse_id is None:
        return None

    htan_id = str(htan_id).strip()
    synapse_id = str(synapse_id).strip()

    if htan_id in ("", "nan") or synapse_id in ("", "nan"):
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
    length: int = 16,
) -> str | None:
    payload_fields = payload_fields_by_component.get(component)
    if payload_fields is None:
        raise ValueError(f"No necessary fields defined for component: '{component}'")

    cleaned = []
    for field in payload_fields:
        val = row.get(field)
        if val is None or pd.isna(val):
            return None

        val = str(val).strip()
        if val == "" or val.lower() == "nan":
            return None
        cleaned.append(val)

    # Inject row_index for Spatial and Channel records
    if component in ("SpatialPanel", "ChannelMetadata"):
        row_idx = row.get("row_index")
        if row_idx is None or pd.isna(row_idx):
            return None
        cleaned.append(str(row_idx).strip())

    id_segment = "|".join(cleaned)
    payload = f"{namespace}|{version}|{id_segment}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    token = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return token[:length]


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main() -> None:
    syn = init_synapse_client()
    client = init_bq_client()

    # 1. FILE REGISTRY
    registry_table = "bronze_INDEXING_TABLE_BQ_Hash_File_ID_Registry"
    try:
        registry_df = client.query(f"""
            SELECT BQ_Hash_ID, HTAN_DATA_FILE_ID, Synapse_EntityId
            FROM `{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{registry_table}`
        """).to_dataframe()
        print(f"Loaded {len(registry_df)} existing File IDs")
    except Exception:
        print("File registry not found — initializing new registry")
        registry_df = pd.DataFrame(columns=[
            "BQ_Hash_ID", "HTAN_DATA_FILE_ID", "Synapse_EntityId", "First_Seen", "Source_Component"
        ])

    # 2. RECORD REGISTRY
    with open("record_fields_config.yaml", "r") as f:
        config = yaml.safe_load(f)

    PAYLOAD_FIELDS_BY_COMPONENT = config["fields_by_component"]
    record_registry_table = "bronze_INDEXING_TABLE_BQ_Hash_Record_ID_Registry"

    all_payload_fields = list(dict.fromkeys(
        field for fields in PAYLOAD_FIELDS_BY_COMPONENT.values() for field in fields
    ))
    registry_columns = ["BQ_Hash_Record_ID", *all_payload_fields, "First_Seen", "Source_Component"]
    full_registry_schema = [bigquery.SchemaField(col, "STRING", mode="NULLABLE") for col in registry_columns]

    table_ref = f"{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{record_registry_table}"
    try:
        client.get_table(table_ref)
        print("Record registry table found")
    except Exception:
        print("Record registry not found — initializing new registry")
        bq_table = bigquery.Table(table_ref, schema=full_registry_schema)
        client.create_table(bq_table)

    record_registry_df = client.query(f"""
        SELECT *
        FROM `{HTAN_BQ_PROJECT}.{MEDALLION_LAYER}.{record_registry_table}`
    """).to_dataframe()
    print(f"Loaded {len(record_registry_df)} existing Record IDs")

    # 3. LOAD SOURCE TABLES
    all_file_annotations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Files_Annotation_Fileview_Source`
    """).result().to_dataframe()

    all_file_validations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_Files_With_Validation_Status`
    """).result().to_dataframe()
    subset_file_validations = all_file_validations[
        ["File_EntityId", "Is_Valid", "Validated_On", "Validation_Error_Message", "All_Validation_Error_Messages"]
    ]

    all_record_annotations = client.query("""
        SELECT *
        FROM `htan2-dcc.htan2_synapse_raw.raw_INDEXING_TABLE_All_RecordSets_With_Validation_Status`
    """).result().to_dataframe()
    subset_record_validations = all_record_annotations[
        ["HTAN_Center", "Folder_EntityId", "Record_EntityId", "Folder_Source_Path",
         "Status_Folder_Name", "Component", "Modified_On", "Version_Label", "Bound_Schema_Name", "Schema_Version"]
    ]

    # 4. FILE METADATA PROCESSING
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
        "schema_errors": "Schema_Validation_Errors",
    }

    for component, df in stacked_by_component.items():
        component_safe = component.replace("-", "_").replace(" ", "_")
        table_name = f"bronze_METADATA_TABLE_All_Files_{component_safe}"

        print(f"Processing {component} ({len(df):,} rows)")
        df = df.rename(columns=rename_map)

        if "HTAN_DATA_FILE_ID" in df.columns:
            df["HTAN_DATA_FILE_ID"] = df["HTAN_DATA_FILE_ID"].astype(str)
            df["Synapse_EntityId"] = df["File_EntityId"].astype(str)

            df = df.merge(registry_df, how="left", on=["HTAN_DATA_FILE_ID", "Synapse_EntityId"])

            has_htan = df["HTAN_DATA_FILE_ID"].notna() & df["HTAN_DATA_FILE_ID"].str.strip().ne("") & df["HTAN_DATA_FILE_ID"].str.lower().ne("nan")
            has_syn = df["Synapse_EntityId"].notna() & df["Synapse_EntityId"].str.strip().ne("") & df["Synapse_EntityId"].str.lower().ne("nan")
            needs_id = df["BQ_Hash_ID"].isna() & has_htan & has_syn

            df.loc[needs_id, "BQ_Hash_ID"] = df.loc[needs_id].apply(
                lambda r: mint_bq_hash(r["HTAN_DATA_FILE_ID"], r["Synapse_EntityId"]),
                axis=1,
            )

            new_registry_rows = (
                df.loc[needs_id, ["BQ_Hash_ID", "HTAN_DATA_FILE_ID", "Synapse_EntityId"]]
                .drop_duplicates()
                .replace("nan", np.nan)
                .dropna(subset=["BQ_Hash_ID"])
            )

            if not new_registry_rows.empty:
                new_registry_rows["First_Seen"] = datetime.utcnow()
                new_registry_rows["Source_Component"] = component
                load_bq(
                    client,
                    HTAN_BQ_PROJECT,
                    MEDALLION_LAYER,
                    registry_table,
                    new_registry_rows,
                    write_mode="append",
                )
                registry_df = pd.concat([registry_df, new_registry_rows], ignore_index=True)

            df = df[["BQ_Hash_ID"] + [c for c in df.columns if c != "BQ_Hash_ID"]]
            df = df.merge(subset_file_validations, on="File_EntityId", how="inner").dropna(how="all")
            df = df.drop(columns=["Synapse_EntityId"], errors="ignore")

            load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, table_name, df)

    # 5. RECORDSET PROCESSING
    component_dfs_records = defaultdict(list)
    for _, row in all_record_annotations.iterrows():
        record_view_id_raw = row.get("Record_EntityId")
        component = row.get("Component")

        if pd.isna(record_view_id_raw) or str(record_view_id_raw).strip() in ("", "None", "nan"):
            continue

        record_view_id_raw = str(record_view_id_raw).strip()
        if "." in record_view_id_raw:
            record_view_id, version_str = record_view_id_raw.split(".", 1)
            version = int(version_str)
        else:
            record_view_id = record_view_id_raw
            version = None

        try:
            rs_meta = syn.get(record_view_id, version=version, downloadFile=False)
            file_handle_id = rs_meta.dataFileHandleId
            validation_fh_id = getattr(rs_meta, "validationFileHandleId", None)

            download_info = syn._getFileHandleDownload(
                fileHandleId=file_handle_id,
                objectId=record_view_id,
                objectType="FileEntity",
            )
            download_url = download_info.get("preSignedURL")
            if not download_url:
                raise ValueError("Could not retrieve a valid records data download URL from Synapse.")

            data_df = pd.read_csv(download_url).dropna(how="all").reset_index(drop=True)

            if not validation_fh_id:
                print(f"RecordSet {record_view_id_raw} has no validationFileHandleId")
                validation_df = pd.DataFrame(index=data_df.index)
                validation_df["Validation_Passed"] = "False"
                validation_df["Validation_Error_Message"] = "Files were not validated using the Curator"
                validation_df["All_Validation_Messages"] = "Files are missing validationFileHandleId"
                validation_df = validation_df.reset_index().rename(columns={"index": "row_index"})
            else:
                validation_info = syn._getFileHandleDownload(
                    fileHandleId=validation_fh_id,
                    objectId=record_view_id,
                    objectType="FileEntity",
                )
                validation_path = validation_info["preSignedURL"]
                validation_df = pd.read_csv(validation_path).rename(columns={
                    "is_valid": "Validation_Passed",
                    "validation_error_message": "Validation_Error_Message",
                    "all_validation_messages": "All_Validation_Messages",
                })

            data_df = data_df.reset_index().rename(columns={"index": "row_index"})
            merged_df = data_df.merge(validation_df, on="row_index", how="left")

            merged_df["HTAN_Center"] = row["HTAN_Center"]
            merged_df["Folder_EntityId"] = row["Folder_EntityId"]
            merged_df["Component"] = component
            merged_df["Record_EntityId"] = record_view_id_raw
            merged_df["Status_Folder_Name"] = row["Status_Folder_Name"]
            component_dfs_records[component].append(merged_df)

        except Exception as e:
            print(f"Failed downloading {record_view_id_raw}: {e}")

    stacked_by_component_records = {
        component: pd.concat(dfs, ignore_index=True)
        for component, dfs in component_dfs_records.items()
    }

    # 6. LOG RECORD HASHES & PUSH TABLES
    for component, df in stacked_by_component_records.items():
        component_safe = component.replace("-", "_").replace(" ", "_")
        table_name = f"bronze_METADATA_TABLE_All_Records_{component_safe}"

        payload_fields = PAYLOAD_FIELDS_BY_COMPONENT[component]

        # 1. Mint hash directly for all rows (no merge needed)
        df["BQ_Hash_Record_ID"] = df.apply(
            lambda row: mint_record_id(
                row=row.to_dict(),
                component=component,
                payload_fields_by_component=PAYLOAD_FIELDS_BY_COMPONENT,
            ),
            axis=1,
        )

        # 2. Check directly against known hashes in the registry
        existing_hashes = set(record_registry_df["BQ_Hash_Record_ID"].dropna())
        is_new = df["BQ_Hash_Record_ID"].notna() & ~df["BQ_Hash_Record_ID"].isin(existing_hashes)

        # 3. Only pull records whose hashes are genuinely not in BigQuery yet
        registry_payload_cols = [c for c in payload_fields if c in df.columns and c != "BQ_Hash_Record_ID"]
        registry_cols = ["BQ_Hash_Record_ID", *registry_payload_cols]

        new_registry_rows = (
            df.loc[is_new, registry_cols]
            .drop_duplicates(subset=["BQ_Hash_Record_ID"])
            .replace("nan", np.nan)
            .dropna(subset=["BQ_Hash_Record_ID"])
        )

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
                write_mode="append",
            )
            record_registry_df = pd.concat([record_registry_df, new_registry_rows], ignore_index=True)

        df = df.rename(columns=rename_map)
        df = df.drop(columns=["row_index"], errors="ignore")

        load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, table_name, df)

    # 7. METADATA TABLES TO BRONZE
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "bronze_INDEXING_TABLE_All_Files_With_Schema_Information",
        all_file_validations,
    )

    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "bronze_INDEXING_TABLE_All_RecordSets_With_Schema_Information",
        subset_record_validations,
    )


if __name__ == "__main__":
    main()