#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Synapse to Raw
- Extract file-level metadata/annotations from Synapse
- Normalize annotations to BQ-compatible repeated RECORD schema
- Load into BigQuery bronze tables

Requires (env):
- GOOGLE_CLOUD_PROJECT (defaults to 'htan2-dcc')
- BQ_DATASET (defaults to 'htan2_synapse_raw')
- Optional:
  - HTAN_DEV_PARENT (defaults to 'syn68755168')
  - CENTER_LIAISONS_URL (preferred) or CENTER_LIAISONS_PATH
  - SCHEMA_BINDING_CONFIG_URL (defaults to htan2_project_setup raw URL)

Authors: Dar'ya Pozhidayeva
Updated: 2025-03-24
"""

from __future__ import annotations

import os
import re
import time
import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yaml
from synapseclient import EntityViewSchema, EntityViewType

from client_load import (
    load_bq,
    init_bq_client,
    init_synapse_client,
)

# --------------------------------------------------------------------------------------
# Settings (env-overridable)
HTAN_BQ_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")
MEDALLION_LAYER = os.getenv("BQ_DATASET", "htan2_synapse_raw")

HTAN_DEV = os.getenv("HTAN_DEV_PARENT", "syn68755168")

SCHEMA_BINDING_CONFIG_URL = os.getenv(
    "SCHEMA_BINDING_CONFIG_URL",
    "https://raw.githubusercontent.com/ncihtan/htan2_project_setup/refs/heads/main/schema_binding_config.yml",
)

CENTER_LIAISONS_URL = os.getenv("CENTER_LIAISONS_URL", "").strip()
CENTER_LIAISONS_PATH = os.getenv("CENTER_LIAISONS_PATH", "configs/center_liasons.yaml")

HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT_SECONDS", "60"))
SYNAPSE_RETRIES = int(os.getenv("SYNAPSE_RETRIES", "5"))
SYNAPSE_BACKOFF_BASE_SECONDS = float(os.getenv("SYNAPSE_BACKOFF_BASE_SECONDS", "0.75"))

# --------------------------------------------------------------------------------------
#Set Logging
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# Helper Functions
def http_get_text(url: str, timeout: int = HTTP_TIMEOUT) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

def syn_rest_get(syn, path: str) -> Dict[str, Any]:
    """
    Synapse REST GET with retry for transient failures.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, SYNAPSE_RETRIES + 1):
        try:
            return syn.restGET(path)
        except Exception as e:
            last_err = e
            sleep_s = SYNAPSE_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            time.sleep(sleep_s)
    raise last_err  # type: ignore[misc]

def safe_contains(series: pd.Series, pattern: str, flags: int = re.IGNORECASE) -> pd.Series:
    s = series.fillna("").astype(str)
    return s.str.contains(pattern, flags=flags, regex=True)

#--------------------------------------------------------------------------------------
#Core Functions Needed
def data_frames_from_config(binding_dictionary: Dict[str, Any]) -> pd.DataFrame:
    project_rows: List[Dict[str, Any]] = []
    for _, value in binding_dictionary.items():
        projects = value.get("projects", [])
        for project in projects:
            project_rows.append(
                {
                    "HTAN_Center": project.get("name"),
                    "Folder_EntityId": project.get("synapse_id"),
                    "Annotation_EntityId": project.get("fileview_id"),
                    "Folder_Source_Path": project.get("subfolder"),
                }
            )
    return pd.DataFrame(project_rows)

def ensure_entity_view(
    syn,
    project_id: str,
    project_name: str,
    entity_type: EntityViewType,
    parent_id: str,
    add_annotation_columns: bool):
    """
    Create OR UPDATE a Synapse EntityView (FolderView or FileView).
    """
    try:
        view_name = f"{project_name}_{entity_type.name.capitalize()}View"

        view = EntityViewSchema(
            name=view_name,
            parent=parent_id,
            scopes=[project_id],
            includeEntityTypes=[entity_type],
            addDefaultViewColumns=True,
            addAnnotationColumns=add_annotation_columns,
        )

        view = syn.store(view)
        log.info("Touched %s for %s: %s", f"{entity_type.name}View", project_name, view.id)

        return view.id

    except Exception as e:
        log.exception(
            "%sView failed for %s (%s): %s",
            entity_type.name,
            project_name,
            project_id,
            e,
        )
        return None

def count_view_rows(syn, view_id: Optional[str], label: str) -> int:
    if not view_id or pd.isna(view_id):
        return 0
    try:
        q = f"SELECT COUNT(*) AS n FROM {view_id}"
        return int(syn.tableQuery(q).asDataFrame().iloc[0]["n"])
    except Exception as e:
        log.warning("Failed %s count for %s: %s", label, view_id, e)
        return 0


def count_files_via_fileview(syn, file_view_id: Optional[str], folder_id: Optional[str]) -> int:
    if not file_view_id or not folder_id or pd.isna(file_view_id) or pd.isna(folder_id):
        return 0
    try:
        query = f"""
        SELECT COUNT(*) AS n
        FROM {file_view_id}
        WHERE path LIKE '%/{folder_id}/%'
        """
        return int(syn.tableQuery(query).asDataFrame().iloc[0]["n"])
    except Exception as e:
        log.warning("Failed FileView count (view=%s, folder=%s): %s", file_view_id, folder_id, e)
        return 0


def get_validation_summary(syn, entity_id: str) -> Dict[str, Any]:
    try:
        val = syn_rest_get(syn, f"/entity/{entity_id}/schema/validation")

        result = {
            "is_valid": True,
            "validation_error_message": "",
            "all_validation_messages": "",
            "validated_on": val.get("validatedOn", ""),
        }

        if not val.get("isValid", True):
            result["is_valid"] = False
            result["validation_error_message"] = val.get("validationErrorMessage")
            result["all_validation_messages"] = val.get("allValidationMessages")

        return result

    except Exception as e:
        return {
            "is_valid": None,
            "validation_error_message": f"Validation lookup failed: {str(e)}",
            "all_validation_messages": "",
            "validated_on": "",
        }

def collect_all_fileviews(syn, phase2_centers: pd.DataFrame, view_col: str = "Fileview_EntityId") -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []

    BASE_COLUMNS = [
        "id",
        "name",
        "parentId",
        "projectId",
        "createdOn",
        "createdBy",
        "modifiedOn",
        "modifiedBy",
        "etag",
        "path",
        "type",
        "currentVersion",
        "dataFileHandleId",
        "dataFileName",
        "dataFileSizeBytes",
        "dataFileMD5Hex",
        "dataFileConcreteType",
        "dataFileBucket",
        "dataFileKey",
        "benefactorId",
        "description",
    ]

    view_ids = (
        phase2_centers.get(view_col, pd.Series(dtype="object"))
        .dropna()
        .astype(str)
        .unique()
        .tolist()
    )

    for view_id in view_ids:
        try:
            log.info("Querying base file metadata from %s", view_id)
            query = f"SELECT {', '.join(BASE_COLUMNS)} FROM {view_id}"
            df = syn.tableQuery(query).asDataFrame()
            df["source_fileview"] = view_id

            if df.empty:
                dfs.append(df)
                continue

            log.info("Running schema validation per file for %s (%d files)", view_id, len(df))
            validation_results = df["id"].apply(lambda eid: get_validation_summary(syn, str(eid)))

            df["is_valid"] = validation_results.map(lambda x: x.get("is_valid"))
            df["validation_error_message"] = validation_results.map(lambda x: x.get("validation_error_message"))
            df["all_validation_messages"] = validation_results.map(lambda x: x.get("all_validation_messages"))
            df["validated_on"] = validation_results.map(lambda x: x.get("validated_on"))

            dfs.append(df)

        except Exception as e:
            log.exception("Failed fileview query/validation for %s: %s", view_id, e)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    final_df = final_df.rename(
        columns={
            "is_valid": "Is_Valid",
            "validation_error_message": "Validation_Error_Message",
            "all_validation_messages": "All_Validation_Error_Messages",
            "validated_on": "Validated_On",
        }
    )

    return final_df


def load_center_liaisons() -> pd.DataFrame:
    if CENTER_LIAISONS_URL:
        text = http_get_text(CENTER_LIAISONS_URL)
        data = yaml.safe_load(text)
        return pd.DataFrame(data.get("htan_centers", []))

    with open(CENTER_LIAISONS_PATH, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return pd.DataFrame(data.get("htan_centers", []))


def fetch_all_projects(syn) -> pd.DataFrame:
    all_projects: List[Dict[str, Any]] = []
    token: Optional[str] = None

    while True:
        path = "/projects" if not token else f"/projects?nextPageToken={token}"
        res = syn_rest_get(syn, path)
        all_projects.extend(res.get("results", []))
        token = res.get("nextPageToken")
        if not token:
            break

    return pd.DataFrame(all_projects)

#MAIN PROGRAM-------------------------------------------------------------------------------------
def main() -> None:
    #Instantiate the relevant clients
    syn = init_synapse_client()
    client = init_bq_client()
    
    #Create all center table by establishing fileviews/folderviews (if needed)
    all_centers = fetch_all_projects(syn)

    if all_centers.empty or "name" not in all_centers.columns:
        raise RuntimeError("Synapse /projects returned no results or missing expected fields.")

    phase2_centers = all_centers[safe_contains(all_centers["name"], r"(HTAN2_|htan2-testing1)")]
    phase2_centers = phase2_centers[~safe_contains(phase2_centers["name"], r"(HTAN2_BQDEVPROJECT)")].copy()

    phase2_centers = phase2_centers.rename(
        columns={
            "name": "HTAN_Center",
            "id": "Project_EntityId",
            "lastActivity": "Last_Activity",
            "modifiedOn": "Modified_On",
            "modifiedBy": "Modified_By",
        }
    )

    phase2_centers["Folderview_EntityId"] = None
    phase2_centers["Fileview_EntityId"] = None

    for i, row in phase2_centers.iterrows():
        project_id = str(row["Project_EntityId"])
        project_name = str(row["HTAN_Center"])

        folder_view_id = ensure_entity_view(
            syn=syn,
            project_id=project_id,
            project_name=project_name,
            entity_type=EntityViewType.FOLDER,
            parent_id=HTAN_DEV,
            add_annotation_columns=False,
        )

        file_view_id = ensure_entity_view(
            syn=syn,
            project_id=project_id,
            project_name=project_name,
            entity_type=EntityViewType.FILE,
            parent_id=HTAN_DEV,
            add_annotation_columns=False,
        )

        phase2_centers.at[i, "Folderview_EntityId"] = folder_view_id
        phase2_centers.at[i, "Fileview_EntityId"] = file_view_id

    phase2_centers["Current_Total_Files"] = phase2_centers["Fileview_EntityId"].apply(
        lambda v: count_view_rows(syn, v, "FileView")
    )
    phase2_centers["Current_Total_Folders"] = phase2_centers["Folderview_EntityId"].apply(
        lambda v: count_view_rows(syn, v, "FolderView")
    )

    contacts_df = load_center_liaisons()
    if not contacts_df.empty and "HTAN_Center" in contacts_df.columns:
        phase2_centers = phase2_centers.merge(contacts_df, on="HTAN_Center", how="left")
        
    #Load BQ Table----------------------------------------------------------------------------------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "raw_INDEXING_TABLE_All_Source_Phase2_Centers",
        phase2_centers,
    )
    
    # Construct ALL file view table ----------------------------------------------------------------------------------------  
    big_fileview_df = collect_all_fileviews(syn, phase2_centers, view_col="Fileview_EntityId")
    
    rename_map = {
        "id": "File_EntityId",
        "parentId": "Folder_EntityId",
        "projectId": "Synapse_Project_EntityId",
        "benefactorId": "Benefactor_EntityId",
        "description": "Description",
        "type": "Entity_Type",
        "path": "Path",
        "createdOn": "Created_On",
        "createdBy": "Created_By",
        "modifiedOn": "Modified_On",
        "modifiedBy": "Modified_By",
        "etag": "Etag",
        "currentVersion": "Current_Version",
        "dataFileHandleId": "File_Handle_Id",
        "dataFileName": "File_Name",
        "dataFileSizeBytes": "File_Size_Bytes",
        "dataFileMD5Hex": "File_MD5",
        "dataFileConcreteType": "File_Handle_Type",
        "dataFileBucket": "S3_Bucket",
        "dataFileKey": "S3_Key",
        "source_fileview": "Source_Fileview",
        "Is_Valid": "Is_Valid",
        "Validation_Error_Message": "Validation_Error_Message",
        "All_Validation_Error_Messages": "All_Validation_Error_Messages",
        "Validated_On": "Validated_On",
    }
    
    if not big_fileview_df.empty:
        big_fileview_df = big_fileview_df.rename(columns=rename_map)
        
        
    #Schema Binding folder check----------------------------------------------------------------------------------------
    results: List[Dict[str, Any]] = []
    error_results: List[Dict[str, Any]] = []

    for _, row in phase2_centers.iterrows():
        folder_view_id = row.get("Folderview_EntityId")
        if not folder_view_id or pd.isna(folder_view_id):
            continue

        project_name = str(row.get("HTAN_Center", ""))
        project_id = str(row.get("Project_EntityId", ""))

        try:
            folder_df = syn.tableQuery(f"SELECT * FROM {folder_view_id}").asDataFrame()
        except Exception as e:
            log.warning("Folder view query failed for %s (%s): %s", project_name, folder_view_id, e)
            continue

        if "path" in folder_df.columns:
            folder_df["status_folder"] = folder_df["path"].apply(
                lambda p: p.split("/")[1]
                if isinstance(p, str) and project_name and p.startswith(f"{project_name}/")
                else None
            )
        else:
            folder_df["status_folder"] = None

        for _, folder_row in folder_df.iterrows():
            folder_id = str(folder_row.get("id", ""))
            folder_name = folder_row.get("name", None)
            folder_status = folder_row.get("status_folder", None)

            if not folder_id:
                continue

            try:
                binding = syn_rest_get(syn, f"/entity/{folder_id}/schema/binding")
                schema_info = binding.get("jsonSchemaVersionInfo", {}) or {}
                schema_short_id = schema_info.get("$id") or ""

                results.append(
                    {
                        "HTAN_Center": project_name,
                        "Project_EntityId": project_id,
                        "Folder_EntityId": folder_id,
                        "Folder_Name": folder_name,
                        "Status_Folder_Name": folder_status,
                        "Bound_Schema_Name": schema_short_id,
                    }
                )

            except Exception as e:
                error_results.append(
                    {
                        "HTAN_Center": project_name,
                        "Project_EntityId": project_id,
                        "Folder_EntityId": folder_id,
                        "Folder_Name": folder_name,
                        "Status_Folder_Name": folder_status,
                        "Error": str(e),
                    }
                )

    schema_status_df = pd.DataFrame(results)
    schema_errors_df = pd.DataFrame(error_results)

    #Add the Component using the schema----------------------------------------------------------------------------------------
    if not schema_status_df.empty and "Bound_Schema_Name" in schema_status_df.columns:
        parts = schema_status_df["Bound_Schema_Name"].fillna("").astype(str).str.split("-", n=2, expand=True)
        schema_status_df["Component"] = parts[1] if parts.shape[1] > 1 else None

        schema_status_df["Component"] = schema_status_df["Component"].apply(
            lambda c: (
                [f"{m.group(1)}{d}" for d in m.group(2)]
                if (isinstance(c, str) and (m := re.match(r"^(.*?)(\d+)$", c)))
                else [c]
            )
        )
        #Manage some Component inconsistencies----------------------------------------------------------------------------------------
        schema_status_df["Schema_Version"] = schema_status_df["Bound_Schema_Name"].str.extract(r"(\d+\.\d+\.\d+)$")

        schema_status_df = schema_status_df.explode("Component").reset_index(drop=True)

        schema_status_df["Component"] = schema_status_df["Component"].astype(str).str.replace(
            "BiospecimenData", "Biospecimen", regex=False
        )
        schema_status_df["Component"] = schema_status_df["Component"].astype(str).str.replace(
            "DigitalPathologyData", "DigitalPathology", regex=False
        )

        fileview_lookup = phase2_centers.set_index("HTAN_Center")["Fileview_EntityId"].to_dict()
        schema_status_df["Fileview_EntityId"] = schema_status_df["HTAN_Center"].map(fileview_lookup)

        schema_status_df["Folder_File_Count"] = schema_status_df.apply(
            lambda r: count_files_via_fileview(syn, r.get("Fileview_EntityId"), r.get("Folder_EntityId")),
            axis=1,
        )

        Component = schema_status_df[["Folder_EntityId", "Component"]].drop_duplicates()
    else:
        Component = pd.DataFrame(columns=["Folder_EntityId", "Component"])

    #Load BQ Table----------------------------------------------------------------------------------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "raw_INDEXING_TABLE_All_Folders_With_Bound_Schemas",
        schema_status_df)
    
    #Load BQ Table----------------------------------------------------------------------------------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "raw_INDEXING_TABLE_All_Folders_Without_Bound_Schemas",
        schema_errors_df)
    
    #Include folder schema information in file validation table--------------------------------------------
    subset_schema_status_df = schema_status_df[["Folder_EntityId", "Status_Folder_Name", "Component", "Bound_Schema_Name", "Schema_Version"]]

    big_fileview_df = big_fileview_df.merge(subset_schema_status_df, on="Folder_EntityId", how="inner")
    
    #Load BQ Table----------------------------------------------------------------------------------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "raw_INDEXING_TABLE_All_Files_With_Validation_Status",
        big_fileview_df)
    
    #Create and push annotation fileviews and Record Sets based on config provided Sage Bionetworks (Aditi Gopalan)-------------------------------------------------------------
    config_text = http_get_text(SCHEMA_BINDING_CONFIG_URL)
    config = yaml.safe_load(config_text) or {}

    file_schema_bindings = (config.get("schema_bindings", {}) or {}).get("file_based", {}) or {}
    record_schema_bindings = (config.get("schema_bindings", {}) or {}).get("record_based", {}) or {}

    #Load record sets and validation information-------------------------------------------------------------------
    files = data_frames_from_config(file_schema_bindings)
    if not files.empty:
        files = files[files["HTAN_Center"].isin(phase2_centers["HTAN_Center"])].copy()
        split_cols = files["Folder_Source_Path"].fillna("").astype(str).str.split("/", expand=True)
        while split_cols.shape[1] < 4:
            split_cols[split_cols.shape[1]] = None
        files[["Status_Folder_Name", "SubFolder_Layer1", "SubFolder_Layer2", "SubFolder_Layer3"]] = split_cols.iloc[:, :4]
        files = files.merge(Component, on="Folder_EntityId", how="left")

    #Load BQ Table----------------------------------------------------------------------------------------
    load_bq(
        client,
        HTAN_BQ_PROJECT,
        MEDALLION_LAYER,
        "raw_INDEXING_TABLE_All_Files_Annotation_Fileview_Source",
        files)

    #Load record sets and validation information----------------------------------------------------------------------------------------
    records = data_frames_from_config(record_schema_bindings)
    if not records.empty:
        records = records[records["HTAN_Center"].isin(phase2_centers["HTAN_Center"])].copy()
        split_cols = records["Folder_Source_Path"].fillna("").astype(str).str.split("/", expand=True)
        while split_cols.shape[1] < 3:
            split_cols[split_cols.shape[1]] = None
        records[["Status_Folder_Name", "SubFolder_Layer1", "SubFolder_Layer2"]] = split_cols.iloc[:, :3]
        records = records.merge(Component, on="Folder_EntityId", how="left")
        records.rename(columns={"Annotation_EntityId": "Record_EntityId"}, inplace=True)
        
        #Load BQ Table----------------------------------------------------------------------------------------
        load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            "raw_INDEXING_TABLE_All_Records_Annotation_Source",
            records)
        
        #Add more information to the same table and push as another table with similar but distinct information
        records["Number_Valid_Rows"] = None
        records["Total_Rows"] = None
        records["Manifest_Percent_Valid"] = None
        records["Manifest_Version"] = None
        records["Modified_On"] = None
        
        for idx, row in records.iterrows():
            record_set_id = row.get("Record_EntityId")
        
            if pd.isna(record_set_id):
                continue
        
            try:
                rs = syn.get(record_set_id, downloadFile=False)
                version = rs.get("versionLabel")
                date_modified = rs.get("modifiedOn")
                validation_summary = getattr(rs, "validationSummary", None) or {}
        
                number_valid = validation_summary.get("numberOfValidChildren")
                total_rows = validation_summary.get("totalNumberOfChildren")
        
                records.at[idx, "Number_Valid_Rows"] = number_valid
                records.at[idx, "Total_Rows"] = total_rows
                records.at[idx, "Version_Label"] = version
                records.at[idx, "Modified_On"] = date_modified
        
                if pd.notna(total_rows) and total_rows != 0 and pd.notna(number_valid):
                    records.at[idx, "Manifest_Percent_Valid"] = (number_valid / total_rows) * 100
        
            except Exception as e:
                print(f"Failed {record_set_id}: {e}")
        
        record_subset_schema_status_df = schema_status_df[["Folder_EntityId","Bound_Schema_Name", "Schema_Version"]]
        records = records.merge(record_subset_schema_status_df, on="Folder_EntityId", how="left")
        
        #Load BQ Table----------------------------------------------------------------------------------------
        load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            "raw_INDEXING_TABLE_All_RecordSets_With_Validation_Status",
            records)
        
        #EXCEPTION TABLE FOR RELEASE----------------------------------------------------------------------------------------
        url = "https://docs.google.com/spreadsheets/d/1Gidm_ecocokvPQCw9Laz0ITB9FvIxd-v6-CIjyjtKz0/export?format=csv&gid=0"
        # Skip the header lines in the doc.
        bypass_table = pd.read_csv(url, skiprows=5)
        #Load to BQ
        load_bq(
            client,
            HTAN_BQ_PROJECT,
            MEDALLION_LAYER,
            "raw_INDEXING_TABLE_All_Bypass_Validation_Table",
            bypass_table)

if __name__ == "__main__":
    main()
