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
Updated: 2026-07-27
"""
from __future__ import annotations

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import yaml
from client_load import init_bq_client, init_synapse_client, load_bq
from synapseclient import EntityViewSchema, EntityViewType

# --------------------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------------------
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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

EXCLUDED_PROJECTS_REGEX = r"(HTAN2_BQDEVPROJECT|htan2-testing1)"

# --------------------------------------------------------------------------------------
# Logging Setup
# --------------------------------------------------------------------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper())
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------
def http_get_text(url: str, timeout: int = HTTP_TIMEOUT) -> str:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

def syn_rest_get(syn, path: str) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(1, SYNAPSE_RETRIES + 1):
        try:
            return syn.restGET(path)
        except Exception as e:
            last_err = e
            sleep_s = SYNAPSE_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            time.sleep(sleep_s)
    raise last_err  # type: ignore[misc]

def data_frames_from_config(binding_dictionary: Dict[str, Any]) -> pd.DataFrame:
    project_rows: List[Dict[str, Any]] = []
    for _, value in binding_dictionary.items():
        for project in value.get("projects", []):
            project_rows.append({
                "HTAN_Center": project.get("name"),
                "Folder_EntityId": project.get("synapse_id"),
                "Annotation_EntityId": project.get("fileview_id"),
                "Folder_Source_Path": project.get("subfolder"),
            })
    return pd.DataFrame(project_rows)

def ensure_entity_view(syn, project_id: str, project_name: str, entity_type: EntityViewType, parent_id: str) -> Optional[str]:
    try:
        view_name = f"{project_name}_{entity_type.name.capitalize()}View"
        view = EntityViewSchema(
            name=view_name,
            parent=parent_id,
            scopes=[project_id],
            includeEntityTypes=[entity_type],
            addDefaultViewColumns=True,
            addAnnotationColumns=False,
        )
        view = syn.store(view)
        log.info("Touched %sView for %s: %s", entity_type.name, project_name, view.id)
        return view.id
    except Exception as e:
        log.exception("%sView failed for %s (%s): %s", entity_type.name, project_name, project_id, e)
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

def get_validation_summary(syn, entity_id: str) -> Dict[str, Any]:
    try:
        val = syn_rest_get(syn, f"/entity/{entity_id}/schema/validation")
        return {
            "is_valid": val.get("isValid", True),
            "validation_error_message": val.get("validationErrorMessage", "") if not val.get("isValid", True) else "",
            "all_validation_messages": val.get("allValidationMessages", "") if not val.get("isValid", True) else "",
            "validated_on": val.get("validatedOn", ""),
        }
    except Exception as e:
        return {
            "is_valid": None,
            "validation_error_message": f"Validation lookup failed: {str(e)}",
            "all_validation_messages": "",
            "validated_on": "",
        }

def collect_all_fileviews(syn, phase2_centers: pd.DataFrame, view_col: str = "Fileview_EntityId", max_workers: int = MAX_WORKERS) -> pd.DataFrame:
    dfs: List[pd.DataFrame] = []
    BASE_COLUMNS = [
        "id", "name", "parentId", "projectId", "createdOn", "createdBy",
        "modifiedOn", "modifiedBy", "etag", "path", "type", "currentVersion",
        "dataFileHandleId", "dataFileName", "dataFileSizeBytes", "dataFileMD5Hex",
        "dataFileConcreteType", "dataFileBucket", "dataFileKey", "benefactorId", "description",
    ]

    view_ids = phase2_centers.get(view_col, pd.Series(dtype="object")).dropna().astype(str).unique().tolist()

    for view_id in view_ids:
        try:
            log.info("Querying base file metadata from %s", view_id)
            query = f"SELECT {', '.join(BASE_COLUMNS)} FROM {view_id}"
            df = syn.tableQuery(query).asDataFrame()
            df["source_fileview"] = view_id

            if not df.empty:
                file_ids = df["id"].tolist()
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    validation_results = list(executor.map(lambda eid: get_validation_summary(syn, str(eid)), file_ids))

                df["is_valid"] = [r.get("is_valid") for r in validation_results]
                df["validation_error_message"] = [r.get("validation_error_message") for r in validation_results]
                df["all_validation_messages"] = [r.get("all_validation_messages") for r in validation_results]
                df["validated_on"] = [r.get("validated_on") for r in validation_results]

            dfs.append(df)
        except Exception as e:
            log.exception("Failed fileview query/validation for %s: %s", view_id, e)

    final_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    return final_df.rename(columns={
        "is_valid": "Is_Valid",
        "validation_error_message": "Validation_Error_Message",
        "all_validation_messages": "All_Validation_Error_Messages",
        "validated_on": "Validated_On",
    })

def load_center_liaisons() -> pd.DataFrame:
    if CENTER_LIAISONS_URL:
        text = http_get_text(CENTER_LIAISONS_URL)
        data = yaml.safe_load(text)
    else:
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

def fetch_folder_schema(syn, item: Dict[str, Any]) -> Dict[str, Any]:
    folder_id = item["Folder_EntityId"]
    try:
        binding = syn_rest_get(syn, f"/entity/{folder_id}/schema/binding")
        schema_info = binding.get("jsonSchemaVersionInfo", {}) or {}
        item["Bound_Schema_Name"] = schema_info.get("$id", "")
        item["is_error"] = False
    except Exception as e:
        item["Error"] = str(e)
        item["is_error"] = True
    return item

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main() -> None:
    syn = init_synapse_client()
    client = init_bq_client()

    #Fetch & Filter Projects
    all_centers = fetch_all_projects(syn)
    if all_centers.empty or "name" not in all_centers.columns:
        raise RuntimeError("Synapse /projects returned no results or missing expected fields.")

    phase2_centers = all_centers[
        all_centers["name"].fillna("").str.contains(r"HTAN2_", case=False) &
        ~all_centers["name"].fillna("").str.contains(EXCLUDED_PROJECTS_REGEX, case=False, regex=True)
    ].copy()

    phase2_centers = phase2_centers.rename(columns={
        "name": "HTAN_Center",
        "id": "Project_EntityId",
        "lastActivity": "Last_Activity",
        "modifiedOn": "Modified_On",
        "modifiedBy": "Modified_By",
    })

    #Attach Views & Fetch Item Counts
    for i, row in phase2_centers.iterrows():
        project_id, project_name = str(row["Project_EntityId"]), str(row["HTAN_Center"])
        phase2_centers.at[i, "Folderview_EntityId"] = ensure_entity_view(syn, project_id, project_name, EntityViewType.FOLDER, HTAN_DEV)
        phase2_centers.at[i, "Fileview_EntityId"] = ensure_entity_view(syn, project_id, project_name, EntityViewType.FILE, HTAN_DEV)

    phase2_centers["Current_Total_Files"] = phase2_centers["Fileview_EntityId"].apply(lambda v: count_view_rows(syn, v, "FileView"))
    phase2_centers["Current_Total_Folders"] = phase2_centers["Folderview_EntityId"].apply(lambda v: count_view_rows(syn, v, "FolderView"))

    contacts_df = load_center_liaisons()
    if not contacts_df.empty and "HTAN_Center" in contacts_df.columns:
        phase2_centers = phase2_centers.merge(contacts_df, on="HTAN_Center", how="left")

    load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Source_Phase2_Centers", phase2_centers)

    #Collect File Views
    big_fileview_df = collect_all_fileviews(syn, phase2_centers, view_col="Fileview_EntityId")
    big_fileview_df = big_fileview_df.rename(columns={
        "id": "File_EntityId", "parentId": "Folder_EntityId", "projectId": "Synapse_Project_EntityId",
        "benefactorId": "Benefactor_EntityId", "description": "Description", "type": "Entity_Type",
        "path": "Path", "createdOn": "Created_On", "createdBy": "Created_By", "modifiedOn": "Modified_On",
        "modifiedBy": "Modified_By", "etag": "Etag", "currentVersion": "Current_Version",
        "dataFileHandleId": "File_Handle_Id", "dataFileName": "File_Name", "dataFileSizeBytes": "File_Size_Bytes",
        "dataFileMD5Hex": "File_MD5", "dataFileConcreteType": "File_Handle_Type", "dataFileBucket": "S3_Bucket",
        "dataFileKey": "S3_Key", "source_fileview": "Source_Fileview",
    })

    #Folder Schema Processing (Parallelized)
    folder_items_to_check = []
    for _, row in phase2_centers.iterrows():
        folder_view_id = row.get("Folderview_EntityId")
        if not folder_view_id or pd.isna(folder_view_id):
            continue

        project_name, project_id = str(row.get("HTAN_Center", "")), str(row.get("Project_EntityId", ""))
        try:
            folder_df = syn.tableQuery(f"SELECT * FROM {folder_view_id}").asDataFrame()
        except Exception as e:
            log.warning("Folder view query failed for %s (%s): %s", project_name, folder_view_id, e)
            continue

        if "path" in folder_df.columns:
            folder_df["status_folder"] = folder_df["path"].apply(
                lambda p: p.split("/")[1] if isinstance(p, str) and project_name and p.startswith(f"{project_name}/") else None
            )
        else:
            folder_df["status_folder"] = None

        for _, folder_row in folder_df.iterrows():
            folder_id = str(folder_row.get("id", ""))
            if folder_id:
                folder_items_to_check.append({
                    "HTAN_Center": project_name,
                    "Project_EntityId": project_id,
                    "Folder_EntityId": folder_id,
                    "Folder_Name": folder_row.get("name"),
                    "Status_Folder_Name": folder_row.get("status_folder"),
                })

    results, error_results = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_folder_schema, syn, item) for item in folder_items_to_check]
        for future in as_completed(futures):
            res = future.result()
            if res.pop("is_error", False):
                error_results.append(res)
            else:
                results.append(res)

    schema_status_df = pd.DataFrame(results)
    schema_errors_df = pd.DataFrame(error_results)

    #Schema Normalization
    if not schema_status_df.empty and "Bound_Schema_Name" in schema_status_df.columns:
        parts = schema_status_df["Bound_Schema_Name"].fillna("").astype(str).str.split("-", n=2, expand=True)
        schema_status_df["Component"] = parts[1] if parts.shape[1] > 1 else None

        schema_status_df["Component"] = schema_status_df["Component"].apply(
            lambda c: [f"{m.group(1)}{d}" for d in m.group(2)] if (isinstance(c, str) and (m := re.match(r"^(.*?)(\d+)$", c))) else [c]
        )
        schema_status_df["Schema_Version"] = schema_status_df["Bound_Schema_Name"].str.extract(r"(\d+\.\d+\.\d+)$")
        schema_status_df = schema_status_df.explode("Component").reset_index(drop=True)

        #Standardize Component Names
        replace_map = {"BiospecimenData": "Biospecimen", "DigitalPathologyData": "DigitalPathology"}
        schema_status_df["Component"] = schema_status_df["Component"].replace(replace_map)

        Component = schema_status_df[["Folder_EntityId", "Component"]].drop_duplicates()
    else:
        Component = pd.DataFrame(columns=["Folder_EntityId", "Component"])

    load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Folders_With_Bound_Schemas", schema_status_df)
    load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Folders_Without_Bound_Schemas", schema_errors_df)

    #Merge & Push Validation Files
    if not big_fileview_df.empty and not schema_status_df.empty:
        subset_schema_status_df = schema_status_df[["Folder_EntityId", "Status_Folder_Name", "Component", "Bound_Schema_Name", "Schema_Version"]].drop_duplicates()
        big_fileview_df = big_fileview_df.merge(subset_schema_status_df, on="Folder_EntityId", how="inner")

    load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Files_With_Validation_Status", big_fileview_df)

    #YAML Config Schemas
    config = yaml.safe_load(http_get_text(SCHEMA_BINDING_CONFIG_URL)) or {}
    file_bindings = (config.get("schema_bindings", {}) or {}).get("file_based", {}) or {}
    record_bindings = (config.get("schema_bindings", {}) or {}).get("record_based", {}) or {}

    #File Schemas
    files = data_frames_from_config(file_bindings)
    if not files.empty:
        files = files[files["HTAN_Center"].isin(phase2_centers["HTAN_Center"])].copy()
        split_cols = files["Folder_Source_Path"].fillna("").astype(str).str.split("/", expand=True)
        for col_idx in range(4):
            files[f"SubFolder_Layer{col_idx}"] = split_cols[col_idx] if col_idx < split_cols.shape[1] else None
        files = files.rename(columns={"SubFolder_Layer0": "Status_Folder_Name"})
        files = files.merge(Component, on="Folder_EntityId", how="left")
        load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Files_Annotation_Fileview_Source", files)

    # Record Schemas
    records = data_frames_from_config(record_bindings)
    if not records.empty:
        records = records[records["HTAN_Center"].isin(phase2_centers["HTAN_Center"])].copy()
        split_cols = records["Folder_Source_Path"].fillna("").astype(str).str.split("/", expand=True)
        for col_idx in range(3):
            records[f"SubFolder_Layer{col_idx}"] = split_cols[col_idx] if col_idx < split_cols.shape[1] else None
        records = records.rename(columns={"SubFolder_Layer0": "Status_Folder_Name", "Annotation_EntityId": "Record_EntityId"})
        records = records.merge(Component, on="Folder_EntityId", how="left")

        load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Records_Annotation_Source", records)

        # Record Validation Worker Function
        def fetch_record_details(record_id: str) -> Dict[str, Any]:
            if pd.isna(record_id):
                return {}
            try:
                rs = syn.get(record_id, downloadFile=False)
                summary = getattr(rs, "validationSummary", None) or {}
                valid_num = summary.get("numberOfValidChildren")
                total_num = summary.get("totalNumberOfChildren")
                pct_valid = (valid_num / total_num * 100) if (total_num and valid_num is not None) else None
                return {
                    "Number_Valid_Rows": valid_num,
                    "Total_Rows": total_num,
                    "Version_Label": rs.get("versionLabel"),
                    "Modified_On": rs.get("modifiedOn"),
                    "Manifest_Percent_Valid": pct_valid,
                }
            except Exception as e:
                log.warning("Failed record download %s: %s", record_id, e)
                return {}

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            record_details = list(executor.map(fetch_record_details, records["Record_EntityId"].tolist()))

        details_df = pd.DataFrame(record_details)
        records = pd.concat([records.reset_index(drop=True), details_df.reset_index(drop=True)], axis=1)

        if not schema_status_df.empty:
            record_subset_schema_status_df = schema_status_df[["Folder_EntityId", "Bound_Schema_Name", "Schema_Version"]].drop_duplicates()
            records = records.merge(record_subset_schema_status_df, on="Folder_EntityId", how="left")

        load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_RecordSets_With_Validation_Status", records)

    #Load Bypass Table
    bypass_url = "https://docs.google.com/spreadsheets/d/1Gidm_ecocokvPQCw9Laz0ITB9FvIxd-v6-CIjyjtKz0/export?format=csv&gid=0"
    bypass_table = pd.read_csv(bypass_url, skiprows=5)
    load_bq(client, HTAN_BQ_PROJECT, MEDALLION_LAYER, "raw_INDEXING_TABLE_All_Bypass_Validation_Table", bypass_table)

if __name__ == "__main__":
    main()
