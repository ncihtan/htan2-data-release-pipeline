#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gold layer helper functions.
"""
import re
import numpy as np
import pandas as pd
from google.cloud.exceptions import NotFound


def print_sub_section(title: str) -> None:
    """
    Print subsection headers.

    Args:
        title (str): The title to be printed.
    """
    border = "=" * (len(title) + 8)
    print(f"\n{border}\n>>> {title.upper()} <<<\n{border}\n")


def query_bigquery_table(client, project_id: str, dataset_id: str, table_id: str) -> pd.DataFrame:
    """
    Get an entire table from BigQuery as a Pandas DataFrame.

    Args:
        client: A BigQuery client instance.
        project_id (str): BigQuery project name.
        dataset_id (str): BigQuery dataset name.
        table_id (str): BigQuery table name.

    Returns:
        pd.DataFrame: The BigQuery table as a DataFrame.
    """
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    print(query)
    return client.query(query).to_dataframe()


def fetch_table(
    client,
    project: str,
    raw_dataset: str,
    bronze_dataset: str,
    silver_dataset: str,
    gold_dataset: str,
    table_id: str,
    dm_dataset: str | None = None,
) -> pd.DataFrame:
    """Resolves the dataset from table_id prefix using the provided dataset variables."""
    prefix = table_id.split("_", 1)[0].lower()

    if prefix == "raw":
        dataset = raw_dataset
    elif prefix == "bronze":
        dataset = bronze_dataset
    elif prefix == "silver":
        dataset = silver_dataset
    elif prefix == "gold":
        dataset = gold_dataset
    elif prefix == "dm" and dm_dataset is not None:
        dataset = dm_dataset
    else:
        raise ValueError(f"Unknown or unconfigured dataset prefix '{prefix}' in table_id: '{table_id}'")

    return query_bigquery_table(client, project, dataset, table_id)


def apply_age_masking(
    df: pd.DataFrame,
    client,
    project: str,
    dataset: str,
    table_id: str,
    max_age_days: int,
) -> pd.DataFrame:
    """Applies age masking to specific age columns flagged in silver layer error messages."""
    age_cols = [c for c in df.columns if "AGE_IN_" in c]
    if not age_cols:
        return df

    silver_tid = table_id.replace("bronze_", "silver_")

    # Query matching AGE_OVER_90, etc.
    age_masking_query = f"""
        SELECT BQ_Hash_Record_ID, Release_Error_Messages
        FROM `{project}.{dataset}.{silver_tid}`
        WHERE Release_Error_Messages LIKE "%AGE_OVER_90%"
    """
    age_masking_results = client.query(age_masking_query).to_dataframe()

    # Track whether any age column was obfuscated for a given record
    df["AGE_IS_OBFUSCATED"] = False

    if not age_masking_results.empty and "BQ_Hash_Record_ID" in age_masking_results.columns:
        # Stringify error messages to support list/dict structures returned by BigQuery
        age_masking_results["msg_str"] = age_masking_results["Release_Error_Messages"].astype(str)

        for col in age_cols:
            # Word boundaries (\b) prevent partial matching on similar column names
            pattern = rf"\b{re.escape(col)}\b"
            matching_ids = set(
                age_masking_results[
                    age_masking_results["msg_str"].str.contains(pattern, regex=True, na=False)
                ]["BQ_Hash_Record_ID"].dropna()
            )

            if matching_ids:
                col_mask = df["BQ_Hash_Record_ID"].isin(matching_ids)

                # Check for string dtype to prevent pyarrow string assignment crash
                val = str(max_age_days) if pd.api.types.is_string_dtype(df[col]) else max_age_days

                # Overwrite ONLY the matching column for identified records
                df.loc[col_mask, col] = val
                df.loc[col_mask, "AGE_IS_OBFUSCATED"] = True

    return df


def derive_age_in_months(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived variables for age in months.
    https://docs.google.com/document/d/1Zs3vpMUxcbpbggOkxKkXzaFQvcuCZalwn0uv2ruUeQQ/edit?tab=t.0
    """
    for col in [c for c in df.columns if "AGE_IN_" in c]:
        new_col = col.replace("AGE_IN_DAYS_", "AGE_IN_MONTHS_APPROXIMATED_")

        # Duplicates the column, inheriting the same PyArrow string format if it was a string
        df[new_col] = df[col]

        numeric_age = pd.to_numeric(df[col], errors="coerce")
        is_positive = numeric_age > 0

        # Calculate the integer ages
        calculated_ages = ((numeric_age[is_positive] - 1) * 12 / 365).astype(int)

        # Convert int ages to string if the target column is string-typed
        if pd.api.types.is_string_dtype(df[new_col]):
            df.loc[is_positive, new_col] = calculated_ages.astype(str)
        else:
            df.loc[is_positive, new_col] = calculated_ages

    return df


def sync_pinned_release_versions(
    client,
    project: str,
    raw_dataset: str,
    releases_list: pd.DataFrame,
    ready_files: pd.DataFrame,
    load_bq_fn,
    target_table_name: str = "raw_INDEXING_TABLE_Exclusion_List_Files_Pinned_Version",
) -> tuple[int, dict, dict, dict]:
    """
    Identifies the latest release version, pins newly ready files in BigQuery,
    and returns the latest release number alongside lookup maps for biospecimens,
    participants, and files.

    Args:
        client: BigQuery client instance.
        project (str): BigQuery project ID.
        raw_dataset (str): BigQuery raw dataset ID.
        releases_list (pd.DataFrame): DataFrame containing 'Status_Folder_Name'.
        ready_files (pd.DataFrame): Pre-filtered DataFrame of files ready for release.
        load_bq_fn (callable): Function used to load DataFrames into BigQuery.
        target_table_name (str): Target BigQuery table name for pinned versions.

    Returns:
        tuple[int, dict, dict, dict]: (latest_release, bio_map, part_map, file_map).
    """
    # Get all releases and identify the latest release
    releases_df = releases_list.copy()
    releases_df["Version_Number"] = pd.to_numeric(
        releases_df["Status_Folder_Name"].str.extract(r"v(\d+)", expand=False),
        errors="coerce",
    ).astype("Int64")

    latest_release = int(releases_df["Version_Number"].max())
    version_list = [
        f"v{latest_release}_ingest",
        f"v{latest_release}_staging",
        f"v{latest_release}_release",
    ]

    columns = [
        "HTAN_Center",
        "File_EntityId",
        "File_Name",
        "HTAN_PARTICIPANT_ID",
        "HTAN_BIOSPECIMEN_ID",
        "Release_Version",
    ]

    # Check if the pinned version tracker table exists
    try:
        release_tracker_existing = query_bigquery_table(
            client, project, raw_dataset, target_table_name
        )
        if not release_tracker_existing.empty and "File_EntityId" in release_tracker_existing.columns:
            release_tracker_existing["File_EntityId"] = (
                release_tracker_existing["File_EntityId"].astype(str).str.strip()
            )
    except NotFound:
        release_tracker_existing = pd.DataFrame(columns=columns)
        load_bq_fn(
            client,
            project,
            raw_dataset,
            target_table_name,
            release_tracker_existing,
            write_disposition="WRITE_APPEND",
        )

    # Prepare ready files and append any new File_EntityIds to the tracker
    ready_release = ready_files.copy()
    ready_release = ready_release.rename(
        columns={"HTAN_ORIGINATING_BIOSPECIMEN_ID": "HTAN_BIOSPECIMEN_ID"}
    )

    if not ready_release.empty and "File_EntityId" in ready_release.columns:
        ready_release["File_EntityId"] = ready_release["File_EntityId"].astype(str).str.strip()
        ready_release = ready_release.drop_duplicates(subset=["File_EntityId"])

        if not release_tracker_existing.empty and "File_EntityId" in release_tracker_existing.columns:
            existing_ids = set(release_tracker_existing["File_EntityId"].dropna())
            ready_release = ready_release[~ready_release["File_EntityId"].isin(existing_ids)]

        if not ready_release.empty:
            matching_cols = [
                c for c in columns if c in ready_release.columns and c != "Release_Version"
            ]
            release_tracker = ready_release[matching_cols].copy()

            for col in columns:
                if col not in release_tracker.columns and col != "Release_Version":
                    release_tracker[col] = None

            release_tracker["Release_Version"] = [version_list] * len(release_tracker)
            release_tracker = release_tracker[columns]

            load_bq_fn(
                client,
                project,
                raw_dataset,
                target_table_name,
                release_tracker,
            )

            release_tracker_existing = pd.concat(
                [release_tracker_existing, release_tracker], ignore_index=True
            )

    # Establish lookup maps from the release tracker
    tracker_clean = release_tracker_existing.dropna(subset=["Release_Version"])

    bio_map = (
        tracker_clean.dropna(subset=["HTAN_BIOSPECIMEN_ID"])
        .set_index("HTAN_BIOSPECIMEN_ID")["Release_Version"]
        .to_dict()
    )
    part_map = (
        tracker_clean.dropna(subset=["HTAN_PARTICIPANT_ID"])
        .set_index("HTAN_PARTICIPANT_ID")["Release_Version"]
        .to_dict()
    )
    file_map = (
        tracker_clean.dropna(subset=["File_EntityId"])
        .set_index("File_EntityId")["Release_Version"]
        .to_dict()
    )

    return latest_release, bio_map, part_map, file_map

def match_status_in_map(df, id_col, entity_map):
    if id_col not in df.columns or df.empty:
        return pd.Series(False, index=df.index)
    return pd.Series(
        [pd.notna(s) and f"'{s}'" in str(entity_map.get(val, '')) for val, s in zip(df[id_col], df["Status_Folder_Name"])],
        index=df.index
    )


def extract_panel_and_file_slice(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame | None, pd.DataFrame]:
    """
    Extracts panel metadata (if `HTAN_PANEL_ID` exists) and returns a standardized
    file metadata slice with `HTAN_PANEL_ID` guaranteed as a column.
    """
    panel_slice = None
    if "HTAN_PANEL_ID" in df.columns:
        panel_cols = [c for c in ["HTAN_PANEL_ID", "Component", "File_EntityId"] if c in df.columns]
        panel_slice = df[panel_cols].copy()

    base_cols = [
        "File_Name",
        "File_EntityId",
        "HTAN_Center",
        "Status_Folder_Name",
        "BQ_Hash_ID",
        "Component",
        "Schema_Version",
        "HTAN_DATA_FILE_ID",
    ]
    file_slice = df.reindex(columns=base_cols + ["HTAN_PANEL_ID"]).copy()
    return panel_slice, file_slice


def get_valid_provenance_ids(
    bronze_prov: pd.DataFrame, file_ids: pd.Series
) -> tuple[set, set]:
    """
    Subsets `bronze_prov` to `file_ids` and returns sets of valid
    (HTAN_PARTICIPANT_ID, HTAN_BIOSPECIMEN_ID).
    """
    prov = bronze_prov[bronze_prov["File_EntityId"].isin(file_ids)].rename(
        columns={"HTAN_ORIGINATING_BIOSPECIMEN_ID": "HTAN_BIOSPECIMEN_ID"}
    )
    valid_parts = (
        set(prov["HTAN_PARTICIPANT_ID"].dropna())
        if "HTAN_PARTICIPANT_ID" in prov.columns
        else set()
    )
    valid_bios = (
        set(prov["HTAN_BIOSPECIMEN_ID"].dropna())
        if "HTAN_BIOSPECIMEN_ID" in prov.columns
        else set()
    )
    return valid_parts, valid_bios


def filter_records_by_provenance(
    df: pd.DataFrame, valid_parts: set, valid_bios: set
) -> pd.DataFrame:
    """
    Filters a record DataFrame to rows whose participant or biospecimen ID is
    associated with valid files.
    """
    has_part = "HTAN_PARTICIPANT_ID" in df.columns
    has_bio = "HTAN_BIOSPECIMEN_ID" in df.columns

    if has_part and has_bio:
        return df[
            df["HTAN_PARTICIPANT_ID"].isin(valid_parts)
            | df["HTAN_BIOSPECIMEN_ID"].isin(valid_bios)
        ]
    if has_part:
        return df[df["HTAN_PARTICIPANT_ID"].isin(valid_parts)]
    if has_bio:
        return df[df["HTAN_BIOSPECIMEN_ID"].isin(valid_bios)]
    return df


def concat_or_empty(dfs: list[pd.DataFrame], fallback_columns: list[str]) -> pd.DataFrame:
    """Concatenates a list of DataFrames, or returns an empty DataFrame with `fallback_columns`."""
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=fallback_columns)