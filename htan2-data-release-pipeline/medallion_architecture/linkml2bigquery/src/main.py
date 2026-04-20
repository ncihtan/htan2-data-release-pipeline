#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Pulling the Data Model

    This module handles the retrieval, normalization, and caching of HTAN2
    Data Model versions from GitHub into BigQuery. It ensures that all
    available schema versions are stored in a centralized location for
    downstream validation and processing in the SILVER layer.

Configurations: None

Functions:

    - normalize_github_version(v)
    - normalize_bq_version(table_id)
    - init_bq_client()
    - load_bq(client, project, dataset, table, data, 
        schema=None, write_mode="truncate")
    - main()

Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 04-16-2026
Date Updated: 
Modified By:  
"""

import re
from google.cloud import bigquery
from model_load import (
    get_latest_model,
    download_model,
    convert_json_to_df
)

#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################

PROJECT = "htan2-dcc"
DATASET = "htan2_data_model_cache"

#####################################################
#                 HELPER FUNCTIONS
#####################################################

def normalize_github_version(v):
    """
    Normalize GitHub version names to BigQuery version names
    (e.g. v0.0.0 --> 0_0_0).

    Args:
        - v (str): Data model version from GitHub folder name.

    Returns:
        - (str): Normalized version name.
    """
    return v.lstrip("v").replace(".", "_")

def normalize_bq_version(table_id):
    """
    Normalize BigQuery version names (e.g. 
    HTAN2_Data_Model_v0_0_0 --> 0_0_0).
    
    Args:
        - table_id (str): BigQuery table name.

    Returns:
        - (str): Normalized BigQuery name.
    """
    match = re.search(r'v(\d+_\d+_\d+)', table_id)
    return match.group(1) if match else None

def init_bq_client():
    """
    Initialize and return the BigQuery client.

    """
    return bigquery.Client()

def load_bq(client, project, dataset, table, data, schema=None, write_mode="truncate"):
    """
    Load table into BigQuery.

    Args:
        - client (BigQuery instance): BigQuery client object
        - project (str): GCP project
        - dataset (str): GCP dataset name
        - table (str): GCP table name
        - data (pandas.DataFrame): Data to be loaded to BigQuery
        - schema (dict, optional): BigQuery table schema
        - write_mode (str, optional): "truncate", "append", or "empty"
    """

    table_bq = f"{project}.{dataset}.{table}"
    print(f"Loading {table_bq} to BigQuery (mode={write_mode})")

    # Clean column names
    data.columns = data.columns.str.replace('[^0-9a-zA-Z]+', '_', regex=True)

    # Default schema
    if schema is None:
        schema = [bigquery.SchemaField(name, 'STRING') for name in data.columns]

    # Map write mode → BigQuery disposition
    write_map = {
        "truncate": "WRITE_TRUNCATE",
        "append": "WRITE_APPEND",
        "empty": "WRITE_EMPTY"
    }

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=write_map.get(write_mode, "WRITE_TRUNCATE"),
        autodetect=False,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        source_format=bigquery.SourceFormat.CSV
    )

    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )

    job.result()  # wait for completion
    print(f"Loaded {len(data)} rows into {table_bq}")

#####################################################
#                       MAIN
#####################################################

def main():
    """
    Caches HTAN2 Data Model release on htan2-dcc BigQuery project.
    """

    # Initialize BigQuery client
    client = init_bq_client()

    # Get all version of the data model on GitHub
    github_data_models = get_latest_model(mode="all")
    github_versions = {
        normalize_github_version(v) for v in github_data_models
    }

    # Get all version of the data model from BigQuery
    data_model_tables = list(client.list_tables(f"{PROJECT}.{DATASET}"))
    bq_versions = {
        normalize_bq_version(table.table_id)
        for table in data_model_tables
        if table.table_id.startswith("HTAN2_Data_Model_")
    }

    missing_in_bq = github_versions - bq_versions

    if missing_in_bq:
        for version in missing_in_bq:

            # Normalize to GitHub
            github_model_version = f"v{version.replace("_", ".")}"

            data = download_model(github_model_version)
            tabular_data_model = convert_json_to_df(data)

            # Add a column with Data Model Version
            tabular_data_model["Schema_Version"] = github_model_version

            # Push data model dictionary to BQ
            load_bq(
                client,
                PROJECT,
                DATASET,
                f"HTAN2_Data_Model_v{version}",
                tabular_data_model
            )

if __name__ == "__main__":
    main()
