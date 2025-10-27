#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTAN BQ-Hash Generator: Synapse to Raw

Configurations:
    You need Google BigQuery credentials with permission to write to the htan2-dcc project. 

Functions:
    - main()
    
Author: Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>
Date Created: 10-21-2025
Date Updated: 10-22-2025
"""
from google.cloud import bigquery
import hashlib
import base64
from google.cloud.exceptions import NotFound
from client_load import load_bq

def table_exists(project_id: str, dataset_id: str, table_id: str) -> bool:
    """Return True if the BigQuery table exists, else False."""
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        client.get_table(table_ref)  # Make an API request
        return True
    except NotFound:
        return False


def mint_bq_hash(htan_id: str, synapse_id: str, namespace: str = "HTAN", version: str = "v1", length: int = 16) -> str:
    """
    Generate a stable, public hash ID for BigQuery dataflow,
    using both HTAN ID and Synapse entity ID as inputs.

    Args:
        htan_id (str): HTAN_Data_File_ID (or equivalent).
        synapse_id (str): Synapse entityId (e.g., 'syn12345678').
        namespace (str): Logical grouping to avoid cross-system collisions.
        version (str): Optional version to allow future algorithm updates.
        length (int): Output length (in characters) for compact, unique IDs.

    Returns:
        str: URL-safe Base64 encoded, deterministic hash string.
    """
    # Canonicalize and combine the input identifiers
    combined = f"{htan_id}|{synapse_id}".strip()
    payload = f"{namespace}|{version}|{combined}".encode("utf-8")

    # Compute deterministic SHA-256 digest
    digest = hashlib.sha256(payload).digest()

    # Encode as URL-safe Base64 and truncate
    token = base64.urlsafe_b64encode(digest).decode("utf-8").rstrip("=")
    return token[:length]

def main():
    #Login to BQ
    client = bigquery.Client()
    #Fetch all current annotated files
    annotated_files = client.query("""SELECT project_id, entity_id, key, value, name  FROM `htan2-dcc.htan2_synapse_raw.raw_METADATA_annotations`,
                                   UNNEST(annotations) AS annotation_value
                                   WHERE key = 'HTAN_Data_File_ID' """).result().to_dataframe()
    annotated_files = annotated_files.explode("value", ignore_index=True)
    #Apply hashing
    annotated_files["BQ_Hash"] = annotated_files.apply(lambda row: mint_bq_hash(row["value"], row["entity_id"]),axis=1)
    #Post hashing table
    if table_exists("htan2-dcc", "htan2_synapse_raw", "raw_INDEXING_hash_minting_table"):
        print("Table exists! Minting new values.")
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
        job = client.load_table_from_dataframe(annotated_files, "htan2-dcc.htan2_synapse_raw.raw_INDEXING_hash_minting_table", job_config=job_config)
        job.result()  # Wait for completion

    else:
        load_bq(client, 'htan2-dcc', 'htan2_synapse_raw', 'raw_INDEXING_hash_minting_table', annotated_files)
