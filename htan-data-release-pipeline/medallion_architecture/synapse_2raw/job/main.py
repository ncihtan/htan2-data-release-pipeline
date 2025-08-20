#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Synapse to Raw
    This module extracts file-level metadata and annotations from the Synapse Snowflake data 
    warehouse and promotes it into the medallion architecture. It authenticates with Snowflake using 
    user credentials, queries the node_latest table for selected projects, normalizes and transforms the 
    annotations into a BigQuery-compatible repeated record schema, and then loads the results into the 
    htan2-dcc BigQuery project under the synapse_raw dataset. This script serves as the ingestion step 
    that converts raw warehouse JSON annotations into structured bronze-level tables for downstream processing.

Configurations:
    To run this script you must provide valid Snowflake credentials (SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, and SNOWFLAKE_PAT) 
    so the connector can authenticate and query the Synapse data warehouse. You also need Google BigQuery 
    credentials with permission to write to the htan2-dcc project. 

Functions:
    - main()
    
Author:       Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>, Adam Taylor <adam.taylor@sagebase.org>
Date Created: 8-20-2025
Date Updated: 8-20-2025
"""
import os
import sys
import json
import logging
from collections.abc import Iterable

import snowflake.connector
from google.cloud import bigquery
import pandas as pd  # noqa: F401 (kept if you log/inspect)
import dotenv
import yaml

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


# -----------------------------------------------------------------------------
# Annotation transformation (FIXED)
# -----------------------------------------------------------------------------
def _to_string_list(val):
    """Coerce any value into a list[str] suitable for BQ REPEATED STRING."""
    # Best-effort unwrap numpy types without hard dependency
    try:
        import numpy as np  # optional
        if isinstance(val, (np.generic,)):
            val = val.item()
        if isinstance(val, np.ndarray):
            val = val.tolist()
    except Exception:
        pass

    # If it's a JSON-looking string, try parsing
    if isinstance(val, str):
        s = val.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                val = json.loads(s)
            except Exception:
                # keep as raw string if parsing fails
                pass

    # Normalize to list
    if isinstance(val, list):
        items = val
    elif isinstance(val, Iterable) and not isinstance(val, (str, bytes, dict)):
        items = list(val)
    elif val is None:
        items = []
    else:
        items = [val]

    # Convert everything to strings
    out = []
    for x in items:
        if x is None:
            continue
        if isinstance(x, (dict, list)):
            out.append(json.dumps(x, ensure_ascii=False))
        else:
            out.append(str(x))
    return out


def transform_annotations(annotations_obj):
    """Transform warehouse annotations into BQ schema:
       REPEATED RECORD<key STRING, type STRING, value REPEATED STRING>.
    """
    try:
        # Allow stringified JSON
        if isinstance(annotations_obj, str):
            try:
                annotations_obj = json.loads(annotations_obj)
            except Exception:
                logging.warning("annotations_obj is a non-JSON string; treating as empty.")
                return []

        if not isinstance(annotations_obj, dict):
            logging.warning(f"Expected dict for annotations_obj but got {type(annotations_obj).__name__}")
            return []

        # Some warehouses nest under "annotations"; others are flat
        anns = annotations_obj.get("annotations", annotations_obj)
        if not isinstance(anns, dict):
            logging.warning("Unexpected annotations structure; treating as empty.")
            return []

        records = []
        for k, v in anns.items():
            # v may be {"type": "...", "value": ...} or may be the raw value
            if isinstance(v, dict) and "value" in v:
                raw_val = v.get("value")
                ann_type = v.get("type", "STRING")
            else:
                raw_val = v
                ann_type = "STRING"

            coerced = _to_string_list(raw_val)
            if len(coerced) == 0:
                continue

            records.append({"key": k, "type": str(ann_type), "value": coerced})
        return records
    except Exception as e:
        logging.warning(f"Failed to transform annotations: {e}")
        return []


# -----------------------------------------------------------------------------
# Snowflake
# -----------------------------------------------------------------------------
def login_to_snowflake():
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    pat = os.getenv("SNOWFLAKE_PAT")  # personal access token/password
    logging.info(f"Using user: {user}, account: {account}")
    if not user or not account or not pat:
        logging.error("Missing SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, or SNOWFLAKE_PAT.")
        sys.exit(1)
    try:
        conn = snowflake.connector.connect(user=user, account=account, password=pat)
        logging.info("Successfully connected to Snowflake using PAT.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)


def run_snowflake_query(conn, query):
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        logging.info("Snowflake query executed successfully.")
        return results
    except Exception as e:
        logging.error(f"Failed to execute Snowflake query: {e}")
        sys.exit(1)
    finally:
        if cursor is not None:
            cursor.close()
        try:
            conn.close()
        except Exception:
            pass


# -----------------------------------------------------------------------------
# BigQuery
# -----------------------------------------------------------------------------
def write_to_bigquery(results, project_id, dataset_id, table_id):
    """Load list[dict] into BigQuery with a fixed schema, truncating the table."""
    try:
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("project_id", "STRING"),
                bigquery.SchemaField("project_name", "STRING"),
                bigquery.SchemaField("entity_id", "STRING"),
                bigquery.SchemaField("name", "STRING"),
                bigquery.SchemaField("component", "STRING"),
                bigquery.SchemaField(
                    "annotations",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("key", "STRING"),
                        bigquery.SchemaField("type", "STRING"),
                        bigquery.SchemaField("value", "STRING", mode="REPEATED"),
                    ],
                ),
                bigquery.SchemaField("annotation_count", "INTEGER"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        # Load via JSON rows
        load_job = client.load_table_from_json(results, table_ref, job_config=job_config)
        load_job.result()
        logging.info(f"Data loaded into BigQuery {project_id}.{dataset_id}.{table_id} successfully.")
    except Exception as e:
        logging.error(f"Failed to write to BigQuery: {e}")
        sys.exit(1)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    # Load config
    cfg_path = os.getenv("CENTER_CONFIG", "center_config.yml")
    if not os.path.exists(cfg_path):
        logging.error(f"Config file not found: {cfg_path}")
        sys.exit(1)

    with open(cfg_path) as f:
        config = yaml.safe_load(f)

    project_map = config.get("project_id_mapping", {})
    default_value = project_map.get("default", "Unknown Project")

    # Build CASE WHEN clause
    case_lines = [
        f"        WHEN '{pid}' THEN '{name}'"
        for pid, name in project_map.items()
        if pid != "default"
    ]
    case_block = "\n".join(case_lines)

    # Guard: the IN list must not be empty
    in_ids = [pid for pid in project_map if pid != "default"]
    if not in_ids:
        logging.error("No project IDs found in project_id_mapping.")
        sys.exit(1)

    # Snowflake SQL
    snowflake_query = f"""
    SELECT
      CONCAT('syn', nl.project_id) AS project_id,
      CASE nl.project_id
      {case_block}
        ELSE '{default_value}'
      END AS project_name,
      CONCAT('syn', nl.ID) AS entity_id,
      nl.NAME,
      nl.ANNOTATIONS:"annotations"."Component"."value"[0]::STRING AS component,
      nl.ANNOTATIONS AS annotations_json,
      ARRAY_SIZE(OBJECT_KEYS(nl.ANNOTATIONS:"annotations")) AS annotation_count
    FROM synapse_data_warehouse.synapse.node_latest nl
    WHERE nl.PROJECT_ID IN ({", ".join(f"'{pid}'" for pid in in_ids)})
      AND nl.node_type = 'file'
    """

    # Destination
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")
    dataset_id = os.getenv("BQ_DATASET", "synapse_raw")
    table_id = os.getenv("BQ_TABLE", "synape_annotations_dyp")  # keep your existing name

    # Query Snowflake
    conn = login_to_snowflake()
    results = run_snowflake_query(conn, snowflake_query)
    logging.info(f"Query returned {len(results)} rows.")

    # Convert results to dicts for BQ
    results_dict = []
    for row in results:
        rec = {
            "project_id": row[0],
            "project_name": row[1],
            "entity_id": row[2],
            "name": row[3],
            "component": row[4],
            "annotations": transform_annotations(row[5]),
            "annotation_count": row[6],
        }
        results_dict.append(rec)

    # Sanity log: show a couple of samples
    for i, r in enumerate(results_dict[:3]):
        keys = [a["key"] for a in r.get("annotations", [])]
        logging.info(f"Sample row {i}: entity_id={r['entity_id']} component={r['component']} ann_keys={keys}")
        if r.get("annotations"):
            logging.info(f"  First annotation: {r['annotations'][0]}")

    # Load to BigQuery
    write_to_bigquery(results_dict, project_id, dataset_id, table_id)


if __name__ == "__main__":
    main()

