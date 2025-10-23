#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Synapse to Raw
- Extract file-level metadata/annotations from Synapse Snowflake DW
- Normalize annotations to BQ-compatible repeated RECORD schema
- Load into BigQuery bronze tables

Requires:
- SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_PAT
- GOOGLE_CLOUD_PROJECT (defaults to 'htan2-dcc'), BQ_DATASET, BQ_TABLE
- center_config.yml containing project_id_mapping

Authors: Dar'ya Pozhidayeva, Adam Taylor
Updated: 2025-08-20
"""
import os
import sys
import json
import logging
from collections.abc import Iterable

import snowflake.connector
from google.cloud import bigquery
import pandas as pd  # noqa: F401
import dotenv
import yaml

# -----------------------------------------------------------------------------
# Setup
# -----------------------------------------------------------------------------
dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")


# -----------------------------------------------------------------------------
# Annotation transformation
# -----------------------------------------------------------------------------
def _to_string_list(val):
    """Coerce any value into list[str] for BQ REPEATED STRING."""
    try:
        import numpy as np  # optional
        if isinstance(val, (np.generic,)):
            val = val.item()
        if isinstance(val, np.ndarray):
            val = val.tolist()
    except Exception:
        pass

    if isinstance(val, str):
        s = val.strip()
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                val = json.loads(s)
            except Exception:
                pass

    if isinstance(val, list):
        items = val
    elif isinstance(val, Iterable) and not isinstance(val, (str, bytes, dict)):
        items = list(val)
    elif val is None:
        items = []
    else:
        items = [val]

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
    """Transform warehouse annotations into:
       REPEATED RECORD<key STRING, type STRING, value REPEATED STRING>.
    """
    try:
        if isinstance(annotations_obj, str):
            try:
                annotations_obj = json.loads(annotations_obj)
            except Exception:
                logging.warning("annotations_obj is a non-JSON string; treating as empty.")
                return []

        if not isinstance(annotations_obj, dict):
            logging.warning(f"Expected dict for annotations_obj but got {type(annotations_obj).__name__}")
            return []

        anns = annotations_obj.get("annotations", annotations_obj)
        if not isinstance(anns, dict):
            logging.warning("Unexpected annotations structure; treating as empty.")
            return []

        records = []
        for k, v in anns.items():
            if isinstance(v, dict) and "value" in v:
                raw_val = v.get("value")
                ann_type = v.get("type", "STRING")
            else:
                raw_val = v
                ann_type = "STRING"

            coerced = _to_string_list(raw_val)
            if not coerced:
                continue
            records.append({"key": k, "type": str(ann_type), "value": coerced})
        return records
    except Exception as e:
        logging.warning(f"Failed to transform annotations: {e}")
        return []


# -----------------------------------------------------------------------------
# Snowflake helpers
# -----------------------------------------------------------------------------
def login_to_snowflake():
    user = os.getenv("SNOWFLAKE_USER")
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    pat = os.getenv("SNOWFLAKE_PAT")
    logging.info(f"Using user: {user}, account: {account}")
    if not user or not account or not pat:
        logging.error("Missing SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, or SNOWFLAKE_PAT.")
        sys.exit(1)
    try:
        conn = snowflake.connector.connect(user=user, account=account, password=pat)
        logging.info("Successfully connected to Snowflake.")
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
# BigQuery load
# -----------------------------------------------------------------------------
def write_to_bigquery(results, project_id, dataset_id, table_id):
    """Load list[dict] into BigQuery with fixed schema, truncating the table."""
    try:
        client = bigquery.Client(project=project_id)
        table_ref = client.dataset(dataset_id).table(table_id)

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("project_id", "STRING"),
                bigquery.SchemaField("project_name", "STRING"),
                bigquery.SchemaField("entity_id", "STRING"),
                bigquery.SchemaField("folder_id", "STRING"),  # NEW
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
            # If you prefer the table to evolve automatically in the future, uncomment:
            # schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )

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
    # Config
    cfg_path = os.getenv("CENTER_CONFIG", "center_config.yml")
    if not os.path.exists(cfg_path):
        logging.error(f"Config file not found: {cfg_path}")
        sys.exit(1)

    with open(cfg_path) as f:
        config = yaml.safe_load(f)

    project_map = config.get("project_id_mapping", {})
    default_value = project_map.get("default", "Unknown Project")

    # CASE WHEN block: compare numeric to numeric
    case_lines = [
        f"        WHEN {int(pid.replace('syn',''))} THEN '{name}'"
        for pid, name in project_map.items()
        if pid != "default"
    ]
    case_block = "\n".join(case_lines)

    # IN list (numeric project IDs)
    in_ids = [pid for pid in project_map if pid != "default"]
    if not in_ids:
        logging.error("No project IDs found in project_id_mapping.")
        sys.exit(1)
    in_numeric = ", ".join(f"{int(pid.replace('syn',''))}" for pid in in_ids)

    # Snowflake SQL
    snowflake_query = f"""
    SELECT
      CONCAT('syn', nl.project_id) AS project_id,
      CASE nl.project_id
{case_block}
        ELSE '{default_value}'
      END AS project_name,
      CONCAT('syn', nl.id) AS entity_id,
      CONCAT('syn', nl.parent_id) AS folder_id,
      nl.name,
      nl.ANNOTATIONS:"annotations"."Component"."value"[0]::STRING AS component,
      nl.ANNOTATIONS AS annotations_json,
      ARRAY_SIZE(OBJECT_KEYS(nl.ANNOTATIONS:"annotations")) AS annotation_count
    FROM synapse_data_warehouse.synapse.node_latest nl
    WHERE nl.project_id IN ({in_numeric})
      AND nl.node_type = 'file'
    """

    # Destinations
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "htan2-dcc")  # set env if your project is 'htan-dcc'
    dataset_id = os.getenv("BQ_DATASET", "htan2_synapse_raw")
    table_id = os.getenv("BQ_TABLE", "raw_METADATA_annotations")

    # Execute
    conn = login_to_snowflake()
    results = run_snowflake_query(conn, snowflake_query)
    logging.info(f"Query returned {len(results)} rows.")

    # Convert to dicts for BQ
    results_dict = []
    for row in results:
        rec = {
            "project_id": row[0],
            "project_name": row[1],
            "entity_id": row[2],
            "folder_id": row[3],            # NEW
            "name": row[4],
            "component": row[5],
            "annotations": transform_annotations(row[6]),
            "annotation_count": row[7],
        }
        results_dict.append(rec)

    # Sample logs
    for i, r in enumerate(results_dict[:3]):
        keys = [a["key"] for a in r.get("annotations", [])]
        logging.info(f"Sample row {i}: entity_id={r['entity_id']} component={r['component']} ann_keys={keys}")
        if r.get("annotations"):
            logging.info(f"  First annotation: {r['annotations'][0]}")

    #TEMPORARY FILTER
    results_dict = {
    k: v for k, v in results_dict.items()
    if v.get("folder_id") == "syn70197818"
    }
    
    # Load to BigQuery
    write_to_bigquery(results_dict, project_id, dataset_id, table_id)


if __name__ == "__main__":
    main()
