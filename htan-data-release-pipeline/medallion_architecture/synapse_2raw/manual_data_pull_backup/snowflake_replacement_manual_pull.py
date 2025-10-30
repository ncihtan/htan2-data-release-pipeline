from synapseclient import Synapse
import pandas as pd
from google.cloud import bigquery

# ----------------------------------------------------------------------
# 1. Setup and authentication
# ----------------------------------------------------------------------
syn = Synapse()
syn.login()
#PROVIDE SYNAPSE ID
PROJECT_ID = "syn70776743"
project_name = syn.get(PROJECT_ID, downloadFile=False).name

# ----------------------------------------------------------------------
# 2. Helper to normalize types
# ----------------------------------------------------------------------
def dtype_name(val):
    if val is None:
        return "STRING"
    if isinstance(val, bool):
        return "BOOLEAN"
    if isinstance(val, int):
        return "INTEGER"
    if isinstance(val, float):
        return "DOUBLE"
    return "STRING"

# ----------------------------------------------------------------------
# 3. Recursive traversal
# ----------------------------------------------------------------------
def traverse_and_collect(parent_id, results):
    """
    Recursively walk through Synapse and build grouped table:
    One COMPONENT row per file + sub-rows for each annotation.
    """
    for child in syn.getChildren(parent_id):
        if child["type"] == "org.sagebionetworks.repo.model.Folder":
            traverse_and_collect(child["id"], results)
        elif child["type"] == "org.sagebionetworks.repo.model.FileEntity":
            entity = syn.get(child["id"], downloadFile=False)
            ann_raw = syn.get_annotations(entity)

            ann_items = []
            for k, v in ann_raw.items():
                values = v if isinstance(v, list) else [v]
                for val in values:
                    ann_items.append((k.upper(), dtype_name(val), val))

            # Separate COMPONENT
            component_rows = [(k, t, v) for (k, t, v) in ann_items if k == "COMPONENT"]
            other_rows = [(k, t, v) for (k, t, v) in ann_items if k != "COMPONENT"]

            if component_rows:
                comp_key, comp_type, comp_value = component_rows[0]
            else:
                comp_key, comp_type, comp_value = "COMPONENT", "STRING", None

            annotation_count = len(ann_items)

            # Main row
            results.append({
                "entity_id": entity.id,
                "project_id": PROJECT_ID,
                "project_name": project_name,
                "folder_id": entity.parentId,
                "name": entity.name,
                "component": comp_key,
                "component_value": comp_value,
                "annotations.key": None,
                "annotations.type": None,
                "annotations.value": None,
                "annotation_count": annotation_count
            })

            # Annotation sub-rows
            for (k, t, v) in other_rows:
                results.append({
                    "entity_id": None,
                    "project_id": None,
                    "project_name": None,
                    "folder_id": None,
                    "name": None,
                    "component": None,
                    "component_value": None,
                    "annotations.key": k,
                    "annotations.type": t,
                    "annotations.value": v,
                    "annotation_count": None
                })

# ----------------------------------------------------------------------
# 4. Build flat grouped DataFrame
# ----------------------------------------------------------------------
results = []
traverse_and_collect(PROJECT_ID, results)

df = pd.DataFrame(results)

# Add Row numbering for main rows only
main_mask = df["project_id"].notna()
df.loc[main_mask, "Row"] = range(1, main_mask.sum() + 1)

# Reorder columns
df = df[[
    "Row",
    "project_id",
    "project_name",
    "entity_id",
    "folder_id",
    "name",
    "component",
    "component_value",
    "annotations.key",
    "annotations.type",
    "annotations.value",
    "annotation_count"
]]

# ----------------------------------------------------------------------
# 5. Convert to nested BigQuery structure
# ----------------------------------------------------------------------
# Gather all annotation rows (where annotations.key is not null)
annotation_rows = df[df["annotations.key"].notna()].copy()
annotation_rows["annotations.value"] = annotation_rows["annotations.value"].astype(str)

# Group annotations per entity_id
annotations_grouped = (
    annotation_rows.groupby(df["entity_id"].ffill())
    .apply(lambda g: [
        {"key": k, "type": t, "value": v}
        for k, t, v in zip(g["annotations.key"], g["annotations.type"], g["annotations.value"])
    ])
)

# Merge grouped annotations back with main rows
main_rows = df[main_mask].copy()
main_rows["annotations"] = main_rows["entity_id"].map(annotations_grouped)

nested_df = main_rows[[
    "project_id",
    "project_name",
    "entity_id",
    "folder_id",
    "name",
    "component_value",
    "annotations",
    "annotation_count"
]].rename(columns={"component_value": "component"})

# ----------------------------------------------------------------------
# 6. Load nested table into BigQuery
# ----------------------------------------------------------------------
client = bigquery.Client(project="htan2-dcc")

table_id = "htan2_synapse_raw.raw_METADATA_annotations_center_manual"

schema = [
    bigquery.SchemaField("project_id", "STRING"),
    bigquery.SchemaField("project_name", "STRING"),
    bigquery.SchemaField("entity_id", "STRING"),
    bigquery.SchemaField("folder_id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("component", "STRING"),
    bigquery.SchemaField(
        "annotations",
        "RECORD",
        mode="REPEATED",
        fields=[
            bigquery.SchemaField("key", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("value", "STRING"),
        ],
    ),
    bigquery.SchemaField("annotation_count", "INTEGER"),
]

job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")

load_job = client.load_table_from_dataframe(nested_df, table_id, job_config=job_config)
