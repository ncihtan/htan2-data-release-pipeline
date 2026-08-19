"""
Medallion Architecture: Gold to ISB-CGC

    This module stages GOLD tables from the medallion architecture
    in BigQuery for submission to ISB-CGC. It retrieves tables from
    BigQuery, transforms them as needed for ISB-CGC submission, and
    writes the resulting tables to a staging dataset for subsequent
    transfer. It also generates metadata describing each table included
    in the submission.

Configurations:

    This module depends on three JSON files used for adding
    descriptions to tables uploaded to BigQuery:
        - component_descriptions.json
        - data_model_descriptions.json
        - id_prov_descriptions.json

Functions:
    - print_sub_section(title)
    - query_bigquery_table(client, project_id, dataset_id, table_id)
    - load_bq(client, project, dataset, table, data, 
        schema=None, write_mode="truncate")
    - friendly_component(component)
    - main()
    
Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 08-19-2026
Date Updated: 
Modified By:  
"""

import re
import os
import json
import pandas as pd

from google.cloud import bigquery

#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################

# HTAN
PROJECT = "htan2-dcc"
GOLD_DATASET = "htan2_medallion_gold"
DATA_MODEL_DATASET = "htan2_data_model_cache"
ISBCGC_DATASET = "htan2_isbcgcbq_current_release"

# ISB-CGC
ISBCGC_PROJECT = "isb-cgc-bq"
HTAN2_DATASET = "HTAN2"

# STANDARD METADATA
DATASETS =  "HTAN2 and HTAN2_versioned"
ACCESS = "open"
STATUS = "current"
PROGRAM = "htan2"
REF_GEN = "NULL"
SOURCE = "htan"

#####################################################
#                 HELPER FUNCTIONS
#####################################################

def print_sub_section(title):
    """
    Print subsection headers.

    Args:
        - title (string): The title to be printed.
    """
    border = "=" * (len(title) + 8)
    print(f"\n{border}\n>>> {title.upper()} <<<\n{border}\n")

def query_bigquery_table(client, project_id, dataset_id, table_id):
    """
    Get an entire table from BigQuery as a Pandas DataFrame.

    Args:
        - client (BigQuery instance): A BigQuery client object.
        - project_id (str): BigQuery project name.
        - dataset_id (str): BigQuery dataset name.
        - table_id (str): BigQuery table name.
    
    Returns:
        - (pandas.DataFrame): The BigQuery table as a dataframe.
    """
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    return client.query(query).to_dataframe()

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

def friendly_component(component):
    """
    Splits up component names (e.g., scRNALevel1) into human-readable
    text (e.g., sc RNA Level 1).

    Args:
        - component (string): Name of the assay/module.

    Returns:
        - component (string): Modified name of assay/module into
        human-readable text.
    """
    # Add spaces around known assay acronyms
    replacements = {
        "scRNA": "SC RNA",
        "snRNA": "SN RNA",
        "scDNA": "SC DNA",
        "snDNA": "SN DNA",
        "scATAC": "SC ATAC",
        "snATAC": "SN ATAC",
        "WES": "WES",
        "WGS": "WGS",
    }

    # Replace acronyms first
    for old, new in replacements.items():
        component = component.replace(old, f" {new} ")

    component = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", component) # Split remaining camel case
    component = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", component) # Split letters and numbers
    component = re.sub(r"(\d+)and(\d+)", r"\1 and \2", component) # Fix "3and4" -> "3 and 4"
    component = re.sub(r"\s+", " ", component).strip() # Clean up whitespace

    return component

#####################################################
#                  MAP 2 ISB-CGC
#####################################################

def main():
    """
    Entry point to mapping HTAN2 GOLD metadata tables (metadata, 
    provenance, data model) to ISB-CGC.
    """

    # Get release and timeline specific info from user
    sub_window = input("\nMonth and year corresponding to the end of the data release submission window: ")
    release = input("\nCurrent HTAN Data Release (e.g., 7.0): ")
    model_version = input("\nData Model version (e.g., v1.5.0): ")

    # Initialize clients
    client = bigquery.Client()

    # Create log folder
    log_path = "./log"
    os.makedirs(log_path, exist_ok=True)

    # Save table metadata here
    table_mapping = []

    print_sub_section("PULLING RELEVANT TABLES")

    ##########################
    # CURRENT ISB-CGC TABLES
    ##########################

    # Uncomment when HTAN2 dataset has been created in isb-cgc-bq
    # Comment left: 08/13/2026 by Yamina Katariya

    # old_tables = {
    #     table.table_id: query_bigquery_table(
    #         client,
    #         ISBCGC_PROJECT,
    #         HTAN2_DATASET,
    #         table.table_id
    #     )
    #     for table in client.list_tables(f"{ISBCGC_PROJECT}.{HTAN2_DATASET}")
    # }

    # print(f"Collected {len(old_tables)} tables from `{ISBCGC_PROJECT}.{HTAN2_DATASET}`\n")

    ##########################
    # PULL DATA MODEL
    ##########################

    versioned_table = f"HTAN2_Data_Model_{model_version.replace(".", "_")}"

    versioned_data_model = query_bigquery_table(client,
                                             PROJECT,
                                             DATA_MODEL_DATASET,
                                             versioned_table)

    print(f"Retrieved {versioned_table} as the model corresponding to the release.")

    ##########################
    # STAGE GOLD METADATA
    ##########################

    print_sub_section("STAGING GOLD METADATA")

    # Open COMPONENT JSON
    with open('component_descriptions.json', 'r', encoding='utf-8') as file:
        bq_descriptions = json.load(file)
    bq_descriptions_map = {
        item["name"]: item["description"]
        for item in bq_descriptions
    }

    # Get all GOLD layer metadata tables
    gold_tables = list(client.list_tables(f"{PROJECT}.{GOLD_DATASET}"))
    gold_metadata = [
        table.table_id
        for table in gold_tables
        if table.table_id.startswith("gold_RELEASED_METADATA_TABLE_")
    ]

    for table_id in gold_metadata:

        # Pull the metadata table from BQ
        df = query_bigquery_table(client, PROJECT, GOLD_DATASET, table_id)

        if df.empty:
            continue

        # Extract useful naming information
        metadata_type = table_id.split("_")[-2]
        component = table_id.split("_")[-1]
        latest_status_folder_name = max(
            df["Status_Folder_Name"],
            key=lambda x: int(re.search(r"\d+", x).group()))
        latest_release = latest_status_folder_name.split("_")[0].replace("v", "r")
        data_type = None

        # Determine the metadata and data type based on the component
        if metadata_type == "Files":
            metadata_type = "File"
            data_type = "file_metadata"
        elif metadata_type == "Records" and component == "Biospecimen":
            metadata_type = "Sample"
            data_type = "biospecimen"
        elif metadata_type == "Records" and component in ["ChannelMetadata", "MolecularAssignment", "SpatialPanel"]:
            metadata_type = "File"
            data_type = "file_metadata"
        else:
            metadata_type = "Clinical"
            data_type = "clinical_data"

        # Filter the data model to return only component-related attributes
        data_model = versioned_data_model[versioned_data_model['Component'] == component]

        # Drop unnecessary columns in GOLD tables
        columns_to_keep = data_model['Attribute'].to_list() + \
                          ["BQ_Hash_ID",
                           "BQ_Hash_Record_ID",
                           "File_EntityId",
                           "Record_EntityId",
                           "Folder_EntityId",
                           "Project_EntityId",
                           "File_Size_Bytes",
                           "File_MD5",
                           "HTAN_Center",
                           "Status_Folder_Name",
                           "Component"]
        df = df[[col for col in columns_to_keep if col in df.columns]]

        # Build the schema as a list of dictionaries, one for each attribute
        schema = []
        for attrs in df.columns:

            matches = data_model.loc[
                data_model["Attribute"] == attrs,
                "Description"
            ]

            if not matches.empty:
                description = matches.iloc[0]
            else:
                description = bq_descriptions_map.get(attrs, "")

            schema.append({
                "name": attrs,
                "type": 'STRING',
                "description": description
            })

        # Rename table to ISB-CGC-BQ friendly
        isbcgc_table_name = f"HTAN2_{component}_{metadata_type}_Metadata"
        component_friendly = friendly_component(component)

        # Construct isb-cgc metadata for each GOLD metadata table
        table_mapping.append({
            "Current table ID": f"{PROJECT}.{ISBCGC_DATASET}.{isbcgc_table_name}",
            "Datasets": DATASETS,
            "Table name (current)": f"{isbcgc_table_name}_current",
            "Table name (versioned)": f"{isbcgc_table_name}_{latest_release}",
            "Description": (
                f"This table contains the {metadata_type.lower()} metadata for "
                f"{component_friendly} data extracted from HTAN Phase 2 Synapse "
                f"projects in {sub_window} and corresponds to HTAN2 Data Release {release}."
            ),
            "Friendly Name": f"HTAN PHASE 2 {metadata_type.upper()} METADATA FOR {component_friendly.upper()}",
            "Access": ACCESS,
            "Status": STATUS,
            "Program": PROGRAM,
            "Category": "metadata" if metadata_type == "File" else "clinical_biospecimen_data",
            "Reference genome": REF_GEN,
            "Source": SOURCE,
            "Data type": data_type,
            "Version": latest_release,
            "Number of Rows": len(df),
            "New Rows Added": len(df)
        })

        # Load table to isb-cgc shared dataset
        load_bq(
            client,
            PROJECT,
            ISBCGC_DATASET,
            isbcgc_table_name,
            df,
            schema=schema
        )

    ##########################
    # STAGE DATA MODEL
    ##########################

    print_sub_section("STAGING DATA MODEL")

    # Extract the data model version (e.g., v2.0.0) and generate the table name
    data_model_name = f"{versioned_table.split("_v")[0]}_Schema"

    # Open DATA MODEL JSON
    with open('data_model_descriptions.json', 'r', encoding='utf-8') as file:
        dm_descriptions = json.load(file)

    table_mapping.append({
        "Current table ID": f"{PROJECT}.{ISBCGC_DATASET}.{data_model_name}",
        "Datasets": DATASETS,
        "Table name (current)": f"{data_model_name}_current",
        "Table name (versioned)": f"{data_model_name}_{latest_release}",
        "Description": (
            f"Data was extracted from the the HTAN Phase 2 Data Model "
            f"{model_version} in {sub_window}. For more details see: "
            f"https://htan2-data-model.readthedocs.io/en/main/"
        ),
        "Friendly Name": f"HTAN PHASE 2 DATA MODEL SCHEMA ({model_version})",
        "Access": ACCESS,
        "Status": STATUS,
        "Program": PROGRAM,
        "Category": "metadata",
        "Reference genome": REF_GEN,
        "Source": SOURCE,
        "Data type": "data_dictionary",
        "Version": latest_release,
        "Number of Rows": len(versioned_data_model),
        "New Rows Added": len(versioned_data_model)
    })

    load_bq(
        client,
        PROJECT,
        ISBCGC_DATASET,
        data_model_name,
        versioned_data_model,
        schema=dm_descriptions
    )

    ##########################
    # STAGE PROVENANCE TABLE
    ##########################

    print_sub_section("STAGING PROVENANCE TABLE")

    # Declare isb-cgc provenance table name
    isbcgc_prov_table_name = "HTAN2_ID_Provenance_Chain"

    # Open ID PROV JSON
    with open('id_prov_descriptions.json', 'r', encoding='utf-8') as file:
        prov_descriptions = json.load(file)
    prov_descriptions_map = {
        item["name"]: item["description"]
        for item in prov_descriptions
    }

    # Load table from GOLD layer
    id_prov = query_bigquery_table(
        client,
        PROJECT,
        GOLD_DATASET,
        "gold_RELEASED_INDEXING_TABLE_All_Files_and_Records_ID_Provenance",
    )

    # Build prov schema. For component attributes, get from component JSON
    prov_schema = []
    for attrs in id_prov.columns:

        matches = versioned_data_model.loc[
            versioned_data_model["Attribute"] == attrs,
            "Description"
        ]

        if not matches.empty:
            description = matches.iloc[0]
        else:
            description = prov_descriptions_map.get(attrs, "")

            if description == "":
                description = bq_descriptions_map.get(attrs, "")

        prov_schema.append({
            "name": attrs,
            "type": 'STRING',
            "description": description
        })

    table_mapping.append({
        "Current table ID": f"{PROJECT}.{ISBCGC_DATASET}.{isbcgc_prov_table_name}",
        "Datasets": DATASETS,
        "Table name (current)": f"{isbcgc_prov_table_name}_current",
        "Table name (versioned)": f"{isbcgc_prov_table_name}_{latest_release}",
        "Description": (
            f"This table contains biospecimen and participant IDs for each "
            f"HTAN2 data file. This table was created in July 2026 and uploaded "
            f"{sub_window} and corresponds to the HTAN Phase 2 Data Release {release}."
        ),
        "Friendly Name": "HTAN PHASE 2 ID PROVENANCE CHAIN",
        "Access": ACCESS,
        "Status": STATUS,
        "Program": PROGRAM,
        "Category": "metadata",
        "Reference genome": REF_GEN,
        "Source": SOURCE,
        "Data type": "file_metadata",
        "Version": latest_release,
        "Number of Rows": len(id_prov),
        "New Rows Added": len(id_prov)
    })

    load_bq(
        client,
        PROJECT,
        ISBCGC_DATASET,
        isbcgc_prov_table_name,
        id_prov,
        schema=prov_schema
    )

    ##########################
    # GENERATE METADATA TABLE
    ##########################

    print_sub_section("STAGING ISB-CGC METADATA TABLE")

    table_mapping_df = pd.DataFrame(table_mapping)
    table_mapping_name = f"{log_path}/Metadata_Table_Descriptions_{latest_release.upper()}.xlsx"
    table_mapping_df.to_excel(table_mapping_name, header=True, index=False)

    print(f"Saved to {table_mapping_name}")


if __name__ == "__main__":
    main()
