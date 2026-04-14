#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Bronze to Silver
"""

import os
import shutil
from datetime import datetime
import pandas as pd
from htan_validators.component_validator import HTANComponentValidator
from htan_validators.provenance_validator import HTANProvenanceValidator
from client_load import (
    load_bq,
    init_bq_client,
    init_synapse_client
)
from model_load import (
    download_model,
    convert_json_to_df
)

#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################

PROJECT = "htan2-dcc"
RAW_DATASET = "htan2_synapse_raw"
BRONZE_DATASET = "htan2_medallion_bronze"
SILVER_DATASET = "htan2_medallion_silver"

#####################################################
#                 HELPER FUNCTIONS
#####################################################

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

def rename_curator_val_results(df, metadata_type):
    """
    Rename columns containing Curator Validation results.

    Args:
        - df (pandas.DataFrame): Component-level metadata table.
        - metadata_type (str): Synapse structure annotation type (Files or Records).

    Returns:
        - df (pandas.DataFrame): Component-level metadata table.
    """
    if metadata_type == "Files":
        df = df.rename(columns={'Schema_Version': 'Curator_Schema_Version',
                                'Is_Valid': 'Curator_Validation_Passed',
                                'Validation_Error_Message': 'Curator_Violations',
                                'All_Validation_Error_Messages': 'Curator_Error_Messages',
                                'Validated_On': 'Curator_Validation_Timestamp'})
    elif metadata_type == "Records":
        df = df.rename(columns={'Schema_Version': 'Curator_Schema_Version',
                                'Validation_Passed': 'Curator_Validation_Passed',
                                'Validation_Error_Message': 'Curator_Violations',
                                'All_Validation_Messages': 'Curator_Error_Messages'})
    return df

def print_sub_section(title):
    """
    Print subsection headers.

    Args:
        - title (string): The title to be printed.
    """
    border = "=" * (len(title) + 8)
    print(f"\n{border}\n>>> {title.upper()} <<<\n{border}\n")

def normalize_keys(df, cols):
    """
    Convert BQ nulls to empty strings.

    Args:
        - df (pandas.DataFrame): Dataframe containing key columns.
        - cols (list): List of column names to normalize.
    
    Returns:
        - df (pandas.DataFrame): Dataframe with normalized key columns.
    """
    for col in cols:
        df[col] = df[col].fillna("").astype(str).str.strip()
    return df

def combine_lists(a, b):
    """
    Combine two list-like objects into one list.

    Args:
        - a (list or any): First list or value.
        - b (list or any): Second list or value.

    Returns:
        - (list or None): Combined list or None if both are empty.
    """
    if not isinstance(a, list):
        a = []
    if not isinstance(b, list):
        b = []
    return a + b if (a or b) else None

def make_hashable(df):
    """
    Convert list values in a dataframe into hashable values.

    Args:
        - df (pandas.DataFrame): Dataframe containing list columns.

    Returns:
        - (pandas.DataFrame): Dataframe with lists converted to tuples.
    """
    return df.applymap(
        lambda x: tuple(x) if isinstance(x, list) else x
    )

def check_validation_passed(violations, error_list):

    return int(not bool(any(err in (violations if isinstance(violations, list) else [])
                            for err in error_list)))

#####################################################
#                    SILVER LAYER
#####################################################

def main():
    """
    Entry-point to the SILVER LAYER.
    """

    ##########################
    # Initialize Clients
    ##########################
    client = init_bq_client()
    syn = init_synapse_client()

    ##########################
    # Provenance Table
    ##########################

    print_sub_section("PULLING THE PROVENANCE TABLE")

    prov_table = query_bigquery_table(client, PROJECT, BRONZE_DATASET,
                                      "bronze_INDEXING_TABLE_All_Files_and_Records_ID_Provenance")

    load_bq(client, PROJECT, SILVER_DATASET,
            "silver_INDEXING_TABLE_All_Files_and_Records_ID_Provenance",
            prov_table)

    prov_error_table = prov_table

    ##########################
    # Exclusion List
    ##########################

    print_sub_section("PULLING THE EXCLUSION LIST")

    exclusion_query = f"""
        SELECT *
        FROM `{PROJECT}.{RAW_DATASET}.raw_INDEXING_TABLE_Exclusion_List_Form_Results`
        WHERE Status = "EXCLUDE"
    """
    exclusion_list = client.query(exclusion_query).to_dataframe()
    exclusion_list['HTAN_Center'] = exclusion_list['HTAN_Center'].str.replace(' ', '_')
    exclusion_list['HTAN_Center'] = exclusion_list['HTAN_Center'].str.replace('HTAN2_Testing',
                                                                              'htan2-testing1')

    load_bq(client, PROJECT, SILVER_DATASET,
            "silver_INDEXING_TABLE_Current_Excluded_Files",
            exclusion_list)

    ##########################
    # Data Model
    ##########################

    print_sub_section("PULLING THE DATA MODEL")

    # Get both File and Record Set Curator validation results
    file_schema_query = f"""
        SELECT File_EntityId, File_Name, Component, Schema_Version
        FROM `{PROJECT}.{BRONZE_DATASET}.bronze_INDEXING_TABLE_All_Files_With_Schema_Information`
    """
    file_schemas = client.query(file_schema_query).to_dataframe()

    record_schema_query = f"""
        SELECT Record_EntityId, Folder_EntityId, Component, HTAN_Center, Schema_Version
        FROM `{PROJECT}.{BRONZE_DATASET}.bronze_INDEXING_TABLE_All_RecordSets_With_Schema_Information`
    """
    record_schemas = client.query(record_schema_query).to_dataframe()

    # Get all unique versions of the data model used by Curator
    all_versions = pd.concat([file_schemas['Schema_Version'],
                              record_schemas['Schema_Version']])
    unique_versions = all_versions.dropna().unique().tolist()

    # Download the versions of the data model used to temp folder
    tmp_path = "./data_models_tmp"
    os.makedirs(tmp_path, exist_ok=True)
    for version in unique_versions:
        version = f"v{version}"
        model = download_model(version)
        data_model = convert_json_to_df(model)

        file_path = tmp_path + f"/Data_Model_{version}.csv"

        # Save Data Model
        print(f"\nSaving Data Model Version {version} to {file_path}")
        data_model.to_csv(file_path, header=True, index=False)

    # Push new data models to BQ
    model_tables = list(client.list_tables(f"{PROJECT}.htan2_data_model_cache"))
    model_tables = [table.table_id for table in model_tables]

    for version in unique_versions:
        bq_version = version.replace('.', '_')
        bq_model_table = f"HTAN2_Data_Model_v{bq_version}"

        if bq_model_table not in model_tables:

            print(f"\nPushing uncached Data Model Version {version} to BigQuery")
            tabular_data_model = pd.read_csv(f"./data_models_tmp/Data_Model_v{version}.csv")
            load_bq(
                client,
                PROJECT,
                "htan2_data_model_cache",
                bq_model_table,
                tabular_data_model
            )

    ##########################
    # Release Validation
    ##########################

    print_sub_section("STARTING RELEASE VALIDATION")

    # Get all BRONZE layer metadata tables
    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = [
        table.table_id
        for table in bronze_tables
        if table.table_id.startswith("bronze_METADATA_TABLE_All_")
    ]

    # Initializing dictionary with release validation results
    #   - Key: Silver BQ table id
    #   - Value: Metadata table with appended errors
    validation_tables = {}

    for table_id in bronze_metadata:

        # Initialize the kind of metadata being checked and the component
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]

        # Pull the metadata table from BQ
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)

        if metadata_type == "Files":
            df = df.merge(file_schemas,
                          on=["File_EntityId", "File_Name", "Component"],
                          how='left')

        elif metadata_type == "Records":
            df = df.merge(record_schemas,
                          on=['Record_EntityId', 'Folder_EntityId', 'Component', 'HTAN_Center'],
                          how='left')

        df = rename_curator_val_results(df, metadata_type)

        print(f"\nValidating {component}")

        # Begin component validation
        comp_validator = HTANComponentValidator()
        df = comp_validator.validate(df, syn, metadata_type, component, exclusion_list)

        # Begin provenance validation
        prov_validator = HTANProvenanceValidator()
        df, prov_error_table = prov_validator.validate(client,
                                                       df,
                                                       prov_error_table,
                                                       metadata_type,
                                                       component)

        # Push component validation results to BQ
        table_id = table_id.replace("bronze_", "silver_", 1)
        validation_tables[table_id] = df

    ##########################
    # Error Table Generation
    ##########################

    print_sub_section("GENERATING ERROR TABLES")

    # Group error types
    component_errors = ["INVALID_HTAN_ID",
                        "UNRESOLVED_ID_PATH",
                        "MISSING_HTAN_ID",
                        "DUPLICATE_HTAN_ID",
                        "EXCLUDED_FILE"]

    provenance_errors = ["MISSING_CENTER_RECORD",
                         "MISSING_DEMOGRAPHICS",
                         "ID_CROSS_VALIDATION"]
    file_error_rows = []
    record_error_rows = []

    # Clean provenance error table
    prov_error_table = prov_error_table[prov_error_table["HTAN_DATA_FILE_ID"].notna()].copy()
    prov_error_table = normalize_keys(
        prov_error_table,
        ["HTAN_DATA_FILE_ID", "File_Name", "File_EntityId"]
    )
    prov_error_table = prov_error_table[
        [
            "HTAN_DATA_FILE_ID",
            "File_Name",
            "File_EntityId",
            "Release_Violations",
            "Release_Error_Messages"
        ]
    ].drop_duplicates(
        subset=["HTAN_DATA_FILE_ID", "File_Name", "File_EntityId"],
        keep="last"
    )

    # Loop through all validation results and generate error tables
    for key, df in validation_tables.items():

        metadata_type = key.split("_")[4]
        component = key.split("_")[5]
        df["Release_Validation_Timestamp"] = datetime.now()

        # Record Set metadata tables
        if metadata_type != "Files":

            # Append record set error indexing table
            for _, row in df.iterrows():

                component_passed = check_validation_passed(
                    row["Release_Violations"],
                    component_errors
                )
                provenance_passed = check_validation_passed(
                    row["Release_Violations"],
                    provenance_errors
                )
                curator_passed = int(str(row['Curator_Validation_Passed']).lower() == "true")

                record_error_rows.append({
                    "Component": row['Component'],
                    "Folder_EntityId": row['Folder_EntityId'],
                    "HTAN_Center": row['HTAN_Center'],
                    "HTAN_PARTICIPANT_ID": row.get("HTAN_PARTICIPANT_ID", None),
                    "HTAN_BIOSPECIMEN_ID": row.get("HTAN_BIOSPECIMEN_ID", None),
                    "HTAN_PARENT_ID": row.get("HTAN_PARENT_ID", None),
                    "Curator_Violations": row["Curator_Violations"],
                    "Curator_Error_Messages": row["Curator_Error_Messages"],
                    "Release_Violations": row["Release_Violations"],
                    "Release_Error_Messages": row["Release_Error_Messages"],
                    "Curator_Validation_Passed": curator_passed,
                    "Component_Validation_Passed": component_passed,
                    "Provenance_Validation_Passed": provenance_passed,
                    "Validation_Completion": f"{curator_passed+component_passed+provenance_passed}/3"
                })

            # Push modified metadata table with appended errors
            load_bq(client, PROJECT, SILVER_DATASET, key, df)
            continue

        # File metadata tables
        df = normalize_keys(df, ["HTAN_DATA_FILE_ID", "File_Name", "File_EntityId"])

        # Merge provenance errors into file metadata tables
        for col in ["Release_Error_Messages", "Release_Violations"]:
            df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

        df = df.merge(
            prov_error_table,
            on=["HTAN_DATA_FILE_ID", "File_Name", "File_EntityId"],
            how="left",
            suffixes=("", "_prov")
        )

        df["Release_Violations"] = df.apply(
            lambda r: combine_lists(r["Release_Violations"], r["Release_Violations_prov"]),
            axis=1
        )

        df["Release_Error_Messages"] = df.apply(
            lambda r: combine_lists(r["Release_Error_Messages"], r["Release_Error_Messages_prov"]),
            axis=1
        )

        df["Release_Validation_Passed"] = df["Release_Violations"].apply(
            lambda x: False if x else True
        )

        df = df.drop(columns=[
            "Release_Violations_prov",
            "Release_Error_Messages_prov"
        ])

        # Append file error indexing table
        for _, row in df.iterrows():

            component_passed = check_validation_passed(
                row["Release_Violations"],
                component_errors
            )
            provenance_passed = check_validation_passed(
                row["Release_Violations"],
                provenance_errors
            )
            curator_passed = int(str(row['Curator_Validation_Passed']).lower() == "true")

            file_error_rows.append({
                "Component": row['Component'],
                "File_EntityId": row['File_EntityId'],
                "File_Name": row['File_Name'],
                "HTAN_Center": row['HTAN_Center'],
                "HTAN_DATA_FILE_ID": row['HTAN_DATA_FILE_ID'],
                "HTAN_PARENT_ID": row["HTAN_PARENT_ID"],
                "Curator_Violations": row["Curator_Violations"],
                "Curator_Error_Messages": row["Curator_Error_Messages"],
                "Release_Violations": row["Release_Violations"],
                "Release_Error_Messages": row["Release_Error_Messages"],
                "Curator_Validation_Passed": curator_passed,
                "Component_Validation_Passed": component_passed,
                "Provenance_Validation_Passed": provenance_passed,
                "Validation_Completion": f"{curator_passed+component_passed+provenance_passed}/3"
            })

        # Push modified metadata table with appended errors
        load_bq(client, PROJECT, SILVER_DATASET, key, df)

    # Generate file/record set indexing error tables
    file_error_table = pd.DataFrame(file_error_rows)
    file_error_table = file_error_table.drop_duplicates(
        subset=['Component',
                'File_EntityId',
                'File_Name',
                'HTAN_Center',
                'HTAN_DATA_FILE_ID'],
        keep="first")
    load_bq(client, PROJECT, SILVER_DATASET,
            "silver_INDEXING_TABLE_All_File_Errors",
            file_error_table)

    record_error_table = pd.DataFrame(record_error_rows)
    record_error_table = record_error_table.drop_duplicates(
        subset=['Component',
                'Folder_EntityId',
                'HTAN_Center',
                'HTAN_PARTICIPANT_ID',
                'HTAN_BIOSPECIMEN_ID'],
        keep="first")
    load_bq(client, PROJECT, SILVER_DATASET,
            "silver_INDEXING_TABLE_All_Record_Errors",
            record_error_table)

    # Remove temporary data model folder
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    main()
