#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Bronze to Silver
"""

import os
import shutil
import pandas as pd
from validators.component_validator import HTANComponentValidator
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
    # Exclusion List
    ##########################

    print_sub_section("EXCLUSION LIST")

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

    print_sub_section("DATA MODEL")

    # Get both File and Record Set Curator validation results
    file_schema_query = f"""
        SELECT File_EntityId, File_Name, Etag, Schema_Version
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

    ##############################
    # Component-Level Validation
    ##############################

    print_sub_section("COMPONENT VALIDATION")

    # Get all BRONZE layer metadata tables
    bronze_tables = list(client.list_tables(f"{PROJECT}.{BRONZE_DATASET}"))
    bronze_metadata = [
        table.table_id
        for table in bronze_tables
        if table.table_id.startswith("bronze_METADATA_TABLE_All_")
    ]

    for table_id in bronze_metadata:

        # Initialize the kind of metadata being checked and the component
        metadata_type = table_id.split("_")[4]
        component = table_id.split("_")[5]

        # Pull the metadata table from BQ
        df = query_bigquery_table(client, PROJECT, BRONZE_DATASET, table_id)

        if metadata_type == "Files":
            df = df.merge(file_schemas,
                          on=["File_EntityId", "File_Name", "Etag"],
                          how='left')
        elif metadata_type == "Records":
            df = df.merge(record_schemas,
                          on=['Record_EntityId', 'Folder_EntityId', 'Component', 'HTAN_Center'],
                          how='left')

        df = rename_curator_val_results(df, metadata_type)

        # Begin validation
        validator = HTANComponentValidator()
        df = validator.validate(df, syn, metadata_type, component, exclusion_list)

        # Push component validation results to BQ
        table_id = table_id.replace("bronze_", "silver_", 1)
        load_bq(client, PROJECT, SILVER_DATASET, table_id, df)

    # Remove temporary data model folder
    shutil.rmtree(tmp_path)

if __name__ == "__main__":
    main()
