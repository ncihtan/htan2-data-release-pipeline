#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Silver to Gold

Authors: 
Updated: 
"""

from client_load import (
    load_bq,
    init_bq_client
)

#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################

PROJECT = "htan2-dcc"
SILVER_DATASET = "htan2_medallion_silver"
GOLD_DATASET = "htan2_medallion_gold"
DM_DATASET = "htan2_data_model_cache"

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

def main():
    """
    Entry point into the GOLD layer.
    """

    # Initialize BQ Client
    client = init_bq_client()

    # Get all data models from BQ
    data_models = list(client.list_tables(f"{PROJECT}.{DM_DATASET}"))
    dm_versions = [
        table.table_id
        for table in data_models
        if table.table_id.startswith("HTAN2_Data_Model_")
    ]

    if dm_versions:

        # Get most recent data model table
        latest_model_table = sorted(dm_versions, reverse=True)[0]
        bq_version = latest_model_table.split("HTAN2_Data_Model_")[-1]
        github_version = bq_version.replace("_", ".")

        # Get table and add schema version
        latest_model = query_bigquery_table(client,
                                            PROJECT,
                                            DM_DATASET,
                                            latest_model_table)
        latest_model["Schema_Version"] = github_version

        # Push data model dictionary to BQ GOLD layer
        load_bq(
            client,
            PROJECT,
            GOLD_DATASET,
            'gold_INDEXING_TABLE_Tabular_Data_Model',
            latest_model
        )


if __name__ == "__main__":
    main()
