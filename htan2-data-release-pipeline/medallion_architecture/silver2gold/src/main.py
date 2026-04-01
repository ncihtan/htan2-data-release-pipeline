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
from model_load import (
    get_latest_model,
    download_model,
    convert_json_to_df
)


def main():

    # Initialize BQ Client
    client = init_bq_client()

    # Get the latest version of the data model
    latest_model = get_latest_model(mode="latest")
    data = download_model(latest_model)
    tabular_data_model = convert_json_to_df(data)

    # Add a column with Dat Model Version
    tabular_data_model["Schema_Version"] = latest_model

    # Push data model dictionary to BQ
    load_bq(
        client,
        'htan2-dcc',
        'htan2_medallion_gold',
        'gold_INDEXING_TABLE_Tabular_Data_Model',
        tabular_data_model
    )


if __name__ == "__main__":
    main()
