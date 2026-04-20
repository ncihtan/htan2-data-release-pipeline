"""
Interface with Clients and their Datasets

    This module provides functions for interacting with the BigQuery
    and Synapse clients.

Configurations: None

Functions:
    - init_synapse_client()
    - init_bq_client()
    - load_bq(client, project, dataset, table, data, schema=None)
    - get_description(attribute, schema, add_descriptions)
    
Author:       Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>
Date Created: UNKNOWN
Date Updated: 07-17-2025
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""

from google.cloud import bigquery
import synapseclient
import os

def init_synapse_client():
    """
    Initializes the Synapse client with the specific user account
    information.

    Returns:
        - syn (Synapse instance): Synapse client object
    """
    SYN_PAT = os.environ.get('SYNAPSE_AUTH_TOKEN_BRONZE')
    syn = synapseclient.Synapse()

    try:
        syn.login(authToken=SYN_PAT)
    except synapseclient.core.exceptions.SynapseNoCredentialsError:
        print("Please fill in 'username' and 'password'/'api_key' values in .synapseConfig.")
    except synapseclient.core.exceptions.SynapseAuthenticationError:
        print("Please make sure the credentials in the .synapseConfig file are correct.")

    return syn

def init_bq_client():
    """
    Initialize and return the BigQuery client.

    """

    return bigquery.Client()

def load_bq(client, project, dataset, table, data, schema=None):
    """
    Load table into BigQuery.

    Args:
        - client (BigQuery instance): BigQuery client.
        - project (string): BigQuery project name.
        - dataset (string): BigQuery dataset name.
        - table (string): BigQuery table name.
        - data (pandas.DataFrame): Data to be loaded into BigQuery.
        - schema (dict): The schema of the data being loaded into Big Query.
        
    """

    table_bq = f"{project}.{dataset}.{table}"
    print(f"Loading {table_bq} to BigQuery")

    # Make column names BigQuery friendly
    data.columns = data.columns.str.replace(
       '[^0-9a-zA-Z]+','_', regex=True)

    # If no schema is provided, generate a default schema with STRING type
    if schema is None:
        schema = [bigquery.SchemaField(name, 'STRING') for name in data.columns]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        source_format=bigquery.SourceFormat.CSV
    )

    job = client.load_table_from_dataframe(
        data, table_bq, job_config=job_config
    )

def get_description(attribute, schema, add_descriptions):
    """
    Retrieve an attribute description from an existing BigQuery table. 

    Args:
        - attribute (string): Table attribute of interest.
        - schema (pandas.DataFrame): DataFrame containing table metadata
        - add_description (dict): Attributes and their associated descriptions
            as key-value pairs.
        
    """
    try:
        dsc = schema[schema['Attribute'] == attribute]['Description'].values[0]
        description = (dsc[:1024]) if len(dsc) > 1024 else dsc

    except (IndexError, KeyError):
        try:
            dsc = add_descriptions[attribute]
            description = (dsc[:1024]) if len(dsc) > 1024 else dsc
        except KeyError:
            description = 'Description unavailable. Contact DCC for more information'
            print(f"{attribute} attribute not found in HTAN schema")

    return description
