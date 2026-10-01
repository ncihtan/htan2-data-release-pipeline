#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gold layer helper functions.
"""
import pandas as pd

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
    print(query)
    return client.query(query).to_dataframe()

def process_excluded_panels(exclude_df, expected_panel_counts):
    """Helper to check and calculate fully excluded panels."""
    excluded_panel_counts = (
        exclude_df.groupby('HTAN_PANEL_ID')['File_EntityId']
        .nunique()
        .reset_index(name='Excluded_File_Count')
    )
    panel_check_df = expected_panel_counts.merge(excluded_panel_counts, on='HTAN_PANEL_ID', how='left')
    panel_check_df['Excluded_File_Count'] = panel_check_df['Excluded_File_Count'].fillna(0).astype(int)
    panel_check_df['Fully_Excluded'] = panel_check_df['File_Count'] == panel_check_df['Excluded_File_Count']
    return panel_check_df

def apply_age_masking(df, client, project, dataset, table_id, max_age_days):
    """Applies age masking based on error messages from the silver layer."""
    age_cols = [c for c in df.columns if "AGE_IN_" in c]
    if not age_cols:
        return df
        
    silver_tid = table_id.replace("bronze_", "silver_")
    age_masking_query = f"""
        SELECT BQ_Hash_Record_ID, Release_Error_Messages 
        FROM `{project}.{dataset}.{silver_tid}` 
        WHERE Release_Error_Messages LIKE "%AGE_OVER_90%"
        """
    age_masking_results = client.query(age_masking_query).to_dataframe()

    # Identify matching BQ_Hash_Record_IDs if results exist
    if not age_masking_results.empty and "BQ_Hash_Record_ID" in age_masking_results.columns:
        obfuscated_ids = set(age_masking_results["BQ_Hash_Record_ID"].dropna())
        df["AGE_IS_OBFUSCATED"] = df["BQ_Hash_Record_ID"].isin(obfuscated_ids)
    else:
        df["AGE_IS_OBFUSCATED"] = False

    # Overwrite original age values with max_age_days where obfuscated is True
    for col in age_cols:
        df.loc[df["AGE_IS_OBFUSCATED"], col] = max_age_days
        
    return df

def derive_age_in_months(df):
    """
    ADD DERIVED VARIABLES FOR AGE IN MONTHS
    https://docs.google.com/document/d/1Zs3vpMUxcbpbggOkxKkXzaFQvcuCZalwn0uv2ruUeQQ/edit?tab=t.0
    """
    for col in [c for c in df.columns if "AGE_IN_" in c]:
        new_col = col.replace("AGE_IN_DAYS_", "AGE_IN_MONTHS_APPROXIMATED_")
        df[new_col] = df[col]
        #Check if the number is positive and not null for the calculation.
        numeric_age = pd.to_numeric(df[col], errors="coerce")
        is_positive = numeric_age > 0
        df.loc[is_positive, new_col] = ((numeric_age[is_positive] - 1) * 12 / 365).astype(int)
        
    return df
