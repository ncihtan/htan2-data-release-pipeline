"""
HTAN Validation: Base Class

    This module outlines the BaseValidator class, which serves as the
    foundation for all HTAN validation including:
        - HTANComponentValidator
        - HTANProvenanceValidator

    and provides the following core utilities:

        1. Initialize Release Validation columns in all metadata tables.

        2. Retrieves REGEX patterns from stored HTAN Data Models.

        3. Parses BigQuery lists represented as strings.

        4. Provides a unified logging structure, ensuring the following
        information is recorded for each error:
            - Release_Validation_Passed (bool): Flag denoting release validation status
            - Release_Validation_Timestamp (datetime): Time when data was evaluated.
            - Release_Violations (list): List containing general error categories.
            - Release_Error_Messages (list[dict]): List of detailed error messages,
              where messages are formatted as dictionaries:
                * Key = Error category
                * Value = Detailed message

Author:       Yamina Katariya <ykatariy@systemsbiology.org> 
Date Created: 04-01-2026
Date Updated: 04-17-2026
Modified By:
"""

import ast
import re
import pandas as pd

class BaseValidator:
    """
    Base class for HTAN validators. Provides core utilities for error logging
    and general helper functions.
    """

    def initialize_columns(self, df):
        """
        Ensure all Release_* columns exist in the metadata tables for
        standardized error reporting.

        Args:
            - df (pandas.Dataframe): Component-level metadata table.

        Returns:
            - df (pandas.Dataframe): Component-level metadata table.
        """
        if "Release_Validation_Passed" not in df.columns:
            df["Release_Validation_Passed"] = True

        if "Release_Validation_Timestamp" not in df.columns:
            df["Release_Validation_Timestamp"] = None

        if "Release_Violations" not in df.columns:
            df["Release_Violations"] = None

        if "Release_Error_Messages" not in df.columns:
            df["Release_Error_Messages"] = None

        return df

    def get_regex(self, data_model, component, htan_id):
        """
        Retrieves the REGEX patterns for specific attributes
        from the provided data model.

        Args:
            - data_model (pandas.DataFrame): The versioned, tabular data model.
            - component (str): The HTAN assay type (e.g., Biospecimen).
            - htan_id (str): The attribute to retrieve the REGEX for.

        Returns:
            - regex_pattern (re): The REGEX pattern of interest.
        """
        row = data_model[
            (data_model["Component"] == component) &
            (data_model["Attribute"] == htan_id)
        ]
        regex_pattern = row["Valid_Values"].str.extract(r"Follows REGEX pattern:\s*(.*)").iloc[0, 0]

        return re.compile(regex_pattern)

    def break_bq_list(self, bq_list):
        """
        Convert BigQuery stringified lists (possibly nested) into a proper Python list.

        Args:
            - bq_list (str): The attribute of a BQ table to be converted from a string to a list.

        Returns:
            - bq_list (list): The attribute of a BQ table as a list.
        """

        # Unwrap nested stringified lists
        while isinstance(bq_list, str):
            try:
                bq_list = ast.literal_eval(bq_list)
            except Exception:
                break

        # Handle nulls
        if not isinstance(bq_list, list) and pd.isna(bq_list):
            return []

        # Ensure output is always a list
        if not isinstance(bq_list, list):
            bq_list = [bq_list]

        return bq_list

    def append_error(self, df, idx, error_type, message):
        """
        Append an error to a specific row in the metadata table.

        Args:
            - df (pandas.DataFrame): The metadata table being validated.
            - idx (int): The row index where the error occurred.
            - error_type (str): The standardized category of the error.
            - message (str): Detailed information regarding the violation.

        Returns:
            - df (pandas.DataFrame): Dataframe with updated error logs.
        """

        # Set validation flag + timestamp
        df.at[idx, "Release_Validation_Passed"] = False

        # Initialize lists if needed
        if not isinstance(df.at[idx, "Release_Violations"], list):
            df.at[idx, "Release_Violations"] = []

        if not isinstance(df.at[idx, "Release_Error_Messages"], list):
            df.at[idx, "Release_Error_Messages"] = []

        # Append values
        df.at[idx, "Release_Violations"].append(error_type)
        error_entry = {error_type: message}
        df.at[idx, "Release_Error_Messages"].append(error_entry)

        return df
    
    def query_bigquery_table(self, client, project_id, dataset_id, table_id, attrs="*"):
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
            SELECT {attrs}
            FROM `{project_id}.{dataset_id}.{table_id}`
        """
        return client.query(query).to_dataframe()

    def get_versioned_data_model(self, client, schema_ver):
        """
        Loads a specific version of the data model from that data models
        pulled and saved in tabular form as CSVs.

        Args:
            - schema_ver (str): The version number of the data model.
        
        Returns:
            - (pandas.DataFrame): The loaded data model.
        """
        schema_ver = schema_ver.replace(".", "_")
        model = self.query_bigquery_table(
            client,
            'htan2-dcc',
            'htan2_data_model_cache',
            f"HTAN2_Data_Model_v{schema_ver}"
        )

        return model

    def validate(self, *args, **kwargs):
        """
        Abstract method to be implemented by subclasses.
        Executes the specific validation logic for the component.
        """
        raise NotImplementedError("Subclasses must implement the `validate` method.")
