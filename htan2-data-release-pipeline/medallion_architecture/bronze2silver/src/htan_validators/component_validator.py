"""
HTAN Validation: Component

    This module outlines the HTANComponentValidator class, which inherits
    the BaseValidator class. The HTANComponentValidator provides the
    following checks on component-specific metadata tables:

        1. Primary Key Integrity: Verifies that required HTAN identifiers 
        (like Data File, Biospecimen, or Participant IDs) are present (not null)
        and unique across individual metadata tables.

        2. Synapse ID Authenticity: Pings the Synapse platform to ensure that
        the provided Synapse IDs actually exist and are accessible.
            - File_EntityId
            - FolderEntityId
            - CHANNEL_METADATA_ID
            - PANEL_SYNAPSE_ID

        3. ID Format (REGEX) Matching: Validates that HTAN IDs follow specific
        naming conventions.
            - Biospecimen Parent IDs are all existing Biospecimen or Participant IDs
            - Level 1 Sequencing and Level 2 Imaging Parent IDs are Biospecimen IDs
            - Level 2 (expect Imaging), Level 3, and Level 4 Parent IDs are Data File IDs

        4. Internal ID Linkage: Ensures that IDs referenced in one column actually
        exist in the corresponding primary ID column within the same component table.
            - ADJACENT_BIOSPECIMEN_ID exist in HTAN_BIOSPECIMEN_ID in the Biospecimen Component
            - HTAN_PARENT_ID exist in HTAN_BIOSPECIMEN_ID in the Biospecimen Component

        5. Exclusion List Cross-Referencing: Compares the files uploaded to Synapse to
        those submitted to the Exclusion List Request Form, and flags any files marked
        for exclusion.

Author:       Yamina Katariya <ykatariy@systemsbiology.org> 
Date Created: 04-01-2026
Date Updated: 04-17-2026
Modified By:  
"""

import re
import pandas as pd
import numpy as np

from htan_validators.base_validator import BaseValidator

class HTANComponentValidator(BaseValidator):
    """
    Validator for HTAN component-level checks.
    """

    def _find_and_log_regex_mismatch(self, df, regex_pattern, regex_attr, htan_col):
        """
        Validates strings in a column against one for more regex patterns.

        Args:
            - df (pandas.Dataframe): Component-level metadata table.
            - regex_pattern (str or list): The regex pattern(s) to match against.
            - regex_attr (str or list): The name of the attribute(s) for error logging.
            - htan_col (str): The column name containing the IDs to check

        Returns:
            - df (pandas.Dataframe): Component-level metadata table.
        """

        # Ensure patterns and attributes are lists for iteration
        if not isinstance(regex_pattern, list):
            regex_pattern = [regex_pattern]
        if not isinstance(regex_attr, list):
            regex_attr = [regex_attr]

        # Compile patterns are REGEX
        compiled_patterns = [
            p if hasattr(p, "fullmatch") else re.compile(p)
            for p in regex_pattern
        ]

        # Construct error message list
        if len(regex_attr) == 1:
            attr_text = regex_attr[0]
        else:
            attr_text = " or ".join(regex_attr)

        for idx, ids in df[htan_col].items():

            # Break down list-strings from BQ 
            ids = self.break_bq_list(ids)

            # Check regex patterns
            invalid_ids = [
                pid for pid in ids
                if not pd.isna(pid) and not any(
                    pattern.fullmatch(str(pid)) for pattern in compiled_patterns
                )
            ]

            if invalid_ids:
                self.append_error(
                    df,
                    idx,
                    error_type="INVALID_HTAN_ID",
                    message=f"{invalid_ids} in {htan_col} do not match {attr_text} format."
                )

        return df

    def check_id_linkage(self, df, source_col, reference_col, ids_to_check=None, idx=None):
        """
        Verifies that IDs in a source column exist within a reference column
        in the same table.

        Args:
            - df (pandas.DataFrame): Component-level metadata table.
            - source_col (str): Column containing the IDs that need to be validated.
            - reference_col (str): Column containing the set of valid IDs for source_col.
            - ids_to_check (list, optional): Specific IDs to validate for a single row.
            - idx (int, optional): The specific row index if ids_to_check is provided.
        
        Returns:
            - df (pandas.DataFrame): Component-level metadata table.
        """

        # Get a unique set of reference IDs
        valid_ids = set(df[reference_col].dropna())

        # Logic for checking a specific row index
        if ids_to_check is not None and idx is not None:

            invalid_ids = [
                pid for pid in ids_to_check
                if pid not in valid_ids
            ]

            if invalid_ids:
                self.append_error(
                    df,
                    idx,
                    error_type="UNRESOLVED_ID_PATH",
                    message=f"{invalid_ids} in {source_col} not found in {reference_col}."
                )

            return df

        # Logic for checking the entire column
        invalid_mask = df[source_col].notna() & ~df[source_col].isin(valid_ids)
        for idx in df[invalid_mask].index:
            self.append_error(
                df,
                idx,
                error_type="UNRESOLVED_ID_PATH",
                message=f"{df.at[idx, source_col]} in {source_col} not found in {reference_col}."
            )

        return df

    def check_nulls_and_duplicates(self, df, metadata_type, component):
        """
        Checks that provided HTAN Identifiers are present and unique across
        the component level.

        Args:
            - df (pandas.DataFrame): Component-level metadata table.
            - metadata_type (str): Synapse structure annotation type (Files or Records).
            - component (str): The HTAN assay type (e.g. Biospecimen)

        Return:
            - df (pandas.DataFrame): Component-level metadata table.
        """

        # Assign required HTAN Id based on metadata type and component
        required_ids = []
        if metadata_type == "Files" and component != "SpatialPanel":
            required_ids = ["HTAN_DATA_FILE_ID"]
        elif metadata_type == "Files" and component == "SpatialPanel":
            required_ids = ["HTAN_PANEL_ID"]
        elif metadata_type == "Files" and component == "ChannelMetadata":
            required_ids = ["HTAN_PANEL_ID"]
        elif metadata_type == "Records" and component == "Biospecimen":
            required_ids = ["HTAN_BIOSPECIMEN_ID"]
        elif metadata_type == "Records" and component not in ["Biospecimen", "ChannelMetadata", "SpatialPanel"]:
            required_ids = ["HTAN_PARTICIPANT_ID"]

        for col in required_ids:

            # Standardize nulls from BQ
            df[col] = df[col].replace(['', 'nan', 'NaN', 'None', 'null'], np.nan)

            # Check for missing values
            null_mask = df[col].isna()
            for idx in df[null_mask].index:
                self.append_error(
                    df,
                    idx,
                    error_type="MISSING_HTAN_ID",
                    message=f"{col} is null."
                )

            # Check for duplicate values
            dup_mask = df[col].duplicated(keep=False) & df[col].notna()
            for idx in df[dup_mask].index:
                self.append_error(
                    df,
                    idx,
                    error_type="DUPLICATE_HTAN_ID",
                    message=f"{col} is duplicated."
                )

        return df

    def check_parent_id_type(self, client, df, component):
        """
        Validates that HTAN Parent ID matches the expected format based on the 
        REGEX in the data model and component-specific requirements.

        Args:
            - df (pandas.DataFrame): Component-level metadata table.
            - component (str): The HTAN assay type (e.g. Biospecimen)

        Returns:
            - df (pandas.DataFrame): Component-level metadata table.
        """

        # Get the latest schema version for uploaded data
        # Note: The data model pulled here is the most recent release
        model_ver = sorted(df["Curator_Schema_Version"].dropna().unique().tolist(), reverse=True)
        data_model = self.get_versioned_data_model(client, model_ver[0])

        # Get the REGEX patterns from the data model
        data_file_id_regex = self.get_regex(data_model, "BulkWESLevel1", "HTAN_DATA_FILE_ID")
        biospecimen_id_regex = self.get_regex(data_model, "Biospecimen", "HTAN_BIOSPECIMEN_ID")
        participant_id_regex = self.get_regex(data_model, "Demographics", "HTAN_PARTICIPANT_ID")

        # RULE: Level 2 (except Imaging), Level 3, and Level 4 Parent IDs must be Data File IDs
        if any(x in component for x in ["Level2", "Level3", "Level4"]) and component != "MultiplexMicroscopyLevel2":
            return self._find_and_log_regex_mismatch(df,
                                                     data_file_id_regex,
                                                     "HTAN_DATA_FILE_ID", 
                                                     "HTAN_PARENT_ID")

        # RULE: Biospecimen Parent IDs must be existing Biospecimen or Participant IDs
        elif component == "Biospecimen":
            df = self._find_and_log_regex_mismatch(df,
                                                     [participant_id_regex, biospecimen_id_regex],
                                                     ["HTAN_PARTICIPANT_ID", "HTAN_BIOSPECIMEN_ID"],
                                                     "HTAN_PARENT_ID")
            for idx, ids in df["HTAN_PARENT_ID"].items():
                ids = self.break_bq_list(ids)
                biospecimen_ids = [
                    pid for pid in ids
                    if not pd.isna(pid) and biospecimen_id_regex.fullmatch(str(pid))
                ]
                if biospecimen_ids:
                    df = self.check_id_linkage(df, "HTAN_PARENT_ID", "HTAN_BIOSPECIMEN_ID",
                                          biospecimen_ids, idx)

            return df

        # RULE: Level 1 and Level 2 Imagining Parent IDs must be Biospecimen IDs
        else:
            return self._find_and_log_regex_mismatch(df,
                                                     biospecimen_id_regex,
                                                     "HTAN_BIOSPECIMEN_ID", 
                                                     "HTAN_PARENT_ID")

    def check_synapse_id(self, syn, df, syn_ids_col):
        """
        Verifies that provided Synapse IDs are active Synapse entities.

        Args:
            - syn (Synapse instance): Synapse client object.
            - df (pandas.DataFrame): Component-level metadata table.
            - syn_ids_col (str): Name of column containing Synapse IDs to be validated.

        Returns:
            - df (pandas.DataFrame): Component-level metadata table.
        """

        # Get unique list of Synapse IDs
        missing_ids = set()
        all_syn_ids = df[syn_ids_col].unique().tolist()

        # Test each Synapse ID against the Synapse API
        for syn_id in all_syn_ids:

            try:
                syn.get(syn_id, downloadFile=False)
            except Exception:
                missing_ids.add(syn_id)

        # Flag Synapse IDs that do not exist
        invalid_mask = df[syn_ids_col].isin(missing_ids)
        for idx in df[invalid_mask].index:
            self.append_error(
                df,
                idx,
                error_type="INVALID_SYNAPSE_ID",
                message=f"{df.at[idx, syn_ids_col]} in {syn_ids_col} is not an active Synapse entity."
            )

        return df

    def check_excluded_files(self, df, exclusion_list):
        """
        Cross-reference files against the Exclusion List to be marked for exclusion.

        Args:
            - df (pandas.DataFrame): Component-level metadata table.
            - exclusion_list (pandas.DataFrame): Exclusion list passed as a dataframe.

        Returns:
            - df (pandas.DataFrame): Component-level metadata table.
        """
        if exclusion_list is None or exclusion_list.empty:
            return df

        # Mapping: {component df col: exclusion list col}
        comparison_cols = {
            'File_Name': 'Filename', 
            'File_EntityId': 'entityId',
            'HTAN_Center': 'HTAN_Center'
        }

        # Create a temporary subset of the exclusion list with matching names for a merge
        temp_exclusion = exclusion_list.rename(columns={
            'Filename': 'File_Name',
            'entityId': 'File_EntityId',
            'HTAN_Center': 'HTAN_Center'
        })[list(comparison_cols.keys())]

        # Find overlaps
        overlap = df.reset_index().merge(
            temp_exclusion, 
            on=list(comparison_cols.keys()), 
            how='inner'
        ).set_index('index')

        for idx in overlap.index:
            self.append_error(
                df,
                idx,
                error_type="EXCLUDED_FILE",
                message=(
                    f"File {df.at[idx, 'File_Name']} is marked as 'EXCLUDE'."
                )
            )

        return df

    def validate(self, df, syn, client, metadata_type, component, exclusion_list):
        """
        Main entry point to run all relevant validation checks on 
        a component-level dataframe.

        Args:
            - df (pandas.DataFrame): Component-level metadata table.
            - syn (Synapse instance): Synapse client object.
            - metadata_type (str): Synapse structure annotation type (Files or Records).
            - component (str): The HTAN assay type (e.g. Biospecimen)
            - exclusion_list (pandas.DataFrame): Exclusion list passed as a dataframe.

        Returns:
            - df (pandas.DataFrame): Component-level metadata table.
        """
        # Initialize error columns
        df = self.initialize_columns(df)

        #######################
        # Validation Checks
        #######################

        if not df.empty:

            # Null and duplicate checks (#1)
            df = self.check_nulls_and_duplicates(df, metadata_type, component)

            # Check Synapse ID entity status (#2)
            if metadata_type == "Files":
                df = self.check_synapse_id(syn, df, "File_EntityId")
            elif metadata_type == "Records":
                df = self.check_synapse_id(syn, df, "Folder_EntityId")

            # Check HTAN Parent ID format (#3)
            if (metadata_type == "Files" or component == "Biospecimen") and component != "SpatialPanel":
                df = self.check_parent_id_type(client, df, component)

            # Check internal linkage for Biospecimen (#4)
            if component == "Biospecimen":
                df = self.check_id_linkage(df, "ADJACENT_BIOSPECIMEN_IDS", "HTAN_BIOSPECIMEN_ID")

            # Cross-reference exclusion list (#5)
            if metadata_type == "Files":
                df = self.check_excluded_files(df, exclusion_list)

        return df
