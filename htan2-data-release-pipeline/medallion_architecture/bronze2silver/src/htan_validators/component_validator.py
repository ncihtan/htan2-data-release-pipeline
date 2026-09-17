"""
This module outlines the HTANComponentValidator class, which inherits
the BaseValidator class. The HTANComponentValidator provides the
following checks on component-specific metadata tables:

    1. **Primary Key Integrity:** Verifies that required HTAN identifiers 
    (like Data File, Biospecimen, or Participant IDs) are present (not null),
    unique across individual metadata tables, and match the approved HTAN
    Center prefix.

    2. **Synapse ID Authenticity:** Pings the Synapse platform to ensure that
    the provided Synapse IDs actually exist and are accessible.

        - File_EntityId
        - Folder_EntityId

    3. **ID Format (REGEX) Matching:** Validates that HTAN IDs follow specific
    naming conventions.

        - Biospecimen Parent IDs are all existing Biospecimen or Participant IDs
        - Level 1 Sequencing and Level 2 Imaging Parent IDs are Biospecimen IDs
        - Level 2 (expect Imaging), Level 3, and Level 4 Parent IDs are Data File IDs
        - All IDs contain their associated HTAN Center's prefix

    4. **Internal ID Linkage:** Ensures that IDs referenced in one column actually
    exist in the corresponding primary ID column within the same component table.

        - ADJACENT_BIOSPECIMEN_ID exist in HTAN_BIOSPECIMEN_ID in the Biospecimen Component
        - HTAN_PARENT_ID exist in HTAN_BIOSPECIMEN_ID in the Biospecimen Component

    5. **Exclusion List Cross-Referencing:** Compares the files, clinical, and biospecimen 
    information uploaded to Synapse to those submitted to the Exclusion List Request Form,
    and flags any files, Participant IDs, and Biospecimen IDs marked for exclusion. 

    6. **File Size:** Check the file size of large format files (fastq, ome-tiffs, ect.)
    and tabular format files. A cutoff has been set for large and tabular format files. 
    
        - Large files include and must be > 1MB ["fastq", "bam", "ome-tiff", "tiff", "gzip"]
        - Tabular file formats include and must be >100 Bytes ["csv", "tsv", "txt"]
        - No file can be 0 bytes.
    
    7. **Age Verification:** Ensure that ages submitted are comply with PII and data model
    standards. Age in days must be:

        - less than 32485 days (89 years old)
        - greater than 6570 (18 years old) unless it is confirmed they are Pediatric patients
"""
import re
import pandas as pd
import numpy as np
import yaml
from htan_validators.base_validator import BaseValidator

class HTANComponentValidator(BaseValidator):
    """
    Validator for HTAN component-level checks.
    """

    def _find_and_log_regex_mismatch(self, df, regex_pattern, regex_attr, htan_col):
        """
        Validates strings in a column against one for more regex patterns.

        Args:
            df (pandas.Dataframe):
                Component-level metadata table.

            regex_pattern (str or list):
                The regex pattern(s) to match against.

            regex_attr (str or list):
                The name of the attribute(s) for error logging.

            htan_col (str):
                The column name containing the IDs to check

        Returns:
            df (pandas.Dataframe):
                Component-level metadata table.
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
            df (pandas.DataFrame):
                Component-level metadata table.

            source_col (str):
                Column containing the IDs that need to be validated.

            reference_col (str):
                Column containing the set of valid IDs for source_col.

            ids_to_check (list, optional):
                Specific IDs to validate for a single row.

            idx (int, optional):
                The specific row index if ids_to_check is provided.
        
        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
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
            df (pandas.DataFrame):
                Component-level metadata table.

            metadata_type (str):
                Synapse structure annotation type (Files or Records).

            component (str):
                The HTAN assay type (e.g. Biospecimen)

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """

        # Assign required HTAN Id based on metadata type and component
        required_ids = []
        if metadata_type == "Files" or component == "MolecularAssignment":
            required_ids = ["HTAN_DATA_FILE_ID"]
        elif metadata_type == "Records" and component in ["SpatialPanel", "ChannelMetadata"]:
            required_ids = ["HTAN_PANEL_ID"]
        elif metadata_type == "Records" and component == "Biospecimen":
            required_ids = ["HTAN_BIOSPECIMEN_ID"]
        elif metadata_type == "Records" and component not in \
            ["Biospecimen", "ChannelMetadata", "SpatialPanel", "MolecularAssignment"]:
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

            # Allow the Molecular Test Assay to have duplicated Participant IDs
            # (Participants have multiple mutations/results as rows.)
            if col == 'HTAN_PARTICIPANT_ID' and component == 'MolecularTest':
                continue

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
            df (pandas.DataFrame):
                Component-level metadata table.

            component (str):
                The HTAN assay type (e.g. Biospecimen)

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
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
            syn (Synapse instance):
                Synapse client object.

            df (pandas.DataFrame):
                Component-level metadata table.

            syn_ids_col (str):
                Name of column containing Synapse IDs to be validated.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
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

    def check_excluded_files_part_bio(self, df, exclusion_list, metadata_type, component):
        """
        Cross-reference entities against the Exclusion List to be marked for exclusion.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            exclusion_list (pandas.DataFrame):
                Exclusion list passed as a dataframe.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """
        if exclusion_list is None or exclusion_list.empty:
            return df

        match_on = ['HTAN_Center']
        entity = None

        if metadata_type == "Files":
            match_on += ['File_Name', 'File_EntityId']
            entity = 'File_Name'
        elif metadata_type == "Records" and component == "Biospecimen":
            match_on += ['HTAN_BIOSPECIMEN_ID']
            entity = 'HTAN_BIOSPECIMEN_ID'
            exclusion_list = exclusion_list.rename(
                columns={"HTAN_ORIGINATING_BIOSPECIMEN_ID": "HTAN_BIOSPECIMEN_ID"}
            )
        else:
            match_on += ['HTAN_PARTICIPANT_ID']
            entity = 'HTAN_PARTICIPANT_ID'

        temp_exclusion = exclusion_list[match_on].drop_duplicates()

        # Find overlaps
        overlap = df.reset_index().merge(
            temp_exclusion,
            on=match_on,
            how='inner'
        ).set_index('index')

        for idx in overlap.index:
            self.append_error(
                df,
                idx,
                error_type="EXCLUDED_ENTITY",
                message=(
                    f"Entity {df.at[idx, entity]} is marked as 'EXCLUDE'."
                )
            )

        return df

    def check_file_size(self, df):
        """
        Verifies that provided filesize in synapse matches cut-offs for corrupted files.

        Args:
            syn (Synapse instance):
                Synapse client object.

            df (pandas.DataFrame):
                Component-level metadata table.

            syn_ids_col (str):
                Name of column containing Synapse IDs to be validated.

	        syn_filesize (str):
                Name of column containing file sizes to be validated.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """
        syn_ids_col = "File_EntityId"
        syn_filesize = "File_Size_Bytes"
        syn_filetype = "FILE_FORMAT"

        numeric_sizes = pd.to_numeric(df[syn_filesize], errors="coerce")        
        formats = df[syn_filetype].astype(str).str.lower().str.strip()

        #Define the types of files to be checked for sizes.
        large_formats = ["fastq", "bam", "ome-tiff", "tiff", "gzip"]
        tabular_formats = ["csv", "tsv", "txt"]

        mask_large = formats.isin(large_formats) & (numeric_sizes <= 1000000)
        mask_tabular = formats.isin(tabular_formats) & (numeric_sizes <= 100)
        mask_zero = numeric_sizes == 0
        invalid_mask = mask_large | mask_tabular | mask_zero

        for idx in df[invalid_mask].index:
            self.append_error(
                df,
                idx,
                error_type="SMALL_FILE_SIZE_WARNING",
                message=f"{df.at[idx, syn_ids_col]} is {df.at[idx, syn_filesize]} bytes (format: {df.at[idx, syn_filetype]})! This may be a corrupted file. Must be checked before release."
            )

        return df

    def htan_id_verify(self, df: pd.DataFrame, center_col: str = "HTAN_Center") -> pd.DataFrame:
        """
        Verifies that HTAN Identifiers in each row begin with the exact prefix 
        mapped to that row's HTAN_Center in projects.yaml.
    
        Args:
            df (pandas.DataFrame): 
                Component-level metadata table.

            center_col (str): 
                Column name containing the HTAN center identifier.
    
        Returns:
            df (pandas.DataFrame): 
                Component-level metadata table.
        """
        if center_col not in df.columns:
            return df

        with open("./htan_validators/projects.yaml", "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
        projects = data.get("projects", [])

        # Find all HTAN ID columns in df (excluding the HTAN_Center column itself)
        htan_id_cols = [
            col for col in df.columns
            if "HTAN" in col and "ID" in col and col != center_col
        ]

        if not htan_id_cols:
            return df

        # Group rows by HTAN_Center value
        for center_val, group in df.groupby(center_col):
            if pd.isna(center_val) or not str(center_val).strip():
                continue
        
            query = str(center_val).strip()
            #Match HTAN_Center with yaml HTAN_Center
            valid_prefixes = tuple(
                str(entry["prefix"]).strip()
                for entry in projects
                if "prefix" in entry and query == str(entry.get("HTAN_Center", "")).strip()
            )
        
            if not valid_prefixes:
                continue
        
            #Check all HTAN ID columns for non-matching prefixes
            for col in htan_id_cols:
                if "HTAN_PARENT_ID" != col:
                    col_series = group[col].fillna("").astype(str)
                    #(True if HTAN ID is invalid and doesn't match)
                    invalid_mask = (col_series != "") & (~col_series.str.startswith(valid_prefixes))
                #If the column IS the parent ID, unnest first and then perform a check.
                else:
                    parent_series = group[col].apply(
                        lambda x: x.split(",") if isinstance(x, str) and ("," in x or x.startswith("[")) else x
                    )
                    #Unnest and strip quotes, brackets, backslashes, and whitespace
                    col_series = (
                        parent_series.explode()
                        .fillna("")
                        .astype(str)
                        .str.strip('[]"\'\\ \t\n\r')
                    )
                    #Mark invalid items
                    invalid_items = (col_series != "") & (~col_series.str.startswith(valid_prefixes))
                    #Re-align mask with original group column index (True if ANY parent ID is invalid)
                    invalid_mask = invalid_items.groupby(level=0).any()
        
                for idx in group[invalid_mask].index:
                    expected_prefix = ", ".join(valid_prefixes)
                    self.append_error(
                        df,
                        idx,
                        error_type="INVALID_HTAN_ID",
                        message=f"ID '{df.at[idx, col]}' in column '{col}' does not start with expected prefix '{expected_prefix}' for center '{center_val}'."
                    )

        return df

    def check_ages(self, df):
        """
        Flag columns pertaining to age where the age reported:
            - is > 32485 (89 years old)
            - is < 6570 (18 years old)

        Args:
            df (pandas.DataFrame): 
                Component-level metadata table.
    
        Returns:
            df (pandas.DataFrame): 
                Component-level metadata table.
        """

        # Get all columns relating to age
        age_columns = [col for col in df.columns if 'AGE_' in col]

        if not age_columns:
            return df

        min_age_days = 18 * 365
        max_age_days = 89 * 365
        age_not_available = -1

        for col in age_columns:

            numeric_ages = pd.to_numeric(df[col], errors="coerce")

            # Check if ages are over 89 years old (32485 days)
            over_89 = numeric_ages > max_age_days
            for idx in df[over_89].index:
                self.append_error(
                    df,
                    idx,
                    error_type="AGE_OVER_89",
                    message=f"{df.at[idx, col]} in {col} is greater than 89 years old (32485 days)"
                )

            # Check ig ages are under 18 years old (6570 days)
            under_18 = (numeric_ages < min_age_days) & (numeric_ages != age_not_available)
            for idx in df[under_18].index:
                self.append_error(
                    df,
                    idx,
                    error_type="AGE_UNDER_18",
                    message=f"{df.at[idx, col]} in {col} is under 18 years old (6570 days)"
                )

        return df

    def validate(self, df, syn, client, metadata_type, component, exclusion_list):
        """
        Main entry point to run all relevant validation checks on 
        a component-level dataframe.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            syn (Synapse instance):
                Synapse client object.

            metadata_type (str):
                Synapse structure annotation type (Files or Records).

            component (str):
                The HTAN assay type (e.g. Biospecimen)

            exclusion_list (pandas.DataFrame):
                Exclusion list passed as a dataframe.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
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

                # Check Ages (#7)
                df = self.check_ages(df)

            # Check HTAN Parent ID format (#3)
            if (metadata_type == "Files" or component == "Biospecimen") and component != "SpatialPanel":
                df = self.check_parent_id_type(client, df, component)

            # Check internal linkage for Biospecimen (#4)
            if component == "Biospecimen":
                df = self.check_id_linkage(df, "ADJACENT_BIOSPECIMEN_IDS", "HTAN_BIOSPECIMEN_ID")

            # Cross-reference exclusion list (#5)
            if component not in ["SpatialPanel", "ChannelMetadata", "MolecularAssignment"]:
                df = self.check_excluded_files_part_bio(df, exclusion_list, metadata_type, component)

            # Check to see if the file size is suspicious (#6)
            if metadata_type == "Files" and component != "SpatialLevel3":
                df = self.check_file_size(df)

            # Check that reported HTAN ID contains the correct center (#3)
            df = self.htan_id_verify(df, center_col="HTAN_Center")

        return df
