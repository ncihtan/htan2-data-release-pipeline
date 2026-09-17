"""
This module outlines the HTANProvenanceValidator class, which
inherits the BaseValidator class. The HTANProvenanceValidator
performs validation checks across metadata tables to ensure
the integrity of the HTAN entity provenance chain with the
following checks:

    1. **Center-level Record Completeness:** Ensures each HTAN center
    has submitted at least one Biospecimen and one Demographics
    record.

    2. **Parent-to-Participant Linkages:** Verifies that Participant IDs
    referenced as Biospecimen Parent IDs exist as a Demographics
    record.

    3. **Participants in Non-Demographics Tables:** Ensures that
    Participant IDs used in non-Demographics record sets are present
    in the Demographics table.

    4. **Biospecimen-to-File Linkages:** Ensures that Biospecimen IDs are
    linked to files via the provenance table.

    5. **Panel-to-File Linkages:** Ensures that Panel metadata (Channel,
    Spatial, and MolecularAssignment) are linked to Files and that all Panel
    metadata provided exists in the ChannelMetadata, SpatialPanel, and
    MolecularAssignment record sets.

    6. **Data File ID Cross-Validation:** Ensures that each HTAN_DATA_FILE_ID
    is unique across all centers.

    7. **Panel Exclusions:** Marks and panels (Channel, Spatial, and
    MolecularAssignment) associated with files submitted to the Exclusion Request
    Portal as excluded.
"""

import pandas as pd
import numpy as np
from htan_validators.base_validator import BaseValidator

class HTANProvenanceValidator(BaseValidator):
    """
    Validator for HTAN provenance chain checks.
    """

    def check_bio_and_demo_record_per_center(self, df, id_prov, component):
        """
        Ensures that each HTAN center that has submitted data to the active
        release cycle has submitted at least one Biospecimen and one
        Demographics record.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            id_prov (pandas.DataFrame):
                Provenance table.

            component (str):
                Component being validated.
        
        Returns:
            id_prov (pandas.DataFrame):
                Provenance table with errors.
        """

        valid_centers = set(df["HTAN_Center"].dropna().unique())

        for idx in id_prov.index:
            center = id_prov.at[idx, "HTAN_Center"]

            if pd.notna(center) and center not in valid_centers:
                self.append_error(
                    id_prov,
                    idx,
                    error_type="MISSING_CENTER_RECORD",
                    message=f"Center {center} does not have any corresponding records in {component}."
                )

        return id_prov

    def check_parent_to_participant_linkages(self, client, df, demo_df):
        """
        Validate that Participant IDs referenced as HTAN_PARENT_IDs exist
        in the Demographics metadata table.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            demo_df (pandas.DataFrame):
                Demographics metadata table.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """

        # Get the HTAN Participant ID REGEX
        # Note: The data model pulled here is the most recent release
        model_ver = sorted(df["Curator_Schema_Version"].dropna().unique().tolist(), reverse=True)
        data_model = self.get_versioned_data_model(client, model_ver[0])
        participant_id_regex = self.get_regex(data_model, "Demographics", "HTAN_PARTICIPANT_ID")

        valid_participants = set(demo_df['HTAN_PARTICIPANT_ID'].dropna().unique())

        for idx, ids in df['HTAN_PARENT_ID'].items():

            # Read in BQ lists as Pandas lists
            ids = self.break_bq_list(ids)
            parent_participants = [
                pid for pid in ids 
                if not pd.isna(pid) and participant_id_regex.fullmatch(str(pid))
            ]

            # Check participant IDs not in demographics table
            missing_participants = [
                pid for pid in parent_participants
                if pid not in valid_participants
            ]

            if missing_participants:
                self.append_error(
                    df,
                    idx,
                    error_type="MISSING_DEMOGRAPHICS",
                    message=(
                        f"The following Participant IDs in HTAN_PARENT_ID: {missing_participants} "
                        f"were not submitted as Demographic Clinical metadata."
                    )
                )

        return df

    def check_participants_in_non_demographics(self, df, demo_df):
        """
        Ensure that Participant IDs in non-Demographics tables exist
        in the Demographics metadata table.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            demo_df (pandas.DataFrame):
                Demographics metadata table.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """

        valid_demo_ids = set(demo_df['HTAN_PARTICIPANT_ID'].dropna().unique())

        current_ids = df['HTAN_PARTICIPANT_ID'].dropna().unique()
        missing_ids = [pid for pid in current_ids if pid not in valid_demo_ids]

        if missing_ids:

            invalid_mask = df['HTAN_PARTICIPANT_ID'].isin(missing_ids)

            for idx in df[invalid_mask].index:
                missing_id = df.at[idx, 'HTAN_PARTICIPANT_ID']
                self.append_error(
                    df,
                    idx,
                    error_type="MISSING_DEMOGRAPHICS",
                    message=(
                        f"Participant ID '{missing_id}' was not submitted as a "
                        f"Demographics record."
                    )
                )

        return df

    def check_biospecimen_linked_to_files(self, df, id_prov):
        """
        Validate that all record set Biospecimen IDs are linked to files,
        and that all Biospecimen IDs linked to files exist in the Biospecimen
        metadata table.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            id_prov (pandas.DataFrame):
                Provenance table.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """

        prov_biospecimens = set(id_prov['HTAN_ASSAYED_BIOSPECIMEN_ID'].dropna().unique())
        all_biospecimens = df['HTAN_BIOSPECIMEN_ID'].dropna().unique()

        # Exist in Biospecimen but not in Provenance table
        missing_ids_bio = [bid for bid in all_biospecimens if bid not in prov_biospecimens]
        if missing_ids_bio:

            invalid_mask = df['HTAN_BIOSPECIMEN_ID'].isin(missing_ids_bio)
            for idx in df[invalid_mask].index:
                missing_id = df.at[idx, 'HTAN_BIOSPECIMEN_ID']
                self.append_error(
                    df,
                    idx,
                    error_type="UNUSED_BIOSPECIMEN",
                    message=(
                        f"Biospecimen ID '{missing_id}' is not linked to any files."
                    )
                )

        # Exist in Provenance but not Biospecimen table
        missing_ids_prov = [bid for bid in prov_biospecimens if bid not in all_biospecimens]
        if missing_ids_prov:
            missing_mask = id_prov['HTAN_ASSAYED_BIOSPECIMEN_ID'].isin(missing_ids_prov)
            for idx in id_prov[missing_mask].index:
                self.append_error(
                    id_prov,
                    idx,
                    error_type="MISSING_BIOSPECIMEN",
                    message=(
                        f"Biospecimen ID '{id_prov.at[idx, 'HTAN_ASSAYED_BIOSPECIMEN_ID']}' was "
                        f"not submitted as a Biospecimen record."
                    )
                )

        return df

    def check_panel_data(self, df, table_id, metadata_type, client, htan_id):
        """
        Validate panel ID linkage between File metadata and panel (channel) metadata.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            table_id (str):
                BigQuery table name.

            metadata_type (str):
                Metadata type (Files or Records).

            client (BigQuery instance):
                BigQuery client object.

            htan_id (str):
                Panel attribute name.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """

        # Pull the reference table from BQ
        error_type = ""
        message = ""
        queried_df = self.query_bigquery_table(
            client,
            'htan2-dcc',
            'htan2_medallion_bronze',
            table_id
        )

        if queried_df.empty:
            for idx in df.index:
                self.append_error(
                    df,
                    idx,
                    error_type="UNUSED_PANEL",
                    message="No files have been submitted for this Panel."
                )
            return df

        # Check if HTAN_PANEL_ID is null for File metadata
        if metadata_type == "Files":
            null_mask = df[htan_id].isna()

            for idx in df[null_mask].index:
                self.append_error(
                    df,
                    idx,
                    error_type="MISSING_PANEL",
                    message=f"Associated metadata is not linked because {htan_id} is missing."
                )

        # Get actual IDs from metadata tables to check
        target_ids = df[htan_id].dropna().unique()
        reference_ids = queried_df[htan_id].dropna().unique()

        # Identify panel type (e.g., ChannelMetadata or SpatialPanel)
        panel_type = queried_df['Component'].dropna().unique()[0]

        if panel_type == "MolecularAssignment":
            dup_mask = df[htan_id].duplicated(keep=False) & df[htan_id].notna()
            for idx in df[dup_mask].index:
                self.append_error(
                    df,
                    idx,
                    error_type="DUPLICATE_PANEL",
                    message=f"{df.at[idx, htan_id]} is duplicated."
                )

        # Check if Panel metadata linkage to files
        missing_ids = [pid for pid in target_ids if pid not in reference_ids]
        if missing_ids:

            invalid_mask = df[htan_id].isin(missing_ids)
            for idx in df[invalid_mask].index:
                missing_id = df.at[idx, htan_id]

                # Files: panel ID should have a corresponding record set
                if metadata_type == "Files":
                    error_type = "MISSING_PANEL"
                    message = f"'{missing_id}' was not submitted as a {panel_type} record."

                # Record Sets: panel exists but is not used by any files
                else:
                    error_type = "UNUSED_PANEL"
                    message = f"'{missing_id}' is not linked to any files."

                self.append_error(
                    df,
                    idx,
                    error_type=error_type,
                    message=message
                )

        return df

    def check_excluded_panels(self, df, client, exclusion_ids, table_id, htan_id):
        """
        Evaluates if panels (HTAN_DATA_FILE_ID or HTAN_PANEL_ID) are
        excluded from the current release.

        Args:
            df (pandas.DataFrame):
                Component-level metadata table.

            client (BigQuery instance):
                BigQuery client object.

            exclusion_ids (list):
                List of HTAN_DATA_FILE_IDs that were
                marked as 'EXCLUDE'.
            
            table_id (str):
                BigQuery table name.

            htan_id (str):
                Panel attribute name.

        Returns:
            df (pandas.DataFrame):
                Component-level metadata table.
        """

        file_df = self.query_bigquery_table(
            client,
            'htan2-dcc',
            'htan2_medallion_bronze',
            table_id
        )

        excluded_panels = file_df.loc[
            file_df["HTAN_DATA_FILE_ID"].isin(exclusion_ids["HTAN_DATA_FILE_ID"]),
            htan_id
        ].tolist()

        invalid_mask = df[htan_id].isin(excluded_panels)

        for idx in df[invalid_mask].index:
            excluded_panel = df.at[idx, htan_id]

            self.append_error(
                df,
                idx,
                error_type="EXCLUDED_PANEL",
                message=f"{excluded_panel} is marked as 'EXCLUDE'."
            )

        return df

    def prov_cross_validation(self, id_prov):
        """
        Ensure that each HTAN_DATA_FILE_ID uniquely maps to a single file.

        Args:
            id_prov (pandas.DataFrame):
                Provenance table.

        Returns:
            id_prov (pandas.DataFrame):
                Provenance table with appended errors.
        """

        # Normalize missing values
        id_prov["HTAN_DATA_FILE_ID"] = id_prov["HTAN_DATA_FILE_ID"] \
            .replace(['', 'nan', 'NaN', 'None', 'null'], np.nan)

        # Only work on valid IDs
        valid_df = id_prov[id_prov["HTAN_DATA_FILE_ID"].notna()].copy()

        # Count unique (File_Name, File_EntityId) pairs per ID
        pair_counts = (
            valid_df
            .drop_duplicates(subset=["HTAN_DATA_FILE_ID", "File_Name", "File_EntityId"])
            .groupby("HTAN_DATA_FILE_ID")
            .size()
        )

        # IDs that map to more than one unique file
        invalid_ids = pair_counts[pair_counts > 1].index

        # Build lookup of file names per invalid ID
        id_to_files = (
            valid_df
            .drop_duplicates(subset=["HTAN_DATA_FILE_ID", "File_Name", "File_EntityId"])
            .groupby("HTAN_DATA_FILE_ID")["File_Name"]
            .apply(list)
            .to_dict()
        )

        # Flag rows with those IDs
        invalid_mask = id_prov["HTAN_DATA_FILE_ID"].isin(invalid_ids)

        for idx in id_prov[invalid_mask].index:
            file_id = id_prov.at[idx, "HTAN_DATA_FILE_ID"]
            file_names = id_to_files.get(file_id, [])

            self.append_error(
                id_prov,
                idx,
                error_type="ID_CROSS_VALIDATION",
                message=(
                    f"Data File ID '{file_id}' is assigned to multiple different files: "
                    f"{', '.join(file_names)}."
                )
            )

        return id_prov

    def validate(self, client, df, id_prov, exclusion_list, metadata_type, component):
        """
        Execute all provenance validation checks for a given component.

        Args:
            client (BigQuery instance):
                BigQuery client object.

            df (pandas.DataFrame):
                Component-level metadata table.

            id_prov (pandas.DataFrame):
                Provenance table.

            exclusion_list (pandas.DataFrame):
                Exclusion list passed as a dataframe.

            metadata_type (str):
                Metadata type (Files or Records).

            component (str):
                Component being validated.

        Returns:
            tuple[pandas.DataFrame, pandas.DataFrame]:
                - Updated metadata table with errors logged.
                - Updated provenance table with errors logged.
        """

        # Initialize error columns
        df = self.initialize_columns(df)
        id_prov = self.initialize_columns(id_prov)

        # Query the associated metadata tables (demographics, channel, and panel)
        demo_df = self.query_bigquery_table(
            client,
            'htan2-dcc',
            'htan2_medallion_bronze',
            'bronze_METADATA_TABLE_All_Records_Demographics',
            "HTAN_PARTICIPANT_ID"
        )

        # Get list of excluded HTAN DATA FILE IDs
        excluded_files_ingest = exclusion_list[
            exclusion_list["File_EntityId"].isin(id_prov["File_EntityId"])
        ]

        exclusion_ids = None
        if not excluded_files_ingest.empty:
            exclusion_ids = exclusion_list.merge(
                id_prov[['File_Name', 'File_EntityId',
                         'HTAN_Center', 'HTAN_DATA_FILE_ID', 'Component']],
                on=['File_Name', 'File_EntityId', 'HTAN_Center'],
                how='left',
                indicator=True
            )
            exclusion_ids = exclusion_ids[exclusion_ids['Component'] == component]

        # Initialize panel table couples
        panel_checks = {
            "MultiplexMicroscopyLevel2": (
                "bronze_METADATA_TABLE_All_Records_ChannelMetadata",
                "HTAN_PANEL_ID",
                False
            ),
            "SpatialLevel3": (
                "bronze_METADATA_TABLE_All_Records_SpatialPanel",
                "HTAN_PANEL_ID",
                False
            ),
            "MassSpectrometryImagingLevel3": (
                "bronze_METADATA_TABLE_All_Records_MolecularAssignment",
                "HTAN_DATA_FILE_ID",
                False
            ),
            "ChannelMetadata": (
                "bronze_METADATA_TABLE_All_Files_MultiplexMicroscopyLevel2",
                "HTAN_PANEL_ID",
                True
            ),
            "SpatialPanel": (
                "bronze_METADATA_TABLE_All_Files_SpatialLevel3",
                "HTAN_PANEL_ID",
                True
            ),
            "MolecularAssignment": (
                "bronze_METADATA_TABLE_All_Files_MassSpectrometryImagingLevel3",
                "HTAN_DATA_FILE_ID",
                True
            )
        }

        #######################
        # Validation Checks
        #######################

        if not df.empty:

            # Check Center-level Biospecimen and Demographics completeness (#1)
            if component in ["Biospecimen", "Demographics"]:
                id_prov = self.check_bio_and_demo_record_per_center(df, id_prov, component)

            if component == "Biospecimen":

                # Check Participant IDS in Biospecimen are recoded in Demographics (#2)
                df = self.check_parent_to_participant_linkages(client, df, demo_df)

                # Check Biospecimen IDs are linked to files (#4)
                df = self.check_biospecimen_linked_to_files(df, id_prov)

                # Check Data File IDs are unique across centers (#6)
                id_prov = self.prov_cross_validation(id_prov)

            if component in panel_checks:
                table_id, htan_id, check_exclusions = panel_checks[component]

                # Check Panel (Channel, Spatial, MolecularAssignment) Linkage to Files (#5)
                df = self.check_panel_data(df, table_id, metadata_type, client, htan_id )

                # Check Panel Exclusions (#7)
                if check_exclusions and exclusion_ids is not None:
                    df = self.check_excluded_panels( df, client, exclusion_ids, table_id, htan_id )

            # Check Participant IDs in non-Demographics tables exist in Demographics (#3)
            if metadata_type == "Records" and component not in ["Biospecimen", "Demographics", "ChannelMetadata", "SpatialPanel", "MolecularAssignment"]:
                df = self.check_participants_in_non_demographics(df, demo_df)

        return df, id_prov
