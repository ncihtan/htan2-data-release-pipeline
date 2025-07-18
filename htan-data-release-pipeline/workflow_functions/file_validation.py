"""
File Validation

    This module provides functions for file level validation including:
        - Unique HTAN_Data_File_ID
        - Unique HTAN_Biospecimen_ID
        - Unique HTAN_Participant_ID within demographics manifest(s)
        - Compliance of HTAN_Data_File_ID with HTAN_ID_SOP format
        - Existence of Synapse_ID provided in Synapse metadata
        - Existence of listed Adjacent_Biospecimen_ID as biospecimen entity
        - Presence of Parent IDs
        - Minimum DependsOn attributes are present in metadata manifest

    Additional checks available for internal use, but not mandatory for
    release include:
        - Uniqueness of base filenames
        - Equivalence of a file's Synapse name, alias, and bucket basename
        - Identification of non-data-model columns added to a manifest

Configurations: None

Functions:
    - unique_bios(meta_map,id_prov_table)
    - unique_demographics(meta_map,id_prov_table)
    - adjacent_bios(meta_map,id_prov_table)
    - get_downstream_files(biospecimen_id, id_prov_table)
    - htan_id_unique(file_list)
    - file_name_unique(file_list)
    - htan_id_regex(entities_to_release)
    - basename_regex(entities_to_release)
    - parents_exist(entities_to_release,parent_id_map)
    - get_channel_files(syn, new_release, imaging_all, center_map)
    
Author:       Clarisse Lau <clau@systemsbiology.org>
Date Created: 05-15-2024
Date Updated: 07-17-2025
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""

import pandas as pd
import re
import os
import numpy as np

def unique_bios(meta_map,id_prov_table):
    """
    Check for duplicate Biospecimen IDs.

    Args:
        - meta_map (dict): A dictionary that organizes metadata tables by
            component.
        - id_prov_table (pandas.Dataframe): ID Provenance indexing table as
            a DataFrame.

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """
    error_list = {}
    bios = meta_map['Biospecimen']
    dup_bios = bios[bios.duplicated(
        'HTAN_Biospecimen_ID',keep=False)
        ].sort_values(by=['HTAN_Biospecimen_ID'])

    for i,r in dup_bios.iterrows():
        bios_id = r['HTAN_Biospecimen_ID']
        downstream_files = get_downstream_files(bios_id,id_prov_table)
        manifest_list = list(dup_bios[dup_bios['HTAN_Biospecimen_ID']==bios_id]['Manifest_Id'])

        for i,r in downstream_files.iterrows():
            error_msg = f"""Multiple records found for parent biospecimen {bios_id}
                            in manifests {manifest_list}
                        """
            error_list.update({r['entityId']: error_msg})

    return error_list

def unique_demographics(meta_map,id_prov_table):
    """
    Check for duplicate HTAN_Participant_ID within demographics manifest(s).

    Args:
        - meta_map (dict): A dictionary that organizes metadata tables by
            component.
        - id_prov_table (pandas.Dataframe): ID Provenance indexing table as
            a DataFrame.

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """
    error_list = {}
    bios = meta_map['Demographics']
    dup_bios = bios[bios.duplicated(
        'HTAN_Participant_ID',keep=False)
        ].sort_values(by=['HTAN_Participant_ID'])

    for i,r in dup_bios.iterrows():
        case_id = r['HTAN_Participant_ID']
        downstream_files = get_downstream_files(case_id,id_prov_table)
        manifest_list = list(dup_bios[dup_bios['HTAN_Participant_ID']==case_id]['Manifest_Id'])

        for i,r in downstream_files.iterrows():
            error_msg = f"""Multiple demographics records found for participant
                            {case_id} in manifests {manifest_list}
                        """
            error_list.update({r['entityId']: error_msg})

    return error_list

def adjacent_bios(meta_map,id_prov_table):
    """
    Check that adjacent Biospecimens exist.

    Args:
        - meta_map (dict): A dictionary that organizes metadata tables by
            component.
        - id_prov_table (pandas.Dataframe): ID Provenance indexing table as
            a DataFrame.

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """
    error_list = {}
    bios = meta_map['Biospecimen']
    bios['Adjacent_Biospecimen_IDs'] = bios[
        'Adjacent_Biospecimen_IDs'].astype(str)

    for i,r in bios.iterrows():
        bios_id = r['HTAN_Biospecimen_ID']
        adj_bios = r['Adjacent_Biospecimen_IDs']

        if pd.isna(adj_bios):
            continue
        if adj_bios == 'None':
            continue

        adj_bios = adj_bios.replace(';', ',').replace(' ','')
        adj_ids = adj_bios.split(',')

        for id in adj_ids:
            if id in list(meta_map['Biospecimen']['HTAN_Biospecimen_ID']):
                continue

            downstream_files = get_downstream_files(bios_id,id_prov_table)
            for i,r in downstream_files.iterrows():
                error_msg = f"Upstream biospecimen {bios_id} is missing adjacent biospecimen {id}"
                error_list.update({r['entityId']: error_msg})

    return error_list

def get_downstream_files(biospecimen_id, id_prov_table):
    """
    Get all downstream files for a given HTAN Biospecimen ID.

    Args:
        - biospecimen_id (string): HTAN Biospecimen ID as a string.
        - id_prov_table (pandas.Dataframe): ID Provenance indexing table as
            a DataFrame.

    Returns:
        - files (pandas.DataFrame): DataFrame containing the full Biospecimen
            ancestry path from HTAN_Assayed_Biospecimen_ID to
            HTAN_Originating_Biospecimen_ID for a given HTAN Biospecimen ID. 
    """

    id_prov_table = id_prov_table[~id_prov_table['Biospecimen_Path'].isna()]
    files = id_prov_table[id_prov_table['Biospecimen_Path'].str.contains(biospecimen_id)]

    return files

def htan_id_unique(file_list):
    """
    Check that HTAN Data File ID is unique.

    Args:
        - file_list (pandas.DataFrame): DataFrame containing HTAN File IDs.  

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """

    error_list = {}
    file_ids = file_list[file_list.duplicated(
        'HTAN_Data_File_ID',keep=False)
        ].sort_values(by=['HTAN_Data_File_ID'])

    dup = file_ids.groupby('HTAN_Data_File_ID')['entityId'].apply(
        lambda g: g.values.tolist()
        ).to_dict()

    for i,e in dup.items():
        error_message = f"HTAN ID {i} is used by entities {e}"
        for j in e:
            error_list.update({j: error_message})

    return error_list

def file_name_unique(file_list):
    """
    Check that HTAN filename is unique.

    Args:
        - file_list (pandas.DataFrame): DataFrame containing HTAN File IDs.  

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """

    error_list = {}
    file_list['Basename'] = [os.path.basename(x) for x in list(file_list['Filename'])]
    file_ids = file_list[file_list.duplicated(
        'Basename',keep=False)
        ].sort_values(by=['Basename'])

    dup = file_ids.groupby('Basename')['entityId'].apply(
        lambda g: g.values.tolist()
        ).to_dict()

    for i,e in dup.items():
        error_message = f"Filename {i} is used by entities {e}"
        for j in e:
            error_list.update({j: error_message})

    return error_list

def htan_id_regex(entities_to_release):
    """
    Check that data file IDs conform to HTAN ID format.

    Args:
        - entities_to_release (pandas.DataFrame): DataFrame containing HTAN File IDs.  

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """

    error_list = {}

    for i,r in entities_to_release.iterrows():
        if r['Component'] == 'AccessoryManifest':
            continue

        if 'EXT' in r['HTAN_Data_File_ID']:
            continue

        try:
            match = bool(re.match(
                r'HTA\d{1,2}_\d+_\d+$', 
                r['HTAN_Data_File_ID'])
            )
        except TypeError:
            continue

        else:
            if match is False:
                error = {r['entityId']: 
                    'HTAN ID does not match specified format'
                }
                error_list.update(error)

    return error_list

def basename_regex(entities_to_release):
    """
    Check if data file names contain unsupported characters. 

    Args:
        - entities_to_release (pandas.DataFrame): DataFrame containing HTAN File IDs.  

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """
    error_list = {}

    for i,r in entities_to_release.iterrows():
        if r['Component'] == 'AccessoryManifest':
            continue

        if 'EXT' in r['Filename']:
            continue

        basename = os.path.basename(r['Filename'])
        try:
            match = bool(re.match(
                r'[a-zA-Z0-9\-\.\_\/]', 
                basename)
            )
        except TypeError:
            continue

        else:
            if match is False:
                error = {r['entityId']:
                    'File basename contains unsupported characters (supported: alphanumeric (a-z,A-Z,0-9), dashes(-), periods(.), and underscores(_))'
                }
                error_list.update(error)

    return error_list

def parents_exist(entities_to_release,parent_id_map):
    """
    Check all IDs in parentId column exist in primaryId column. 

    Args:
        - entities_to_release (pandas.DataFrame): DataFrame containing HTAN File IDs.
        - parent_id_map (pandas.Dataframe): Dataframe linking the primary-parent
            file ID relationship.

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs. 
    """

    error_list = {}

    primary_ids = list(parent_id_map['primaryId'])
    parent = list(parent_id_map['parentId'])
    diff = list(set(parent).difference(primary_ids))
    missing = parent_id_map[parent_id_map['parentId'].isin(diff)]
    missing = missing.replace({np.nan: None})

    release_missing = missing[missing['primaryId'].isin(
        entities_to_release['HTAN_Data_File_ID'])
    ]

    for i,r in release_missing.iterrows():
        error_msg = f"File {r['primaryId']} is missing parent {r['parentId']}"
        error_list.update({r['entityId']: error_msg})

    return error_list

def get_channel_files(syn, new_release, imaging_all, center_map):
    """
    Collect auxiliary imaging files and channel metadata Synapse IDs for a new
    release and log any missing channel metadata files.

    Args:
        - syn (Synapse instance): Synapse client object.
        - new_release (pandas.DataFrame): Manifest data as a DataFrame.
        - imaging_all (pandas.DataFrame): Metadata filtered to contain only
            imaging data.
        - center_map (dict): Dictionary mapping HTAN Centers with their 
            Synapse IDs and Center IDs.

    Returns:
        - error_list (dict): Entity IDs and their error messages as key-value pairs.
        - aux_files (list): A list of Synapse file IDs.
    """
    error_list = {}
    new_img = imaging_all[imaging_all['entityId'].isin(new_release['entityId'])]

    # Get files referenced from assay files, but are not formally annotated
    # TODO add in GeoMx reference files
    attribs = [
        'Channel_Metadata_Filename',
        'MERFISH_Positions_File',
        'MERFISH_Codebook_File'
    ]

    new_img = new_release.merge(
        imaging_all[['entityId']+attribs], how='left'
    )
    new_img['Center_ID'] = [x.split('_')[0] for x in 
        list(new_img['HTAN_Data_File_ID'])
    ]

    channel_sub = new_img[[
        'Center_ID'] + attribs
    ].drop_duplicates().reset_index()

    aux_files = channel_sub['MERFISH_Codebook_File'].dropna(
        ).unique().tolist() + channel_sub[
        'MERFISH_Positions_File'].dropna().unique().tolist()

    missing = []

    # Walk down provided Synapse path to get entityId of channel metadata file
    for i,r in channel_sub.iterrows():
        channel = r['Channel_Metadata_Filename']
        id = next((value["synapse_id"] for key, value in center_map.items() \
                   if value.get("center_id") == r['Center_ID'].lower()), None)

        if pd.isna(channel) or channel == 'Not Applicable':
            continue

        # Use Synapse ID directly if provided
        if bool(re.search("^(syn)[0-9]{8}$", channel)):
            aux_files.append(channel)

        else:
            folders = channel.split('/')
            for f in folders:
                children = list(syn.getChildren(id, includeTypes=['folder','file']))
                record = [j for j in children if j['name'] == f]
                if len(record) == 0:
                    missing.append(channel)
                    continue
                else:
                    id=record[0]['id']

            aux_files.append(id)

    release_missing = new_img[new_img['Channel_Metadata_Filename'].isin(missing)]

    for i,r in release_missing.iterrows():
        error_msg = f"Channel metadata file {r['Channel_Metadata_Filename']} not found"
        error_list.update({r['entityId']: error_msg})

    return set(aux_files), error_list
