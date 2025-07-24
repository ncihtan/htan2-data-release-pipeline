#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Raw to Bronze Level

    This module processes Synapse project metadata that has yet to be 
    evaluated as part of a medallion architecture. It introduces the
    infrastructure and data cleaning needed for data promotion to the
    bronze level. 

Configurations:

    This module reads configuration settings from
    'configs/config.json' and 'configs/schema.json'
    for Synapse and BigQuery data retrieval.

Functions:
    - main()
    
Author:       Dar'ya Pozhidayeva <dpozhida@systemsbiology.org>
Date Created: 12-12-2024
Date Updated: 07-09-2025
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""
import glob
import json
import os
import shutil
import re
import pandas as pd
from workflow_functions.client_load import (
    init_bq_client,
    init_synapse_client,
    load_bq,
    get_description)

# ----------------------------------------
#        MODULE-LEVEL CONFIGURATION
# ----------------------------------------

# Instantiate synapse client
syn = init_synapse_client()

with open('configs/config.json', 'r') as file:
    config_file = json.load(file)

# Environment variables
center_map = os.environ.get('HTAN_CENTERS_MAP')
htan_centers = config_file['centers']

# Descriptions for additional BigQuery columns
attributes = os.environ.get('ATTRIBUTE_DESCRIPTIONS')
add_descriptions = config_file['descriptions']

# Identify file-based components
file_components = os.environ.get('ASSAYS')
assays = config_file['assays']

def main():
    """
    Main function to evaluate and promote data to the Bronze level.
    """

    # Instantiate google bigquery client
    client = init_bq_client()

    data_model = client.query("""
            SELECT * FROM `htan-dcc.metadata.data-model`
        """).result().to_dataframe()
    data_model['label'] = [x.replace(' ','').lower() for x in list(data_model['Attribute'])]

    # Get all manifests and group by project (i.e. research center/atlas)
    latest_manifests = client.query("""
            SELECT * FROM `htan-dcc.htan_medallion_raw.raw_synapse_latest_manifests`
        """).result().to_dataframe()
    metadata_manifests = latest_manifests.groupby(["parentId"]).max()
    metadata_manifests = metadata_manifests.groupby(["projectId"])

    # Exclude non-official HTAN centers (e.g. test ones)
    combined_tables = {}
    combined_tables_manifests = []
    for project_id, dataset_group in metadata_manifests:
        center = syn.get(project_id[0], downloadFile = False).name
        if not center in htan_centers:
            continue
        center_id = htan_centers[center]
        print("ATLAS: " + center)
        datasets = dataset_group.to_dict("records")

        # Loop through each dataset in the metadata manifests
        for dataset in datasets:
            manifest_location = "./tmp/" + center_id + "/" + dataset["manifestEntityId"] + "/"
            manifest_path = manifest_location + "synapse_storage_manifest.csv"
            manifest = syn.get(
                dataset["manifestEntityId"],
                downloadLocation=manifest_location,
                ifcollision='overwrite.local'
            )
            os.rename(glob.glob(manifest_location + "*.csv")[0], manifest_path)

            # Read in each manifest
            manifest_data = pd.read_csv(manifest_path)
            manifest_id = manifest.id
            manifest_version = manifest.versionNumber    
            combined_tables_manifests.append({'manifestEntityId': dataset["manifestEntityId"],
                                                'manifest_nrows': len(manifest_data)})
            # Get the schema component
            try:
                component = manifest_data['Component'][0]
            except KeyError:
                print(f"Component not found in manifest {dataset['manifestEntityId']}")
                continue

            # Skip components that are N/A
            if pd.isna(component):
                print("Component is N/A: " + manifest_path + " " + str(manifest_data))
                continue
            print('Template: ' + center + ' ' + component)

            # Reindex table to remove non-standard or user-defined columns
            attr = ['entityId','Uuid','Id']
            try:
                attr = attr + [x.strip(' ') for x in list(
                    data_model[data_model['label'] == 
                    component.lower()]['DependsOn'])[0].split(',')
                    ]
            except IndexError:
                print(component + ' not found in data model')
                continue

            for a in attr:
                if a in ['Data Type','Assay Type']:
                    continue

                try:
                    valid_values = [x.strip(' ') for x in list(
                        data_model[data_model['label'] == 
                        a.replace(' ','').lower()]['Valid_Values'])[0].split(',')
                    ]
                    for value in valid_values:
                        try:
                            attr = attr + [x.strip(' ') for x in list(
                                data_model[data_model['label'] ==
                                value.replace(' ','').lower()]['DependsOn'])[0].split(',')
                            ]
                        except (AttributeError, IndexError, KeyError):
                            continue
                except (AttributeError, IndexError, KeyError):
                    continue

            if component in ['BulkRNA-seqLevel2','BulkWESLevel2']:
                attr.append('HTAN Parent Biospecimen ID')
            if component == 'ImagingLevel2':
                attr.append('HTAN Parent Data File ID')

            attr = list(set(attr))

            manifest_data = (manifest_data.reindex(columns=attr)).astype("string")

            # Add center name, manifest ID, and manifest version columns to table
            manifest_data['HTAN_Center'] = center
            manifest_data['Manifest_Id'] = manifest_id
            manifest_data['Manifest_Version'] = manifest_version

            # Merge tables by component
            if component in combined_tables:
                combined_tables[component]['data'] = pd.concat(
                    [combined_tables[component]['data'],manifest_data]
                ).reset_index(drop=True)
            else:
                combined_tables[component] = {"data": manifest_data}

    # Load the combined indexing table to BigQuery
    combined_tables_manifests_df = pd.DataFrame(combined_tables_manifests)
    manifests_bronze = pd.merge(combined_tables_manifests_df,
                                latest_manifests,
                                on="manifestEntityId",
                                how="inner")
    load_bq(client,'htan-dcc', 'htan_medallion_bronze',
            'bronze_INDEXING_TABLE_Manifests', manifests_bronze)

    all_files_and_manifests = pd.DataFrame()
    latest_files = client.query("""
            SELECT * FROM `htan-dcc.htan_medallion_raw.raw_synapse_latest_fileview`
        """).result().to_dataframe()

    for key, value in combined_tables.items():
        bq_table = value['data']

        # Add file size and md5 columns to non-biospecimen/clinical tables
        if any(a in key for a in assays):
            bq_table = bq_table.merge(
                latest_files[['fileEntityId',
                    'dataFileSizeBytes',
                    'dataFileMD5Hex'
                    ]], 
                how='left', left_on='entityId', right_on='fileEntityId'
            )

            bq_table = bq_table.rename(columns={
                'dataFileSizeBytes': 'File_Size', 
                'dataFileMD5Hex': 'md5'}
            )

        # Create bigquery table schema and set all columns as 'string'
        bq_schema = []
        default_type = 'STRING'

        for column_name, dtype in bq_table.dtypes.items():
            bq_schema.append(
                {
                    'name': re.sub('[^0-9a-zA-Z]+', '_', column_name),
                    'type': default_type if column_name not in 
                        ['File_Size', 'Manifest_Version'] else 'integer',
                    'description': get_description(column_name, data_model, add_descriptions)
                }
            )

        # Make column names BigQuery-friendly
        bq_table.columns = bq_table.columns.str.replace('[^0-9a-zA-Z]+', '_', regex=True)
        bq_table = bq_table.drop_duplicates()
        bq_table['BQ_Hash'] = bq_table.apply(lambda x: hash(tuple(x)), axis = 1)

        # Load table to BigQuery
        bronze_key = "bronze_METADATA_TABLE_" + key
        load_bq(client,'htan-dcc', 'htan_medallion_bronze', bronze_key, bq_table, bq_schema)

        data_subset = bq_table[['entityId', 'Manifest_Id',
                                'Component', 'Manifest_Version',
                                'HTAN_Center', 'BQ_Hash']]
        all_files_and_manifests = pd.concat([all_files_and_manifests, data_subset],
                                            ignore_index=True)

    load_bq(client,'htan-dcc', 'htan_medallion_bronze',
            'bronze_INDEXING_TABLE_All_Files', all_files_and_manifests)

    bios = client.query("""
            SElECT * FROM `htan-dcc.id_provenance.biospecimen_ids`
            """).result().to_dataframe()

    table_list = client.list_tables('htan-dcc.htan_medallion_bronze')

    all_tables = client.query("""
                SELECT table_name,column_name FROM
                `htan-dcc.htan_medallion_bronze.INFORMATION_SCHEMA.COLUMNS`
                """).result().to_dataframe()

    bq_schema = json.load(open('configs/schema.json'))

    f = pd.DataFrame()

    for t in table_list:

        if t.table_id in {"bronze_Manifests", "bronze_CDSSequencingTemplate", "bronze_All_Files"}:
            continue

        tn = t.table_id
        if tn not in ['OtherAssay','ExSeqMinimal'] and 'Auxiliary' not in tn and 'Level' not in tn:
            continue

        print( '' )
        print( ' Processing: ' + str(tn) )
        print( '' )

        cols = list(all_tables[all_tables['table_name'] == tn]['column_name'])

        common_cols = ['HTAN_Data_File_ID',
                    'Filename',
                    'fileEntityId',
                    'Component',
                    'HTAN_Center']

        if 'HTAN_Parent_Biospecimen_ID' in cols:
            common_cols.append('HTAN_Parent_Biospecimen_ID')

        if 'HTAN_Parent_Data_File_ID' in cols:
            common_cols.append('HTAN_Parent_Data_File_ID')

        qco = ', '.join(common_cols)

        df = client.query(f"""
            SELECT {qco}
            FROM `{t.project}.{t.dataset_id}.{t.table_id}`
            """).result().to_dataframe()

        f = pd.concat([f, df], ignore_index=True)

    # Expand comma and semicolon separated parent lists into individual rows
    f = f.assign(HTAN_Parent_Data_File_ID = \
        f.HTAN_Parent_Data_File_ID.str.split("[,;]")).explode('HTAN_Parent_Data_File_ID')
    f = f.assign(HTAN_Parent_Biospecimen_ID = \
        f.HTAN_Parent_Biospecimen_ID.str.split("[,;]")).explode('HTAN_Parent_Biospecimen_ID')
    f = f.applymap(lambda x: x.strip() if isinstance(x, str) else x).drop_duplicates()

    print( '' )
    print( ' Walking parent file ancestry ' )
    print( '' )

    # Join on parent IDs, HTAN assays currently have at most 4 levels
    f = (
        f.merge(f[['HTAN_Data_File_ID','HTAN_Parent_Data_File_ID']], 
                left_on='HTAN_Parent_Data_File_ID',
                right_on='HTAN_Data_File_ID',
                how='left')
        .drop_duplicates()
        .rename(columns={'HTAN_Parent_Data_File_ID_y':'gParent_File_ID',
                        'HTAN_Parent_Data_File_ID_x':'HTAN_Parent_Data_File_ID',
                        'HTAN_Data_File_ID_x':'HTAN_Data_File_ID'})
        .drop(columns='HTAN_Data_File_ID_y')
        .merge(f[['HTAN_Data_File_ID','HTAN_Parent_Data_File_ID']], 
                left_on='gParent_File_ID',
                right_on='HTAN_Data_File_ID',
                how='left')
        .drop_duplicates()
        .rename(columns={'HTAN_Data_File_ID_x':'HTAN_Data_File_ID',
                        'HTAN_Parent_Data_File_ID_y':'ggParent_File_ID',
                        'HTAN_Parent_Data_File_ID_x':'HTAN_Parent_Data_File_ID'})
        .drop(columns='HTAN_Data_File_ID_y')
    )

    # Coalesce 'highest' level parent data file
    f['coalesce'] = f[['ggParent_File_ID','gParent_File_ID', \
        'HTAN_Parent_Data_File_ID']].bfill(axis=1).iloc[:,0]

    f = (
        f.merge(f[['HTAN_Data_File_ID', 'HTAN_Parent_Biospecimen_ID']], 
                left_on='coalesce',
                right_on='HTAN_Data_File_ID',
                how='left')
        .drop_duplicates()
        .assign(HTAN_Parent_Biospecimen_ID_x=lambda x: x[
            'HTAN_Parent_Biospecimen_ID_x'].fillna(x['HTAN_Parent_Biospecimen_ID_y']))
        .rename(columns={'HTAN_Data_File_ID_x':'HTAN_Data_File_ID',
                        'HTAN_Parent_Biospecimen_ID_x':'HTAN_Assayed_Biospecimen_ID'})
        .drop(columns=['HTAN_Data_File_ID_y', 'gParent_File_ID', 'coalesce', 
            'HTAN_Data_File_ID_y', 'HTAN_Parent_Biospecimen_ID_y', 'ggParent_File_ID'])
    )

    # Join file table with biospecimen walker table
    id_prov = f.merge(bios,
                    on='HTAN_Assayed_Biospecimen_ID',
                    how='left').drop_duplicates().rename(columns={"fileEntityId": "entityId"})
    bq_project = 'htan-dcc'
    dest_ds = 'htan_medallion_bronze'
    dest_table = 'bronze_INDEXING_TABLE_Upstream_IDs'
    load_bq(client, bq_project, dest_ds, dest_table, id_prov, bq_schema)

    # Create and load HTAN unique IDs table
    unique_ids_minted_datafiles = id_prov[['entityId',
                                        'HTAN_Data_File_ID',
                                        'HTAN_Center']].drop_duplicates('HTAN_Data_File_ID')
    unique_ids_minted_patients = id_prov[['entityId',
                                        'HTAN_Participant_ID',
                                        'HTAN_Center']].drop_duplicates('HTAN_Participant_ID')

    load_bq(client,'htan-dcc', 'htan_medallion_bronze',
            'bronze_INDEXING_TABLE_Minted_Datafile_IDs', unique_ids_minted_datafiles)
    load_bq(client,'htan-dcc', 'htan_medallion_bronze',
            'bronze_INDEXING_TABLE_Minted_Participant_IDs', unique_ids_minted_patients)

    # Remove temporary folder
    try:
        shutil.rmtree('tmp/')
        print("\n Folder tmp/ removed successfully.")
    except OSError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
