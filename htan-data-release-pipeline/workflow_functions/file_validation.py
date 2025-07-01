import pandas as pd
import re
import os
import numpy as np

def unique_bios(meta_map,id_prov_table):
    """
    Check adjacent biospecimens exist
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
            error_msg = 'Multiple records found for parent biospecimen %s in manifests %s' % (
                bios_id, manifest_list)
            
            error_list.update({r['entityId']: error_msg})
    
    return error_list



def unique_demographics(meta_map,id_prov_table):
    """
    Check adjacent biospecimens exist
    """
    error_list = {}
    
    bios = meta_map['Demographics']
    dup_bios = bios[bios.duplicated(
        'HTAN_Participant_ID',keep=False)
        ].sort_values(by=['HTAN_Participant_ID'])
    
    for i,r in dup_bios.iterrows():
        case_id = r['HTAN Participant ID']
        downstream_files = get_downstream_files(case_id,id_prov_table)
        manifest_list = list(dup_bios[dup_bios['HTAN_Participant_ID']==case_id]['Manifest_Id'])
        
        for i,r in downstream_files.iterrows():
            error_msg = 'Multiple demographics records found for participant %s in manifests %s' % (
                case_id, manifest_list)
            
            error_list.update({r['entityId']: error_msg})
    
    return error_list


def adjacent_bios(meta_map,id_prov_table):
    """
    Check adjacent biospecimens exist
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
        else:
            adj_bios = adj_bios.replace(';', ',').replace(' ','')
            adj_ids = adj_bios.split(',')
            
            for id in adj_ids:
                if id in list(meta_map['Biospecimen']['HTAN_Biospecimen_ID']):
                    continue
                
                else:
                    downstream_files = get_downstream_files(bios_id,id_prov_table)
                    
                    for i,r in downstream_files.iterrows():
                        error_msg = 'Upstream biospecimen %s is missing adjacent biospecimen %s' % (
                            bios_id, id)
                        error_list.update({r['entityId']: error_msg})
    
    return error_list




def get_downstream_files(biospecimen_id, id_prov_table):
    """
    For a given HTAN Biospecimen ID, get all downstream files
    """

    id_prov_table = id_prov_table[~id_prov_table['Biospecimen_Path'].isna()]
    files = id_prov_table[id_prov_table['Biospecimen_Path'].str.contains(biospecimen_id)]
    
    return files


def htan_id_unique(file_list):
    """
    Check that HTAN Data File ID is unique
    """
    
    error_list = {}
    
    file_ids = file_list[file_list.duplicated(
        'HTAN_Data_File_ID',keep=False)
        ].sort_values(by=['HTAN_Data_File_ID'])
    
    dup = file_ids.groupby('HTAN_Data_File_ID')['entityId'].apply(
        lambda g: g.values.tolist()
        ).to_dict()
    
    for i,e in dup.items():
        error_message = 'HTAN ID %s is used by entities %s' % (i,e)
        for j in e:
            error_list.update({j: error_message})
    
    return error_list


def file_name_unique(file_list):
    """
    Check that HTAN filename is unique
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
        error_message = 'Filename %s is used by entities %s' % (i,e)
        for j in e:
            error_list.update({j: error_message})
    
    return error_list


def htan_id_regex(entities_to_release):
    """
    Check that data file IDs conform to HTAN ID format
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
        except:
            continue
        
        else:
            if match == False:
                error = {r['entityId']: 
                    'HTAN ID does not match specified format'
                }

                error_list.update(error)
    
    return error_list


def basename_regex(entities_to_release):
    """
    Check that data file IDs conform to HTAN ID format
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
        except:
            continue
        
        else:
            if match == False:
                error = {r['entityId']: 
                    'File basename contains unsupported characters (supported: alphanumeric (a-z,A-Z,0-9), dashes(-), periods(.), and underscores(_))'
                }

                error_list.update(error)
    
    return error_list


def parents_exist(entities_to_release,parent_id_map):
    """
    Check all IDs in parentId column exist in primaryId column
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
        error_msg = 'File %s is missing parent %s' % (r['primaryId'], r['parentId'])
        error_list.update({r['entityId']: error_msg})
    
    return error_list



def get_channel_files(syn, new_release, imaging_all, center_map):

    error_list = {}
    new_img = imaging_all[imaging_all['entityId'].isin(new_release['entityId'])]
    
    # reference files that are pointed to from assay files, 
    # but are not formally annotated
    
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

    # walk down provided Synapse path to get entityId of channel metadata file
    for i,r in channel_sub.iterrows():
        channel = r['Channel_Metadata_Filename']
        print(channel)
        #id = center_map[r['Center_ID']]['synapse_id']
        id = next((value["synapse_id"] for key, value in center_map.items() if value.get("center_id") == r['Center_ID'].lower()), None)
        
        if pd.isna(channel) or channel == 'Not Applicable':
            continue
        
        # Use Synapse ID directly if provided
        elif bool(re.search("^(syn)[0-9]{8}$", channel)):
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
        error_msg = 'Channel metadata file "%s" not found' % r['Channel_Metadata_Filename']
        error_list.update({r['entityId']: error_msg})
    
    return set(aux_files), error_list


