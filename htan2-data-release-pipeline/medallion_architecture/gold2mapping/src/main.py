#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Gold to Mapping Tables

    This module gets all attributes with enums represented
    as codes (UBERON and Primary Diagnosis) and maps them
    to their human readable names. These mapping tables are
    then uploaded to BigQuery for the HTAN Data Portal to
    reference.

Configurations: None

Functions:
    - get_organ_from_uberon(uberon_id)
    - get_name_from_diagnosis(diagnosis_id)
    
Author: Dar'ya Pozhidayeva, Yamina Katariya
Updated: 07/1/2026
"""
import pandas as pd
from client_load import (
    load_bq,
    init_bq_client
)
import requests

#####################################################
#             SETTING GLOBAL VARIABLES
#####################################################

PROJECT = "htan2-dcc"
GOLD_DATASET = "htan2_medallion_gold"
MAP_DATASET = "htan2_data_mapping_tables"

#####################################################
#                 HELPER FUNCTIONS
#####################################################
def get_organ_from_uberon(uberon_id):
    """
    Fetches the term name and its high-level anatomical ancestors 
    using the EBI Ontology Lookup Service API.
    
    Format example for uberon_id: 'UBERON:0000948' (Heart)
    """
    # Replace colon with underscore for the API URL
    term_id = uberon_id.replace(":", "_")
    url = f"https://www.ebi.ac.uk/ols4/api/ontologies/uberon/terms/http%253A%252F%252Fpurl.obolibrary.org%252Fobo%252F{term_id}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # Get the standard label for the ID
        term_name = data.get("label", "Unknown")

        # To get the high-level organ/ancestors, we look at the hierarchical links
        # You can also fetch 'hierarchicalAncestors' for broader categories
        ancestors_url = data.get("_links", {}).get("ancestors", {}).get("href")

        organs = []
        if ancestors_url:
            anc_response = requests.get(ancestors_url)
            if anc_response.status_code == 200:
                anc_data = anc_response.json()
                # Extract names of ancestral terms
                terms = anc_data.get("_embedded", {}).get("terms", [])
                organs = [t.get("label") for t in terms]

        return {
            "id": uberon_id,
            "name": term_name,
            "lineage/organs": organs
        }

    except requests.exceptions.RequestException as e:
        return {"error": f"Failed to retrieve data: {e}"}

def get_name_from_diagnosis(diagnosis_id):
    """
    Gets the NCit Preferred Name for Diagnosis Codes
    using the NCI EVS Rest API.

    Args:
        - diagnosis_id (string): NCI Diagnosis Code.

    Returns:
        - concept.get("name") (string): NCit Preferred Name from API.
    
    """

    evs_base_url = f"https://api-evsrest.nci.nih.gov/api/v1/concept/ncit/{diagnosis_id}"

    response = requests.get(evs_base_url)

    if response.status_code != 200:
        return None

    concept = response.json()

    return concept.get("name")

##############################################
#                   MAIN
##############################################
def main():
    """
    Retrieves attributes used in the HTAN Phase 2 data model that require
    NCI-defined codes and maps them to their human-readable name.
    """
    # Initialize BQ Client
    client = init_bq_client()

    # Query for attributes with NCI codes (uberon and EVS)
    distinct_codes_query = f"""
    SELECT 
        TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE,
        PRIMARY_DIAGNOSIS_NCI_THESAURUS_ID
    FROM `{PROJECT}.{GOLD_DATASET}.gold_RELEASED_METADATA_TABLE_All_Records_Diagnosis`
    """
    codes = client.query(distinct_codes_query).to_dataframe()

    ########################
    #    UBERON TO ORGAN
    ########################
    uberon_codes = (
        codes[["TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE"]]
        .dropna()
        .drop_duplicates()
    )

    organ_names = []
    for _, row in uberon_codes.iterrows():
        print("Fetching Organ For: " + row['TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE'])
        result = get_organ_from_uberon(row['TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE'])
        print(f"Term: {result['name']}")
        organ_name = result.get('name', 'Unknown/Error').title()
        organ_names.append(organ_name)

    uberon_codes['TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_NAME'] = organ_names

    load_bq(
            client,
            PROJECT,
            MAP_DATASET,
            "HTAN2_Mapping_Uberon_to_Organ",
            uberon_codes
        )

    ########################
    #   DIAGNOSIS to NAME
    ########################
    diagnosis_codes = (
        codes[["PRIMARY_DIAGNOSIS_NCI_THESAURUS_ID"]]
        .dropna()
        .drop_duplicates()
    )

    diagnosis_names = []
    for _, row in diagnosis_codes.iterrows():
        print("Fetching Organ For: " + row['PRIMARY_DIAGNOSIS_NCI_THESAURUS_ID'])
        result = get_name_from_diagnosis(row['PRIMARY_DIAGNOSIS_NCI_THESAURUS_ID'])
        print(f"Term: {result}")
        diagnosis_names.append(result)

    diagnosis_codes['PRIMARY_DIAGNOSIS_NCI_THESAURUS_NAME'] = diagnosis_names

    load_bq(
            client,
            PROJECT,
            MAP_DATASET,
            "HTAN2_Mapping_Diagnosis_to_Name",
            diagnosis_codes
        )

    ########################
    #   PUBLICATION LIST
    ########################

    # JSON file is updated by Alex Lash (project manager) as needed

    url = (
    "https://raw.githubusercontent.com/"
    "ncihtan/htan2-data-release-pipeline/"
    "refs/heads/main/"
    "htan2-data-release-pipeline/"
    "medallion_architecture/gold2mapping/src/"
    "publication_management_file/"
    "publications_manifest_all_eutils.json"
    )

    curated_publications = pd.read_json(url)

    load_bq(
            client,
            PROJECT,
            MAP_DATASET,
            "HTAN2_Mapping_Data_to_Publications",
            curated_publications
        )

if __name__ == "__main__":
    main()
