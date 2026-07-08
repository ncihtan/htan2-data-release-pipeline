#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Medallion Architecture: Gold to Mapping Tables
TBD

Configurations: None

Functions:
    - get_organ_from_uberon(uberon_id)
    
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
    

#####################################################
#                   MAIN 
#####################################################=
def main():
    """
    Entry point into the GOLD layer.
    """
    # Initialize BQ Client
    client = init_bq_client()
    
    distinct_uberon_codes_query = """
    SELECT DISTINCT TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE FROM 
    `htan2-dcc.htan2_medallion_gold.gold_RELEASED_METADATA_TABLE_All_Records_Diagnosis` 
    WHERE TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE IS NOT NULL
    """
    print(distinct_uberon_codes_query)
    organ_names = []
    uberon_codes = client.query(distinct_uberon_codes_query).to_dataframe()
    for index, row in uberon_codes.iterrows():
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

    #UPDATE THE PUBLICATIONS LIST. JSON FILE IS UPDATED BY ALEX LASH AS NEEDED.
    curated_publications = pd.read_json('publication_management_file/publications_manifest_all_eutils.json')
    load_bq(
            client,
            PROJECT,
            MAP_DATASET,
            "HTAN2_Mapping_Data_to_Publications",
            curated_publications
        )

if __name__ == "__main__":
    main()

