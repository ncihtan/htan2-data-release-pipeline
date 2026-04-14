"""
Load the HTAN Data Model from GitHub

    This module pulls the most recent HTAN data model directly from 
    the ncihtan/htan2-data-model GitHub repository. The model is returned
    as JSON files per component, and concatenated into one JSON file or
    Pandas DataFrame here.

Configurations: None

Functions:
    - get_latest_model()
    - download_model(latest_folder)
    - build_condition_from_properties(props)
    - get_conditional_block(schema_content)
    - convert_json_to_df(data)

Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 02-24-2026
Date Updated: 04-01-2026
Modified By:  Yamina Katariya <ykatariy@systemsbiology.org>
"""

import requests
import re
import os
import base64
import json
import pandas as pd


#####################################################
#                GITHUB CONFIGURATION
#####################################################
OWNER = "ncihtan"
REPO = "htan2-data-model"
BRANCH = "main"
BASE_FOLDER = "JSON_Schemas"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

# Initialize persistent session for API
session = requests.Session()
if GITHUB_TOKEN:
    session.headers.update({
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    })


#####################################################
#              EXTERNAL ONTOLOGY LINKS
#####################################################
# Used when the list of valid values is too large to store in the data model itself

UBERON_LINK = "Link to UBERON Codes: https://www.ebi.ac.uk/ols4/ontologies/uberon"
VALID_VALUE_EXCEPTIONS = {
    "ICD_10_DISEASE_CODE": "Link to IDC 10 Codes: https://www.cdc.gov/nchs/icd/icd-10-cm/files.html?CDC_AAref_Val=https://www.cdc.gov/nchs/icd/Comprehensive-Listing-of-ICD-10-CM-Files.htm",
    "SITE_OF_RESECTION_OR_BIOPSY": UBERON_LINK,
    "TISSUE_OR_ORGAN_OF_ORIGIN_UBERON_CODE": UBERON_LINK,
    "THERAPY_ANATOMIC_SITE_UBERON_CODE": UBERON_LINK,
    "GENE_SYMBOL": "Link to Gene Symbols: https://www.genenames.org/cgi-bin/download/custom?col=gd_app_sym&status=Approved&hgnc_dbtag=on&order_by=gd_app_sym_sort&format=text&submit=submit",
    "THERAPEUTIC_AGENTS": "Link to Antineoplastic Agents: https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Drug_or_Substance/Antineoplastic_Agent.xls",
    "PRIMARY_DIAGNOSIS_NCI_THESAURUS_ID": "Link to NCI Primary Diagnosis: https://github.com/ncihtan/phase2_clinical_data_model/raw/refs/heads/main/external/ncit_diagnosis.xlsx",
    "TIMEPOINT": "Link to NCI Permissible values for TIMEPOINT: https://cadsr.cancer.gov/onedata/dmdirect/NIH/NCI/CO/CDEDD?filter=CDEDD.ITEM_ID=5899851%20and%20ver_nr=1"
}

#####################################################
#                     FUNCTIONS
#####################################################

def get_latest_model(mode="all"):
    """
    Retrieves model version information from the GitHub repository tree.

    Args:
        - mode (str): 'latest' to get the most recent version, 'all' for a full list.

    Returns:
        - (str or list): A single version string (vX.X.X) or a sorted list of versions.
    """
    version_pattern = re.compile(r"^JSON_Schemas/(v\d+\.\d+\.\d+)/")

    if mode == "latest":

        # Get the most recent commit to identify current production folder
        commits_url = f"https://api.github.com/repos/{OWNER}/{REPO}/commits"
        params = {"path": BASE_FOLDER, "sha": BRANCH, "per_page": 1}

        response = session.get(commits_url, params=params)
        response.raise_for_status()

        if not response.json():
            raise ValueError("No commits found for the specified path.")

        latest_commit = response.json()[0]
        commit_data = session.get(latest_commit["url"]).json()

        # Parse commit files to find the version folder name
        for file in commit_data.get("files", []):
            match = version_pattern.match(file["filename"])
            if match:
                return match.group(1)

        raise ValueError("No version folder found in the latest commit.")

    elif mode == "all":

        # Traverse the full tree to find all unique version folders
        tree_url = f"https://api.github.com/repos/{OWNER}/{REPO}/git/trees/{BRANCH}?recursive=1"
        response = session.get(tree_url)
        response.raise_for_status()

        tree_data = response.json()
        versions = set()

        for item in tree_data.get("tree", []):
            match = version_pattern.match(item["path"])
            if match:
                versions.add(match.group(1))

        if not versions:
            raise ValueError("No version folders found in the repository.")

        # Returns a sorted list of versions (e.g., v1.0.0, v1.0.1)
        return sorted(list(versions))

    else:
        raise ValueError("Invalid mode. Use 'latest' or 'all'.")


def download_model(folder):
    """
    Downloads all component JSON files from a specific version folder on GitHub.

    Args:
        - folder (str): The version folder name (e.g., 'v1.0.0').

    Returns:
        - attributes (dict): A dictionary where keys are filenames and values are JSON content.
    """

    attributes = {}

    url = f"https://api.github.com/repos/{OWNER}/{REPO}/contents/{BASE_FOLDER}/{folder}?ref={BRANCH}"
    response = session.get(url)
    response.raise_for_status()

    # Iterate through files in the version directory
    for item in response.json():
        if item["type"] == "file":

            file_response = session.get(item["url"])
            file_response.raise_for_status()
            file_json = file_response.json()

            # Github API returns content as base64 encoded string
            content = base64.b64decode(file_json["content"]).decode("utf-8")
            attributes[item["name"]] = json.loads(content)

    return attributes

def build_condition_from_properties(props):
    """
    Parses a JSON schema 'if' property block into a human-readable string.

    Args:
        - props (dict): The properties dictionary from a conditional block.

    Returns:
        - (str): A string representation of the logic (e.g. 'Field == Value').
    """

    parts = []

    for field, rule in props.items():

        # Handle exact value logic
        if "const" in rule:
            value = rule["const"]
            parts.append(f'{field} == {value}')

        # Handle regex pattern logic
        elif "pattern" in rule:
            parts.append(f'{field} matches "{rule["pattern"]}"')

        # Handle existence logic
        else:
            parts.append(f"{field} is present")

    return " AND ".join(parts)


def get_conditional_block(schema_content):
    """
    Parses 'allOf' blocks within a JSON schema to extract 'if/then' dependencies.

    Args:
        - schema_content (dict): The JSON content of a single component schema.

    Returns:
        - conditions (dict): A map of attribute names to their conditional logic string.
    """

    conditions = {}

    for block in schema_content.get("allOf", []):
        if_block = block.get("if", {})
        then_block = block.get("then", {})
        required_fields = then_block.get("required", [])

        condition_text = ""

        # Case 1: if --> properties structure
        if "properties" in if_block:
            condition_text = build_condition_from_properties(
                if_block["properties"]
            )

        # Case 2: if --> anyOf structure
        elif "anyOf" in if_block:
            sub_conditions = []
            for sub in if_block["anyOf"]:
                if "properties" in sub:
                    sub_conditions.append(
                        build_condition_from_properties(sub["properties"])
                    )

            if sub_conditions:
                condition_text = " OR ".join(sub_conditions)

        # Map condition to each required attribute
        for attr in required_fields:
            conditions[attr] = condition_text

    return conditions

def convert_json_to_df(data):
    """
    Flattens a dictionary of JSON schemas into a single tabular DataFrame.

    Args:
        - data (dict): The dictionary of component JSON contents.

    Returns:
        - tabular_data_model (pandas.DataFrame): The flattened HTAN data model.
    """

    rows = []

    for schema_name, schema_content in data.items():

        required_fields = set(schema_content.get("required", []))
        conditional_map = get_conditional_block(schema_content)

        # COMPONENT
        component = schema_name.split("HTAN.")[1].split("-")[0].replace("Data", "")

        # ATTRIBUTES
        properties = schema_content.get("properties", {})
        for attr_name, attr_details in properties.items():

            # DESCRIPTION
            attr_description = attr_details.get("description", "")

            # TYPE
            attr_type = attr_details.get("type", "")
            if attr_type == "array":
                item_type = attr_details.get("items", {}).get("type", "")
                attr_type = f"array[{item_type}]"

            # VALID VALUES
            valid_values = attr_details.get("enum", [])
            pattern = attr_details.get("pattern", None)
            item = attr_details.get("items", {})

            item_pattern = item.get("pattern", None)
            item_enum = item.get("enum", [])

            if valid_values:
                valid_values = ", ".join(valid_values)
            elif pattern:
                valid_values = f"Follows REGEX pattern: {pattern}"
            elif item_pattern:
                valid_values = f"Follows REGEX pattern: {item_pattern}"
            elif item_enum:
                valid_values = ", ".join(item_enum)
            else:
                valid_values = ""

            # Special conditions for too long valid values
            valid_values = VALID_VALUE_EXCEPTIONS.get(attr_name, valid_values)

            # REQUIRED
            is_required = attr_name in required_fields

            # CONDITIONAL IF
            conditional_if = conditional_map.get(attr_name, "")

            rows.append({
                "Component": component,
                "Attribute": attr_name,
                "Description": attr_description,
                "Required": is_required,
                "Conditional If": conditional_if,
                "Type": attr_type,
                "Valid Values": valid_values
            })

    tabular_data_model = pd.DataFrame(rows)
    return tabular_data_model
