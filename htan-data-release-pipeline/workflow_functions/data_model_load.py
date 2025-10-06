"""
Load the HTAN Data Model 

    This module provides functions for generating a table view of the HTAN
    Data Model.

Configurations: None

Functions:
    - make_requests(url, path)
    - get_enums(schema)
    - append_values(definition, class_name, attr_name, enum_lookup, row, path)
    - get_yaml_files(url, path, schema_df)
    - main()
    
Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 10-06-2025
Date Updated: 
Modified By:  
"""

import os
import shutil
import requests
import yaml
import pandas as pd

def make_requests(url, path):
    """
    Make requests to GitHub folder and download YAML files. 

    Args:
        - url (string): Link to raw GitHub contents.
        - path (string): Download folder path.

    Returns:
        - yaml_files (list): List of YAML files within an HTAN module domain.
    """

    # Request content
    r = requests.get(url)
    r.raise_for_status()
    files = r.json()

    yaml_files = [f for f in files if f["name"].endswith(".yaml")]

    # Download files into specified path
    for f in yaml_files:
        print(f"Downloading {f["name"]}...")
        download_url = f["download_url"]
        content = requests.get(download_url).text
        file_path = os.path.join(path, f["name"])
        with open(file_path, "w") as out:
            out.write(content)

    return yaml_files

def get_enums(schema):
    """
    Get enumerations for LinkML class attributes.

    Args:
        - schema (json): Raw LinkML schema object as a json.

    Returns:
        - enum_lookup (json): Collection of attribute enumerations.
    """

    enums = schema.get("enums", {}) or {}
    enum_lookup = {
        ename: list(edef.get("permissible_values", {}).keys())
        for ename, edef in enums.items()
    }

    return enum_lookup

def append_values(definition, class_name, attr_name, enum_lookup, row, path):
    """
    Append attribute metadata to a list of dictionaries.

    Args:
        - definition (json): LinkML class object as a json.
        - class_name (string): Name of HTAN module.
        - attr_name (string): Name of specific attribute.
        - enum_lookup (json): Metadata of the associated attribute.
        - row (list): List of dictionaries containing attribute metadata.
        - path (string): Path to local YAML file download folder.

    Returns:
        - row (list): List of dictionaries containing attribute metadata.
    """
    # Get schema metadata
    desc = definition.get("description") or definition.get("title") or ""
    required = definition.get("required", False)
    uri = definition.get("slot_uri")
    rng = definition.get("range")
    perm_vals = enum_lookup.get(rng)

    # Check enumerations in separate YAML files
    if rng in enum_lookup:
        rng = "string"
    elif rng == "tissue_or_organ_of_origin_uberon_enum":
        with open(path+"/uberon_tissues.yaml", "r") as file:
            uberon = yaml.safe_load(file)
        enum_lookup = get_enums(uberon)
        perm_vals = enum_lookup.get(rng)
    else:
        pass

    # Add new row for schema df
    row.append({
        "Module": path.split('/')[2],
        "Domain": class_name,
        "Attribute": attr_name,
        "Description": desc,
        "Required": required,
        "Conditional If": None,
        "Type": rng,
        "Valid Values": perm_vals,
        "URI": uri
    })

    return row


def get_yaml_files(url, path, schema_df):
    """
    Converts and concatenates YAML formatted schema metadata as a
    Pandas DataFrame.

    Args:
        - url (string): LinkML class object as a json.
        - path (string): Name of HTAN module.
        - schema_df (pandas.DataFrame): Name of specific attribute.

    Returns:
        - pandas.DataFrame: HTAN data model schema as a DataFrame.
    """
    # Download corresponding YAML files
    os.makedirs(path, exist_ok=True)
    yaml_files = make_requests(url, path)

    rows=[]
    for f in yaml_files:

        # Ignore module level YAML files
        ignore_files = ["clinical.yaml",
                        "wes.yaml",
                        "antineoplastic_agent_enum.yaml"] # this one is weird

        if f["name"] in ignore_files:
            pass
        else:
            with open(path+"/"+f["name"], "r") as file:
                schema = yaml.safe_load(file)

            enum_lookup = get_enums(schema)
            classes = schema.get("classes", {}) or {}

            # Attribute metadata can be found in the class
            # or as slots. You must check both.
            for class_name, class_def in classes.items():
                attrs_map = class_def.get("attributes")

                # Append metadata by LinkML class attribute
                if attrs_map:
                    for attr_name, attr_def in attrs_map.items():
                        if attr_def is None:
                            attr_def = {}
                        rows = append_values(attr_def, class_name, attr_name,
                                             enum_lookup, rows, path)
                # Append metadata with LinkML slot information
                else:
                    slot_list = class_def.get("slots", []) or []
                    global_slots = schema.get("slots", {}) or {}
                    for slot_name in slot_list:
                        slot_def = global_slots.get(slot_name, {}) or {}
                        rows = append_values(slot_def, class_name, slot_name,
                                             enum_lookup, rows, path)

                # Get rule/condition if information
                rules_map = class_def.get("slot_usage")
                for item in rows:
                    attr = item.get("Attribute")
                    if (rules_map is not None) and (attr in rules_map):
                        item["Conditional If"] = rules_map[attr]['description']

    # Remove temp folder
    shutil.rmtree("./modules/")

    return pd.concat([schema_df, pd.DataFrame(rows)], ignore_index=True)

def main():
    """
    Main entry into module. Calls the htan-linkml GitHub, extracts the
    Data Model as YAML files, and represents the Data Model schema as a
    DataFrame.

    Returns:
        - pandas.DataFrame: HTAN data model schema as a DataFrame.
    """

    schema_df = pd.DataFrame(columns=['Module', 'Domain', 'Attribute',
                               'Description', 'Required', 'Conditional If',
                               'Type', 'Valid Values', "URI"])

    modules = ['Clinical', 'Core', 'WES']
    for module in modules:
        print(f"\nCollecting HTAN Data Model Schema for {module}")
        url = f"https://api.github.com/repos/ncihtan/htan-linkml/contents/modules/{module}/domains"
        path = f"./modules/{module}/domains"
        schema_df = get_yaml_files(url, path, schema_df)

    schema_df.to_csv("Helooo.csv", header=True)

    return schema_df
