"""
Validate Data Against the HTAN Data Model

    This module provides functions for validating aggregate data
    tables against the HTAN data model.

Configurations: None

Functions:
    - compare_to_data_model(data, module, name)
    
Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 10-15-2025
Date Updated: 
Modified By:  
"""

import os
import pandas as pd
import tempfile
import yaml

from linkml.validator import validate_file
from linkml_runtime.dumpers import yaml_dumper
from workflow_functions.data_model_load import set_up_github_scrape

def compare_to_data_model(data, module, name):
    """
    Compare aggregate data from the gold layer against
    the HTAN data model.

    Args:
        - data (pandas.DataFrame): Aggregate sample data.
        - module (string): HTAN data model module.
        - name (string): HTAN data model domain.
    """
    if not os.path.isdir('./modules/'):
        _ = set_up_github_scrape()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_file_path = f"{tmpdir}/data.csv"
        data.to_csv(tmp_file_path, header=True)

        report = validate_file(
            tmp_file_path,
            f"./modules/{module}/domains/{name.lower()}.yaml",
            name
        )

    # UPDATE BASED ON HOW WE WANT TO CONFIGURE THE ERRORS!
    report_yaml = yaml.safe_load(yaml_dumper.dumps(report))
    report_df = pd.DataFrame(report_yaml['results'])

    with open('ValidationReport.yaml', 'w') as file:
        yaml.dump(report_yaml, file)

    report_df.to_csv("ValidationReport.csv", header=True)

    return report_yaml, report_df
