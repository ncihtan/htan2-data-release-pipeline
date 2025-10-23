"""
Run the HTAN Data Release Pipeline

    This module serves as the entry point into the data validation and
    release staging pipeline.

Configurations: None

Functions:
    - print_section(title)
    
Author:       Yamina Katariya <ykatariy@systemsbiology.org>
Date Created: 07-21-2025
Date Updated: NA
Modified By:  NA
"""
import os

from medallion_architecture import bq_raw2bronze as r2b
from medallion_architecture import bq_bronze2silver as b2s
#from medallion_architecture import bq_silver2gold as s2g

from validators.htan_validation.utils import data_model_load

def print_section(title):
    """
    Print headers for each level of the medallion architecture
    as they are run.

    Args:
        - title (string): The level title to be printed.
    """
    border = "=" * (len(title) + 8)
    print(f"\n{border}\n=== {title.upper()} ===\n{border}\n")

# Set current working directory to .
cwd = os.getcwd()
base_path = cwd.split("htan-data-release-pipeline", maxsplit=1)[0]
curr_path = base_path + \
    'htan-data-release-pipeline/htan-data-release-pipeline/'
os.chdir(curr_path)

# -----------------------
# BQ Destinations
# -----------------------
HTAN_BQ_PROJECT = "htan2-dcc"
BRONZE_LAYER = "htan2_medallion_bronze"
SILVER_LAYER = "htan2_medallion_silver"
GOLD_LAYER = "htan2_medallion_gold"

try:
    print_section("HTAN DATA MODEL")
    htan_schema = data_model_load.main()
except Exception as e:
    print("Failed to load HTAN Data Model")

try:
    print_section("LEVEL: RAW TO BRONZE")
    r2b.main(HTAN_BQ_PROJECT, BRONZE_LAYER)
except Exception as e:
    print("Failed to run RAW TO BRONZE pipeline.")

try:
    print_section("LEVEL: BRONZE TO SILVER")
    b2s.main(HTAN_BQ_PROJECT, SILVER_LAYER, BRONZE_LAYER)
except Exception as e:
    print("Failed to run BRONZE TO SILVER pipeline.")

# try:
#     print_section("LEVEL: SILVER TO GOLD")
#     s2g.main()
# except Exception as e:
#     print("Failed to run SILVER TO GOLD pipeline.")

# Final clean-up
data_model_load.remove_data_model_files()
