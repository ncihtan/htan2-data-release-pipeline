<p align="center" style="background-color: #040104">
  <img src="./htan-data-release-pipeline/images/HTAN_banner.jpg" />
</p>

<a id="introduction"></a>
## Introduction

Timely and reliable data releases are essential to large research initiatives, enabling public access to high-quality datasets that drive scientific discovery. In the Human Tumor Atlas Network (HTAN), data is made available through the [HTAN Data Portal](https://humantumoratlas.org/explore), Sage Bionetwork's [Synapse](https://www.synapse.org/) platform, the National Cancer Institute’s (NCI) Cancer Research Data Commons (CRDC) [General Commons](https://datacommons.cancer.gov/repository/general-commons) (GC), and the [Institute for Systems Biology Cancer Gateway in the Cloud (ISB-CGC)](https://portal.isb-cgc.org/). Before releases, datasets undergo validation to ensure quality, consistency, and compliance with standards.

For the complete standard operating procedure (SOP) for releasing HTAN data, please review [SOP: HTAN Data Release (General)](https://docs.google.com/document/d/1P4rojKgx2Alomjxu4Zu2keJ0jApf8OZl1RNBoJEqkQ0/edit?usp=drive_link). 

#### Team Members:
| Name                   | GitHub             |
| ---------------------- | ------------------ |
| **Dar'ya Pozhidayeva** | *PozhidayevaDarya* |
| **Yamina Katariya**    | *ykatariy*         |
| **Vésteinn Þórsson**   | *vthorsson*        |
| **David L. Gibbs**     | *Gibbsdavidl*      |
| **Clarisse Lau**       | *clarisse-lau*     |

## Table of Contents
* [Introduction](#introduction)
* [Tasks of Interest](#tasks-of-interest)
* [Repository Structure](#repository-structure)
* [Installation](#installation)
  * [Environment](#environment)
* [Data Storage](#data-storage)
  * [Synapse](#synapse)
  * [Google BigQuery](#bigquery)
* [Scheduled Jobs](#scheduled-jobs)
* [Relevant Repositories](#relevant-repos)

<a id="tasks-of-interest"></a>
## Tasks of Interest
- **Pull data from Synapse**:
  - HTAN intended release files and metadata are hosted on Synapse. Clinical and biospecimen metadata are stored in tables, while file-level annotations capture key details like assay type, tissue, platform, and processing level. 
- **Release tracking and metrics in BigQuery**:
  - All metadata is ingested into BigQuery via a Medallion architecture. This architecture organizes data into structured layers-Bronze, Silver, and Gold—with each layer representing increasing levels of validation, curation, and readiness for public release.
- **Validate data and metadata Synapse files**:
  - Validation occurs on both a triggered and continuous timeline, from initial upload to modifications of files, and final release. 
- **Exclude files from final release**
  - HTAN Centers may request specific files or projects be excluded from future data releases. Part of the validation process is pulling the list of files/projects that must be excluded and ensuring they do not appear in the Gold level BigQuery tables. HTAN Centers use this [form]() to submit data exclusions.

**NOTE! This repository does not publish HTAN data on the ISB-CGC and CRDC GC platforms**. The steps for these releases can be found in the [SOP: HTAN ISB-CGC BigQuery Release](https://docs.google.com/document/d/1Kh7Tgor5fxwz1SHcLGTbXENQJW_1Jfml7-hi9jEtMS8/edit?tab=t.0#heading=h.160uf54qkeaf). 

<a id="repository-structure"></a>
## Repository Structure
```
├── htan-data-release-pipeline/
│   ├── configs/
│   │   ├── configs.json
│   │   ├── configs.yaml
│   │   ├── schema.json
│   ├── images/
│   │   ├── BigQuery_medallion_architecture.svg
│   │   ├── HTAN_banner.jpg
│   │   ├── HTAN_logo.png
│   ├── medallion_architecture/
│   │   ├── synapse_2raw.py/
│   │   │   ├── common/
│   │   │   │   ├── utils.py
│   │   │   ├── job/
│   │   │   │   ├── center_config.yml
│   │   │   │   ├── Dockerfile
│   │   │   │   ├── main.py
│   │   │   ├── requirements.txt
│   │   ├── bq_bronze2silver.py
│   │   ├── bq_raw2bronze.py
│   │   ├── bq_silver2gold.py
│   │   ├── README.md
│   ├── utils/
│   │   ├── data_utils.py
│   ├── workflow_functions/
│   │   ├── bq_validation.py
│   │   ├── client_load.py
│   │   ├── data_model_load.py
│   │   ├── data_model_validation.py
│   ├── run.py
|── Dockerfile
├── README.md (Current File)
|── environment.yml
|── requirements.txt
```

<a id="installation"></a>
## Installation
This repository can be cloned locally by running the following `git` command:
```bash
git clone https://github.com/ncihtan/htan-data-release-pipeline.git
```
Please note that Git is required to run the above command. For instructions on downloading Git, please see [the Git Guide](https://github.com/git-guides/install-git).

<a id="environment"></a>
### Environment

#### Conda
This application is built on top of multiple Python packages with specific version requirements. We recommend using `conda` to create an isolated Python environment with all necessary packages. The list of necessary packages can be found at in the [`environment.yml`](./environment.yml) file.

To create the specified `data-release-env` Conda environment, run the following command:
```bash
conda env create -f environment.yml
```

Once the Conda environment is created, it can be activated by:
```bash
conda activate data-release-env
```
After coding inside the environment, it can be deactivated with the command:
```bash
conda deactivate
```

#### Pip
You may also install these packages locally using the [`requirements.txt`](./requirements.txt) file. However, this approach is **not recommended**, as version conflicts with existing packages on your machine may arise.

To install the specified packages on your local machine, run the following command:
```bash
pip install -r requirements.txt
```
<a id="data-storage"></a>
## Data Storage
This project utilizes metadata from the Synapse platform and stores it in [Google BigQuery](https://cloud.google.com/bigquery?hl=en). Login information with the HTAN Data Coordinating Center (DCC) access is required for running this project.

<a id="synapse"></a>
### Synapse

#### Prerequisites
Create a Personal Access Token obtained from your [Synapse.org](https://www.synapse.org/) account under *Settings*.

#### Login
When installing the `synapseclient` package either in a conda environment or on your local machine, the `~/.synapseConfig` file will automatically be added to your home directory. 

Open and modify the `.synapseConfig` file and include your Synapse username and authentication token:
```bash
[authentication]
username = <username>
authtoken = <authtoken>
```

Review the [Synapse Python/Command Line Client Documentation](https://python-docs.synapse.org/en/stable/) for more details on installation, authentication, and configuration. 

<a id="bigquery"></a>
### Google BigQuery
Ensure that you have access to the following BigQuery projects:
- htan-dcc
- htan2-dcc
- isb-cgc-bq

To gain access to the HTAN specific projects, email the Project Manager for HTAN at Sage Bionetworks. To gain access to the ISB-CGC project, email a member of the ISB-CGC team. 

You *may* need a key to access Google Cloud/BigQuery via an outside source such as Python. For this, you will need to generate a JSON key. Follow the [Get Credentials for Google Drive and Google Sheets APIs to use with schematicpy](https://scribehow.com/viewer/Get_Credentials_for_Google_Drive_and_Google_Sheets_APIs_to_use_with_schematicpy__yqfcJz_rQVeyTcg0KQCINA) documentation provided by Sage Bionetworks to generate an access key.

<a id="scheduled-jobs"></a>
## Scheduled Jobs
Scheduled and triggered validation scrips are ran using [Google Cloud Run](https://cloud.google.com/run?hl=en). Below are the active jobs associated with this repository:
- [Job: htan-data-release-pipeline](https://console.cloud.google.com/run/jobs/details/us-west1/htan-data-release-pipeline/executions?inv=1&invt=Ab3eHQ&project=htan-dcc)

For documentation on how to set-up and schedule a Google Cloud Run job, refer to [SOP: Setting-Up and Scheduling Google Cloud Run Jobs]().

<a id="relevant-repos"></a>
## Relevant Repositories
Below are a list of repositories associated with this data release pipeline:

- **CRDC GC Release Pipeline**
  - [htan-to-crdc-map:](https://github.com/ncihtan/htan-to-crdc-map) Metadata generation required for CRDC submission.
  - [htan-to-crdc-nextflow-uploader:](https://github.com/ncihtan/htan-to-crdc-nextflow-uploader) File transfer from Synapse to CRDC.
- **HTAN Exclusion Request Portal**
  - [htan-exclusion-list:](https://github.com/ncihtan/htan-exclusion-list) Streamlit app interface for exclusion requests.
- **HTAN Data Portal**
  - [htan-portal:](https://github.com/ncihtan/htan-portal) Backend for the main HTAN Data Portal website. Managed by MSK.
- **HTAN Dashbord**
  - [hdash_air:](https://github.com/ncihtan/hdash_air) Dashboard that displays project-specific data submission errors. Managed by MSK.


