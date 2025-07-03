<a id="introduction"></a>
## Introduction

Timely and reliable data releases are essential to large research initiatives, enabling public access to high-quality datasets that drive scientific discovery. In the Human Tumor Atlas Network (HTAN), data is made available through Sage Bionetworks [Synapse](https://www.synapse.org/) platform and the National Cancer Institute’s (NCI) Cancer Research Data Commons (CRDC) [General Commons](https://datacommons.cancer.gov/repository/general-commons)(GC). Before releases, datasets undergo validation to ensure quality, consistency, and compliance with standards.

For the complete standard operating procedure (SOP) for releasing HTAN data, please review [SOP: HTAN Phase 2 Data Release](). 

#### Team Members:
| Name                   | GitHub             |
| ---------------------- | ------------------ |
| **Dar'ya Pozhidayeva** | *PozhidayevaDarya* |
| **Yamina Katariya**    | *ykatariy*         |
| **Clarisse Lau**       | *clarisse-lau*     |

## Table of Contents
* [Introduction](#introduction)
* [Tasks of Interest](#tasks-of-interest)
* [Installation](#installation)

<a id="tasks-of-interest"></a>
## Tasks of Interest
- **Pull data from Synapse**:
  - HTAN intended release files and metadata are hosted on Synapse. Clinical and biospecimen metadata are stored in tables, while file-level annotations capture key details like assay type, tissue, platform, and processing level. 
- **Release tracking and metrics in BigQuery**:
  - All metadata is ingested into BigQuery via a Medallion architecture. This architecture organizes data into structured layers-Bronze, Silver, and Gold—with each layer representing increasing levels of validation, curation, and readiness for public release.
- **Validate data and metadata Synapse files**:
  - Validation occurs on both a triggered and continuous timeline, from initial upload to modifications of files, and final release. 
- **Publish HTAN data on the ISB-CGC and CRDC GC platforms**:
  - HTAN's control access data is published on the GC, while all other data is published on the ISB-CGC platform. The steps for these releases can be found in the [SOP: HTAN ISB-CGC BigQuery Release](https://docs.google.com/document/d/1Kh7Tgor5fxwz1SHcLGTbXENQJW_1Jfml7-hi9jEtMS8/edit?tab=t.0#heading=h.160uf54qkeaf). 

<a id="installation"></a>
## Installation
This repository can be cloned locally by running the following `git` command:
```bash
git clone https://github.com/ncihtan/htan-data-release-pipeline.git
```
Please note that Git is required to run the above command. For instructions on downloading Git, please see [the Git Guide](https://github.com/git-guides/install-git).

