# About

The HTAN Phase 2 Release Validation pipeline contains several quality-control measures between raw data ingestion and the pubic facing [HTAN Data Portal](https://humantumoratlas.org/), [Cancer Research Data Commons (CRDC)](https://datacommons.cancer.gov/), [Database of Genotypes and Phenotypes (dbGaP)](https://dbgap.ncbi.nlm.nih.gov/home/), and [Institute for Systems Biology Cancer Gateway in the Cloud (ISB-CGC)](https://portal.isb-cgc.org/) data accessing sites. At a high level, the architecture consists of:

- **Synapse Curator:** An add-on to Sage Bionetwork’s existing Synapse platform. Curator collects file, folder, and record-based annotations and validates them against the HTAN Phase 2 Data Model (developed using LinkML). Annotations that pass the data model validation are marked with a green symbol. Annotations that don’t remain yellow.

- **HTAN BigQuery:** A collection of BigQuery tables that follow a Medallion Architecture. 
    - **RAW:** All HTAN files and records submitted to Curator is pulled into the RAW layer via Synapse fileviews.
    - **BRONZE:** All metadata, regardless of its Curator validation status, is reorganized into aggregate  tables in the BRONZE layer. Here, the provenance chain is built.
    - **SILVER:** BRONZE layer metadata is then validated for release using Release Validation rules described in {doc}`Component Validator <component>` and {doc}`Provenance Validator <provenance>`. Validation results are appended to all file and record set metadata tables and summarized in error tables in the SILVER layer. 
    - **GOLD:** SILVER layer metadata that passes all validation (Curator, Component, and Provenance) are promoted to the GOLD layer and staged for release.

Validation results generated in the SILVER layer will be communicated to HTAN Centers via their liaison.
