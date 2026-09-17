# BigQuery Data Release Architecture

The ***htan2-dcc*** project contains multiple jobs used to transfer metadata submitted to HTAN by contributors in Synapse to several data portals for public use. Metadata ingested from Synapse undergo's several instances of validation and transformations before it is staged for release. Here we track and display every step of the process from the ingestion of raw metadata, validation, collection of supplemental metadata, staging, and post-release logging. The flow of metadata through BigQuery is managed by ISB and administration of the ***htan2-dcc*** project is managed by Sage Bionetworks.

## Repository and Dataset Structure

For each folder in this Github repository, we have attached the associated GCP dataset output and labeled the category found in our table dictionary:

| Category | Directory | GCP Dataset(s) |
| -------- | -------- | -------- |
| RAW | [synapse2raw](./synapse2raw) | [htan2_synapse_raw](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_synapse_raw) |
| BRONZE | [bronze2provenance](./bronze2provenance/) | [htan2_medallion_bronze](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_medallion_bronze) |
| BRONZE | [raw2bronze](./raw2bronze/) | [htan2_medallion_bronze](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_medallion_bronze) |
| SILVER | [bronze2silver](./bronze2silver/) | [htan2_medallion_silver](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_medallion_silver) |
| GOLD | [silver2gold](./silver2gold/) | [htan2_medallion_gold](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_medallion_gold), [htan2_medallion_gold_release_archive](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_medallion_gold_release_archive), [htan2_released_accessibility_status](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_released_accessibility_status) |
| ISB-CGC | [gold2isbcgc](./gold2isbcgc/) | [htan2_isbcgcbq_current_release](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_isbcgcbq_current_release), [htan2_isbcgcbq_curated_tables_archive](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_isbcgcbq_curated_tables_archive) |
| PORTAL | [gold2mapping](./gold2mapping/) | [htan2_data_mapping_tables](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_data_mapping_tables) |
| DATA MODEL | [linkml2bigquery](./linkml2bigquery/) | [htan2_data_model_cache](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_data_model_cache) |

## BigQuery Table Dictionary

Link: https://docs.google.com/spreadsheets/d/1mSy4QW2sJIEtHoK_cZwVtUgZkIMohw1Vfe_PdSuYNDA/edit?gid=0#gid=0 

In the BigQuery Table Dictionary above, we provide descriptions for each table within the ***htan2-dcc*** GCP project, along with links to their respective schemas. Each table is organized by its GCP dataset and general category. The sections below provide an overview of the purpose and function of each repository folder, organized by category.

### RAW

In the RAW layer, the [`synapse2raw`](./synapse2raw/) directory pulls project and folder **metadata**, indexes information directly from Synapse into the [`htan2_synapse_raw`](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_synapse_raw) dataset, and preserves the source information with minimal transformation. This layer captures the submitted files, records, schemas, annotations, Curator validation status, and other Synapse project information that serves as the primary source for downstream processing.

### BRONZE

In the BRONZE layer, the [`raw2bronze`](./raw2bronze/) directory uses the project and folder metadata to query Synapse and retrieve file and record set metadata. Metadata from all HTAN project in Synapse are aggregated into component-specific tables. Curator validation and Synapse indexing information are maintained from the RAW layer. 

Indexing and registry tables that track file/record set identifiers, schema information, and provenance relationships for use in downstream processing are created within the [`bronze2provenance`](./bronze2provenance/) directory.

Results from BRONZE layer processing (both directories mentioned above) are published to the [htan2_medallion_bronze](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_medallion_bronze) GCP dataset.

### SILVER

The SILVER layer applies release validation checks to the metadata collected in the BRONZE layer before it can proceed to release. In the [`bronze2silver`](./bronze2silver/) folder, metadata is pulled from the BRONZE layer and filtered to contain files and record sets relevant to the current HTAN Data Release (e.g. 8.0). It combines Curator, component-level, and provenance validation, records validation errors and results for files and record sets, applies exclusion and bypass lists (housed in the RAW layer), and generates tables identifying metadata that has passed validation and is eligible for promotion to the GOLD layer. 

More detailed documentation for rules and checks relating to the release validation process can be found here: https://htan2-data-release-pipeline.readthedocs.io/en/latest/

### GOLD

The GOLD layer generate staged tables for release in several different ways:
1. It creates a catalog of Synapse file and record set entities that have passed validation from the SILVER layer. This catalog of entities is used to move files and record sets over from ingest to staging folders in Synapse. 
2. It creates metadata tables (similar to the ones found in the BRONZE and SILVER layers) based on Synapse entities that have been moved to the release folders in Synapse.
3. It logs released entities for specific HTAN data releases
4. It stages metadata, provenance, and data model tables for ISB-CGC transformation and Portal release. 

All scripts used to produce the GOLD layer can be found in the [`silver2gold`](./silver2gold/) directory. All outputs for these scripts are published to several datasets in GCP as described above. 

### ISB-CGC

The [`gold2isbcgc`](./gold2isbcgc/) directory takes staged metadata, provenance, and data model tables from the GOLD layer and applies the minimal transformations required for ISB-CGC, creating staged tables for release in the [htan2_isbcgcbq_current_release](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_isbcgcbq_current_release) dataset. An ISB-CGC table metadata log is also generated and stored in the Google Shared Drive: [HTAN2 to ISB-CGC-BQ](https://drive.google.com/drive/u/0/folders/0AP14Y3U4c0PdUk9PVA). Results from here are shared with ISB-CGC team members.

### PORTAL

The HTAN Portal uses staged tables in the GOLD layer to publish metadata. The views created in the [htan2_data_portal](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_data_portal) are used to moved releasable metadata into a ClickHouse database. The [`gold2mapping`](./gold2mapping/) folder gets all attributes where UBERON codes are used and maps them to their human-readable names for ease-of-use on the Portal. These mapping tables can be found in the [htan2_data_mapping_tables](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_data_mapping_tables) GCP dataset.

### DATA MODEL

The [`linkml2bigquery`](./linkml2bigquery/) folder gets all data model releases from the [htan2-data-model](https://github.com/ncihtan/htan2-data-model) GitHub repository and converts the data models represented as JSON files into a tabular format compatible with BigQuery. There are then cached in the [htan2_data_model_cache](https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1shtan2-dcc!2shtan2_data_model_cache) GCP dataset for easy retrieval. 
