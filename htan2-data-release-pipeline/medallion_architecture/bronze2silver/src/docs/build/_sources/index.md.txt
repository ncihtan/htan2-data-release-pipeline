# HTAN Phase 2 Data Release Validation 

The release validation process performs automated quality control checks on submitted metadata and generates validation reports that are used to assess release readiness. The workflow validates both file-level and record-set-level metadata, appends validation results to metadata tables, and generates centralized error indexing tables for downstream review in BigQuery.

## Validation Workflow

The release validation pipeline performs the following steps:

1. Loads metadata and provenance tables from BigQuery
2. Retrieves the current exclusion list
3. Collects schema validation results generated during metadata curation
4. Runs component-level validation checks
5. Runs provenance and cross-center validation checks
6. Appends validation results and error messages to metadata tables
7. Generates centralized error indexing tables for files and record sets
8. Writes validated outputs back to BigQuery

## Validation Categories

The release process combines results from three validation categories:

| Validation Category | Description | Owner |
|---|---|---|
| Curator Validation | Schema-based validation results generated during metadata curation | Synapse |
| Base Validation | Error reporting and formatting | HTAN BigQuery |
| Component Validation | Validation of identifier structure, required fields, duplicates, and metadata consistency | HTAN BigQuery |
| Provenance Validation | Cross-table validation of relationships and dependencies between metadata entities | HTAN BigQuery |

Each metadata entry receives structured error reporting, along with aggregated validation messages and error types, via the {doc}`Base Validator <base>`.

For more information regarding the HTAN BigQuery Release Validation categories, please see {doc}`Component Validator <component>` and {doc}`Provenance Validator <provenance>`.

## Exclusion List Handling

The pipeline loads a centrally maintained exclusion list containing files that should be excluded from the pending release. Excluded files are tracked separately and incorporated into validation results.

HTAN Centers may add items to the exclusion list via the [HTAN Exclusion Request Portal](https://htan2-exclusion-list-app-899713014598.us-west1.run.app/).


```{toctree}
:hidden:
:caption: General

about.md
support.md
```

```{toctree}
:hidden:
:caption: Release Validation

base.md
component.md
provenance.md
```
