project_id = "htan-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan-dcc/gcr.io/syn-bq-metadata:latest"
secret_id = "synapse_service_pat" 

# service account variables
google_service_account = {
  sa = {
    email = "bq-metadata@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq-metadata"

# job variables
cloud_run_name = "update-bq-metadata-tables"
job_name =  "update-bq-metadata-tables-trigger"
job_description = "Updates metadata tables in 'combined_assays' BigQuery dataset"
job_schedule = "0 2 * * *"
time_zone = "America/New_York"
