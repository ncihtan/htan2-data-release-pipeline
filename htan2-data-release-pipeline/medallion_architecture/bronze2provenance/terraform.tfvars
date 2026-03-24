project_id = "htan2-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan2-dcc/gcr.io/bronze2provenance:latest"
secret_id = "synapse_dyp_secret" 

# service account variables
google_service_account = {
  sa = {
    email = "bq-medallion-jobs@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq_medallion_jobs"

# job variables
cloud_run_name = "bronze2provenance"
job_name =  "bronze2provenance"
job_description = "Update bronze provenance metadata table constructed from raw data in Google Bigquery (htan2-dcc)."
job_schedule = "0 7 * * *"
time_zone = "America/Oregon"
