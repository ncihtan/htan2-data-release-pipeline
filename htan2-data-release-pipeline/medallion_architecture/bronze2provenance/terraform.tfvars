project_id = "htan-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan-dcc/gcr.io/raw2bronze:latest"
secret_id = "synapse_dyp_secret" 

# service account variables
google_service_account = {
  sa = {
    email = "bq-medallion-jobs@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq_medallion_jobs"

# job variables
cloud_run_name = "raw2bronze"
job_name =  "raw2bronze"
job_description = "Update bronze metadata pulled from raw data in Google Bigquery (htan2-dcc)."
job_schedule = "0 6 * * *"
time_zone = "America/Oregon"
