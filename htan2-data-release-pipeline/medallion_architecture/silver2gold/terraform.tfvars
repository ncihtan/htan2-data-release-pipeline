project_id = "htan-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan-dcc/gcr.io/silver2gold:latest"
secret_id = "synapse_dyp_secret" 

# service account variables
google_service_account = {
  sa = {
    email = "bq-medallion-jobs@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq_medallion_jobs"

# job variables
cloud_run_name = "silver2gold"
job_name =  "silver2gold"
job_description = "Update gold metadata pulled from silver data in Google Bigquery (htan2-dcc)."
job_schedule = "0 3 * * *"
time_zone = "America/Oregon"
