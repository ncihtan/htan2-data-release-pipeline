project_id = "htan-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan-dcc/gcr.io/bronze2silver:latest"
secret_id = "synapse_dyp_secret" 

# service account variables
google_service_account = {
  sa = {
    email = "bq-medallion-jobs@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq_medallion_jobs"

# job variables
cloud_run_name = "bronze2silver"
job_name =  "bronze2silver"
job_description = "Update silver metadata pulled from bronze data in Google Bigquery (htan2-dcc)."
job_schedule = "0 3 * * *"
time_zone = "America/Oregon"
