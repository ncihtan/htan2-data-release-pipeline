project_id = "htan2-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan2-dcc/gcr.io/synapse2raw:latest"
secret_id = "synapse_dyp_secret"

# service account variables
google_service_account = {
  sa = {
    email = "bq-medallion-jobs@htan2-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq_medallion_jobs"

# job variables
cloud_run_name = "synapse2raw"
job_name =  "synapse2raw"
job_description = "Update raw metadata pulled from Synapse in Google Bigquery (htan2-dcc)."
job_schedule = "0 3 * * *"
time_zone = "America/Oregon"
