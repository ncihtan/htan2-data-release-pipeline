project_id = "htan2-dcc"
region = "us-east1"
image_url = "us-docker.pkg.dev/htan2-dcc/gcr.io/linkml2bigquery:latest"

secrets = {
  app_id = "github-app-id"
  installation_id = "github-installation-id"
  private_key = "github-private-key"
  secret_id = "synapse_dyp_secret"
}

# service account variables
google_service_account = {
  sa = {
    email = "bq-medallion-jobs@htan-dcc.iam.gserviceaccount.com"
  }
}
account_id = "bq_medallion_jobs"

# job variables
cloud_run_name = "linkml2bigquery"
job_name =  "linkml2bigquery"
job_description = "Cache new data model version in Google Bigquery (htan2-dcc)."
job_schedule = "0 6 * * *"
time_zone = "America/Oregon"
