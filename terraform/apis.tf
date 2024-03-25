
variable "gcp_service_list" {
  description ="The list of apis necessary for the project"
  type = list(string)
  default = [
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "compute.googleapis.com",
    "pubsublite.googleapis.com"
  ]
}

resource "google_project_service" "gcp_services" {
  for_each = toset(var.gcp_service_list)
  project = var.GCP_PROJECT_ID
  service = each.key
}