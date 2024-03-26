
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

# resource "google_project_service" "serviceusage" {
#   project = var.GCP_PROJECT_ID
#   service = "serviceusage.googleapis.com"
#   disable_dependent_services = true
# }

# resource "google_project_service" "cloudresourcemanager" {
#   project = var.GCP_PROJECT_ID
#   service =  "cloudresourcemanager.googleapis.com"
#   disable_dependent_services = true
# }

resource "google_project_service" "gcp_services" {
  for_each = toset(var.gcp_service_list)
  project = var.GCP_PROJECT_ID
  service = each.key
}