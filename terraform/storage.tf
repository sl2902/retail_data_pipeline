resource "google_storage_bucket" "data-lake-bucket" {
  location = var.GCP_REGION
  name     = var.GCP_BUCKET_NAME
  
  #optional settings 
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
  public_access_prevention = "enforced"
  force_destroy            = true
}