- **Downgrading from 9.0 to 8.x is not supported when multiple pubsub notifications
  are configured on a bucket.** The 8.x `google_storage_notification.pubsub_notification`
  resource cannot manage notifications created by the 9.0 `null_resource` + `gcloud`
  workflow. Reverting will cause Terraform to attempt to recreate notifications via
  the v8 resource, which fails due to the GCS API's one-update-per-second limit when
  more than one notification exists. Existing notifications will also become unmanaged
  (orphaned) in state. To downgrade, first reduce to a single notification, or manually
  delete notifications via `gcloud` before applying 8.x.