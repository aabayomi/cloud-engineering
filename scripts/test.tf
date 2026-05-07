provisioner "local-exec" {
  command = <<-SCRIPT
    #!/bin/bash
    # Note: NOT using 'set -e' — we handle errors explicitly per-command
    # so transient gcloud failures and grep no-matches don't kill the run.

    BUCKET_NAME="${google_storage_bucket.bucket.name}"

    # Retry helper for gcloud calls (handles eventual consistency)
    retry() {
      local n=0 max=5 delay=3
      until "$@"; do
        n=$((n+1))
        if [ $n -ge $max ]; then
          echo "Command failed after $max attempts: $*" >&2
          return 1
        fi
        echo "Attempt $n failed, retrying in $${delay}s..." >&2
        sleep $delay
        delay=$((delay*2))
      done
    }

    # Collect existing notifications as "id|topic" lines
    EXISTING=$(gcloud storage buckets notifications list "gs://$BUCKET_NAME" 2>/dev/null | awk '
      /^  id:/    { id=$2 }
      /^  topic:/ { topic=$2; sub(/^\/\/pubsub.googleapis.com\//, "", topic); print id "|" topic }
    ' | tr -d "'") || EXISTING=""

    # Delete existing notifications whose topics are NOT in the desired list
    while IFS='|' read -r nc_id nc_topic; do
      [[ -z "$nc_id" ]] && continue
      keep=0
      %{ for n in var.notifications ~}
      [[ "$nc_topic" == "${n.topic_id}" ]] && keep=1
      %{ endfor ~}
      if [[ $keep -eq 0 ]]; then
        echo "Removing notification $nc_id (topic: $nc_topic)"
        gcloud storage buckets notifications delete \
          "projects/_/buckets/$BUCKET_NAME/notificationConfigs/$nc_id" 2>/dev/null || true
        sleep 2
      else
        echo "Keeping notification $nc_id (topic: $nc_topic)"
      fi
    done <<< "$EXISTING"

    # Re-fetch existing topics AFTER deletes so we have a fresh view
    EXISTING_TOPICS=$(gcloud storage buckets notifications list "gs://$BUCKET_NAME" 2>/dev/null \
      | awk '/^  topic:/ { sub(/^\/\/pubsub.googleapis.com\//, "", $2); print $2 }' \
      | tr -d "'") || EXISTING_TOPICS=""

    %{ for n in var.notifications ~}
    if echo "$EXISTING_TOPICS" | grep -qxF "${n.topic_id}"; then
      echo "Notification already exists for topic: ${n.topic_id}, skipping"
    else
      echo "Creating notification for topic: ${n.topic_id}"
      retry gcloud storage buckets notifications create "gs://$BUCKET_NAME" \
        --topic="${n.topic_id}" \
        --payload-format="${local.gcloud_payload_format[n.payload_format]}"
      sleep 2
    fi
    %{ endfor ~}

    exit 0
  SCRIPT
}
