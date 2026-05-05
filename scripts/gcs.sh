#!/usr/bin/env bash
# Test GCS bucket notifications → Pub/Sub topic routing.
#
# For each (prefix → topic) mapping, this script:
#   1. Creates a temporary subscription on the topic
#   2. Uploads a test object under the prefix
#   3. Pulls messages and verifies the object name matches
#   4. Cleans up the subscription and object
#
# Usage:
#   ./gcs-notifications-test.sh -p <project_id> -b <bucket_name>
#
# Requires: gcloud, gsutil, jq

set -euo pipefail

PROJECT=""
BUCKET=""

usage() {
  echo "Usage: $0 -p <project_id> -b <bucket_name>" >&2
  exit 1
}

while getopts "p:b:h" opt; do
  case "$opt" in
    p) PROJECT="$OPTARG" ;;
    b) BUCKET="$OPTARG" ;;
    h|*) usage ;;
  esac
done

[[ -z "$PROJECT" || -z "$BUCKET" ]] && usage

# prefix → expected topic name (must match the `name` in your topic modules)
declare -a MAPPINGS=(
  "ingest/|ingest"
  "egress/|egress"
  "egress1/|egress1"
  "egress2/|egress2"
  "egress3/upload/|egress3"
)

RUN_ID="$(date +%s)-$$"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

PASS=0
FAIL=0
FAILED_CASES=()

echo "=== GCS notification test ==="
echo "Project: $PROJECT"
echo "Bucket:  gs://$BUCKET"
echo "Run ID:  $RUN_ID"
echo

# --- Sanity check: list configured notifications on the bucket ---
echo "--- Configured notifications on gs://$BUCKET ---"
gsutil notification list "gs://$BUCKET" || {
  echo "ERROR: could not list notifications. Check bucket name and permissions." >&2
  exit 1
}
echo

for mapping in "${MAPPINGS[@]}"; do
  PREFIX="${mapping%%|*}"
  TOPIC="${mapping##*|}"
  SUB="gcs-notif-test-${TOPIC}-${RUN_ID}"
  OBJECT="${PREFIX}test-${RUN_ID}.txt"

  echo "--- Test: prefix='$PREFIX' → topic='$TOPIC' ---"

  # 1. Temporary pull subscription
  if ! gcloud pubsub subscriptions create "$SUB" \
        --topic="$TOPIC" \
        --project="$PROJECT" \
        --ack-deadline=20 \
        --message-retention-duration=10m \
        --quiet >/dev/null 2>&1; then
    echo "  SKIP: could not create subscription on topic '$TOPIC' (does it exist in project '$PROJECT'?)"
    FAIL=$((FAIL+1))
    FAILED_CASES+=("$TOPIC (subscription create failed)")
    echo
    continue
  fi

  # Brief settle so the subscription is ready before the upload event fires
  sleep 2

  # 2. Upload a test object
  LOCAL="$TMPDIR/payload-${TOPIC}.txt"
  echo "test payload for $TOPIC at $(date -u +%FT%TZ)" > "$LOCAL"
  gsutil -q cp "$LOCAL" "gs://$BUCKET/$OBJECT"
  echo "  uploaded: gs://$BUCKET/$OBJECT"

  # 3. Pull. OBJECT_FINALIZE may take a few seconds; retry with backoff.
  FOUND=""
  for attempt in 1 2 3 4 5; do
    OUT="$(gcloud pubsub subscriptions pull "$SUB" \
            --project="$PROJECT" \
            --auto-ack \
            --limit=10 \
            --format=json 2>/dev/null || echo "[]")"

    # objectId attribute on the message tells us which object triggered it
    MATCH="$(echo "$OUT" | jq -r --arg obj "$OBJECT" \
      '.[] | select(.message.attributes.objectId == $obj) | .message.attributes.objectId' \
      | head -n1)"

    if [[ -n "$MATCH" ]]; then
      FOUND="$MATCH"
      break
    fi
    sleep $((attempt * 2))
  done

  if [[ -n "$FOUND" ]]; then
    echo "  PASS: received notification for $FOUND on topic '$TOPIC'"
    PASS=$((PASS+1))
  else
    echo "  FAIL: no matching message on topic '$TOPIC' for $OBJECT"
    FAIL=$((FAIL+1))
    FAILED_CASES+=("$TOPIC (no message for $OBJECT)")
  fi

  # 4. Cleanup
  gsutil -q rm "gs://$BUCKET/$OBJECT" || true
  gcloud pubsub subscriptions delete "$SUB" --project="$PROJECT" --quiet >/dev/null 2>&1 || true
  echo
done

echo "=== Summary ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
if (( FAIL > 0 )); then
  printf '  - %s\n' "${FAILED_CASES[@]}"
  exit 1
fi