#!/usr/bin/env bash
# Test GCS bucket notifications → Pub/Sub topic routing using existing
# subscriptions named "<topic>.rba".
#
# For each (prefix → topic) mapping this script:
#   1. Uploads a uniquely-named test object under the prefix.
#   2. Pulls (with auto-ack) from <topic>.rba.
#   3. Confirms a message exists whose `objectId` attribute matches the
#      object that was just uploaded.
#   4. Deletes the test object.
#
# Required permissions:
#   - storage.objects.create / delete on the bucket
#   - pubsub.subscriptions.consume on each <topic>.rba subscription
#     (roles/pubsub.subscriber)
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

# prefix → topic short name. Subscription is assumed to be "<topic>.rba".
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

# Pull retry settings. GCS notification delivery is usually <5s but can
# spike, and pull batches are not guaranteed to return a message even when
# one is available — retrying is normal.
PULL_ATTEMPTS=10
PULL_INTERVAL=3
PULL_LIMIT=50

echo "=== GCS notification test (subscription-based) ==="
echo "Project: $PROJECT"
echo "Bucket:  gs://$BUCKET"
echo "Run ID:  $RUN_ID"
echo

echo "--- Configured notifications on gs://$BUCKET ---"
gsutil notification list "gs://$BUCKET" || {
  echo "ERROR: could not list notifications. Check bucket name and permissions." >&2
  exit 1
}
echo

for mapping in "${MAPPINGS[@]}"; do
  PREFIX="${mapping%%|*}"
  TOPIC="${mapping##*|}"
  SUB="${TOPIC}.rba"
  OBJECT="${PREFIX}test-${RUN_ID}.txt"

  echo "--- Test: prefix='$PREFIX' → topic='$TOPIC' (sub: $SUB) ---"

  # Upload a uniquely-named test object to trigger OBJECT_FINALIZE.
  LOCAL="$TMPDIR/payload-${TOPIC}.txt"
  echo "test payload for $TOPIC at $(date -u +%FT%TZ)" > "$LOCAL"
  gsutil -q cp "$LOCAL" "gs://$BUCKET/$OBJECT"
  echo "  uploaded: gs://$BUCKET/$OBJECT"

  # Pull with auto-ack. Repeat a few times because:
  #   - the notification may take a few seconds to land
  #   - a single pull is not guaranteed to drain all available messages
  FOUND=""
  for ((i=1; i<=PULL_ATTEMPTS; i++)); do
    sleep "$PULL_INTERVAL"

    OUT="$(gcloud pubsub subscriptions pull "$SUB" \
            --project="$PROJECT" \
            --auto-ack \
            --limit="$PULL_LIMIT" \
            --format=json 2>/dev/null || echo "[]")"

    # Each pulled message has its GCS attributes under .message.attributes.
    # We match on objectId; the unique RUN_ID guarantees no false positives.
    MATCH="$(echo "$OUT" | jq -r --arg obj "$OBJECT" '
      .[]
      | select(.message.attributes.objectId == $obj)
      | .message.attributes.objectId
    ' | head -n1)"

    if [[ -n "$MATCH" ]]; then
      FOUND="$MATCH"
      break
    fi
  done

  if [[ -n "$FOUND" ]]; then
    echo "  PASS: notification for $FOUND received on $SUB"
    PASS=$((PASS+1))
  else
    echo "  FAIL: no message with objectId='$OBJECT' on $SUB after $((PULL_ATTEMPTS * PULL_INTERVAL))s"
    FAIL=$((FAIL+1))
    FAILED_CASES+=("$TOPIC (no message on $SUB)")
  fi

  gsutil -q rm "gs://$BUCKET/$OBJECT" || true
  echo
done

echo "=== Summary ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
if (( FAIL > 0 )); then
  printf '  - %s\n' "${FAILED_CASES[@]}"
  exit 1