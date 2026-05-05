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

# set -euo pipefail

# PROJECT=""
# BUCKET=""

# usage() {
#   echo "Usage: $0 -p <project_id> -b <bucket_name>" >&2
#   exit 1
# }

# while getopts "p:b:h" opt; do
#   case "$opt" in
#     p) PROJECT="$OPTARG" ;;
#     b) BUCKET="$OPTARG" ;;
#     h|*) usage ;;
#   esac
# done

# [[ -z "$PROJECT" || -z "$BUCKET" ]] && usage

# # prefix → expected topic name (must match the `name` in your topic modules)
# declare -a MAPPINGS=(
#   "ingest/|ingest"
#   "egress/|egress"
#   "egress1/|egress1"
#   "egress2/|egress2"
#   "egress3/upload/|egress3"
# )

# RUN_ID="$(date +%s)-$$"
# TMPDIR="$(mktemp -d)"
# trap 'rm -rf "$TMPDIR"' EXIT

# PASS=0
# FAIL=0
# FAILED_CASES=()

# echo "=== GCS notification test ==="
# echo "Project: $PROJECT"
# echo "Bucket:  gs://$BUCKET"
# echo "Run ID:  $RUN_ID"
# echo

# # --- Sanity check: list configured notifications on the bucket ---
# echo "--- Configured notifications on gs://$BUCKET ---"
# gsutil notification list "gs://$BUCKET" || {
#   echo "ERROR: could not list notifications. Check bucket name and permissions." >&2
#   exit 1
# }
# echo

# for mapping in "${MAPPINGS[@]}"; do
#   PREFIX="${mapping%%|*}"
#   TOPIC="${mapping##*|}"
#   SUB="gcs-notif-test-${TOPIC}-${RUN_ID}"
#   OBJECT="${PREFIX}test-${RUN_ID}.txt"

#   echo "--- Test: prefix='$PREFIX' → topic='$TOPIC' ---"

#   # 1. Temporary pull subscription
#   if ! gcloud pubsub subscriptions create "$SUB" \
#         --topic="$TOPIC" \
#         --project="$PROJECT" \
#         --ack-deadline=20 \
#         --message-retention-duration=10m \
#         --quiet >/dev/null 2>&1; then
#     echo "  SKIP: could not create subscription on topic '$TOPIC' (does it exist in project '$PROJECT'?)"
#     FAIL=$((FAIL+1))
#     FAILED_CASES+=("$TOPIC (subscription create failed)")
#     echo
#     continue
#   fi

#   # Brief settle so the subscription is ready before the upload event fires
#   sleep 2

#   # 2. Upload a test object
#   LOCAL="$TMPDIR/payload-${TOPIC}.txt"
#   echo "test payload for $TOPIC at $(date -u +%FT%TZ)" > "$LOCAL"
#   gsutil -q cp "$LOCAL" "gs://$BUCKET/$OBJECT"
#   echo "  uploaded: gs://$BUCKET/$OBJECT"

#   # 3. Pull. OBJECT_FINALIZE may take a few seconds; retry with backoff.
#   FOUND=""
#   for attempt in 1 2 3 4 5; do
#     OUT="$(gcloud pubsub subscriptions pull "$SUB" \
#             --project="$PROJECT" \
#             --auto-ack \
#             --limit=10 \
#             --format=json 2>/dev/null || echo "[]")"

#     # objectId attribute on the message tells us which object triggered it
#     MATCH="$(echo "$OUT" | jq -r --arg obj "$OBJECT" \
#       '.[] | select(.message.attributes.objectId == $obj) | .message.attributes.objectId' \
#       | head -n1)"

#     if [[ -n "$MATCH" ]]; then
#       FOUND="$MATCH"
#       break
#     fi
#     sleep $((attempt * 2))
#   done

#   if [[ -n "$FOUND" ]]; then
#     echo "  PASS: received notification for $FOUND on topic '$TOPIC'"
#     PASS=$((PASS+1))
#   else
#     echo "  FAIL: no matching message on topic '$TOPIC' for $OBJECT"
#     FAIL=$((FAIL+1))
#     FAILED_CASES+=("$TOPIC (no message for $OBJECT)")
#   fi

#   # 4. Cleanup
#   gsutil -q rm "gs://$BUCKET/$OBJECT" || true
#   gcloud pubsub subscriptions delete "$SUB" --project="$PROJECT" --quiet >/dev/null 2>&1 || true
#   echo
# done

# echo "=== Summary ==="
# echo "Passed: $PASS"
# echo "Failed: $FAIL"
# if (( FAIL > 0 )); then
#   printf '  - %s\n' "${FAILED_CASES[@]}"
#   exit 1
# fi



#!/usr/bin/env bash
# Test GCS bucket notifications → Pub/Sub topic routing using Cloud Logging.
#
# This script does NOT create or pull from Pub/Sub subscriptions. Instead it:
#   1. Lists configured notifications on the bucket (sanity check).
#   2. Uploads a uniquely-named object under each prefix.
#   3. Queries Cloud Logging for the Pub/Sub Publish audit log entry that
#      GCS produces when delivering the notification, and confirms it
#      landed on the expected topic.
#
# Required permissions:
#   - storage.objects.create / delete on the bucket
#   - logging.logEntries.list (roles/logging.viewer) on the project
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

# prefix → expected topic short name
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

# How long to wait for the audit log entry to appear.
LOG_TIMEOUT=60
LOG_POLL_INTERVAL=5

echo "=== GCS notification test (log-based) ==="
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

# Lower bound for log queries; small skew buffer for clock drift.
START_TS="$(date -u -v-30S +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
            || date -u -d '-30 seconds' +%Y-%m-%dT%H:%M:%SZ)"

for mapping in "${MAPPINGS[@]}"; do
  PREFIX="${mapping%%|*}"
  TOPIC="${mapping##*|}"
  OBJECT="${PREFIX}test-${RUN_ID}.txt"

  echo "--- Test: prefix='$PREFIX' → topic='$TOPIC' ---"

  LOCAL="$TMPDIR/payload-${TOPIC}.txt"
  echo "test payload for $TOPIC at $(date -u +%FT%TZ)" > "$LOCAL"
  gsutil -q cp "$LOCAL" "gs://$BUCKET/$OBJECT"
  echo "  uploaded: gs://$BUCKET/$OBJECT"

  # Poll Cloud Logging for a Publish entry on the expected topic that
  # references this object's name.
  FOUND=""
  WAITED=0
  while (( WAITED < LOG_TIMEOUT )); do
    sleep "$LOG_POLL_INTERVAL"
    WAITED=$((WAITED + LOG_POLL_INTERVAL))

    FILTER="resource.type=pubsub_topic"
    FILTER="$FILTER AND resource.labels.topic_id=\"$TOPIC\""
    FILTER="$FILTER AND protoPayload.methodName=\"google.pubsub.v1.Publisher.Publish\""
    FILTER="$FILTER AND timestamp>=\"$START_TS\""

    OUT="$(gcloud logging read "$FILTER" \
            --project="$PROJECT" \
            --limit=50 \
            --format=json 2>/dev/null || echo "[]")"

    MATCH="$(echo "$OUT" | jq -r --arg obj "$OBJECT" '
      .[]
      | (.protoPayload.request.messages // [])[]?
      | (.attributes.objectId // .attributes.object_id // empty)
      | select(. == $obj)
    ' | head -n1)"

    if [[ -n "$MATCH" ]]; then
      FOUND="$MATCH"
      break
    fi
  done

  if [[ -n "$FOUND" ]]; then
    echo "  PASS: GCS published notification for $FOUND to topic '$TOPIC' (verified via audit log)"
    PASS=$((PASS+1))
  else
    echo "  FAIL: no Publish log entry found for $OBJECT on topic '$TOPIC' within ${LOG_TIMEOUT}s"
    FAIL=$((FAIL+1))
    FAILED_CASES+=("$TOPIC (no log entry for $OBJECT)")
  fi

  gsutil -q rm "gs://$BUCKET/$OBJECT" || true
  echo
done

echo "=== Summary ==="
echo "Passed: $PASS"
echo "Failed: $FAIL"
if (( FAIL > 0 )); then
  printf '  - %s\n' "${FAILED_CASES[@]}"
  echo
  echo "If all cases failed with 'no log entry', Pub/Sub data-access audit"
  echo "logs are likely disabled. Either enable them, or ask an admin to"
  echo "create read-only test subscriptions you can pull from."
  exit 1
fi