#!/usr/bin/env bash
# Test GCS bucket notifications → Pub/Sub topic routing using existing
# subscriptions named "<topic>.rba".
#
# For each (prefix → topic) mapping this script verifies all four event
# types the notifications subscribe to:
#   - OBJECT_FINALIZE       (triggered by upload)
#   - OBJECT_METADATA_UPDATE (triggered by `gsutil setmeta`)
#   - OBJECT_ARCHIVE        (triggered by overwrite on a versioned bucket)
#   - OBJECT_DELETE         (triggered by final cleanup)
#
# OBJECT_ARCHIVE only fires on versioned buckets. Non-versioned buckets
# produce OBJECT_DELETE on overwrite instead, so the archive check is
# skipped automatically when versioning is off.
#
# Required permissions:
#   - storage.objects.create / delete / update on the bucket
#   - pubsub.subscriptions.consume on each <topic>.rba subscription
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
SKIP=0
FAILED_CASES=()

PULL_ATTEMPTS=10
PULL_INTERVAL=3
PULL_LIMIT=50

echo "=== GCS notification test (subscription-based, all event types) ==="
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

# Detect bucket versioning. Only versioned buckets emit OBJECT_ARCHIVE.
VERSIONING="$(gsutil versioning get "gs://$BUCKET" 2>/dev/null | awk '{print $NF}')"
if [[ "$VERSIONING" == "Enabled" ]]; then
  ARCHIVE_SUPPORTED=1
  echo "Bucket versioning: Enabled (OBJECT_ARCHIVE will be tested)"
else
  ARCHIVE_SUPPORTED=0
  echo "Bucket versioning: Suspended/off (OBJECT_ARCHIVE checks will be skipped)"
fi
echo

# --- Helper: pull from a subscription until a message matching both
# objectId and eventType is seen. ---
# Args: $1=subscription, $2=expected objectId, $3=expected eventType
# Returns 0 on match, 1 on timeout. Echoes "found" or "" on stdout.
pull_for_event() {
  local sub="$1"
  local obj="$2"
  local event="$3"
  local out match

  for ((i=1; i<=PULL_ATTEMPTS; i++)); do
    sleep "$PULL_INTERVAL"

    out="$(gcloud pubsub subscriptions pull "$sub" \
            --project="$PROJECT" \
            --auto-ack \
            --limit="$PULL_LIMIT" \
            --format=json 2>/dev/null || echo "[]")"

    match="$(echo "$out" | jq -r \
      --arg obj "$obj" \
      --arg event "$event" '
        .[]
        | select(.message.attributes.objectId == $obj)
        | select(.message.attributes.eventType == $event)
        | .message.attributes.eventType
      ' | head -n1)"

    if [[ -n "$match" ]]; then
      echo "$match"
      return 0
    fi
  done
  return 1
}

# --- Helper: record a result and print a one-line status. ---
record() {
  local status="$1"   # PASS / FAIL / SKIP
  local label="$2"    # human-readable case label
  local detail="$3"   # extra info

  case "$status" in
    PASS) echo "    PASS: $label — $detail"; PASS=$((PASS+1)) ;;
    FAIL) echo "    FAIL: $label — $detail"; FAIL=$((FAIL+1));
          FAILED_CASES+=("$label: $detail") ;;
    SKIP) echo "    SKIP: $label — $detail"; SKIP=$((SKIP+1)) ;;
  esac
}

for mapping in "${MAPPINGS[@]}"; do
  PREFIX="${mapping%%|*}"
  TOPIC="${mapping##*|}"
  SUB="${TOPIC}.rba"
  OBJECT="${PREFIX}test-${RUN_ID}.txt"

  echo "=== Test: prefix='$PREFIX' → topic='$TOPIC' (sub: $SUB) ==="

  LOCAL="$TMPDIR/payload-${TOPIC}.txt"
  echo "test payload for $TOPIC at $(date -u +%FT%TZ)" > "$LOCAL"

  # 1. OBJECT_FINALIZE — first upload.
  echo "  [1/4] OBJECT_FINALIZE"
  gsutil -q cp "$LOCAL" "gs://$BUCKET/$OBJECT"
  if pull_for_event "$SUB" "$OBJECT" "OBJECT_FINALIZE" >/dev/null; then
    record PASS "$TOPIC OBJECT_FINALIZE" "$OBJECT"
  else
    record FAIL "$TOPIC OBJECT_FINALIZE" "no message for $OBJECT"
    # If the upload event didn't fire, skip the rest — the object may not
    # actually exist, and the cleanup at the end still tries to delete.
    gsutil -q rm "gs://$BUCKET/$OBJECT" || true
    echo
    continue
  fi

  # 2. OBJECT_METADATA_UPDATE — change custom metadata on the live object.
  echo "  [2/4] OBJECT_METADATA_UPDATE"
  gsutil -q setmeta -h "x-goog-meta-test-marker:${RUN_ID}" "gs://$BUCKET/$OBJECT"
  if pull_for_event "$SUB" "$OBJECT" "OBJECT_METADATA_UPDATE" >/dev/null; then
    record PASS "$TOPIC OBJECT_METADATA_UPDATE" "$OBJECT"
  else
    record FAIL "$TOPIC OBJECT_METADATA_UPDATE" "no message for $OBJECT"
  fi

  # 3. OBJECT_ARCHIVE — overwrite the object on a versioned bucket.
  # On a versioned bucket, an overwrite archives the current live version
  # and creates a new live version. The archive event fires for the old
  # generation. On a non-versioned bucket, the same operation produces
  # OBJECT_DELETE instead, so we skip.
  echo "  [3/4] OBJECT_ARCHIVE"
  if (( ARCHIVE_SUPPORTED )); then
    echo "overwrite payload for $TOPIC at $(date -u +%FT%TZ)" > "$LOCAL"
    gsutil -q cp "$LOCAL" "gs://$BUCKET/$OBJECT"
    if pull_for_event "$SUB" "$OBJECT" "OBJECT_ARCHIVE" >/dev/null; then
      record PASS "$TOPIC OBJECT_ARCHIVE" "$OBJECT"
    else
      record FAIL "$TOPIC OBJECT_ARCHIVE" "no message for $OBJECT"
    fi
  else
    record SKIP "$TOPIC OBJECT_ARCHIVE" "bucket versioning disabled"
  fi

  # 4. OBJECT_DELETE — final cleanup also serves as the delete-event check.
  # On a versioned bucket this deletes the live version (still produces
  # OBJECT_DELETE for that generation). To fully clean up, we also remove
  # any noncurrent versions left behind.
  echo "  [4/4] OBJECT_DELETE"
  gsutil -q rm "gs://$BUCKET/$OBJECT" || true
  if pull_for_event "$SUB" "$OBJECT" "OBJECT_DELETE" >/dev/null; then
    record PASS "$TOPIC OBJECT_DELETE" "$OBJECT"
  else
    record FAIL "$TOPIC OBJECT_DELETE" "no message for $OBJECT"
  fi

  # On a versioned bucket, also purge any noncurrent versions of the
  # test object so we leave nothing behind. -a includes all versions.
  if (( ARCHIVE_SUPPORTED )); then
    gsutil -q rm -a "gs://$BUCKET/$OBJECT" 2>/dev/null || true
  fi

  echo
done

echo "=== Summary ==="
echo "Passed:  $PASS"
echo "Failed:  $FAIL"
echo "Skipped: $SKIP"
if (( FAIL > 0 )); then
  printf '  - %s\n' "${FAILED_CASES[@]}"
  exit 1
fi