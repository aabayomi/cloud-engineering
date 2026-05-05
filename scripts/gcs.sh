#!/usr/bin/env bash
#
# gcs.sh
#
# Tests prefix filtering on the example_storage_bucket_notification module.
# The bucket has 5 notifications, each pointing to a different topic:
#   ingest, egress, egress1, egress2, egress3
#
# This script:
#   1. Creates one short-lived pull subscription per topic
#   2. Drains anything stale
#   3. Uploads test objects covering each prefix + a non-matching control
#   4. Pulls each subscription and asserts the right (and only the right)
#      events landed
#   5. Cleans up test objects and subscriptions
#
# Run AFTER `terraform apply` once you've assigned prefixes.
#
# Empty prefix ("") means "match all objects" — the topic should receive
# events for EVERY uploaded object including the control objects, and the
# cross-talk check is skipped (there's no such thing as cross-talk when the
# topic accepts everything). This lets you run the script before adding
# prefixes to verify baseline fan-out, and again after adding them to
# verify filtering.

set -euo pipefail

# -----------------------------------------------------------------------------
# Config — edit to match your terraform outputs / actual values
# -----------------------------------------------------------------------------
PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
BUCKET="${BUCKET:?set BUCKET (the bucket attached to example_storage_bucket_notification)}"

# Topic name -> prefix you assigned in storage.tf
# Update these to match the object_name_prefix values in your module.
# Use "" for a notification with no prefix (matches everything).
declare -A TOPIC_PREFIX=(
  ["ingest"]="ingest/"
  ["egress"]="egress/"
  ["egress1"]="egress1/"
  ["egress2"]="egress2/"
  ["egress3"]=""
)

# A control prefix that NO notification should match
CONTROL_PREFIX="control/"

RUN_ID="test-$(date +%s)-$$"
TMPFILE=$(mktemp)
echo "payload-$RUN_ID" > "$TMPFILE"

SUB_PREFIX="gcs-notif-test-${RUN_ID}"
declare -A SUB_NAMES=()

# -----------------------------------------------------------------------------
# Cleanup on exit
# -----------------------------------------------------------------------------
cleanup() {
  echo
  echo "==> Cleanup"
  for topic in "${!TOPIC_PREFIX[@]}"; do
    sub="${SUB_NAMES[$topic]:-}"
    if [[ -n "$sub" ]]; then
      gcloud pubsub subscriptions delete "$sub" \
        --project="$PROJECT_ID" --quiet >/dev/null 2>&1 || true
    fi
  done

  # Remove test objects (best-effort).
  # Skip empty-prefix entries — they'd expand to "gs://BUCKET/RUN_ID*"
  # which is fine, but the per-topic uploaded objects are already covered
  # by the named-prefix sweeps and the explicit removes below.
  for prefix in "${TOPIC_PREFIX[@]}" "$CONTROL_PREFIX"; do
    [[ -z "$prefix" ]] && continue
    gcloud storage rm --recursive "gs://$BUCKET/${prefix}${RUN_ID}*" \
      --quiet >/dev/null 2>&1 || true
  done
  # Catch any root-level objects we created (root control + empty-prefix uploads)
  gcloud storage rm "gs://$BUCKET/*${RUN_ID}*" \
    --quiet >/dev/null 2>&1 || true
  rm -f "$TMPFILE"
}
trap cleanup EXIT

# -----------------------------------------------------------------------------
# 1. Create a throwaway pull subscription per topic
# -----------------------------------------------------------------------------
echo "==> Creating test subscriptions"
for topic in "${!TOPIC_PREFIX[@]}"; do
  sub="${SUB_PREFIX}-${topic}"
  SUB_NAMES["$topic"]="$sub"
  gcloud pubsub subscriptions create "$sub" \
    --project="$PROJECT_ID" \
    --topic="$topic" \
    --ack-deadline=60 \
    --message-retention-duration=10m \
    --expiration-period=1h >/dev/null
  echo "    $sub  ->  $topic"
done

# Drain anything queued from before this test started
echo "==> Draining stale messages"
for topic in "${!TOPIC_PREFIX[@]}"; do
  gcloud pubsub subscriptions pull "${SUB_NAMES[$topic]}" \
    --project="$PROJECT_ID" --auto-ack --limit=100 \
    >/dev/null 2>&1 || true
done

# -----------------------------------------------------------------------------
# 2. Upload one matching object per prefix + a control object
# -----------------------------------------------------------------------------
echo "==> Uploading test objects"
declare -A UPLOADED_OBJECTS=()
for topic in "${!TOPIC_PREFIX[@]}"; do
  prefix="${TOPIC_PREFIX[$topic]}"
  if [[ -z "$prefix" ]]; then
    # Empty prefix: upload at root, embed topic name so we can identify it
    obj="empty-${topic}-${RUN_ID}.txt"
    label="(empty prefix - uploaded at root)"
  else
    obj="${prefix}${RUN_ID}.txt"
    label=""
  fi
  gcloud storage cp "$TMPFILE" "gs://$BUCKET/$obj" --quiet >/dev/null
  UPLOADED_OBJECTS["$topic"]="$obj"
  echo "    gs://$BUCKET/$obj  $label"
done

CONTROL_OBJ="${CONTROL_PREFIX}${RUN_ID}.txt"
gcloud storage cp "$TMPFILE" "gs://$BUCKET/$CONTROL_OBJ" --quiet >/dev/null
echo "    gs://$BUCKET/$CONTROL_OBJ  (control - should match nothing)"

# Also upload a root-level object (no prefix) — also a control
ROOT_OBJ="root-${RUN_ID}.txt"
gcloud storage cp "$TMPFILE" "gs://$BUCKET/$ROOT_OBJ" --quiet >/dev/null
echo "    gs://$BUCKET/$ROOT_OBJ  (root control - should match nothing)"

# Notifications take ~0.1s but Pub/Sub delivery adds a bit; give it room
echo "==> Waiting 10s for notifications to propagate"
sleep 10

# -----------------------------------------------------------------------------
# 3. Pull each subscription and check what landed
# -----------------------------------------------------------------------------
echo "==> Verifying delivery"
FAILURES=0

# Total number of distinct objects we uploaded this run (per-topic + 2 controls)
TOTAL_UPLOADED=$(( ${#TOPIC_PREFIX[@]} + 2 ))

for topic in "${!TOPIC_PREFIX[@]}"; do
  sub="${SUB_NAMES[$topic]}"
  expected_obj="${UPLOADED_OBJECTS[$topic]}"
  expected_prefix="${TOPIC_PREFIX[$topic]}"

  # Pull everything available on this subscription
  msgs=$(gcloud pubsub subscriptions pull "$sub" \
    --project="$PROJECT_ID" \
    --auto-ack \
    --limit=50 \
    --format="value(message.attributes.eventType,message.attributes.objectId)" \
    2>/dev/null || true)

  # All messages from this run, across all our uploads
  run_msgs=$(echo "$msgs" | grep -F "$RUN_ID" || true)
  run_finalize_count=$(echo "$run_msgs" | grep -c "OBJECT_FINALIZE" || true)

  if [[ -z "$expected_prefix" ]]; then
    # ---- Empty prefix: should receive events for EVERY uploaded object ----
    # Cross-talk doesn't apply; instead we verify fan-out is complete.
    display_prefix="(empty)"

    if [[ "$run_finalize_count" -ge "$TOTAL_UPLOADED" ]]; then
      printf "  PASS  topic=%-10s prefix=%-12s hits=%d/%d (matches all)\n" \
        "$topic" "$display_prefix" "$run_finalize_count" "$TOTAL_UPLOADED"
    else
      printf "  FAIL  topic=%-10s prefix=%-12s hits=%d/%d (expected all uploads)\n" \
        "$topic" "$display_prefix" "$run_finalize_count" "$TOTAL_UPLOADED"
      echo "        messages from this run:"
      echo "$run_msgs" | sed 's/^/          /'
      FAILURES=$((FAILURES + 1))
    fi
  else
    # ---- Non-empty prefix: must hit expected object, no cross-talk ----
    expected_hits=$(echo "$msgs" \
      | grep -F "$expected_obj" | grep -c "OBJECT_FINALIZE" || true)

    # Any messages from this run NOT for the expected prefix = cross-talk
    unexpected=$(echo "$run_msgs" | grep -v -F "/$expected_prefix" \
      | grep -v -F " $expected_prefix" || true)
    unexpected_count=$(echo -n "$unexpected" | grep -c . || true)

    if [[ "$expected_hits" -ge 1 && "$unexpected_count" -eq 0 ]]; then
      printf "  PASS  topic=%-10s prefix=%-12s hits=%d cross-talk=0\n" \
        "$topic" "$expected_prefix" "$expected_hits"
    else
      printf "  FAIL  topic=%-10s prefix=%-12s hits=%d cross-talk=%d\n" \
        "$topic" "$expected_prefix" "$expected_hits" "$unexpected_count"
      if [[ -n "$unexpected" ]]; then
        echo "        unexpected messages:"
        echo "$unexpected" | sed 's/^/          /'
      fi
      FAILURES=$((FAILURES + 1))
    fi
  fi
done

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "All checks passed."
  exit 0
else
  echo "$FAILURES topic(s) failed."
  exit 1
fi