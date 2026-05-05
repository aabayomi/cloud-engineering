# GCS → Pub/Sub Notification Test

A script and runbook for verifying that GCS bucket notifications are correctly
routed to their expected Pub/Sub topics.

---

## Background

### What is Pub/Sub?

Cloud Pub/Sub is Google Cloud's managed messaging service. It implements the
publish/subscribe pattern: producers ("publishers") send messages to a named
channel ("topic"), and consumers ("subscribers") receive messages by attaching
a "subscription" to that topic. Publishers and subscribers don't know about
each other — the topic decouples them.

The core objects:

- **Topic** — a named channel. Messages are published to a topic.
- **Subscription** — a named queue attached to a topic. Each message published
  to a topic is delivered to every subscription on that topic. Each
  subscription independently tracks which messages have been acknowledged.
- **Message** — the payload plus a set of attributes (a string-to-string map).
  GCS notifications use attributes heavily to convey metadata like the bucket
  name, object name, and event type.

Two subscription delivery modes exist:

- **Pull** — the consumer asks for messages. The script (when it had
  permissions) used pull subscriptions because they're easy to script.
- **Push** — Pub/Sub POSTs messages to an HTTPS endpoint. Common in production
  for triggering Cloud Run, Cloud Functions, etc.

Acknowledgement matters: once a subscriber acks a message, that subscription
will not receive it again. **Acking a message in a subscription you don't own
will hide that message from the real consumer.** This is why the test design
in this project avoids touching subscriptions that already have downstream
consumers.

### What is a GCS bucket notification?

A bucket notification is a configuration on a Cloud Storage bucket that tells
GCS: "when something happens to an object in this bucket, publish a message to
this Pub/Sub topic." Each notification config has:

- A target topic (full resource path).
- A list of event types to fire on:
  - `OBJECT_FINALIZE` — a new object was successfully written.
  - `OBJECT_METADATA_UPDATE` — metadata on an existing object changed.
  - `OBJECT_DELETE` — an object was deleted (or overwritten, if not versioned).
  - `OBJECT_ARCHIVE` — an object version was archived (versioned buckets).
- An optional `object_name_prefix` filter — only fire on objects whose name
  starts with this prefix.
- A payload format — `JSON_API_V1` (full object metadata as JSON) or `NONE`
  (just the attributes, no body).

The published message always carries useful attributes regardless of payload
format, including:

- `bucketId` — the bucket that triggered the event.
- `objectId` — the full name of the object (including any prefix path).
- `eventType` — one of the event types above.
- `payloadFormat` — what's in the message body.

A single bucket can have many notification configs. The configuration in this
project uses one bucket with five notification configs, each filtered by a
different `object_name_prefix`, routing to a different topic. This pattern lets
a single shared bucket fan out events to per-tenant or per-purpose topics.

### What this test verifies

The Terraform in `storage.tf` declares five `(prefix → topic)` mappings:

| Prefix | Expected topic |
|--------|---------------|
| `ingest/` | `ingest` |
| `egress/` | `egress` |
| `egress1/` | `egress1` |
| `egress2/` | `egress2` |
| `egress3/upload/` | `egress3` |

The test confirms two things:

1. **Configuration correctness** — by listing the live notifications on the
   bucket and showing they match what's expected.
2. **End-to-end delivery** — by uploading a uniquely-named test object under
   each prefix and checking that GCS actually published a notification to the
   expected topic.

The end-to-end check requires either subscription access or audit log access
(see Permissions below).

---

## Prerequisites

### Tools

- `gcloud` (authenticated as a user with project access)
- `gsutil` (ships with `gcloud`)
- `jq`

Authenticate once with `gcloud auth login` and confirm the active account with
`gcloud config list account`.

### Permissions

The script needs permissions in two categories. Required for both paths:

- `storage.objects.create` and `storage.objects.delete` on the bucket (so it
  can upload and clean up test objects). `roles/storage.objectAdmin` on the
  bucket is sufficient.
- `storage.buckets.get` (to list notification configs). Included in
  `roles/storage.objectAdmin` and `roles/storage.legacyBucketReader`.

For the end-to-end check, **one of**:

- `roles/logging.viewer` on the project, **and** Pub/Sub data-access audit
  logs (`DATA_WRITE`) enabled for `pubsub.googleapis.com` on the project.
  This is the path the current script uses.
- `roles/pubsub.subscriber` on a pre-existing test subscription per topic.
  This requires script changes; see "Alternative: subscription-based" below.

To check whether Pub/Sub audit logs are enabled:

```bash
gcloud projects get-iam-policy <PROJECT_ID> --format=json \
  | jq '.auditConfigs[] | select(.service == "pubsub.googleapis.com")'
```

If that command prints nothing, audit logs are not enabled for Pub/Sub and the
end-to-end check will report `no log entry found` for every test, even when
notifications are actually working. See "Enabling Pub/Sub audit logs" below.

---

## Running the test

```bash
chmod +x gcs-notifications-test.sh
./gcs-notifications-test.sh -p <PROJECT_ID> -b <BUCKET_NAME>
```

Flags:

- `-p` — the GCP project that owns the topics.
- `-b` — the GCS bucket name (no `gs://` prefix).

Example:

```bash
./gcs-notifications-test.sh \
  -p cs-csdbs-ip00000002-dev8681 \
  -b cs-gcs-csdbs-ip00000002-notification-test-dev1807
```

### What it does, step by step

1. **Lists configured notifications** on the bucket via
   `gsutil notification list`. This is the configuration-correctness check —
   you should see one notification per `(prefix → topic)` mapping.
2. **For each mapping**, in order:
   a. Generates a unique object name like `ingest/test-1778008085-45250.txt`.
      The run ID combines a Unix timestamp and the script's PID so concurrent
      runs don't collide.
   b. Uploads a small text file to that object name.
   c. Polls Cloud Logging every 5 seconds (up to 60s) for a log entry where:
      - `resource.type = pubsub_topic`
      - `resource.labels.topic_id = <expected topic>`
      - `protoPayload.methodName = google.pubsub.v1.Publisher.Publish`
      - `timestamp >= <test start time>`
      - The publish request includes a message with `attributes.objectId`
        equal to the object that was just uploaded.
   d. On match: PASS. On timeout: FAIL.
   e. Deletes the test object.
3. **Prints a summary** of pass/fail counts.

### Interpreting results

| Output | Meaning |
|--------|---------|
| `PASS: GCS published notification for <object> to topic '<topic>'` | The (prefix → topic) mapping works end-to-end. |
| `FAIL: no Publish log entry found … within 60s` | Either the notification isn't firing, or audit logs aren't capturing it. Check the audit config first before assuming the notification is broken. |
| `ERROR: could not list notifications` | You don't have read access on the bucket, or the bucket name is wrong. Fix this first; nothing else will work without it. |

The "configured notifications" output at the top of the run is the definitive
source of truth for what GCS will do. If that listing matches the expected
mappings, the routing logic is correct regardless of whether the end-to-end
check succeeds.

---

## Enabling Pub/Sub audit logs

If the end-to-end check fails because audit logs aren't enabled, an admin can
enable them with one of two approaches.

**Console:** IAM & Admin → Audit Logs → find "Cloud Pub/Sub" → enable
`Data Write`.

**Terraform** (preferred; idempotent and reviewable):

```hcl
resource "google_project_iam_audit_config" "pubsub" {
  project = var.project_id
  service = "pubsub.googleapis.com"

  audit_log_config {
    log_type = "DATA_WRITE"
  }
}
```

A few notes on the cost/risk side worth raising in the change request:

- Data-access logs can be high-volume. Pub/Sub `Publish` happens once per
  notification. For a low-traffic project this is negligible; for a project
  with high publish rates it adds meaningful log ingestion cost.
- `DATA_READ` is not needed for this test — only `DATA_WRITE`.
- The change applies to all topics in the project, not just the ones being
  tested.

---

## Alternative: subscription-based testing

If audit logs cannot be enabled, an admin can instead create one read-only
test subscription per topic and grant the tester `roles/pubsub.subscriber` on
each. The script then uses pull-without-ack to peek at messages.

Terraform sketch:

```hcl
locals {
  test_topics = ["ingest", "egress", "egress1", "egress2", "egress3"]
}

resource "google_pubsub_subscription" "test" {
  for_each = toset(local.test_topics)
  name     = "${each.key}-test-sub"
  topic    = each.key
  project  = var.project_id

  message_retention_duration = "600s"
  ack_deadline_seconds       = 20
}

resource "google_pubsub_subscription_iam_member" "test_subscriber" {
  for_each     = toset(local.test_topics)
  project      = var.project_id
  subscription = google_pubsub_subscription.test[each.key].name
  role         = "roles/pubsub.subscriber"
  member       = "user:tester@example.com"
}
```

The script would then call:

```bash
gcloud pubsub subscriptions pull "<topic>-test-sub" \
  --project="$PROJECT" \
  --limit=50 \
  --format=json
```

without `--auto-ack`, so the messages remain available for any real consumer.

---

## Troubleshooting

**`could not list notifications`**
You don't have read access on the bucket, or the bucket name is wrong. Without
this, no other check is meaningful.

**All cases FAIL with `no log entry found`**
The most likely cause is that Pub/Sub `DATA_WRITE` audit logs are not enabled.
Verify with the `gcloud projects get-iam-policy ... | jq` command above. If
audit logs are enabled and the test still fails, check whether the bucket
notification config actually exists for that prefix (the listing at the top of
the run will tell you).

**Some cases PASS, some FAIL**
This is the interesting case — it usually means a specific notification config
is misconfigured (wrong prefix, wrong topic, missing entirely) or a specific
topic was deleted out from under the notification. Cross-reference the failing
prefix against the `gsutil notification list` output.

**`PERMISSION_DENIED` on subscription create**
You're hitting the original (now-removed) code path. The current script does
not create subscriptions. If you see this, you're running an old version.

**Object uploads succeed but logs show nothing**
Possible cause: the GCS service account
(`service-<project_number>@gs-project-accounts.iam.gserviceaccount.com`) lacks
`roles/pubsub.publisher` on the topic. The bucket notification config exists,
but GCS can't actually deliver. Check IAM on each topic.

**Test passes but downstream consumers don't receive messages**
This test only verifies that GCS published to the topic. It does not verify
that any specific subscription is healthy or that downstream consumers are
processing messages. Those are separate checks.

---

## Limitations

- **Audit-log dependency.** The end-to-end check is only as reliable as the
  audit log pipeline. If logs are delayed or sampled, the test may report a
  false FAIL within the 60-second window.
- **No negative testing.** The script confirms `ingest/foo.txt` produces a
  message on `ingest`. It does not confirm that `ingest/foo.txt` is *not*
  delivered to `egress`. With overlapping prefixes this would be worth adding;
  with the current non-overlapping prefixes the risk is low.
- **No message-content validation.** The script only matches on `objectId`.
  It does not verify that `eventType`, `bucketId`, or the JSON payload are
  correct. For most cases this is fine because GCS controls those fields;
  if your downstream consumers parse them, consider extending the check.
- **Single event type tested.** Uploads trigger `OBJECT_FINALIZE` only. The
  notification configs subscribe to four event types each; the others
  (`OBJECT_METADATA_UPDATE`, `OBJECT_DELETE`, `OBJECT_ARCHIVE`) are not
  exercised by this script.

---

## References

- Pub/Sub overview: https://cloud.google.com/pubsub/docs/overview
- GCS Pub/Sub notifications: https://cloud.google.com/storage/docs/pubsub-notifications
- Pub/Sub audit logging: https://cloud.google.com/pubsub/docs/audit-logging
- Cloud Logging query language: https://cloud.google.com/logging/docs/view/logging-query-language