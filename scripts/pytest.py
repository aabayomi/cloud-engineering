"""
pytest suite for GCS bucket notification → Pub/Sub topic routing.

Mirrors the logic of gcs-notifications-test.sh, testing all four event types:
  - OBJECT_FINALIZE       (triggered by upload)
  - OBJECT_METADATA_UPDATE (triggered by metadata update)
  - OBJECT_ARCHIVE        (triggered by overwrite on a versioned bucket)
  - OBJECT_DELETE         (triggered by deletion)

Configuration (via pytest CLI options or environment variables):
  --project   / GCS_PROJECT   : GCP project ID  (required)
  --bucket    / GCS_BUCKET    : GCS bucket name  (required)

Run:
  pytest test_gcs_notifications.py \
      --project=my-project --bucket=my-bucket -v
"""

import json
import os
import subprocess
import tempfile
import time
from datetime import datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAPPINGS = [
    ("ingest/", "ingest"),
    ("egress/", "egress"),
    ("egress1/", "egress1"),
    ("egress2/", "egress2"),
    ("egress3/upload/", "egress3"),
]

PULL_ATTEMPTS = 10
PULL_INTERVAL = 3   # seconds between polls
PULL_LIMIT = 50     # messages per pull


# ---------------------------------------------------------------------------
# pytest hooks / fixtures
# ---------------------------------------------------------------------------

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--project", default=None, help="GCP project ID")
    parser.addoption("--bucket", default=None, help="GCS bucket name (no gs:// prefix)")


@pytest.fixture(scope="session")
def project(request: pytest.FixtureRequest) -> str:
    val = request.config.getoption("--project") or os.environ.get("GCS_PROJECT", "")
    if not val:
        pytest.fail("GCP project required: --project=<id> or GCS_PROJECT env var")
    return val


@pytest.fixture(scope="session")
def bucket(request: pytest.FixtureRequest) -> str:
    val = request.config.getoption("--bucket") or os.environ.get("GCS_BUCKET", "")
    if not val:
        pytest.fail("GCS bucket required: --bucket=<name> or GCS_BUCKET env var")
    return val


@pytest.fixture(scope="session")
def run_id() -> str:
    return f"{int(time.time())}-{os.getpid()}"


@pytest.fixture(scope="session")
def versioning_enabled(bucket: str) -> bool:
    """Return True when the bucket has object versioning enabled."""
    result = subprocess.run(
        ["gsutil", "versioning", "get", f"gs://{bucket}"],
        capture_output=True, text=True,
    )
    return result.stdout.strip().endswith("Enabled")


@pytest.fixture(scope="session")
def tmpdir_session(tmp_path_factory: pytest.TempPathFactory):
    return tmp_path_factory.mktemp("gcs_test")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _pull_for_event(
    project: str,
    subscription: str,
    object_id: str,
    event_type: str,
    attempts: int = PULL_ATTEMPTS,
    interval: int = PULL_INTERVAL,
    limit: int = PULL_LIMIT,
) -> bool:
    """
    Poll *subscription* until a message matching both *object_id* and
    *event_type* is seen, or we exhaust *attempts*.

    Returns True on match, False on timeout.
    """
    for _ in range(attempts):
        time.sleep(interval)
        result = subprocess.run(
            [
                "gcloud", "pubsub", "subscriptions", "pull", subscription,
                f"--project={project}",
                "--auto-ack",
                f"--limit={limit}",
                "--format=json",
            ],
            capture_output=True, text=True,
        )
        try:
            messages = json.loads(result.stdout or "[]")
        except json.JSONDecodeError:
            messages = []

        for msg in messages:
            attrs = msg.get("message", {}).get("attributes", {})
            if attrs.get("objectId") == object_id and attrs.get("eventType") == event_type:
                return True

    return False


def _gsutil(*args: str) -> None:
    """Run a gsutil command, raising on non-zero exit."""
    subprocess.run(["gsutil", "-q", *args], check=True)


def _gsutil_safe(*args: str) -> None:
    """Run a gsutil command, ignoring errors (mirrors `|| true`)."""
    subprocess.run(["gsutil", "-q", *args])


# ---------------------------------------------------------------------------
# Parametrised test class
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("prefix,topic", MAPPINGS)
class TestGCSNotifications:
    """One test per (prefix, topic, event_type) combination."""

    # ------------------------------------------------------------------
    # Shared per-mapping setup: upload the initial object once.
    # Pytest parametrize creates a fresh instance per (prefix, topic)
    # pair, but we need the FINALIZE step to run before the others.
    # We use a class-scoped fixture (via autouse) to orchestrate the
    # upload and track the object name used by remaining tests.
    # ------------------------------------------------------------------

    @pytest.fixture(autouse=True, scope="class")
    def setup_object(
        self,
        request: pytest.FixtureRequest,
        project: str,
        bucket: str,
        run_id: str,
        tmpdir_session,
        versioning_enabled: bool,
    ) -> None:
        """Upload the test object once; store state on the class."""
        prefix, topic = request.param  # injected by parametrize
        object_name = f"{prefix}test-{run_id}.txt"
        subscription = f"{topic}.rba"

        local_file = tmpdir_session / f"payload-{topic}.txt"
        local_file.write_text(
            f"test payload for {topic} at "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        )

        # Store on the class so individual test methods can read them.
        self.__class__._project = project
        self.__class__._bucket = bucket
        self.__class__._object = object_name
        self.__class__._subscription = subscription
        self.__class__._local_file = local_file
        self.__class__._versioning = versioning_enabled
        self.__class__._finalize_ok = False

        # Upload — this triggers OBJECT_FINALIZE.
        _gsutil("cp", str(local_file), f"gs://{bucket}/{object_name}")

        yield  # tests run here

        # Teardown: delete the test object (and all versions if versioned).
        _gsutil_safe("rm", f"gs://{bucket}/{object_name}")
        if versioning_enabled:
            _gsutil_safe("rm", "-a", f"gs://{bucket}/{object_name}")

    # ------------------------------------------------------------------
    # Test 1: OBJECT_FINALIZE
    # ------------------------------------------------------------------

    def test_object_finalize(self) -> None:
        found = _pull_for_event(
            self._project,
            self._subscription,
            self._object,
            "OBJECT_FINALIZE",
        )
        self.__class__._finalize_ok = found
        assert found, (
            f"[{self._subscription}] No OBJECT_FINALIZE message for {self._object}"
        )

    # ------------------------------------------------------------------
    # Test 2: OBJECT_METADATA_UPDATE
    # ------------------------------------------------------------------

    def test_object_metadata_update(self) -> None:
        if not self._finalize_ok:
            pytest.skip("Skipping: OBJECT_FINALIZE did not succeed; object may not exist")

        _gsutil(
            "setmeta",
            f"-h", f"x-goog-meta-test-marker:{int(time.time())}",
            f"gs://{self._bucket}/{self._object}",
        )
        found = _pull_for_event(
            self._project,
            self._subscription,
            self._object,
            "OBJECT_METADATA_UPDATE",
        )
        assert found, (
            f"[{self._subscription}] No OBJECT_METADATA_UPDATE message for {self._object}"
        )

    # ------------------------------------------------------------------
    # Test 3: OBJECT_ARCHIVE
    # ------------------------------------------------------------------

    def test_object_archive(self) -> None:
        if not self._versioning:
            pytest.skip("Bucket versioning disabled — OBJECT_ARCHIVE will not fire")
        if not self._finalize_ok:
            pytest.skip("Skipping: OBJECT_FINALIZE did not succeed; object may not exist")

        # Overwrite the object; this archives the previous live version.
        self._local_file.write_text(
            f"overwrite payload at "
            f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}\n"
        )
        _gsutil("cp", str(self._local_file), f"gs://{self._bucket}/{self._object}")

        found = _pull_for_event(
            self._project,
            self._subscription,
            self._object,
            "OBJECT_ARCHIVE",
        )
        assert found, (
            f"[{self._subscription}] No OBJECT_ARCHIVE message for {self._object}"
        )

    # ------------------------------------------------------------------
    # Test 4: OBJECT_DELETE
    # ------------------------------------------------------------------

    def test_object_delete(self) -> None:
        if not self._finalize_ok:
            pytest.skip("Skipping: OBJECT_FINALIZE did not succeed; object may not exist")

        # Delete the live version — triggers OBJECT_DELETE.
        # (Teardown in setup_object also deletes, but that runs after
        # the assertion so this explicit call ensures we catch the event.)
        _gsutil_safe("rm", f"gs://{self._bucket}/{self._object}")

        found = _pull_for_event(
            self._project,
            self._subscription,
            self._object,
            "OBJECT_DELETE",
        )
        assert found, (
            f"[{self._subscription}] No OBJECT_DELETE message for {self._object}"
        )
