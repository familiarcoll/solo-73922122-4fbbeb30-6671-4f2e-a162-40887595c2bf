"""Runtime configuration.

All persistent state lives under ``DATA_DIR`` which is expected to be a shared
volume when several API instances are running.
"""
from __future__ import annotations

import os
from pathlib import Path

DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
INGEST_DIR = DATA_DIR / "ingest"
SHARD_DIR = DATA_DIR / "shards" / "current"
STAGING_DIR = DATA_DIR / "shards" / "staging"
DB_PATH = DATA_DIR / "app.db"

# API upload limits (part of the public contract).
MAX_UPLOAD_FILES = 3
MAX_ROWS_PER_CSV = 1000

# Hard cap on the number of shards produced at ingestion.
MAX_SHARDS = 64
# Target number of rows per shard when building the library.
SHARD_TARGET_ROWS = int(os.environ.get("SHARD_TARGET_ROWS", "100"))

# Background purging worker.
WORKER_INTERVAL_SECONDS = float(os.environ.get("WORKER_INTERVAL_SECONDS", "0.25"))
# A shard lease that has not been refreshed for that long is considered stale
# and may be taken over by another instance after a restart.
LEASE_TIMEOUT_SECONDS = float(os.environ.get("LEASE_TIMEOUT_SECONDS", "30"))
# A withdrawal task that has been PROCESSING without an update for that long
# is reclaimed (instance crash protection).
TASK_TIMEOUT_SECONDS = float(os.environ.get("TASK_TIMEOUT_SECONDS", "60"))
# Artificial delay inside each purge step. Zero in production; used by the
# crash-recovery tests to kill a process mid-purge deterministically.
PURGE_STEP_DELAY_SECONDS = float(os.environ.get("PURGE_STEP_DELAY_SECONDS", "0"))
# Extra delay right before the old shard file is removed and the purified
# staging file is published. Used to crash inside the most sensitive window.
PURGE_SWAP_DELAY_SECONDS = float(os.environ.get("PURGE_SWAP_DELAY_SECONDS", "0"))
# Test-only admin endpoints (graceful stop / hard self-exit). Never enable in
# production deployments.
ENABLE_TEST_ADMIN = os.environ.get("ENABLE_TEST_ADMIN", "") == "1"

INSTANCE_ID = os.environ.get("INSTANCE_ID") or f"pid-{os.getpid()}"

CSV_HEADER = "subject_id,sample_id,value\n"


def ensure_dirs() -> None:
    for path in (DATA_DIR, INGEST_DIR, SHARD_DIR, STAGING_DIR):
        path.mkdir(parents=True, exist_ok=True)
