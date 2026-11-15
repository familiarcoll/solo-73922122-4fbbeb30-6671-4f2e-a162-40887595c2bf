"""FastAPI application: ingestion, withdrawals and shard downloads."""
from __future__ import annotations

import logging
import threading
import time
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse

from . import config, ingest as ingest_mod, worker
from .db import connect, init_db
from .models import (
    HealthResponse,
    IngestResponse,
    ShardStatus,
    WithdrawalCreated,
    WithdrawalStatus,
)
from .storage import CsvValidationError, shard_path

logger = logging.getLogger("api")

_state = threading.local()


def get_conn():
    """One SQLite connection per worker thread (FastAPI sync endpoints)."""
    conn = getattr(_state, "conn", None)
    if conn is None:
        conn = connect()
        init_db(conn)
        _state.conn = conn
    return conn


class WorkerThread(threading.Thread):
    """Background purge loop bound to this API instance."""

    def __init__(self) -> None:
        super().__init__(name=f"purge-worker-{config.INSTANCE_ID}", daemon=True)
        self._stop = threading.Event()
        self.conn = connect()
        init_db(self.conn)

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        worker.startup_recovery(self.conn)
        while not self._stop.is_set():
            try:
                progressed = worker.tick(self.conn)
            except Exception:  # pragma: no cover - defensive
                logger.exception("worker loop error")
                progressed = False
            self._stop.wait(config.WORKER_INTERVAL_SECONDS if not progressed else 0.02)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    config.ensure_dirs()
    conn = get_conn()
    init_db(conn)
    worker.startup_recovery(conn)
    purge_worker = WorkerThread()
    purge_worker.start()
    logger.info("instance %s started", config.INSTANCE_ID)
    try:
        yield
    finally:
        purge_worker.stop()
        purge_worker.join(timeout=5)


app = FastAPI(
    title="Feature Store Withdrawal API",
    description=(
        "One-shot CSV ingestion and subject withdrawal with withheld / "
        "purified shard downloads. Two instances share one volume and one "
        "SQLite database."
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _not_ingested() -> HTTPException:
    return HTTPException(status_code=409, detail="library has not been ingested yet")


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.post("/ingest", response_model=IngestResponse, status_code=201)
def ingest_endpoint(files: list[UploadFile] = File(...)) -> IngestResponse:
    """Create the library from 1-3 CSV files. Rejected after the first time."""
    if len(files) > config.MAX_UPLOAD_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"at most {config.MAX_UPLOAD_FILES} CSV files may be uploaded",
        )
    payloads: list[bytes] = []
    for upload in files:
        data = upload.file.read()
        if not data:
            raise HTTPException(status_code=400, detail=f"{upload.filename}: empty file")
        payloads.append(data)

    conn = get_conn()
    try:
        result = ingest_mod.ingest(conn, payloads)
    except CsvValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ingest_mod.AlreadyIngestedError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    shards = [
        {
            "shard_id": m["shard_id"],
            "status": "AVAILABLE",
            "total_rows": m["total_rows"],
            "file": m["current_file"],
        }
        for m in result["shards"]
    ]
    return IngestResponse(
        status=result["status"],
        files_ingested=result["files_ingested"],
        total_rows=result["total_rows"],
        shard_count=result["shard_count"],
        shards=shards,  # type: ignore[arg-type]
    )


@app.post("/withdrawals", response_model=WithdrawalCreated, status_code=202)
def create_withdrawal(subject_id: str) -> WithdrawalCreated:
    """Register a withdrawal. Returns an idempotent, queryable task id."""
    if not subject_id or not subject_id.strip():
        raise HTTPException(status_code=400, detail="subject_id is required")
    conn = get_conn()
    if not ingest_mod.is_ingested(conn):
        raise _not_ingested()
    result = worker.create_withdrawal(conn, subject_id.strip())
    return WithdrawalCreated(**result)


@app.get("/withdrawals/{task_id}", response_model=WithdrawalStatus)
def withdrawal_status(task_id: str) -> WithdrawalStatus:
    conn = get_conn()
    task = worker.get_task(conn, task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    shard_states = [
        {
            "shard_id": row["shard_id"],
            "state": row["state"],
            "purged_rows_in_shard": row["purged_rows_in_shard"],
        }
        for row in worker.get_task_shards(conn, task_id)
    ]
    return WithdrawalStatus(
        task_id=task["task_id"],
        subject_id=task["subject_id"],
        status=task["status"],
        affected_shards=task["affected_shards"],
        affected_rows=task["affected_rows"],
        purged_rows=task["purged_rows"],
        source_purged=task["source_purged"],
        error=task["error"],
        created_at=task["created_at"],
        updated_at=task["updated_at"],
        shards=shard_states,  # type: ignore[arg-type]
    )


@app.get("/shards")
def list_shards() -> dict:
    conn = get_conn()
    if not ingest_mod.is_ingested(conn):
        raise _not_ingested()
    rows = conn.execute(
        "SELECT shard_id, status, total_rows, current_file FROM shards ORDER BY shard_id"
    ).fetchall()
    return {
        "shards": [
            {
                "shard_id": r["shard_id"],
                "status": r["status"],
                "available": r["status"] == "AVAILABLE",
                "total_rows": r["total_rows"],
                "file": r["current_file"],
            }
            for r in rows
        ]
    }


@app.get("/shards/{shard_id}/status", response_model=ShardStatus)
def shard_status(shard_id: int) -> ShardStatus:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM shards WHERE shard_id=?", (shard_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="shard not found")
    return ShardStatus(
        shard_id=row["shard_id"],
        status=row["status"],
        total_rows=row["total_rows"],
        available=row["status"] == "AVAILABLE",
        file=row["current_file"],
    )


@app.get("/shards/{shard_id}/download")
def download_shard(shard_id: int) -> FileResponse:
    """Stream a shard CSV. Withheld shards are refused immediately."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM shards WHERE shard_id=?", (shard_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="shard not found")
    if row["status"] != "AVAILABLE":
        raise HTTPException(
            status_code=409,
            detail=(
                f"shard {shard_id} is {row['status']} due to an in-flight "
                "withdrawal; download is stopped until purification completes"
            ),
        )
    path = shard_path(shard_id)
    if not path.exists():
        # File-level failure must never silently serve partial data.
        raise HTTPException(
            status_code=409,
            detail=f"shard {shard_id} file is unavailable on the shared volume",
        )
    return FileResponse(
        path,
        media_type="text/csv",
        filename=row["current_file"],
    )


@app.get("/healthz", response_model=HealthResponse)
def healthz() -> HealthResponse:
    conn = get_conn()
    return HealthResponse(
        status="ok",
        instance_id=config.INSTANCE_ID,
        ingested=ingest_mod.is_ingested(conn),
    )


# --------------------------------------------------------------------------- #
# Test-only admin surface (disabled unless ENABLE_TEST_ADMIN=1).
# Used because some sandboxes disallow sending POSIX signals to child
# processes; the process terminates itself instead, including a hard
# os._exit() path that skips cleanup just like SIGKILL.
# --------------------------------------------------------------------------- #
if config.ENABLE_TEST_ADMIN:
    import os

    @app.post("/test-admin/stop", include_in_schema=False)
    def test_admin_stop() -> JSONResponse:
        def _do_stop() -> None:
            time.sleep(0.1)
            os._exit(0)

        threading.Thread(target=_do_stop, daemon=True).start()
        return JSONResponse({"stopping": True})

    @app.post("/test-admin/hard-exit", include_in_schema=False)
    def test_admin_hard_exit() -> JSONResponse:
        # Skip all Python/finally/atexit handling: equivalent to a crash for
        # durability purposes (no flush, no atexit, no buffer flush).
        os._exit(137)


def main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(__import__("os").environ.get("PORT", "8000")),
    )


if __name__ == "__main__":  # pragma: no cover
    main()
