"""Withdrawal task lifecycle and the background purging worker.

Concurrency model
-----------------
Every mutating database step runs inside ``BEGIN IMMEDIATE`` transactions, so
the two API instances serialise on SQLite's write lock. Purge work on a given
shard additionally requires an exclusive row in ``shard_leases``.

A purified shard is always *rebuilt from the SQLite index and the source CSVs*
- an instance never restores an old file. A shard returns to AVAILABLE only in
the same transaction that deletes the withdrawn index rows and verifies that no
unfinished withdrawal still touches the shard. Two concurrent withdrawals of
different subjects therefore cannot resurrect each other's data.
"""
from __future__ import annotations

import logging
import sqlite3
import time
import uuid
from datetime import datetime, timedelta, timezone

from . import config, storage

logger = logging.getLogger("withdrawal")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Recovery
# --------------------------------------------------------------------------- #
def startup_recovery(conn: sqlite3.Connection) -> None:
    """Reclaim state left behind by killed instances."""
    now = utcnow()
    # Stale shard leases can always be retaken; the work itself is idempotent.
    conn.execute("DELETE FROM shard_leases")

    # Shard work left in RUNNING has no live lease anymore (leases were just
    # cleared), so it goes back to PENDING for any worker to retry.
    conn.execute(
        """
        UPDATE withdrawal_shards SET state='PENDING'
        WHERE state='RUNNING'
        """
    )

    # Tasks with unfinished shards go back to PENDING. This covers both a
    # crash shortly after the task flipped to PROCESSING (the TASK_TIMEOUT
    # below would not apply yet) and long-stuck tasks.
    conn.execute(
        """
        UPDATE withdrawals
        SET status='PENDING', updated_at=:now
        WHERE status IN ('PENDING', 'PROCESSING')
          AND EXISTS (
              SELECT 1 FROM withdrawal_shards ws
              WHERE ws.task_id = withdrawals.task_id AND ws.state != 'DONE'
          )
        """,
        {"now": now.isoformat()},
    )

    # Belt-and-braces: tasks stuck PROCESSING beyond the timeout.
    timeout = (now - timedelta(seconds=config.TASK_TIMEOUT_SECONDS)).isoformat()
    conn.execute(
        """
        UPDATE withdrawals SET status='PENDING', updated_at=:now
        WHERE status='PROCESSING' AND updated_at < :timeout
        """,
        {"now": now.isoformat(), "timeout": timeout},
    )

    # If ingestion died mid-way, the meta flag was never written: drop orphans.
    ingested = conn.execute(
        "SELECT 1 FROM meta WHERE key='ingested' AND value='1'"
    ).fetchone()
    if not ingested:
        for path in config.SHARD_DIR.glob("shard-*.csv"):
            path.unlink(missing_ok=True)
        for path in config.INGEST_DIR.glob("source-*.csv"):
            path.unlink(missing_ok=True)

    # Staging files are always safe to delete: they are only published through
    # an explicit rename inside the purge flow.
    for path in config.STAGING_DIR.glob("*.tmp.csv"):
        path.unlink(missing_ok=True)


# --------------------------------------------------------------------------- #
# Task creation (idempotent per subject)
# --------------------------------------------------------------------------- #
def create_withdrawal(conn: sqlite3.Connection, subject_id: str) -> dict:
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT * FROM withdrawals WHERE subject_id=? ORDER BY created_at LIMIT 1",
            (subject_id,),
        ).fetchone()
        if existing is not None:
            conn.execute("COMMIT")
            return {
                "task_id": existing["task_id"],
                "subject_id": existing["subject_id"],
                "status": existing["status"],
                "affected_shards": existing["affected_shards"],
                "affected_rows": existing["affected_rows"],
                "reused": True,
            }

        affected = conn.execute(
            """
            SELECT shard_id, COUNT(*) AS n
            FROM samples
            WHERE subject_id = ?
            GROUP BY shard_id
            ORDER BY shard_id
            """,
            (subject_id,),
        ).fetchall()
        affected_rows = sum(r["n"] for r in affected)
        task_id = f"wdl-{uuid.uuid4().hex}"

        if not affected:
            # Nothing to remove: complete immediately, still a queryable task.
            conn.execute(
                """
                INSERT INTO withdrawals
                    (task_id, subject_id, status, affected_shards, affected_rows,
                     purged_rows, source_purged, created_at, updated_at)
                VALUES (?, ?, 'COMPLETED', 0, 0, 0, 0, ?, ?)
                """,
                (task_id, subject_id, now.isoformat(), now.isoformat()),
            )
            conn.execute("COMMIT")
            return {
                "task_id": task_id,
                "subject_id": subject_id,
                "status": "COMPLETED",
                "affected_shards": 0,
                "affected_rows": 0,
                "reused": False,
            }

        shard_ids = [r["shard_id"] for r in affected]
        conn.execute(
            """
            INSERT INTO withdrawals
                (task_id, subject_id, status, affected_shards, affected_rows,
                 purged_rows, source_purged, created_at, updated_at)
            VALUES (?, ?, 'PENDING', ?, ?, 0, 0, ?, ?)
            """,
            (
                task_id,
                subject_id,
                len(shard_ids),
                affected_rows,
                now.isoformat(),
                now.isoformat(),
            ),
        )
        conn.executemany(
            """
            INSERT INTO withdrawal_shards (task_id, shard_id, state)
            VALUES (?, ?, 'PENDING')
            """,
            [(task_id, shard_id) for shard_id in shard_ids],
        )
        # Stop new downloads immediately, before the background work starts.
        conn.executemany(
            "UPDATE shards SET status='WITHHELD', updated_at=? WHERE shard_id=?",
            [(now.isoformat(), shard_id) for shard_id in shard_ids],
        )
        # Source CSVs that contain rows of the withdrawn subject.
        sources = conn.execute(
            """
            SELECT DISTINCT source_file FROM samples WHERE subject_id=?
            ORDER BY source_file
            """,
            (subject_id,),
        ).fetchall()
        conn.executemany(
            """
            INSERT INTO withdrawal_sources (task_id, source_file, purged)
            VALUES (?, ?, 0)
            """,
            [(task_id, r["source_file"]) for r in sources],
        )
        conn.execute("COMMIT")
        return {
            "task_id": task_id,
            "subject_id": subject_id,
            "status": "PENDING",
            "affected_shards": len(shard_ids),
            "affected_rows": affected_rows,
            "reused": False,
        }
    except Exception:
        conn.execute("ROLLBACK")
        raise


def get_task(conn: sqlite3.Connection, task_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM withdrawals WHERE task_id=?", (task_id,)
    ).fetchone()


def get_task_shards(conn: sqlite3.Connection, task_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM withdrawal_shards
        WHERE task_id=? ORDER BY shard_id
        """,
        (task_id,),
    ).fetchall()


# --------------------------------------------------------------------------- #
# Worker
# --------------------------------------------------------------------------- #
def _acquire_lease(
    conn: sqlite3.Connection, task_id: str, shard_id: int
) -> bool:
    now = utcnow()
    lease_timeout = (
        now - timedelta(seconds=config.LEASE_TIMEOUT_SECONDS)
    ).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "DELETE FROM shard_leases WHERE heartbeat_at < ?",
            (lease_timeout,),
        )
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO shard_leases
                (shard_id, owner, task_id, leased_at, heartbeat_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (shard_id, config.INSTANCE_ID, task_id, now.isoformat(), now.isoformat()),
        )
        acquired = cur.rowcount == 1
        if acquired:
            conn.execute(
                """
                UPDATE withdrawal_shards SET state='RUNNING'
                WHERE task_id=? AND shard_id=? AND state='PENDING'
                """,
                (task_id, shard_id),
            )
            conn.execute(
                """
                UPDATE withdrawals
                SET status='PROCESSING', updated_at=?
                WHERE task_id=? AND status='PENDING'
                """,
                (now.isoformat(), task_id),
            )
        conn.execute("COMMIT")
        return acquired
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _release_lease(conn: sqlite3.Connection, shard_id: int) -> None:
    conn.execute(
        "DELETE FROM shard_leases WHERE shard_id=? AND owner=?",
        (shard_id, config.INSTANCE_ID),
    )


def _rebuild_shard_rows(
    conn: sqlite3.Connection, shard_id: int, exclude_subject: str
) -> list[tuple[str, str, str]]:
    """Rebuild the purified content of a shard from index + source CSVs.

    Rows are looked up by ``sample_id`` rather than row number, because source
    CSVs are rewritten in place by earlier withdrawals, which shifts row
    numbers. The withdrawn subject is filtered out explicitly so that retrying
    after a crash is idempotent.
    """
    index_rows = conn.execute(
        """
        SELECT sample_id, source_file
        FROM samples
        WHERE shard_id=?
        ORDER BY row_id
        """,
        (shard_id,),
    ).fetchall()

    source_cache: dict[str, dict[str, tuple[str, str, str]]] = {}

    def source_by_sample_id(name: str) -> dict[str, tuple[str, str, str]]:
        if name not in source_cache:
            source_cache[name] = {
                row[1]: row for row in storage.read_source_rows(name)
            }
        return source_cache[name]

    published: list[tuple[str, str, str]] = []
    for entry in index_rows:
        lookup = source_by_sample_id(entry["source_file"])
        row = lookup.get(entry["sample_id"])
        if row is None:
            raise RuntimeError(
                f"source CSV {entry['source_file']} is missing indexed "
                f"sample_id={entry['sample_id']!r}; shard {shard_id} kept withheld"
            )
        if row[0] == exclude_subject:
            continue  # this task's target rows must never be republished
        published.append(row)

    return published


def _process_shard(conn: sqlite3.Connection, task: sqlite3.Row) -> bool:
    """Purge one shard for the task. Returns False if another worker owns it."""
    task_id = task["task_id"]
    subject_id = task["subject_id"]

    pending = conn.execute(
        """
        SELECT shard_id FROM withdrawal_shards
        WHERE task_id=? AND state != 'DONE'
        ORDER BY shard_id LIMIT 1
        """,
        (task_id,),
    ).fetchone()
    if pending is None:
        return True
    shard_id = pending["shard_id"]

    if not _acquire_lease(conn, task_id, shard_id):
        return False

    if config.PURGE_SWAP_DELAY_SECONDS:  # test crash-injection point
        time.sleep(config.PURGE_SWAP_DELAY_SECONDS)

    try:
        # 1. Rebuild current purified content from the authoritative index.
        published = _rebuild_shard_rows(conn, shard_id, subject_id)

        # 2. Stage the new shard (only rows that remain in the index).
        staged = storage.staging_path(shard_id, task_id)
        storage.atomic_write(staged, storage.render_csv(published))
        if config.PURGE_STEP_DELAY_SECONDS:  # test crash-injection point
            time.sleep(config.PURGE_STEP_DELAY_SECONDS)

        # 3. Remove the old downloadable file and stale staging copies, then
        #    publish the purified file. The DB keeps the shard WITHHELD until
        #    the index transaction below commits.
        target = storage.shard_path(shard_id)
        storage.swap_staged_shard(shard_id, staged, target)

        # 4. Transactional index removal + status reconciliation.
        now = utcnow()
        conn.execute("BEGIN IMMEDIATE")
        try:
            cur = conn.execute(
                "DELETE FROM samples WHERE shard_id=? AND subject_id=?",
                (shard_id, subject_id),
            )
            removed_here = cur.rowcount
            remaining = conn.execute(
                "SELECT COUNT(*) AS n FROM samples WHERE shard_id=?",
                (shard_id,),
            ).fetchone()["n"]
            conn.execute(
                """
                UPDATE withdrawal_shards
                SET state='DONE', purged_rows_in_shard=?
                WHERE task_id=? AND shard_id=?
                """,
                (removed_here, task_id, shard_id),
            )
            unfinished = conn.execute(
                """
                SELECT 1 FROM withdrawal_shards ws
                JOIN withdrawals w ON w.task_id = ws.task_id
                WHERE ws.shard_id=? AND ws.state != 'DONE'
                  AND w.task_id != ?
                LIMIT 1
                """,
                (shard_id, task_id),
            ).fetchone()
            new_status = "WITHHELD" if unfinished else "AVAILABLE"
            conn.execute(
                """
                UPDATE shards
                SET status=?, total_rows=?, updated_at=?
                WHERE shard_id=?
                """,
                (new_status, remaining, now.isoformat(), shard_id),
            )
            conn.execute(
                """
                UPDATE withdrawals
                SET purged_rows = purged_rows + ?, updated_at=?
                WHERE task_id=?
                """,
                (removed_here, now.isoformat(), task_id),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        logger.info(
            "task %s: shard %s purged (%s rows removed, %s remain)",
            task_id,
            shard_id,
            removed_here,
            len(published),
        )
        return True
    finally:
        _release_lease(conn, shard_id)


def _finish_task(conn: sqlite3.Connection, task: sqlite3.Row) -> None:
    """Purge source CSVs and mark the task COMPLETED once all shards are done."""
    task_id = task["task_id"]
    subject_id = task["subject_id"]

    remaining_shards = conn.execute(
        """
        SELECT COUNT(*) AS n FROM withdrawal_shards
        WHERE task_id=? AND state != 'DONE'
        """,
        (task_id,),
    ).fetchone()["n"]
    if remaining_shards:
        return

    # Source CSVs: rewrite removing only this subject's rows.
    sources = conn.execute(
        "SELECT source_file, purged FROM withdrawal_sources WHERE task_id=?",
        (task_id,),
    ).fetchall()
    for src in sources:
        if src["purged"]:
            continue
        storage.rewrite_source_without(src["source_file"], subject_id)

    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "UPDATE withdrawal_sources SET purged=1 WHERE task_id=?",
            (task_id,),
        )
        conn.execute(
            """
            UPDATE withdrawals
            SET status='COMPLETED', source_purged=?, updated_at=?
            WHERE task_id=?
            """,
            (len(sources), now.isoformat(), task_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info("task %s completed for subject %s", task_id, subject_id)


def tick(conn: sqlite3.Connection) -> bool:
    """One worker iteration. Returns True if some progress was made."""
    task = conn.execute(
        """
        SELECT * FROM withdrawals
        WHERE status IN ('PENDING', 'PROCESSING')
        ORDER BY created_at LIMIT 1
        """,
    ).fetchone()
    if task is None:
        return False

    try:
        progressed = _process_shard(conn, task)
        if not progressed:
            return False
        task = get_task(conn, task["task_id"])
        assert task is not None
        _finish_task(conn, task)
        return True
    except Exception as exc:  # noqa: BLE001 - worker must survive any single failure
        logger.exception("worker tick failed for task %s", task["task_id"])
        # Keep the task retryable but surface the last error for observability.
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "UPDATE withdrawals SET error=?, updated_at=? WHERE task_id=?",
                (
                    f"{type(exc).__name__}: {exc}"[:500],
                    utcnow().isoformat(),
                    task["task_id"],
                ),
            )
            conn.execute("COMMIT")
        except Exception:  # pragma: no cover - defensive
            conn.execute("ROLLBACK")
        return False
