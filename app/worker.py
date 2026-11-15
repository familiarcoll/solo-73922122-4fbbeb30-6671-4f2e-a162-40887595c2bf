"""Withdrawal task lifecycle and the background purging worker.

Concurrency model
-----------------
Every mutating database step runs inside ``BEGIN IMMEDIATE`` transactions, so
the two API instances serialise on SQLite's write lock. Purge work on a given
shard additionally requires an exclusive row in ``shard_leases``.

Fencing across rolling restarts
-------------------------------
A lease row records both the owning ``INSTANCE_ID`` and a process-unique
``incarnation``. File-touching steps (staging, shard swap, source rewrite)
re-check ownership inside a write transaction immediately before acting and
raise :class:`~app.storage.LeaseLostError` if another instance has taken over.
Consequently:

* :func:`startup_recovery` reclaims only leases whose heartbeat is older than
  :data:`~app.config.LEASE_TIMEOUT_SECONDS` - a live instance's lease and its
  staging files are never touched by a newly started peer;
* a lagging old worker whose lease expired cannot delete or overwrite the
  successor's published shard, every destructive operation is fenced;
* a genuinely crashed worker's lease expires and any instance resumes the
  work automatically, while the shard stays ``WITHHELD`` until completion.

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
from .storage import LeaseLostError

logger = logging.getLogger("withdrawal")

# Test-only latch: set through /test-admin/stall so the worker freezes at the
# staging point without heartbeating (simulating a wedged instance). Off in
# production because ENABLE_TEST_ADMIN and PURGE_STALL_SECONDS are unset there.
stall_injected = False


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------- #
# Recovery
# --------------------------------------------------------------------------- #
def startup_recovery(conn: sqlite3.Connection) -> None:
    """Reclaim state left behind by *crashed* instances.

    Live peers keep their leases: only heartbeats older than the lease timeout
    are considered dead. This is what makes a rolling restart safe - instance B
    starting while A is mid-purge must not steal A's lease or delete its files.
    Query endpoints are independent of this routine, so a new instance answers
    task queries immediately without waiting for any in-flight purge.
    """
    now = utcnow()
    lease_timeout = (
        now - timedelta(seconds=config.LEASE_TIMEOUT_SECONDS)
    ).isoformat()

    conn.execute("BEGIN IMMEDIATE")
    try:
        dead = conn.execute(
            "SELECT shard_id FROM shard_leases WHERE heartbeat_at < ?",
            (lease_timeout,),
        ).fetchall()
        dead_shard_ids = [r["shard_id"] for r in dead]

        # Only genuinely stale leases may be retaken.
        conn.execute(
            "DELETE FROM shard_leases WHERE heartbeat_at < ?",
            (lease_timeout,),
        )

        # RUNNING shard work whose owner is gone goes back to PENDING so any
        # worker can retry. Shards still actively leased are left untouched.
        if dead_shard_ids:
            placeholders = ",".join("?" for _ in dead_shard_ids)
            conn.execute(
                f"""
                UPDATE withdrawal_shards SET state='PENDING'
                WHERE state='RUNNING'
                  AND shard_id IN ({placeholders})
                """,
                dead_shard_ids,
            )

        # Tasks still carrying unfinished shard work go back to PENDING - but
        # only shards with no live lease are recoverable, so a task a peer is
        # actively purging is never flipped underneath it.
        conn.execute(
            """
            UPDATE withdrawals
            SET status='PENDING', updated_at=:now
            WHERE status IN ('PENDING', 'PROCESSING')
              AND EXISTS (
                  SELECT 1 FROM withdrawal_shards ws
                  WHERE ws.task_id = withdrawals.task_id AND ws.state != 'DONE'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM withdrawal_shards ws2
                  JOIN shard_leases sl ON sl.shard_id = ws2.shard_id
                  WHERE ws2.task_id = withdrawals.task_id AND ws2.state != 'DONE'
              )
            """,
            {"now": now.isoformat()},
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # If ingestion died mid-way, the meta flag was never written: drop orphans.
    ingested = conn.execute(
        "SELECT 1 FROM meta WHERE key='ingested' AND value='1'"
    ).fetchone()
    if not ingested:
        for path in config.SHARD_DIR.glob("shard-*.csv"):
            path.unlink(missing_ok=True)
        for path in config.INGEST_DIR.glob("source-*.csv"):
            path.unlink(missing_ok=True)

    # Staging cleanup is lease-aware: files owned by an active lease (or younger
    # than the lease timeout) must survive; only unreferenced, expired leftovers
    # of crashed processes are reaped.
    _reap_orphan_staging(conn, now)


def _reap_orphan_staging(conn: sqlite3.Connection, now: datetime) -> None:
    cutoff = (
        now - timedelta(seconds=config.LEASE_TIMEOUT_SECONDS)
    ).timestamp()
    live_shards = {
        r["shard_id"]
        for r in conn.execute("SELECT shard_id FROM shard_leases").fetchall()
    }
    for path in config.STAGING_DIR.glob("*.tmp.csv"):
        shard_id = storage.staging_shard_id(path)
        if shard_id is None or shard_id in live_shards:
            continue
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink(missing_ok=True)
        except FileNotFoundError:
            pass


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
# Leases (fenced ownership)
# --------------------------------------------------------------------------- #
def _acquire_lease(
    conn: sqlite3.Connection, task_id: str, shard_id: int
) -> bool:
    """Take an exclusive shard lease, reclaiming only heartbeated-out leases."""
    now = utcnow()
    lease_timeout = (
        now - timedelta(seconds=config.LEASE_TIMEOUT_SECONDS)
    ).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        # Reap just the expired leases, never a live peer's.
        conn.execute(
            "DELETE FROM shard_leases WHERE heartbeat_at < ?",
            (lease_timeout,),
        )
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO shard_leases
                (shard_id, owner, incarnation, task_id, leased_at, heartbeat_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                shard_id,
                config.INSTANCE_ID,
                config.INCARNATION_ID,
                task_id,
                now.isoformat(),
                now.isoformat(),
            ),
        )
        acquired = cur.rowcount == 1
        if acquired:
            # A dead predecessor may have left staging files behind. Ownership
            # has just been established inside this transaction, so removing
            # them here cannot touch a live instance's files.
            for stale in storage.list_staging_files(shard_id):
                stale.unlink(missing_ok=True)

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


def _owns_lease(conn: sqlite3.Connection, shard_id: int) -> bool:
    """True only if this exact incarnation currently holds the shard lease."""
    row = conn.execute(
        "SELECT 1 FROM shard_leases WHERE shard_id=? AND owner=? AND incarnation=?",
        (shard_id, config.INSTANCE_ID, config.INCARNATION_ID),
    ).fetchone()
    return row is not None


def _heartbeat(conn: sqlite3.Connection, shard_id: int) -> None:
    """Refresh the lease and fence the caller if it was taken over."""
    now = utcnow()
    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _owns_lease(conn, shard_id):
            raise LeaseLostError(
                f"shard {shard_id}: lease lost to another instance"
            )
        conn.execute(
            "UPDATE shard_leases SET heartbeat_at=? WHERE shard_id=? AND owner=? AND incarnation=?",
            (
                now.isoformat(),
                shard_id,
                config.INSTANCE_ID,
                config.INCARNATION_ID,
            ),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def _release_lease(conn: sqlite3.Connection, shard_id: int) -> None:
    # Only release our own lease; a successor's lease must be left in place.
    conn.execute(
        """
        DELETE FROM shard_leases
        WHERE shard_id=? AND owner=? AND incarnation=?
        """,
        (shard_id, config.INSTANCE_ID, config.INCARNATION_ID),
    )


# --------------------------------------------------------------------------- #
# Worker
# --------------------------------------------------------------------------- #
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
        # Keep the lease alive across the injected delay; this is also where a
        # rolling-restart successor waits for the timeout before taking over.
        _heartbeat(conn, shard_id)
        time.sleep(config.PURGE_SWAP_DELAY_SECONDS)

    try:
        # Fence 1: a successor may have taken over while we waited.
        _heartbeat(conn, shard_id)

        # 1. Rebuild current purified content from the authoritative index.
        published = _rebuild_shard_rows(conn, shard_id, subject_id)

        # 2. Stage the new shard (only rows that remain in the index).
        staged = storage.staging_path(shard_id, task_id)
        storage.atomic_write(staged, storage.render_csv(published))

        # Test-only wedge: stop heartbeating here so the lease goes stale and a
        # peer can take over while this incarnation is still alive.
        if stall_injected and config.PURGE_STALL_SECONDS:
            logger.warning(
                "test stall injected on shard %s for %ss (no heartbeats)",
                shard_id,
                config.PURGE_STALL_SECONDS,
            )
            time.sleep(config.PURGE_STALL_SECONDS)

        if config.PURGE_STEP_DELAY_SECONDS:  # test crash-injection point
            _heartbeat(conn, shard_id)
            time.sleep(config.PURGE_STEP_DELAY_SECONDS)

        # 3. Publish atomically, but only while we still own the lease. The
        #    fenced transaction means a lagging worker can never replace a
        #    successor's finished shard; os.replace never leaves a gap.
        target = storage.shard_path(shard_id)
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _owns_lease(conn, shard_id):
                raise LeaseLostError(
                    f"shard {shard_id}: lease lost before shard publish"
                )
            storage.publish_staged_shard(staged, target)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        # 4. Transactional index removal + status reconciliation. Re-check the
        #    lease inside the same write transaction that mutates the index.
        now = utcnow()
        conn.execute("BEGIN IMMEDIATE")
        try:
            if not _owns_lease(conn, shard_id):
                raise LeaseLostError(
                    f"shard {shard_id}: lease lost before index update"
                )
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
            _release_lease(conn, shard_id)
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
    except LeaseLostError:
        # A successor owns this shard now. Stop quietly, touch nothing.
        logger.warning(
            "task %s: abandoning shard %s, lease was taken over",
            task_id,
            shard_id,
        )
        return False
    finally:
        # No-op if the lease was already released or lost to a successor.
        try:
            _release_lease(conn, shard_id)
        except sqlite3.Error:
            pass


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

    # Hold one serialised write transaction across the whole source-purge
    # section. BEGIN IMMEDIATE makes the two instances queue on the SQLite
    # write lock, so two concurrent withdrawals can never read/filter/write
    # the same source CSV at the same time and resurrect each other's rows.
    # The file writes are atomic replaces (never an unlink-then-create gap);
    # if one fails the DB changes roll back and a later tick retries.
    conn.execute("BEGIN IMMEDIATE")
    try:
        current = conn.execute(
            "SELECT status FROM withdrawals WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if current is None or current["status"] == "COMPLETED":
            conn.execute("COMMIT")
            return

        sources = conn.execute(
            "SELECT source_file, purged FROM withdrawal_sources WHERE task_id=?",
            (task_id,),
        ).fetchall()
        pending_sources = [s for s in sources if not s["purged"]]

        # Source CSVs: rewrite removing only this subject's rows. Readers
        # always see a complete file because atomic_write uses tmp+rename.
        for src in pending_sources:
            storage.rewrite_source_without(src["source_file"], subject_id)

        now = utcnow()
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
    except LeaseLostError:
        # Another instance took over; the background loop retries later.
        logger.warning("worker tick lost lease for task %s", task["task_id"])
        return False
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
