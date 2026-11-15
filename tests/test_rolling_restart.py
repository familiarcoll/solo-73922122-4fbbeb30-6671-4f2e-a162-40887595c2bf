"""Rolling-restart handoff between two live instances sharing one volume.

Reproduces the reported defect: instance A has written its staging area but not
published yet when instance B starts. B must be able to (1) answer task
queries immediately, (2) take over and finish the purge once A's lease is
provably stale, while A - still alive and resuming its old work afterwards -
must never delete or overwrite the successor's result.
"""
from __future__ import annotations

import time

import httpx

from conftest import Server, make_csv, read_csv_rows, wait_task, free_port

# A lease is considered dead after this long. Short enough for a fast test,
# long enough that heartbeats keep a genuinely live worker protected.
LEASE_TIMEOUT = "1.5"
SLOW_ENV = {
    "LEASE_TIMEOUT_SECONDS": LEASE_TIMEOUT,
    "PURGE_SWAP_DELAY_SECONDS": "0.5",
    "PURGE_STEP_DELAY_SECONDS": "0.2",
    "PURGE_STALL_SECONDS": "8",
    "SHARD_TARGET_ROWS": "25",
}


def _ingest(client: httpx.Client, n: int = 120) -> int:
    rows = [(f"s{i % 5}", f"k-{i}", f"v{i}") for i in range(n)]
    r = client.post(
        "/ingest", files=[("files", ("f.csv", make_csv(rows), "text/csv"))]
    )
    assert r.status_code == 201, r.text
    return r.json()["shard_count"]


def _wait_processing(client: httpx.Client, task_id: str) -> None:
    deadline = time.time() + 8
    while time.time() < deadline:
        if client.get(f"/withdrawals/{task_id}").json()["status"] == "PROCESSING":
            return
        time.sleep(0.05)
    raise AssertionError("task never entered PROCESSING")


def test_rolling_restart_handoff_does_not_corrupt_finished_shard(tmp_data):
    # --- A starts first and begins the withdrawal ------------------------- #
    a = Server(tmp_data, free_port(), env_extra=SLOW_ENV)
    a.wait_ready()
    try:
        with a.client() as c:
            shard_count = _ingest(c)
            assert shard_count >= 3
            r = c.post("/withdrawals", params={"subject_id": "s0"})
            assert r.status_code == 202
            task_id = r.json()["task_id"]
            _wait_processing(c, task_id)
            # Freeze A at the staging point; its heartbeats stop and the lease
            # goes stale while the process stays alive.
            assert c.post("/test-admin/stall").status_code == 200

        # --- B starts while A is alive, holding a lease and staging files - #
        b = Server(tmp_data, free_port(), env_extra=SLOW_ENV)
        b.wait_ready()
        try:
            with b.client() as cb:
                # New instance answers queries immediately; no waiting for A.
                task = cb.get(f"/withdrawals/{task_id}")
                assert task.status_code == 200
                assert task.json()["task_id"] == task_id

            # A's lease is still fresh at B's startup: recovery must not reap
            # A's in-use staging files.
            staging = tmp_data / "shards" / "staging"
            assert list(staging.iterdir()), "A's in-use staging files were deleted"

            # Wait for A's lease to become provably stale, then B takes over.
            time.sleep(float(LEASE_TIMEOUT) + 0.8)

            with b.client() as cb:
                final = wait_task(cb, task_id, timeout=25)
                assert final["status"] == "COMPLETED"

                # The reported failure mode: task COMPLETED, shard AVAILABLE,
                # but the file was deleted by the lagging old worker -> 409.
                shards = cb.get("/shards").json()["shards"]
                assert all(s["status"] == "AVAILABLE" for s in shards)
                remaining = []
                for s in shards:
                    d = cb.get(f"/shards/{s['shard_id']}/download")
                    assert d.status_code == 200, (
                        f"shard {s['shard_id']} download returned "
                        f"{d.status_code}: {d.text}"
                    )
                    remaining.extend(read_csv_rows(d.content))

            # Purified content: only the withdrawn subject's rows are gone,
            # every other sample survives in order.
            assert all(row[0] != "s0" for row in remaining)
            assert len(remaining) == 120 - 120 // 5
            assert {row[0] for row in remaining} == {"s1", "s2", "s3", "s4"}

            # A's stall ends and its lagging worker resumes. It must detect the
            # lost lease and never delete or overwrite B's published files.
            time.sleep(3.0)
            with a.client() as ca, b.client() as cb:
                for shard_id in range(shard_count):
                    da = ca.get(f"/shards/{shard_id}/download")
                    db = cb.get(f"/shards/{shard_id}/download")
                    assert da.status_code == db.status_code == 200
                    assert da.content == db.content
                assert (
                    ca.get(f"/withdrawals/{task_id}").json()["status"]
                    == "COMPLETED"
                )

            # Staging area is clean once every lease has been released.
            time.sleep(0.5)
            assert list(staging.iterdir()) == []
        finally:
            b.stop()
    finally:
        a.stop()


def test_starting_instance_never_reaps_live_lease_files(tmp_data):
    """B's startup must leave A's fresh lease and staging files untouched."""
    a = Server(tmp_data, free_port(), env_extra=SLOW_ENV)
    a.wait_ready()
    try:
        with a.client() as c:
            _ingest(c, n=120)
            task_id = c.post(
                "/withdrawals", params={"subject_id": "s0"}
            ).json()["task_id"]
            _wait_processing(c, task_id)
            assert c.post("/test-admin/stall").status_code == 200

        b = Server(tmp_data, free_port(), env_extra=SLOW_ENV)
        b.wait_ready()
        try:
            staging = tmp_data / "shards" / "staging"
            # A's heartbeat is far younger than the lease timeout at startup.
            assert list(staging.iterdir()), "live instance staging was reaped"
            with b.client() as cb:
                assert cb.get(f"/withdrawals/{task_id}").status_code == 200
                # affected shards stay withheld while purification is in flight
                for s in cb.get("/shards").json()["shards"]:
                    if s["status"] != "AVAILABLE":
                        assert (
                            cb.get(
                                f"/shards/{s['shard_id']}/download"
                            ).status_code
                            == 409
                        )
        finally:
            b.stop()
    finally:
        a.stop()
