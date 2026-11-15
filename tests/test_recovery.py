"""Crash / write-failure recovery tests using real subprocesses."""
from __future__ import annotations

import os
import stat
import time
from pathlib import Path

import httpx

from conftest import Server, make_csv, read_csv_rows, wait_task, free_port


def _ingest_rows(client: httpx.Client, n: int = 120) -> int:
    rows = [(f"s{i % 5}", f"k-{i}", f"v{i}") for i in range(n)]
    r = client.post(
        "/ingest", files=[("files", ("f.csv", make_csv(rows), "text/csv"))]
    )
    assert r.status_code == 201, r.text
    return r.json()["shard_count"]


def test_sigkill_mid_purge_then_restart_resumes(tmp_data):
    # Many small shards + a long delay before each file swap keeps the task
    # in flight long enough to kill the process mid-purge.
    slow_env = {
        "PURGE_STEP_DELAY_SECONDS": "0.3",
        "PURGE_SWAP_DELAY_SECONDS": "0.8",
        "SHARD_TARGET_ROWS": "25",
    }
    srv = Server(tmp_data, free_port(), env_extra=slow_env)
    srv.wait_ready()
    try:
        c = srv.client()
        shard_count = _ingest_rows(c)
        assert shard_count >= 3
        r = c.post("/withdrawals", params={"subject_id": "s0"})
        assert r.status_code == 202
        task_id = r.json()["task_id"]

        # wait until the worker demonstrably entered purge processing, then
        # hard-exit the process without cleanup (os._exit ~ SIGKILL)
        deadline = time.time() + 8
        saw_processing = False
        while time.time() < deadline:
            st = c.get(f"/withdrawals/{task_id}").json()["status"]
            if st == "PROCESSING":
                saw_processing = True
                break
            time.sleep(0.05)
        assert saw_processing, "task never entered PROCESSING before kill"
        # at least one shard must still be unfinished when we crash
        live = c.get(f"/withdrawals/{task_id}").json()
        assert any(ws["state"] != "DONE" for ws in live["shards"]), live
        srv.kill()

        # restart on the same shared volume with a slow per-shard delay so the
        # resumed task is still in flight when we first query it; startup
        # recovery reclaims the task, then the worker finishes it automatically
        restarted = Server(
            tmp_data,
            free_port(),
            env_extra={
                "SHARD_TARGET_ROWS": "25",
                "PURGE_STEP_DELAY_SECONDS": "0.4",
                "PURGE_SWAP_DELAY_SECONDS": "0.4",
            },
        )
        restarted.wait_ready()
        try:
            with restarted.client() as c2:
                status = c2.get(f"/withdrawals/{task_id}").json()
                assert status["status"] in ("PENDING", "PROCESSING")
                # no affected shard serves data while purification is unfinished
                for s in c2.get("/shards").json()["shards"]:
                    if s["status"] != "AVAILABLE":
                        assert c2.get(
                            f"/shards/{s['shard_id']}/download"
                        ).status_code == 409

                final = wait_task(c2, task_id)
                assert final["status"] == "COMPLETED"

                remaining = []
                for s in c2.get("/shards").json()["shards"]:
                    assert s["status"] == "AVAILABLE"
                    remaining.extend(
                        read_csv_rows(
                            c2.get(f"/shards/{s['shard_id']}/download").content
                        )
                    )
                assert all(row[0] != "s0" for row in remaining)
                assert len(remaining) == 120 - 120 // 5
                assert list((tmp_data / "shards" / "staging").iterdir()) == []
        finally:
            restarted.stop()
    finally:
        if srv.proc.poll() is None:
            srv.stop()


def test_write_failure_keeps_shard_withheld_and_retries(tmp_data):
    """If the shared volume rejects writes, the shard stays stopped and,
    after the volume recovers, the background worker finishes the job."""
    srv = Server(tmp_data, free_port())
    srv.wait_ready()
    try:
        c = srv.client()
        _ingest_rows(c, n=60)
        r = c.post("/withdrawals", params={"subject_id": "s1"})
        task_id = r.json()["task_id"]
        wait_task(c, task_id)  # baseline: one purge works fine

        # second withdrawal; make the staging directory read-only first
        r = c.post("/withdrawals", params={"subject_id": "s2"})
        assert r.status_code == 202
        task2 = r.json()["task_id"]
        staging = tmp_data / "shards" / "staging"
        os.chmod(staging, stat.S_IRUSR | stat.S_IXUSR)
        try:
            deadline = time.time() + 6
            blocked = False
            while time.time() < deadline:
                shards = c.get("/shards").json()["shards"]
                if any(s["status"] != "AVAILABLE" for s in shards):
                    blocked = True
                    for s in shards:
                        if s["status"] != "AVAILABLE":
                            assert c.get(
                                f"/shards/{s['shard_id']}/download"
                            ).status_code == 409
                    break
                time.sleep(0.1)
            assert blocked, "expected the second withdrawal to withhold shards"
            time.sleep(1.0)
            # still not completed while writes fail
            assert c.get(f"/withdrawals/{task2}").json()["status"] in (
                "PENDING",
                "PROCESSING",
            )
        finally:
            os.chmod(staging, stat.S_IRWXU)

        final = wait_task(c, task2)
        assert final["status"] == "COMPLETED"
        remaining = []
        for s in c.get("/shards").json()["shards"]:
            assert s["status"] == "AVAILABLE"
            remaining.extend(
                read_csv_rows(c.get(f"/shards/{s['shard_id']}/download").content)
            )
        assert {row[0] for row in remaining}.isdisjoint({"s1", "s2"})
    finally:
        # make sure permissions are sane before teardown
        staging = tmp_data / "shards" / "staging"
        if staging.exists():
            os.chmod(staging, stat.S_IRWXU)
        srv.stop()


def test_restart_without_pending_work_is_clean(tmp_data):
    srv = Server(tmp_data, free_port())
    srv.wait_ready()
    try:
        c = srv.client()
        _ingest_rows(c, n=40)
        task_id = c.post(
            "/withdrawals", params={"subject_id": "s3"}
        ).json()["task_id"]
        wait_task(c, task_id)
    finally:
        srv.stop()

    srv2 = Server(tmp_data, free_port())
    srv2.wait_ready()
    try:
        with srv2.client() as c:
            assert c.get(f"/withdrawals/{task_id}").json()["status"] == "COMPLETED"
            shards = c.get("/shards").json()["shards"]
            rows = []
            for s in shards:
                assert s["status"] == "AVAILABLE"
                rows.extend(
                    read_csv_rows(c.get(f"/shards/{s['shard_id']}/download").content)
                )
            assert all(r[0] != "s3" for r in rows)
            assert c.get("/healthz").json()["ingested"] is True
    finally:
        srv2.stop()
