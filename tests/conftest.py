"""Shared pytest fixtures and CSV helpers."""
from __future__ import annotations

import io
import csv
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parent.parent
# Local interpreter needs the bootstrapped dependency directory.
PYLIBS = Path("/workspace/.pylibs")


def make_csv(rows: list[tuple[str, str, str]]) -> bytes:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["subject_id", "sample_id", "value"])
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def read_csv_rows(data: bytes) -> list[tuple[str, str, str]]:
    text = data.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    next(reader)  # header
    return [tuple(r) for r in reader if r]  # type: ignore[misc]


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class Server:
    """A uvicorn subprocess running against a temporary shared DATA_DIR."""

    def __init__(self, data_dir: Path, port: int, env_extra: dict | None = None):
        self.data_dir = data_dir
        self.port = port
        env = os.environ.copy()
        env["DATA_DIR"] = str(data_dir)
        env["INSTANCE_ID"] = f"test-{port}"
        env["PYTHONPATH"] = (
            str(ROOT) + (os.pathsep + str(PYLIBS) if PYLIBS.exists() else "")
        )
        env["WORKER_INTERVAL_SECONDS"] = "0.05"
        # Purge crash-injection delays default to off unless a test opts in.
        env["PURGE_STEP_DELAY_SECONDS"] = "0"
        env["PURGE_SWAP_DELAY_SECONDS"] = "0"
        # Test-only self-termination endpoints: this sandbox blocks POSIX
        # signals against child processes, so servers must stop themselves.
        env["ENABLE_TEST_ADMIN"] = "1"
        if env_extra:
            env.update(env_extra)
        self.proc = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                str(port),
            ],
            cwd=str(ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

    @property
    def base(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def client(self) -> httpx.Client:
        return httpx.Client(base_url=self.base, timeout=10)

    def wait_ready(self, timeout: float = 20) -> None:
        deadline = time.time() + timeout
        last_exc: Exception | None = None
        while time.time() < deadline:
            if self.proc.poll() is not None:
                out = self.proc.stdout.read().decode() if self.proc.stdout else ""
                raise RuntimeError(f"server exited early:\n{out}")
            try:
                r = httpx.get(f"{self.base}/healthz", timeout=1)
                if r.status_code == 200:
                    return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
            time.sleep(0.15)
        raise RuntimeError(f"server not ready: {last_exc}")

    def stop(self) -> str:
        if self.proc.poll() is None:
            try:
                httpx.post(f"{self.base}/test-admin/stop", timeout=2)
            except Exception:
                pass
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                # Last resort in environments where signals are allowed.
                try:
                    self.proc.terminate()
                    self.proc.wait(timeout=5)
                except (PermissionError, ProcessLookupError):
                    pass
        out = self.proc.stdout.read().decode() if self.proc.stdout else ""
        return out

    def kill(self) -> None:
        """Hard self-exit (os._exit, no cleanup) to simulate a process crash."""
        if self.proc.poll() is None:
            try:
                httpx.post(f"{self.base}/test-admin/hard-exit", timeout=2)
            except Exception:
                pass
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    self.proc.kill()
                    self.proc.wait(timeout=10)
                except (PermissionError, ProcessLookupError):
                    pass


@pytest.fixture()
def tmp_data(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    return data_dir


@pytest.fixture()
def server(tmp_data: Path) -> Server:
    srv = Server(tmp_data, free_port())
    srv.wait_ready()
    try:
        yield srv
    finally:
        srv.stop()


def wait_task(client: httpx.Client, task_id: str, timeout: float = 20) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get(f"/withdrawals/{task_id}")
        assert r.status_code == 200
        body = r.json()
        if body["status"] == "COMPLETED":
            return body
        if body["status"] == "FAILED":
            raise AssertionError(f"task failed: {body}")
        time.sleep(0.1)
    raise AssertionError("task did not complete in time")
