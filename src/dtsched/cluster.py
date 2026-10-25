"""Launch a local cluster: one coordinator plus N worker *processes*.

Each component is a separate OS process ("python -m dtsched ..."), exactly
as you would run them on separate machines, so killing one with SIGKILL is a
faithful crash test. Used by "dtsched run", the demo and the tests.
"""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from typing import IO


class LocalCluster:
    def __init__(
        self,
        db_path: str,
        workers: int = 3,
        *,
        heartbeat_interval: float = 0.5,
        lease_ttl: float = 3.0,
        worker_timeout: float = 2.0,
        coordinator_interval: float = 0.25,
        poll_interval: float = 0.1,
        log: IO[str] | None = None,
    ) -> None:
        self.db_path = db_path
        self.n_workers = workers
        self.heartbeat_interval = heartbeat_interval
        self.lease_ttl = lease_ttl
        self.worker_timeout = worker_timeout
        self.coordinator_interval = coordinator_interval
        self.poll_interval = poll_interval
        self.log = log
        self.coordinator: subprocess.Popen[bytes] | None = None
        self.workers: dict[str, subprocess.Popen[bytes]] = {}
        self._next = 1

    def _spawn(self, *args: str) -> subprocess.Popen[bytes]:
        cmd = [sys.executable, "-m", "dtsched", "--db", self.db_path, *args]
        out = self.log if self.log is not None else None
        return subprocess.Popen(cmd, stdout=out, stderr=out, env={**os.environ})

    def start(self) -> LocalCluster:
        self.coordinator = self._spawn(
            "coordinator",
            "--interval",
            str(self.coordinator_interval),
            "--worker-timeout",
            str(self.worker_timeout),
        )
        for _ in range(self.n_workers):
            self.add_worker()
        return self

    def add_worker(self) -> str:
        worker_id = f"worker-{self._next}"
        self._next += 1
        self.workers[worker_id] = self._spawn(
            "worker",
            "--id",
            worker_id,
            "--heartbeat",
            str(self.heartbeat_interval),
            "--lease-ttl",
            str(self.lease_ttl),
            "--poll",
            str(self.poll_interval),
        )
        return worker_id

    def kill_worker(self, worker_id: str) -> None:
        """SIGKILL a worker: no cleanup, no goodbye. Simulates a machine crash."""
        proc = self.workers[worker_id]
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)

    def stop(self, timeout: float = 10.0) -> None:
        """Gracefully stop everything (SIGTERM, then SIGKILL stragglers)."""
        procs = [p for p in [*self.workers.values(), self.coordinator] if p is not None]
        for p in procs:
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + timeout
        for p in procs:
            try:
                p.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()

    def __enter__(self) -> LocalCluster:
        return self.start()

    def __exit__(self, *exc: object) -> None:
        self.stop()
