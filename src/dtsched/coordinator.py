"""Coordinator: the cluster's housekeeping loop.

Workers pull work themselves, so the coordinator is not on the hot path of
every task. Its jobs are the ones nobody else will do:

* declare workers dead when their heartbeats stop, and recover their tasks;
* recover tasks whose lease expired (belt and braces for the above);
* turn due cron/interval schedules into tasks.

Every step is a single idempotent database transaction, so running two
coordinators is safe (just wasteful), and a crashed coordinator loses nothing:
restart it and it carries on from what is in the database.
"""

from __future__ import annotations

import logging
import signal
import threading

from .store import Store

log = logging.getLogger("dtsched.coordinator")


class Coordinator:
    def __init__(self, db_path: str, *, interval: float = 0.5, worker_timeout: float = 5.0):
        self.store = Store(db_path)
        self.interval = interval
        self.worker_timeout = worker_timeout
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: self.stop())

    def tick(self) -> dict[str, list]:
        reaped = self.store.reap(self.worker_timeout)
        for w in reaped.dead_workers:
            log.warning("worker %s missed heartbeats; declared dead", w)
        for t in reaped.retried:
            log.warning("task %d recovered from lost worker/lease; retry scheduled", t)
        for t in reaped.dead_lettered:
            log.warning("task %d recovered but out of attempts; dead-lettered", t)
        fired = self.store.fire_due_schedules()
        for t in fired:
            log.info("schedule fired: task %d", t)
        return {
            "dead_workers": reaped.dead_workers,
            "retried": reaped.retried,
            "dead_lettered": reaped.dead_lettered,
            "scheduled": fired,
        }

    def run(self) -> None:
        log.info("coordinator started (worker_timeout=%.1fs)", self.worker_timeout)
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:  # a DB hiccup must not kill the housekeeping loop
                log.exception("coordinator tick failed")
            self._stop.wait(self.interval)
        log.info("coordinator stopped")
