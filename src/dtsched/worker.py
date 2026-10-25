"""Worker process: claim tasks, run them in a child process, report the outcome.

Each task runs in its own child process (started with the "spawn" method)
so that a task which hangs past its timeout can actually be killed. Python
threads can't be killed from the outside, so "run it in a thread and stop
waiting" would leave the hung task running forever.

While a task runs, a background thread sends heartbeats. Each heartbeat
extends the leases the worker holds; if the coordinator has meanwhile
declared this worker dead and handed the task to someone else, the heartbeat
reveals that and the worker kills its now-orphaned copy of the task.
"""

from __future__ import annotations

import importlib
import inspect
import json
import logging
import multiprocessing as mp
import os
import signal
import socket
import threading
import time
import traceback
import uuid
from dataclasses import dataclass
from multiprocessing.connection import Connection
from typing import Any

from .store import Store, Task

log = logging.getLogger("dtsched.worker")


@dataclass(frozen=True)
class TaskContext:
    """Passed to task functions that declare a ctx parameter."""

    task_id: int
    attempt: int
    worker_id: str


@dataclass(frozen=True)
class Outcome:
    ok: bool
    result: Any = None
    error: str | None = None
    timed_out: bool = False
    lease_lost: bool = False


def resolve(name: str) -> Any:
    """Import "package.module:function" and return the function."""
    module_name, _, attr = name.partition(":")
    obj: Any = importlib.import_module(module_name)
    for part in attr.split("."):
        obj = getattr(obj, part)
    if not callable(obj):
        raise TypeError(f"{name} is not callable")
    return obj


def _watch_parent(parent_pid: int) -> None:
    # If the worker is SIGKILLed nobody will reap this child, and on macOS
    # there is no PR_SET_PDEATHSIG. Poll instead and exit when re-parented.
    while True:
        if os.getppid() != parent_pid:
            os._exit(1)
        time.sleep(0.2)


def _child_main(conn: Connection, name: str, args: list, kwargs: dict, ctx: TaskContext) -> None:
    threading.Thread(target=_watch_parent, args=(os.getppid(),), daemon=True).start()
    try:
        fn = resolve(name)
        if "ctx" in inspect.signature(fn).parameters:
            kwargs = {**kwargs, "ctx": ctx}
        result = fn(*args, **kwargs)
        conn.send(("ok", json.dumps(result)))
    except BaseException as exc:  # noqa: BLE001 - report everything, incl. SystemExit
        tb = traceback.format_exception(type(exc), exc, exc.__traceback__)
        conn.send(("error", f"{type(exc).__name__}: {exc}\n{''.join(tb[-3:])}".strip()))
    finally:
        conn.close()


class Worker:
    """Pulls tasks from the store until stopped.

    Parameters
    db_path:
        SQLite database shared with the coordinator and other workers.
    poll_interval:
        Sleep between claim attempts when the queue is empty.
    heartbeat_interval:
        How often to heartbeat. Must be comfortably below the coordinator's
        worker_timeout and below lease_ttl.
    lease_ttl:
        How long a claim stays valid without a heartbeat renewing it.
    """

    def __init__(
        self,
        db_path: str,
        worker_id: str | None = None,
        *,
        poll_interval: float = 0.2,
        heartbeat_interval: float = 1.0,
        lease_ttl: float = 10.0,
    ) -> None:
        if heartbeat_interval >= lease_ttl:
            raise ValueError("heartbeat_interval must be smaller than lease_ttl")
        self.db_path = db_path
        self.id = worker_id or f"{socket.gethostname().split('.')[0]}-{uuid.uuid4().hex[:6]}"
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.lease_ttl = lease_ttl
        self.store = Store(db_path)
        self._stop = threading.Event()
        # Latest (sequence number, task ids we hold) reported by a heartbeat.
        self._hb_seq = 0
        self._held: tuple[int, set[int]] | None = None
        self._mp = mp.get_context("spawn")

    # lifecycle

    def stop(self) -> None:
        """Ask the worker to exit after the task it is running (graceful)."""
        self._stop.set()

    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, lambda *_: self.stop())

    def run(self, max_tasks: int | None = None) -> int:
        """Run until :meth: stop is called (or max_tasks are done)."""
        self.store.register_worker(self.id, socket.gethostname(), os.getpid())
        hb = threading.Thread(target=self._heartbeat_loop, name="heartbeat", daemon=True)
        hb.start()
        log.info("worker %s started (pid %d)", self.id, os.getpid())
        done = 0
        try:
            while not self._stop.is_set():
                if self.run_once():
                    done += 1
                    if max_tasks is not None and done >= max_tasks:
                        break
                else:
                    self._stop.wait(self.poll_interval)
        finally:
            self._stop.set()
            hb.join(timeout=2)
            self.store.stop_worker(self.id)
            log.info("worker %s stopped after %d task(s)", self.id, done)
        return done

    def _heartbeat_loop(self) -> None:
        store = Store(self.db_path)  # own connection for this thread
        while not self._stop.wait(self.heartbeat_interval):
            seq = self._hb_seq + 1
            self._hb_seq = seq
            try:
                self._held = (seq, store.heartbeat(self.id, self.lease_ttl))
            except Exception:  # keep heartbeating through transient DB errors
                log.exception("heartbeat failed")
        store.close()

    # -- one task ---------------------------------------------------------

    def run_once(self) -> bool:
        """Claim and run one task. Returns False if nothing was ready."""
        task = self.store.claim(self.id, self.lease_ttl)
        if task is None:
            return False
        log.info("claimed task %d %s (attempt %d)", task.id, task.name, task.attempts)
        outcome = self.execute(task)
        assert task.lease_token is not None
        if outcome.ok:
            if not self.store.complete(task.id, task.lease_token, outcome.result):
                log.warning("task %d finished but lease was lost; result discarded", task.id)
        elif outcome.lease_lost:
            log.warning("task %d abandoned: lease was reassigned", task.id)
        else:
            event = "timeout" if outcome.timed_out else "failed"
            verdict = self.store.fail(task.id, task.lease_token, outcome.error or "", event=event)
            log.info("task %d %s -> %s", task.id, event, verdict)
        return True

    def execute(self, task: Task) -> Outcome:
        ctx = TaskContext(task.id, task.attempts, self.id)
        recv, send = self._mp.Pipe(duplex=False)
        proc = self._mp.Process(
            target=_child_main,
            args=(send, task.name, task.args, task.kwargs, ctx),
            name=f"task-{task.id}",
            daemon=True,
        )
        # Only heartbeats that started after the claim committed know about it.
        claimed_seq = self._hb_seq
        proc.start()
        send.close()  # so recv sees EOF if the child dies without answering
        deadline = time.monotonic() + task.timeout
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    error = f"timed out after {task.timeout:g}s"
                    return Outcome(False, error=error, timed_out=True)
                held = self._held
                if held is not None and held[0] > claimed_seq and task.id not in held[1]:
                    return Outcome(False, error="lease lost", lease_lost=True)
                if recv.poll(min(remaining, 0.1)):
                    try:
                        status, payload = recv.recv()
                    except EOFError:
                        proc.join(timeout=1)
                        return Outcome(False, error=f"task process died (exit {proc.exitcode})")
                    if status == "ok":
                        return Outcome(True, result=json.loads(payload))
                    return Outcome(False, error=payload)
        finally:
            recv.close()
            if proc.is_alive():
                proc.kill()
            proc.join(timeout=5)
