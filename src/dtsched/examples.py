"""Example tasks used by the demo, the tests and the README.

A task is any importable function whose arguments and return value are
JSON-serialisable. Declare a ``ctx`` parameter to receive a
:class:`~dtsched.worker.TaskContext` with the task id and attempt number.
"""

from __future__ import annotations

import os
import sqlite3
import time

from .worker import TaskContext


def add(a: float, b: float) -> float:
    return a + b


def sleep(seconds: float) -> float:
    """Sleep, then return how long we slept. Useful for timeouts and kill tests."""
    time.sleep(seconds)
    return seconds


def record_pid_then_sleep(pid_file: str, seconds: float) -> float:
    """Write this process's pid to "pid_file" and sleep (for orphan tests)."""
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    time.sleep(seconds)
    return seconds


def flaky(succeed_on_attempt: int, ctx: TaskContext) -> str:
    """Fail deterministically until attempt "succeed_on_attempt"."""
    if ctx.attempt < succeed_on_attempt:
        raise ConnectionError(f"simulated transient failure on attempt {ctx.attempt}")
    return f"succeeded on attempt {ctx.attempt}"


def always_fails(message: str = "this task always fails") -> None:
    raise RuntimeError(message)


def crash() -> None:
    """Kill the task process abruptly, as a segfault or OOM kill would."""
    os._exit(13)


def charge(ledger_path: str, order_id: str, amount_cents: int, sleep_before_ack: float = 0.0):
    """An *idempotent* side effect: record a payment exactly once per order.

    The task may run more than once (at-least-once delivery), e.g. if the
    worker dies after writing to the ledger but before acknowledging. The
    "order_id" primary key turns the second write into a no-op, so the
    customer is charged once no matter how many times the task executes.
    """
    with sqlite3.connect(ledger_path, timeout=30) as db:
        db.execute(
            "CREATE TABLE IF NOT EXISTS ledger (order_id TEXT PRIMARY KEY, amount_cents INTEGER)"
        )
        db.execute("CREATE TABLE IF NOT EXISTS executions (order_id TEXT, pid INTEGER, ts REAL)")
        db.execute("INSERT INTO executions VALUES (?, ?, ?)", (order_id, os.getpid(), time.time()))
        cur = db.execute("INSERT OR IGNORE INTO ledger VALUES (?, ?)", (order_id, amount_cents))
        charged = cur.rowcount == 1
    time.sleep(sleep_before_ack)  # window in which a crash causes a re-run
    return {"order_id": order_id, "charged_now": charged}
