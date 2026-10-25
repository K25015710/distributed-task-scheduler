"""SQLite-backed task store: the single source of truth for the cluster.

Every state transition (submit, claim, heartbeat, complete, fail, reap) is a
short "BEGIN IMMEDIATE" transaction. "IMMEDIATE" takes SQLite's write lock
up front, so two workers racing to claim the same task are serialised by the
database and exactly one of them wins. WAL mode lets readers (the dashboard,
the CLI) keep reading while a writer holds that lock.

Task life cycle::
    queued -> claim -> running -> complete -> succeeded
    fail / timeout / lease expired / worker died
    backoff -> attempts < max_attempts ?  -> no -> dead_letter
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from .backoff import backoff_delay
from .cron import CronSchedule

QUEUED = "queued"
RUNNING = "running"
SUCCEEDED = "succeeded"
DEAD_LETTER = "dead_letter"
CANCELLED = "cancelled"
STATES = (QUEUED, RUNNING, SUCCEEDED, DEAD_LETTER, CANCELLED)

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT    NOT NULL,
    args             TEXT    NOT NULL DEFAULT '[]',
    kwargs           TEXT    NOT NULL DEFAULT '{}',
    priority         INTEGER NOT NULL DEFAULT 0,
    state            TEXT    NOT NULL DEFAULT 'queued',
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL DEFAULT 3,
    timeout          REAL    NOT NULL DEFAULT 30,
    backoff_base     REAL    NOT NULL DEFAULT 1,
    backoff_max      REAL    NOT NULL DEFAULT 60,
    run_at           REAL    NOT NULL,
    created_at       REAL    NOT NULL,
    started_at       REAL,
    finished_at      REAL,
    worker_id        TEXT,
    lease_token      TEXT,
    lease_expires_at REAL,
    result           TEXT,
    last_error       TEXT,
    idempotency_key  TEXT UNIQUE,
    schedule_id      INTEGER
);
-- The claim query: highest priority first, then oldest due time.
CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks (state, priority DESC, run_at, id);
CREATE INDEX IF NOT EXISTS idx_tasks_worker ON tasks (worker_id, state);

CREATE TABLE IF NOT EXISTS task_events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id   INTEGER NOT NULL,
    ts        REAL    NOT NULL,
    event     TEXT    NOT NULL,
    worker_id TEXT,
    attempt   INTEGER,
    detail    TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_task ON task_events (task_id, id);

CREATE TABLE IF NOT EXISTS workers (
    id              TEXT PRIMARY KEY,
    hostname        TEXT,
    pid             INTEGER,
    state           TEXT NOT NULL,
    started_at      REAL NOT NULL,
    last_heartbeat  REAL NOT NULL,
    current_task_id INTEGER,
    succeeded       INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS schedules (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT    NOT NULL UNIQUE,
    task_name    TEXT    NOT NULL,
    args         TEXT    NOT NULL DEFAULT '[]',
    kwargs       TEXT    NOT NULL DEFAULT '{}',
    cron         TEXT,
    interval     REAL,
    priority     INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    timeout      REAL    NOT NULL DEFAULT 30,
    next_run_at  REAL    NOT NULL,
    enabled      INTEGER NOT NULL DEFAULT 1
);
"""


@dataclass(frozen=True)
class Task:
    id: int
    name: str
    args: list[Any]
    kwargs: dict[str, Any]
    priority: int
    state: str
    attempts: int
    max_attempts: int
    timeout: float
    backoff_base: float
    backoff_max: float
    run_at: float
    created_at: float
    started_at: float | None
    finished_at: float | None
    worker_id: str | None
    lease_token: str | None
    lease_expires_at: float | None
    result: Any
    last_error: str | None
    idempotency_key: str | None
    schedule_id: int | None

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> Task:
        data = dict(row)
        data["args"] = json.loads(data["args"])
        data["kwargs"] = json.loads(data["kwargs"])
        data["result"] = None if data["result"] is None else json.loads(data["result"])
        return cls(**data)

    def to_dict(self) -> dict[str, Any]:
        d = dict(self.__dict__)
        d.pop("lease_token")  # a capability, not something to publish
        return d


@dataclass(frozen=True)
class SubmitResult:
    task_id: int
    created: bool  # False when an idempotency key matched an existing task


@dataclass(frozen=True)
class ReapResult:
    dead_workers: list[str]
    retried: list[int]
    dead_lettered: list[int]


class Store:
    """Thread-safe handle on the SQLite database at "path".

    Each thread gets its own connection. "clock" is injectable so tests can
    move time forward without sleeping.
    """

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        clock: Callable[[], float] = time.time,
        rand: Callable[[], float] | None = None,
    ) -> None:
        self.path = os.fspath(path)
        self.clock = clock
        self._rand = rand
        self._local = threading.local()
        self.init_schema()

    # -- plumbing ---------------------------------------------------------

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        else:
            conn.execute("COMMIT")

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def init_schema(self) -> None:
        self._conn().executescript(SCHEMA)

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        task_id: int,
        now: float,
        event: str,
        *,
        worker_id: str | None = None,
        attempt: int | None = None,
        detail: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO task_events (task_id, ts, event, worker_id, attempt, detail)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (task_id, now, event, worker_id, attempt, detail),
        )

    # -- submission -------------------------------------------------------

    def submit(
        self,
        name: str,
        args: list[Any] | tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        priority: int = 0,
        delay: float = 0.0,
        run_at: float | None = None,
        max_attempts: int = 3,
        timeout: float = 30.0,
        backoff_base: float = 1.0,
        backoff_max: float = 60.0,
        idempotency_key: str | None = None,
        schedule_id: int | None = None,
    ) -> SubmitResult:
        """Queue a task. "name" is an import path like "package.module:function".

        If "idempotency_key" was used before, nothing new is queued and the
        existing task id is returned, so a client can safely retry a submit
        whose response it never received.
        """
        if ":" not in name:
            raise ValueError(f"task name must look like 'module:function', got {name!r}")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if timeout <= 0:
            raise ValueError("timeout must be > 0")
        args_json = json.dumps(list(args))
        kwargs_json = json.dumps(kwargs or {})
        now = self.clock()
        due = run_at if run_at is not None else now + delay
        with self._tx() as conn:
            if idempotency_key is not None:
                row = conn.execute(
                    "SELECT id FROM tasks WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if row is not None:
                    return SubmitResult(row["id"], created=False)
            cur = conn.execute(
                "INSERT INTO tasks (name, args, kwargs, priority, max_attempts, timeout,"
                " backoff_base, backoff_max, run_at, created_at, idempotency_key, schedule_id)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    name,
                    args_json,
                    kwargs_json,
                    priority,
                    max_attempts,
                    timeout,
                    backoff_base,
                    backoff_max,
                    due,
                    now,
                    idempotency_key,
                    schedule_id,
                ),
            )
            task_id = int(cur.lastrowid)  # type: ignore[arg-type]
            detail = f"priority={priority}" + (f" run_at=+{due - now:.1f}s" if due > now else "")
            self._event(conn, task_id, now, "submitted", detail=detail)
        return SubmitResult(task_id, created=True)

    def cancel(self, task_id: int) -> bool:
        """Cancel a task that has not started yet."""
        now = self.clock()
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE tasks SET state = ?, finished_at = ? WHERE id = ? AND state = ?",
                (CANCELLED, now, task_id, QUEUED),
            )
            if cur.rowcount:
                self._event(conn, task_id, now, "cancelled")
            return cur.rowcount == 1

    # -- workers ----------------------------------------------------------

    def register_worker(self, worker_id: str, hostname: str = "", pid: int = 0) -> None:
        now = self.clock()
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO workers (id, hostname, pid, state, started_at, last_heartbeat)"
                " VALUES (?, ?, ?, 'alive', ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET state = 'alive', pid = excluded.pid,"
                " hostname = excluded.hostname, last_heartbeat = excluded.last_heartbeat",
                (worker_id, hostname, pid, now, now),
            )

    def heartbeat(self, worker_id: str, lease_ttl: float) -> set[int]:
        """Record that "worker_id" is alive and extend the leases it holds.

        Returns the ids of tasks the worker still legitimately holds. If a task
        the worker thinks it is running is missing, its lease was taken away
        (the coordinator decided the worker was dead) and the worker should
        abandon it.
        """
        now = self.clock()
        with self._tx() as conn:
            conn.execute(
                "UPDATE workers SET last_heartbeat = ?, state = 'alive' WHERE id = ?",
                (now, worker_id),
            )
            conn.execute(
                "UPDATE tasks SET lease_expires_at = ? WHERE worker_id = ? AND state = ?",
                (now + lease_ttl, worker_id, RUNNING),
            )
            rows = conn.execute(
                "SELECT id FROM tasks WHERE worker_id = ? AND state = ?", (worker_id, RUNNING)
            ).fetchall()
        return {r["id"] for r in rows}

    def stop_worker(self, worker_id: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE workers SET state = 'stopped', current_task_id = NULL WHERE id = ?",
                (worker_id,),
            )

    def workers(self) -> list[dict[str, Any]]:
        rows = self._conn().execute("SELECT * FROM workers ORDER BY started_at, id").fetchall()
        return [dict(r) for r in rows]

    # -- the claim / complete / fail cycle -------------------------------

    def claim(self, worker_id: str, lease_ttl: float) -> Task | None:
        """Atomically take the highest-priority due task, or return "None".

        The claim increments "attempts" and issues a fresh random
        "lease_token". Only the holder of the current token may complete or
        fail the task; this "fencing token" stops a worker that was presumed
        dead (but was only slow) from overwriting the outcome of the retry.
        """
        now = self.clock()
        token = uuid.uuid4().hex
        with self._tx() as conn:
            row = conn.execute(
                "SELECT id FROM tasks WHERE state = ? AND run_at <= ?"
                " ORDER BY priority DESC, run_at, id LIMIT 1",
                (QUEUED, now),
            ).fetchone()
            if row is None:
                return None
            task_id = row["id"]
            conn.execute(
                "UPDATE tasks SET state = ?, attempts = attempts + 1, worker_id = ?,"
                " lease_token = ?, lease_expires_at = ?, started_at = ? WHERE id = ?",
                (RUNNING, worker_id, token, now + lease_ttl, now, task_id),
            )
            conn.execute(
                "UPDATE workers SET current_task_id = ? WHERE id = ?", (task_id, worker_id)
            )
            task = Task.from_row(
                conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
            )
            self._event(conn, task_id, now, "claimed", worker_id=worker_id, attempt=task.attempts)
        return task

    def complete(self, task_id: int, lease_token: str, result: Any = None) -> bool:
        """Mark a task succeeded. Returns False if the lease is no longer ours."""
        now = self.clock()
        result_json = json.dumps(result)
        with self._tx() as conn:
            row = self._owned(conn, task_id, lease_token)
            if row is None:
                self._event(conn, task_id, now, "stale_completion", detail="lease lost")
                return False
            conn.execute(
                "UPDATE tasks SET state = ?, result = ?, finished_at = ?, lease_token = NULL,"
                " lease_expires_at = NULL WHERE id = ?",
                (SUCCEEDED, result_json, now, task_id),
            )
            conn.execute(
                "UPDATE workers SET succeeded = succeeded + 1, current_task_id = NULL WHERE id = ?",
                (row["worker_id"],),
            )
            self._event(
                conn,
                task_id,
                now,
                "succeeded",
                worker_id=row["worker_id"],
                attempt=row["attempts"],
                detail=f"{now - row['started_at']:.2f}s",
            )
        return True

    def fail(self, task_id: int, lease_token: str, error: str, *, event: str = "failed") -> str:
        """Record a failed attempt by the lease holder.

        Returns "retry" (re-queued with backoff), "dead_letter" (out of
        attempts) or "stale" (the lease was already taken away).
        """
        now = self.clock()
        with self._tx() as conn:
            row = self._owned(conn, task_id, lease_token)
            if row is None:
                self._event(conn, task_id, now, "stale_failure", detail="lease lost")
                return "stale"
            conn.execute(
                "UPDATE workers SET failed = failed + 1, current_task_id = NULL WHERE id = ?",
                (row["worker_id"],),
            )
            return self._fail_locked(conn, row, now, error, event)

    @staticmethod
    def _owned(conn: sqlite3.Connection, task_id: int, lease_token: str) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM tasks WHERE id = ? AND state = ? AND lease_token = ?",
            (task_id, RUNNING, lease_token),
        ).fetchone()

    def _fail_locked(
        self, conn: sqlite3.Connection, row: sqlite3.Row, now: float, error: str, event: str
    ) -> str:
        """Retry-or-dead-letter decision. Caller holds the write transaction."""
        task_id, attempt = row["id"], row["attempts"]
        self._event(
            conn, task_id, now, event, worker_id=row["worker_id"], attempt=attempt, detail=error
        )
        if attempt >= row["max_attempts"]:
            conn.execute(
                "UPDATE tasks SET state = ?, last_error = ?, finished_at = ?, worker_id = NULL,"
                " lease_token = NULL, lease_expires_at = NULL WHERE id = ?",
                (DEAD_LETTER, error, now, task_id),
            )
            self._event(
                conn,
                task_id,
                now,
                "dead_lettered",
                attempt=attempt,
                detail=f"gave up after {attempt} attempt(s)",
            )
            return "dead_letter"
        kwargs = {"rand": self._rand} if self._rand is not None else {}
        delay = backoff_delay(attempt, row["backoff_base"], row["backoff_max"], **kwargs)
        conn.execute(
            "UPDATE tasks SET state = ?, last_error = ?, run_at = ?, worker_id = NULL,"
            " lease_token = NULL, lease_expires_at = NULL WHERE id = ?",
            (QUEUED, error, now + delay, task_id),
        )
        self._event(
            conn,
            task_id,
            now,
            "retry_scheduled",
            attempt=attempt,
            detail=f"next attempt in {delay:.2f}s",
        )
        return "retry"

    # failure detection

    def reap(self, worker_timeout: float) -> ReapResult:
        """Detect dead workers and expired leases and recover their tasks.

        A worker whose last heartbeat is older than ``worker_timeout`` is
        declared dead and every task it was running counts as a failed
        attempt. Separately, any running task whose lease has expired is
        recovered the same way. Safe to call from several processes at once.
        """
        now = self.clock()
        dead_workers: list[str] = []
        retried: list[int] = []
        dead_lettered: list[int] = []
        with self._tx() as conn:
            for w in conn.execute(
                "SELECT id FROM workers WHERE state = 'alive' AND last_heartbeat < ?",
                (now - worker_timeout,),
            ).fetchall():
                dead_workers.append(w["id"])
                conn.execute(
                    "UPDATE workers SET state = 'dead', current_task_id = NULL WHERE id = ?",
                    (w["id"],),
                )
                for row in conn.execute(
                    "SELECT * FROM tasks WHERE worker_id = ? AND state = ?", (w["id"], RUNNING)
                ).fetchall():
                    outcome = self._fail_locked(
                        conn, row, now, f"worker {w['id']} stopped heartbeating", "worker_lost"
                    )
                    (retried if outcome == "retry" else dead_lettered).append(row["id"])
            for row in conn.execute(
                "SELECT * FROM tasks WHERE state = ? AND lease_expires_at < ?", (RUNNING, now)
            ).fetchall():
                outcome = self._fail_locked(conn, row, now, "lease expired", "lease_expired")
                (retried if outcome == "retry" else dead_lettered).append(row["id"])
        return ReapResult(dead_workers, retried, dead_lettered)

    # dead-letter queue

    def dead_letters(self, limit: int = 100) -> list[Task]:
        return self.list_tasks(DEAD_LETTER, limit=limit)

    def requeue_dead_letter(self, task_id: int) -> bool:
        """Give a dead-lettered task a fresh set of attempts (e.g. after a fix)."""
        now = self.clock()
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE tasks SET state = ?, attempts = 0, run_at = ?, finished_at = NULL"
                " WHERE id = ? AND state = ?",
                (QUEUED, now, task_id, DEAD_LETTER),
            )
            if cur.rowcount:
                self._event(conn, task_id, now, "requeued", detail="from dead-letter queue")
            return cur.rowcount == 1

    # recurring schedules

    def add_schedule(
        self,
        name: str,
        task_name: str,
        args: list[Any] | tuple[Any, ...] = (),
        kwargs: dict[str, Any] | None = None,
        *,
        cron: str | None = None,
        interval: float | None = None,
        priority: int = 0,
        max_attempts: int = 3,
        timeout: float = 30.0,
    ) -> int:
        """Create or replace a recurring schedule (exactly one of cron/interval)."""
        if (cron is None) == (interval is None):
            raise ValueError("give exactly one of cron= or interval=")
        if interval is not None and interval <= 0:
            raise ValueError("interval must be > 0")
        now = self.clock()
        first = CronSchedule.parse(cron).next_after(now) if cron else now + interval  # type: ignore[operator]
        with self._tx() as conn:
            conn.execute(
                "INSERT INTO schedules (name, task_name, args, kwargs, cron, interval, priority,"
                " max_attempts, timeout, next_run_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(name) DO UPDATE SET task_name = excluded.task_name,"
                " args = excluded.args, kwargs = excluded.kwargs, cron = excluded.cron,"
                " interval = excluded.interval, priority = excluded.priority,"
                " max_attempts = excluded.max_attempts, timeout = excluded.timeout,"
                " next_run_at = excluded.next_run_at, enabled = 1",
                (
                    name,
                    task_name,
                    json.dumps(list(args)),
                    json.dumps(kwargs or {}),
                    cron,
                    interval,
                    priority,
                    max_attempts,
                    timeout,
                    first,
                ),
            )
            return int(
                conn.execute("SELECT id FROM schedules WHERE name = ?", (name,)).fetchone()["id"]
            )

    def remove_schedule(self, name: str) -> bool:
        with self._tx() as conn:
            return conn.execute("DELETE FROM schedules WHERE name = ?", (name,)).rowcount == 1

    def schedules(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self._conn().execute("SELECT * FROM schedules ORDER BY id")]

    def fire_due_schedules(self) -> list[int]:
        """Turn every due schedule into a task and advance its next run time.

        Each firing gets the idempotency key "schedule:<id>:<due time>", so if
        two coordinators run at once the same occurrence is queued only once.
        Missed occurrences (coordinator was down) are collapsed into one run.
        """
        now = self.clock()
        due = (
            self._conn()
            .execute("SELECT * FROM schedules WHERE enabled = 1 AND next_run_at <= ?", (now,))
            .fetchall()
        )
        created: list[int] = []
        for s in due:
            res = self.submit(
                s["task_name"],
                json.loads(s["args"]),
                json.loads(s["kwargs"]),
                priority=s["priority"],
                max_attempts=s["max_attempts"],
                timeout=s["timeout"],
                idempotency_key=f"schedule:{s['id']}:{s['next_run_at']:.3f}",
                schedule_id=s["id"],
            )
            if res.created:
                created.append(res.task_id)
            if s["cron"]:
                nxt = CronSchedule.parse(s["cron"]).next_after(now)
            else:
                nxt = s["next_run_at"] + s["interval"]
                if nxt <= now:  # skip occurrences missed while we were down
                    nxt = now + s["interval"]
            with self._tx() as conn:
                conn.execute(
                    "UPDATE schedules SET next_run_at = ? WHERE id = ? AND next_run_at = ?",
                    (nxt, s["id"], s["next_run_at"]),
                )
        return created

    # queries
    def get_task(self, task_id: int) -> Task | None:
        row = self._conn().execute("SELECT * FROM tasks WHERE id = ?", (task_id,)).fetchone()
        return None if row is None else Task.from_row(row)

    def list_tasks(self, state: str | None = None, *, limit: int = 50) -> list[Task]:
        if state is not None and state not in STATES:
            raise ValueError(f"unknown state {state!r}; expected one of {STATES}")
        if state is None:
            rows = (
                self._conn()
                .execute("SELECT * FROM tasks ORDER BY id DESC LIMIT ?", (limit,))
                .fetchall()
            )
        else:
            rows = (
                self._conn()
                .execute(
                    "SELECT * FROM tasks WHERE state = ? ORDER BY id DESC LIMIT ?", (state, limit)
                )
                .fetchall()
            )
        return [Task.from_row(r) for r in rows]

    def events(self, task_id: int | None = None, *, limit: int = 1000) -> list[dict[str, Any]]:
        if task_id is None:
            rows = (
                self._conn()
                .execute("SELECT * FROM task_events ORDER BY id LIMIT ?", (limit,))
                .fetchall()
            )
        else:
            rows = (
                self._conn()
                .execute(
                    "SELECT * FROM task_events WHERE task_id = ? ORDER BY id LIMIT ?",
                    (task_id, limit),
                )
                .fetchall()
            )
        return [dict(r) for r in rows]

    def stats(self, *, window: float = 60.0) -> dict[str, Any]:
        """Point-in-time metrics for the dashboard, CLI and "/metrics"."""
        conn = self._conn()
        now = self.clock()
        counts = dict.fromkeys(STATES, 0)
        for r in conn.execute("SELECT state, COUNT(*) AS n FROM tasks GROUP BY state"):
            counts[r["state"]] = r["n"]
        ready, delayed, retrying = conn.execute(
            "SELECT COALESCE(SUM(run_at <= ?), 0), COALESCE(SUM(run_at > ?), 0),"
            " COALESCE(SUM(attempts > 0), 0) FROM tasks WHERE state = ?",
            (now, now, QUEUED),
        ).fetchone()
        workers = dict.fromkeys(("alive", "dead", "stopped"), 0)
        for r in conn.execute("SELECT state, COUNT(*) AS n FROM workers GROUP BY state"):
            workers[r["state"]] = r["n"]
        events = {
            r["event"]: r["n"]
            for r in conn.execute("SELECT event, COUNT(*) AS n FROM task_events GROUP BY event")
        }
        durations = sorted(
            r[0]
            for r in conn.execute(
                "SELECT finished_at - started_at FROM tasks WHERE state = ?"
                " ORDER BY finished_at DESC LIMIT 1000",
                (SUCCEEDED,),
            )
        )
        recent = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE state = ? AND finished_at >= ?",
            (SUCCEEDED, now - window),
        ).fetchone()[0]

        def pct(p: float) -> float | None:
            if not durations:
                return None
            return durations[min(len(durations) - 1, int(p * len(durations)))]

        return {
            "tasks": counts,
            "queue_depth": ready,
            "delayed": delayed,
            "awaiting_retry": retrying,
            "workers": workers,
            "retries": events.get("retry_scheduled", 0),
            "timeouts": events.get("timeout", 0),
            "workers_lost": events.get("worker_lost", 0),
            "leases_expired": events.get("lease_expired", 0),
            "stale_completions": events.get("stale_completion", 0) + events.get("stale_failure", 0),
            "throughput_per_min": recent * 60.0 / window,
            "duration_p50": pct(0.5),
            "duration_p95": pct(0.95),
        }
