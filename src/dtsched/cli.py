"""Command-line interface: "dtsched <command>".

The database path comes from "--db", else "$DTSCHED_DB", else
"./dtsched.db". Every command is a separate process talking to the same
database file, so you can run workers in one terminal, the coordinator in
another and "dtsched status" in a third.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime
from typing import Any

from .api import make_server
from .cluster import LocalCluster
from .coordinator import Coordinator
from .store import STATES, Store, Task
from .worker import Worker


def _ts(t: float | None) -> str:
    return "-" if t is None else datetime.fromtimestamp(t).strftime("%H:%M:%S")


def _table(headers: list[str], rows: list[list[Any]]) -> str:
    cells = [headers] + [["-" if c is None else str(c) for c in r] for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]
    lines = ["  ".join(c.ljust(w) for c, w in zip(r, widths, strict=True)) for r in cells]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(line.rstrip() for line in lines)


def _task_rows(tasks: list[Task]) -> str:
    return _table(
        ["ID", "TASK", "PRI", "STATE", "TRIES", "WORKER", "LAST ERROR"],
        [
            [
                t.id,
                t.name,
                t.priority,
                t.state,
                f"{t.attempts}/{t.max_attempts}",
                t.worker_id,
                (t.last_error or "").splitlines()[0][:60] if t.last_error else None,
            ]
            for t in tasks
        ],
    )


def format_status(store: Store) -> str:
    s = store.stats()
    now = store.clock()
    t = s["tasks"]
    p50 = "-" if s["duration_p50"] is None else f"{s['duration_p50']:.2f}s"
    p95 = "-" if s["duration_p95"] is None else f"{s['duration_p95']:.2f}s"
    out = [
        f"Queue depth {s['queue_depth']} ready, {s['delayed']} delayed"
        f" ({s['awaiting_retry']} awaiting retry)",
        f"Tasks       {t['running']} running, {t['succeeded']} succeeded,"
        f" {t['dead_letter']} dead-lettered, {t['cancelled']} cancelled",
        f"Failures    {s['retries']} retries, {s['timeouts']} timeouts,"
        f" {s['workers_lost']} lost to dead workers, {s['stale_completions']} stale results"
        " discarded",
        f"Latency     p50 {p50}, p95 {p95}; throughput {s['throughput_per_min']:.1f}/min",
        "",
    ]
    workers = store.workers()
    if workers:
        out.append(
            _table(
                ["WORKER", "STATE", "PID", "HEARTBEAT", "TASK", "OK", "FAILED"],
                [
                    [
                        w["id"],
                        w["state"],
                        w["pid"],
                        f"{now - w['last_heartbeat']:.1f}s ago",
                        w["current_task_id"],
                        w["succeeded"],
                        w["failed"],
                    ]
                    for w in workers
                ],
            )
        )
    else:
        out.append("No workers have registered yet.")
    return "\n".join(out)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="dtsched", description="Distributed task scheduler.")
    p.add_argument("--db", default=os.environ.get("DTSCHED_DB", "dtsched.db"))
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("submit", help="queue a task")
    s.add_argument("name", help="import path, e.g. dtsched.examples:add")
    s.add_argument("args", nargs="*", help="positional args, each parsed as JSON if possible")
    s.add_argument("--kwargs", default="{}", help="JSON object of keyword args")
    s.add_argument("--priority", type=int, default=0, help="higher runs first")
    s.add_argument("--delay", type=float, default=0.0, help="seconds before it may run")
    s.add_argument("--max-attempts", type=int, default=3)
    s.add_argument("--timeout", type=float, default=30.0)
    s.add_argument("--backoff-base", type=float, default=1.0)
    s.add_argument("--backoff-max", type=float, default=60.0)
    s.add_argument("--idempotency-key")

    sc = sub.add_parser("schedule", help="manage recurring schedules")
    scs = sc.add_subparsers(dest="schedule_cmd", required=True)
    sa = scs.add_parser("add")
    sa.add_argument("schedule_name")
    sa.add_argument("name")
    sa.add_argument("args", nargs="*")
    sa.add_argument("--kwargs", default="{}")
    when = sa.add_mutually_exclusive_group(required=True)
    when.add_argument("--cron", help='five-field UTC cron expression, e.g. "*/5 * * * *"')
    when.add_argument("--every", type=float, help="interval in seconds")
    sa.add_argument("--priority", type=int, default=0)
    sa.add_argument("--max-attempts", type=int, default=3)
    sa.add_argument("--timeout", type=float, default=30.0)
    scs.add_parser("list")
    sr = scs.add_parser("remove")
    sr.add_argument("schedule_name")

    w = sub.add_parser("worker", help="run a worker process")
    w.add_argument("--id")
    w.add_argument("--heartbeat", type=float, default=1.0)
    w.add_argument("--lease-ttl", type=float, default=10.0)
    w.add_argument("--poll", type=float, default=0.2)

    c = sub.add_parser("coordinator", help="run the coordinator")
    c.add_argument("--interval", type=float, default=0.5)
    c.add_argument("--worker-timeout", type=float, default=5.0)

    r = sub.add_parser("run", help="coordinator + N workers + dashboard, in one command")
    r.add_argument("--workers", type=int, default=3)
    r.add_argument("--port", type=int, default=8080)

    sv = sub.add_parser("serve", help="HTTP API and dashboard")
    sv.add_argument("--host", default="127.0.0.1")
    sv.add_argument("--port", type=int, default=8080)

    st = sub.add_parser("status", help="queue depth, task counts and workers")
    st.add_argument("--json", action="store_true")
    st.add_argument("--watch", type=float, metavar="SECONDS", help="refresh every N seconds")

    ls = sub.add_parser("tasks", help="list recent tasks")
    ls.add_argument("--state", choices=STATES)
    ls.add_argument("--limit", type=int, default=20)

    sh = sub.add_parser("show", help="one task and its event history")
    sh.add_argument("task_id", type=int)

    cn = sub.add_parser("cancel", help="cancel a queued task")
    cn.add_argument("task_id", type=int)

    d = sub.add_parser("dlq", help="inspect or requeue the dead-letter queue")
    ds = d.add_subparsers(dest="dlq_cmd", required=True)
    ds.add_parser("list")
    dr = ds.add_parser("requeue")
    g = dr.add_mutually_exclusive_group(required=True)
    g.add_argument("task_id", type=int, nargs="?")
    g.add_argument("--all", action="store_true")
    return p


def _parse_arg(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s[%(process)d] %(levelname)s %(message)s",
    )
    cmd = args.command

    if cmd == "worker":
        worker = Worker(
            args.db,
            args.id,
            heartbeat_interval=args.heartbeat,
            lease_ttl=args.lease_ttl,
            poll_interval=args.poll,
        )
        worker.install_signal_handlers()
        worker.run()
        return 0
    if cmd == "coordinator":
        coord = Coordinator(args.db, interval=args.interval, worker_timeout=args.worker_timeout)
        coord.install_signal_handlers()
        coord.run()
        return 0
    if cmd == "serve":
        server = make_server(args.db, args.host, args.port)
        print(f"dashboard on http://{args.host}:{server.server_address[1]}/")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        return 0
    if cmd == "run":
        Store(args.db)  # create the schema before the children race to
        server = make_server(args.db, "127.0.0.1", args.port)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        cluster = LocalCluster(
            args.db, args.workers, heartbeat_interval=1.0, lease_ttl=10.0, worker_timeout=5.0
        )
        cluster.start()
        print(
            f"coordinator + {args.workers} workers running; dashboard on "
            f"http://127.0.0.1:{server.server_address[1]}/  (Ctrl-C to stop)"
        )
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            cluster.stop()
            server.shutdown()
        return 0

    store = Store(args.db)
    if cmd == "submit":
        res = store.submit(
            args.name,
            [_parse_arg(a) for a in args.args],
            json.loads(args.kwargs),
            priority=args.priority,
            delay=args.delay,
            max_attempts=args.max_attempts,
            timeout=args.timeout,
            backoff_base=args.backoff_base,
            backoff_max=args.backoff_max,
            idempotency_key=args.idempotency_key,
        )
        note = "" if res.created else " (already submitted with this idempotency key)"
        print(f"task {res.task_id}{note}")
    elif cmd == "schedule":
        if args.schedule_cmd == "add":
            sid = store.add_schedule(
                args.schedule_name,
                args.name,
                [_parse_arg(a) for a in args.args],
                json.loads(args.kwargs),
                cron=args.cron,
                interval=args.every,
                priority=args.priority,
                max_attempts=args.max_attempts,
                timeout=args.timeout,
            )
            print(f"schedule {sid} ({args.schedule_name})")
        elif args.schedule_cmd == "list":
            print(
                _table(
                    ["ID", "NAME", "TASK", "WHEN", "NEXT RUN"],
                    [
                        [
                            s["id"],
                            s["name"],
                            s["task_name"],
                            s["cron"] or f"every {s['interval']:g}s",
                            _ts(s["next_run_at"]),
                        ]
                        for s in store.schedules()
                    ],
                )
            )
        elif not store.remove_schedule(args.schedule_name):
            print(f"no schedule named {args.schedule_name!r}", file=sys.stderr)
            return 1
    elif cmd == "status":
        while True:
            if args.json:
                print(json.dumps({"stats": store.stats(), "workers": store.workers()}, indent=2))
            else:
                if args.watch:
                    print("\033[2J\033[H", end="")
                print(format_status(store))
            if not args.watch:
                break
            time.sleep(args.watch)
    elif cmd == "tasks":
        print(_task_rows(store.list_tasks(args.state, limit=args.limit)))
    elif cmd == "show":
        task = store.get_task(args.task_id)
        if task is None:
            print(f"no task {args.task_id}", file=sys.stderr)
            return 1
        print(json.dumps(task.to_dict(), indent=2, default=str))
        print()
        print(
            _table(
                ["TIME", "EVENT", "ATTEMPT", "WORKER", "DETAIL"],
                [
                    [
                        _ts(e["ts"]),
                        e["event"],
                        e["attempt"],
                        e["worker_id"],
                        (e["detail"] or "").splitlines()[0] if e["detail"] else None,
                    ]
                    for e in store.events(task.id)
                ],
            )
        )
    elif cmd == "cancel":
        if not store.cancel(args.task_id):
            print(f"task {args.task_id} is not queued", file=sys.stderr)
            return 1
        print(f"cancelled {args.task_id}")
    elif cmd == "dlq":
        if args.dlq_cmd == "list":
            print(_task_rows(store.dead_letters()))
        else:
            ids = [t.id for t in store.dead_letters(limit=10_000)] if args.all else [args.task_id]
            n = sum(store.requeue_dead_letter(i) for i in ids)
            print(f"requeued {n} task(s)")
    return 0
