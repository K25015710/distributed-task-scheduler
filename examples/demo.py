"""End-to-end demo: a 3-worker cluster surviving failures.

Run with:  python examples/demo.py   (add --serve to keep the dashboard up)

It starts a coordinator and three worker processes, submits a mix of tasks,
then injects failures while they run:

* a task that fails twice with a transient error, then succeeds (retries + backoff)
* a task that always raises (ends in the dead-letter queue)
* a task that hangs (killed by its timeout, then dead-lettered)
* a payment task whose worker is SIGKILLed mid-flight (reassigned to another
  worker; idempotency means the customer is still charged exactly once)
* a delayed task, a duplicate submission, and a recurring interval schedule

and finally prints the event timeline, the cluster status and the HTTP API's view.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

from dtsched import Store
from dtsched.api import make_server
from dtsched.cli import format_status
from dtsched.cluster import LocalCluster

TERMINAL = {"succeeded", "dead_letter", "cancelled"}


def wait_until(pred, timeout=60.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := pred():
            return result
        time.sleep(0.05)
    raise TimeoutError("demo step timed out")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--serve", action="store_true", help="keep the dashboard running at the end"
    )
    parser.add_argument("--port", type=int, default=8080)
    opts = parser.parse_args()

    workdir = Path(tempfile.mkdtemp(prefix="dtsched-demo-"))
    db = str(workdir / "demo.db")
    ledger = str(workdir / "ledger.db")
    store = Store(db)
    server = make_server(db, port=opts.port if opts.serve else 0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    api = f"http://127.0.0.1:{server.server_address[1]}"

    print(f"Database: {db}")
    print(f"Dashboard: {api}/\n")

    log = open(workdir / "cluster.log", "w")  # noqa: SIM115
    cluster = LocalCluster(
        db, workers=3, heartbeat_interval=0.3, lease_ttl=3.0, worker_timeout=1.5, log=log
    )
    labels: dict[int, str] = {}

    def submit(label: str, name: str, args=(), **kw) -> int:
        res = store.submit(f"dtsched.examples:{name}", list(args), **kw)
        if res.created:
            labels[res.task_id] = label
        return res.task_id

    # Queue the work *before* starting workers, so priority ordering is visible.
    for i in range(4):
        submit(f"batch-{i} (pri 0)", "sleep", [0.4])
    submit("urgent (pri 10)", "add", [40, 2], priority=10)
    submit("flaky", "flaky", [3], max_attempts=5, backoff_base=0.3)
    submit("always-fails", "always_fails", max_attempts=3, backoff_base=0.2)
    submit("hangs", "sleep", [60], timeout=1.0, max_attempts=2, backoff_base=0.2)
    payment = submit(
        "payment", "charge", [ledger, "order-1001", 4999, 3.0], priority=5, backoff_base=0.2
    )
    submit("delayed 2s", "add", [1, 1], delay=2.0)
    dup1 = store.submit("dtsched.examples:add", [7, 7], idempotency_key="report-2026-10-23")
    dup2 = store.submit("dtsched.examples:add", [7, 7], idempotency_key="report-2026-10-23")
    labels[dup1.task_id] = "idempotent submit"
    store.add_schedule("heartbeat-report", "dtsched.examples:add", [0, 1], interval=1.5)

    t0 = time.time()
    cluster.start()
    try:
        # Failure injection: SIGKILL whichever worker picks up the payment.
        victim = wait_until(lambda: store.get_task(payment).worker_id)
        wait_until(lambda: Path(ledger).exists())
        time.sleep(0.5)  # let it write to the ledger, then crash before it acks
        cluster.kill_worker(victim)
        print(f"[t+{time.time() - t0:4.1f}s] injected failure: SIGKILL {victim} mid-payment\n")

        wait_until(lambda: all(store.get_task(t).state in TERMINAL for t in labels), timeout=90)
        time.sleep(0.2)
        store.remove_schedule("heartbeat-report")
        for t in store.list_tasks(limit=1000):
            if t.schedule_id is not None and t.id not in labels:
                labels[t.id] = "scheduled"
        wait_until(lambda: all(store.get_task(t).state in TERMINAL for t in labels), timeout=30)

        print("Event timeline")
        print("--------------")
        for e in store.events(limit=10_000):
            if e["task_id"] not in labels or e["event"] == "submitted":
                continue
            detail = (e["detail"] or "").partition("\n")[0][:60]
            who = e["worker_id"] or ""
            print(
                f"t+{e['ts'] - t0:5.2f}s  #{e['task_id']:<3} {labels[e['task_id']]:<18}"
                f" {e['event']:<16} {who:<9} {detail}"
            )

        print("\nDuplicate submission with the same idempotency key:")
        print(f"  first  -> task {dup1.task_id} (created={dup1.created})")
        print(f"  second -> task {dup2.task_id} (created={dup2.created})")

        with sqlite3.connect(ledger) as conn:
            runs = conn.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
            rows = conn.execute("SELECT * FROM ledger").fetchall()
        print("\nAt-least-once + idempotency (payment task):")
        print(f"  task executions: {runs}   ledger rows: {rows}")

        print("\nCluster status (same as `dtsched status`)")
        print("------------------------------------------")
        print(format_status(store))

        print("\nDead-letter queue")
        for t in store.dead_letters():
            error = (t.last_error or "").partition("\n")[0]
            print(f"  #{t.id} {labels[t.id]:<14} after {t.attempts} attempts: {error}")

        with urllib.request.urlopen(api + "/api/stats") as r:
            stats = json.load(r)
        print(f"\nGET {api}/api/stats ->")
        print("  " + json.dumps({k: stats[k] for k in ("tasks", "workers", "retries", "timeouts")}))

        if opts.serve:
            print(f"\nDashboard still running at {api}/  (Ctrl-C to quit)")
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        cluster.stop()
        server.shutdown()
        log.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
