"""End-to-end failure injection against a real multi-process cluster.

Each test starts a coordinator and several worker *processes* (the same
python -m dtsched commands you'd run in production) and then breaks
something: SIGKILLs a worker mid-task, submits tasks that raise, hang or
crash, and checks the system recovers.
"""

import os
import signal
import sqlite3

import pytest

from dtsched import Store
from dtsched.cluster import LocalCluster

from .conftest import wait_for

pytestmark = pytest.mark.skipif(not hasattr(signal, "SIGKILL"), reason="needs POSIX signals")

FAST = {
    "heartbeat_interval": 0.2,
    "lease_ttl": 2.0,
    "worker_timeout": 1.0,
    "coordinator_interval": 0.1,
    "poll_interval": 0.05,
}

@pytest.fixture
def cluster(db_path, tmp_path):
    Store(db_path)
    log = open(tmp_path / "cluster.log", "w")  # noqa: SIM115
    c = LocalCluster(db_path, workers=3, log=log, **FAST).start()
    yield c
    c.stop()
    log.close()

def state(store, tid):
    return store.get_task(tid).state

def test_work_is_spread_across_workers(db_path, cluster):
    store = Store(db_path)
    ids = [store.submit("dtsched.examples:sleep", [0.3]).task_id for _ in range(12)]
    wait_for(lambda: all(state(store, i) == "succeeded" for i in ids), timeout=30)
    used = {store.get_task(i).worker_id for i in ids}
    assert len(used) >= 2, f"all tasks ran on {used}"

def test_kill_worker_mid_task_reassigns_it(db_path, cluster):
    store = Store(db_path)
    tid = store.submit("dtsched.examples:sleep", [2.0], backoff_base=0.1).task_id
    victim = wait_for(lambda: store.get_task(tid).worker_id)
    cluster.kill_worker(victim)  # SIGKILL: no cleanup, heartbeats just stop

    wait_for(lambda: state(store, tid) == "succeeded", timeout=30)
    task = store.get_task(tid)
    assert task.worker_id != victim
    assert task.attempts == 2
    events = [e["event"] for e in store.events(tid)]
    assert "worker_lost" in events
    workers = {w["id"]: w["state"] for w in store.workers()}
    assert workers[victim] == "dead"


def test_orphaned_task_process_exits_when_worker_is_killed(db_path, cluster, tmp_path):
    store = Store(db_path)
    pid_file = tmp_path / "task.pid"
    tid = store.submit(
        "dtsched.examples:record_pid_then_sleep", [str(pid_file), 30], max_attempts=1
    ).task_id
    victim = wait_for(lambda: store.get_task(tid).worker_id)
    pid = int(wait_for(lambda: pid_file.exists() and pid_file.read_text()))
    cluster.kill_worker(victim)

    def gone():
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        return False

    wait_for(gone, timeout=10)

def test_at_least_once_with_idempotent_task(db_path, cluster, tmp_path):
    """Worker dies after the side effect but before acking: task re-runs, charge once."""
    store = Store(db_path)
    ledger = str(tmp_path / "ledger.db")
    tid = store.submit(
        "dtsched.examples:charge", [ledger, "order-1", 4999, 3.0], backoff_base=0.1
    ).task_id
    victim = wait_for(lambda: store.get_task(tid).worker_id)

    def executed():
        try:
            with sqlite3.connect(ledger) as db:
                return db.execute("SELECT COUNT(*) FROM executions").fetchone()[0] >= 1
        except sqlite3.OperationalError:
            return False

    wait_for(executed)  # side effect has happened...
    cluster.kill_worker(victim)  # ...but the worker never reports success

    wait_for(lambda: state(store, tid) == "succeeded", timeout=30)
    with sqlite3.connect(ledger) as db:
        runs = db.execute("SELECT COUNT(*) FROM executions").fetchone()[0]
        charges = db.execute("SELECT order_id, amount_cents FROM ledger").fetchall()
    assert runs >= 2  # at-least-once: the task really did execute twice
    assert charges == [("order-1", 4999)]  # idempotency: the customer paid once
    assert store.get_task(tid).result == {"order_id": "order-1", "charged_now": False}


def test_raising_task_retries_then_dead_letters(db_path, cluster):
    store = Store(db_path)
    tid = store.submit("dtsched.examples:always_fails", max_attempts=3, backoff_base=0.1).task_id
    wait_for(lambda: state(store, tid) == "dead_letter", timeout=30)
    events = [e["event"] for e in store.events(tid)]
    assert events.count("failed") == 3
    assert events.count("retry_scheduled") == 2
    assert events[-1] == "dead_lettered"


def test_flaky_task_eventually_succeeds(db_path, cluster):
    store = Store(db_path)
    tid = store.submit("dtsched.examples:flaky", [3], max_attempts=5, backoff_base=0.1).task_id
    wait_for(lambda: state(store, tid) == "succeeded", timeout=30)
    assert store.get_task(tid).attempts == 3


def test_hanging_task_times_out(db_path, cluster):
    store = Store(db_path)
    tid = store.submit(
        "dtsched.examples:sleep", [60], timeout=0.5, max_attempts=2, backoff_base=0.1
    ).task_id
    wait_for(lambda: state(store, tid) == "dead_letter", timeout=30)
    assert [e["event"] for e in store.events(tid)].count("timeout") == 2


def test_cluster_survives_losing_most_workers(db_path, cluster):
    store = Store(db_path)
    ids = [
        store.submit("dtsched.examples:sleep", [0.5], backoff_base=0.1).task_id for _ in range(9)
    ]
    wait_for(lambda: sum(state(store, i) == "running" for i in ids) >= 2)
    for wid in ["worker-1", "worker-2"]:
        cluster.kill_worker(wid)
    wait_for(lambda: all(state(store, i) == "succeeded" for i in ids), timeout=60)
    assert {w["id"]: w["state"] for w in store.workers()}["worker-3"] == "alive"


def test_new_worker_picks_up_after_all_workers_die(db_path, cluster):
    store = Store(db_path)
    for wid in list(cluster.workers):
        cluster.kill_worker(wid)
    tid = store.submit("dtsched.examples:add", [20, 22]).task_id
    cluster.add_worker()
    wait_for(lambda: state(store, tid) == "succeeded", timeout=30)
    assert store.get_task(tid).result == 42
