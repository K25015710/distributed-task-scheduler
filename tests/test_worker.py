"""Worker tests: real child processes and real time, one task at a time."""

import threading
import time

import pytest

from dtsched import Coordinator, Store, Worker
from dtsched.worker import resolve

from .conftest import wait_for

@pytest.fixture
def worker(db_path):
    w = Worker(db_path, "w-test", heartbeat_interval=0.1, lease_ttl=2.0)
    w.store.register_worker(w.id)
    return w

def run_task(worker, *args, **kw):
    tid = worker.store.submit(*args, **kw).task_id
    assert worker.run_once()
    return worker.store.get_task(tid)

def test_resolve_import_path():
    assert resolve("dtsched.examples:add")(2, 3) == 5
    with pytest.raises(AttributeError):
        resolve("dtsched.examples:nope")

def test_success_stores_result(worker):
    task = run_task(worker, "dtsched.examples:add", [2, 3])
    assert task.state == "succeeded"
    assert task.result == 5

def test_kwargs_and_ctx_injection(worker):
    task = run_task(worker, "dtsched.examples:flaky", kwargs={"succeed_on_attempt": 1})
    assert task.result == "succeeded on attempt 1"

def test_run_once_returns_false_when_idle(worker):
    assert worker.run_once() is False

def test_exception_is_retried_then_succeeds(worker):
    tid = worker.store.submit("dtsched.examples:flaky", [3], backoff_base=0.05).task_id
    wait_for(lambda: worker.run_once() and worker.store.get_task(tid).state == "succeeded")
    task = worker.store.get_task(tid)
    assert task.attempts == 3
    assert task.result == "succeeded on attempt 3"
    assert "ConnectionError" in task.last_error

def test_exception_message_and_traceback_recorded(worker):
    task = run_task(worker, "dtsched.examples:always_fails", ["kaboom"], max_attempts=1)
    assert task.state == "dead_letter"
    assert task.last_error.startswith("RuntimeError: kaboom")
    assert "examples.py" in task.last_error  # traceback tail included

def test_timeout_kills_the_task(worker):
    start = time.monotonic()
    task = run_task(worker, "dtsched.examples:sleep", [30], timeout=0.5, max_attempts=1)
    assert time.monotonic() - start < 5
    assert task.state == "dead_letter"
    assert "timed out after 0.5s" in task.last_error
    assert [e["event"] for e in worker.store.events(task.id)][-2:] == ["timeout", "dead_lettered"]

def test_process_crash_is_a_failure(worker):
    task = run_task(worker, "dtsched.examples:crash", max_attempts=1)
    assert task.state == "dead_letter"
    assert "exit 13" in task.last_error

def test_unknown_task_name_fails_cleanly(worker):
    task = run_task(worker, "dtsched.examples:does_not_exist", max_attempts=1)
    assert task.state == "dead_letter"
    assert "AttributeError" in task.last_error

def test_non_json_result_is_a_failure(worker):
    task = run_task(worker, "builtins:set", max_attempts=1)
    assert "not JSON serializable" in task.last_error

def test_worker_abandons_task_when_lease_is_taken_away(db_path):
    """If the coordinator reassigns our task, the heartbeat notices and we stop."""
    worker = Worker(db_path, "w-slow", heartbeat_interval=0.1, lease_ttl=2.0)
    tid = worker.store.submit("dtsched.examples:sleep", [30], timeout=60).task_id
    t = threading.Thread(target=worker.run, kwargs={"max_tasks": 1})
    t.start()
    wait_for(lambda: worker.store.get_task(tid).state == "running")
    # Pretend the worker missed its heartbeats: declare every worker dead.
    Store(db_path).reap(worker_timeout=-1)
    t.join(timeout=10)
    assert not t.is_alive(), "worker should give up on the reassigned task quickly"
    task = worker.store.get_task(tid)
    assert task.state == "queued"  # re-queued by the reaper, untouched by the old worker
    assert "stale_completion" not in [e["event"] for e in worker.store.events(tid)]

def test_graceful_stop_finishes_current_task(db_path):
    worker = Worker(db_path, "w-graceful", heartbeat_interval=0.1, lease_ttl=2.0)
    first = worker.store.submit("dtsched.examples:sleep", [0.5]).task_id
    second = worker.store.submit("dtsched.examples:add", [1, 1]).task_id
    t = threading.Thread(target=worker.run)
    t.start()
    wait_for(lambda: worker.store.get_task(first).state == "running")
    worker.stop()
    t.join(timeout=10)
    assert worker.store.get_task(first).state == "succeeded"
    assert worker.store.get_task(second).state == "queued"
    assert worker.store.workers()[0]["state"] == "stopped"

def test_heartbeat_interval_must_be_below_lease(db_path):
    with pytest.raises(ValueError):
        Worker(db_path, heartbeat_interval=5, lease_ttl=5)

def test_coordinator_tick_reaps_and_fires_schedules(db_path):
    coord = Coordinator(db_path, worker_timeout=0.2)
    store = coord.store
    store.register_worker("ghost")
    tid = store.submit("m:f").task_id
    store.claim("ghost", 60)
    store.add_schedule("tick", "dtsched.examples:add", [1, 1], interval=0.05)
    time.sleep(0.3)
    out = coord.tick()
    assert out["dead_workers"] == ["ghost"]
    assert out["retried"] == [tid]
    assert len(out["scheduled"]) == 1

def test_coordinator_run_loop_stops(db_path):
    coord = Coordinator(db_path, interval=0.05)
    t = threading.Thread(target=coord.run)
    t.start()
    time.sleep(0.2)
    coord.stop()
    t.join(timeout=5)
    assert not t.is_alive()
