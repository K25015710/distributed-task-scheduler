"""State-machine tests for the store, driven by a fake clock (no sleeping)."""

import threading

import pytest

from dtsched import Store

LEASE = 10.0

def submit(store, name="dtsched.examples:add", **kw):
    return store.submit(name, [1, 2], **kw).task_id

# submission and claiming

def test_submit_and_claim_roundtrip(store):
    tid = submit(store)
    task = store.claim("w1", LEASE)
    assert task.id == tid
    assert task.state == "running"
    assert task.attempts == 1
    assert task.worker_id == "w1"
    assert task.args == [1, 2]
    assert task.lease_token
    assert store.claim("w2", LEASE) is None  # nothing else to take

def test_claim_order_priority_then_fifo(store, clock):
    low = submit(store, priority=0)
    clock.advance(1)
    high_late = submit(store, priority=5)
    clock.advance(1)
    high_later = submit(store, priority=5)
    order = [store.claim("w", LEASE).id for _ in range(3)]
    assert order == [high_late, high_later, low]

def test_delayed_task_not_claimable_until_due(store, clock):
    tid = submit(store, delay=30)
    assert store.claim("w", LEASE) is None
    clock.advance(29.9)
    assert store.claim("w", LEASE) is None
    clock.advance(0.2)
    assert store.claim("w", LEASE).id == tid

def test_run_at_absolute(store, clock):
    tid = store.submit("m:f", run_at=clock.now + 5).task_id
    assert store.claim("w", LEASE) is None
    clock.advance(5)
    assert store.claim("w", LEASE).id == tid

def test_idempotency_key_dedupes_submissions(store):
    a = store.submit("m:f", idempotency_key="order-42")
    b = store.submit("m:f", idempotency_key="order-42")
    c = store.submit("m:f", idempotency_key="order-43")
    assert a.created and not b.created and c.created
    assert a.task_id == b.task_id != c.task_id
    assert len(store.list_tasks()) == 2

@pytest.mark.parametrize(
    "name, kw",
    [("no_colon", {}), ("m:f", {"max_attempts": 0}), ("m:f", {"timeout": 0})],
)
def test_submit_validation(store, name, kw):
    with pytest.raises(ValueError):
        store.submit(name, **kw)

def test_concurrent_claims_never_double_assign(db_path):
    """Many threads with their own connections race; each task is claimed once."""
    setup = Store(db_path)
    for _ in range(200):
        setup.submit("m:f")
    claimed: list[int] = []
    lock = threading.Lock()

    def claimer(i: int) -> None:
        s = Store(db_path)
        while (t := s.claim(f"w{i}", LEASE)) is not None:
            with lock:
                claimed.append(t.id)
        s.close()

    threads = [threading.Thread(target=claimer, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(claimed) == 200
    assert len(set(claimed)) == 200

def test_cancel_only_queued(store):
    tid = submit(store)
    assert store.cancel(tid)
    assert store.get_task(tid).state == "cancelled"
    assert store.claim("w", LEASE) is None
    tid2 = submit(store)
    store.claim("w", LEASE)
    assert not store.cancel(tid2)

# completion, failure, retries, dead letters

def test_complete_records_result(store):
    submit(store)
    t = store.claim("w", LEASE)
    assert store.complete(t.id, t.lease_token, {"sum": 3})
    done = store.get_task(t.id)
    assert done.state == "succeeded"
    assert done.result == {"sum": 3}
    assert done.lease_token is None

def test_fail_schedules_retry_with_exponential_backoff(store, clock):
    tid = submit(store, max_attempts=4, backoff_base=2, backoff_max=100)
    expected_delays = [2, 4, 8]  # rand=1.0 in the fixture -> no jitter shrinkage
    for delay in expected_delays:
        t = store.claim("w", LEASE)
        assert store.fail(t.id, t.lease_token, "boom") == "retry"
        task = store.get_task(tid)
        assert task.state == "queued"
        assert task.run_at == pytest.approx(clock.now + delay)
        assert store.claim("w", LEASE) is None  # still backing off
        clock.advance(delay)
    t = store.claim("w", LEASE)
    assert t.attempts == 4
    assert store.complete(t.id, t.lease_token, "ok")

def test_backoff_respects_cap(store, clock):
    tid = submit(store, max_attempts=10, backoff_base=10, backoff_max=15)
    for _ in range(3):
        t = store.claim("w", LEASE)
        store.fail(t.id, t.lease_token, "x")
        assert store.get_task(tid).run_at - clock.now <= 15
        clock.advance(15)

def test_max_attempts_moves_to_dead_letter(store, clock):
    tid = submit(store, max_attempts=2, backoff_base=1)
    t = store.claim("w", LEASE)
    assert store.fail(t.id, t.lease_token, "first") == "retry"
    clock.advance(1)
    t = store.claim("w", LEASE)
    assert store.fail(t.id, t.lease_token, "second") == "dead_letter"
    task = store.get_task(tid)
    assert task.state == "dead_letter"
    assert task.last_error == "second"
    assert [d.id for d in store.dead_letters()] == [tid]
    clock.advance(1000)
    assert store.claim("w", LEASE) is None  # dead letters are never retried automatically

def test_requeue_dead_letter_resets_attempts(store):
    tid = submit(store, max_attempts=1)
    t = store.claim("w", LEASE)
    store.fail(t.id, t.lease_token, "x")
    assert store.requeue_dead_letter(tid)
    assert not store.requeue_dead_letter(tid)  # no longer dead-lettered
    t = store.claim("w", LEASE)
    assert t.id == tid and t.attempts == 1

def test_event_history(store, clock):
    tid = submit(store, max_attempts=2)
    t = store.claim("w", LEASE)
    store.fail(t.id, t.lease_token, "x")
    clock.advance(10)
    t = store.claim("w", LEASE)
    store.complete(t.id, t.lease_token)
    events = [e["event"] for e in store.events(tid)]
    assert events == [
        "submitted",
        "claimed",
        "failed",
        "retry_scheduled",
        "claimed",
        "succeeded",
    ]

# leases, fencing tokens and failure detection

def test_heartbeat_extends_lease(store, clock):
    store.register_worker("w")
    submit(store)
    t = store.claim("w", lease_ttl=5)
    clock.advance(4)
    assert store.heartbeat("w", lease_ttl=5) == {t.id}
    clock.advance(4)  # 8s after claim, but only 4s after the heartbeat
    assert store.reap(worker_timeout=30).retried == []
    assert store.get_task(t.id).state == "running"


def test_expired_lease_is_recovered(store, clock):
    store.register_worker("w")
    tid = submit(store)
    store.claim("w", lease_ttl=5)
    clock.advance(6)
    res = store.reap(worker_timeout=1000)  # worker not dead, but its lease ran out
    assert res.retried == [tid]
    assert [e["event"] for e in store.events(tid)][-2:] == ["lease_expired", "retry_scheduled"]


def test_dead_worker_tasks_are_reassigned(store, clock):
    store.register_worker("w1")
    store.register_worker("w2")
    tid = submit(store, backoff_base=1)
    store.claim("w1", LEASE)
    clock.advance(3)
    store.heartbeat("w2", LEASE)  # w2 is alive; w1 went silent
    res = store.reap(worker_timeout=2)
    assert res.dead_workers == ["w1"]
    assert res.retried == [tid]
    states = {w["id"]: w["state"] for w in store.workers()}
    assert states == {"w1": "dead", "w2": "alive"}
    clock.advance(1)
    t = store.claim("w2", LEASE)
    assert t.id == tid and t.attempts == 2


def test_dead_worker_on_last_attempt_dead_letters(store, clock):
    store.register_worker("w1")
    tid = submit(store, max_attempts=1)
    store.claim("w1", LEASE)
    clock.advance(10)
    res = store.reap(worker_timeout=2)
    assert res.dead_lettered == [tid]
    assert store.get_task(tid).state == "dead_letter"


def test_stale_completion_is_rejected(store, clock):
    """A presumed-dead worker that finishes late must not clobber the retry."""
    store.register_worker("slow")
    store.register_worker("fast")
    tid = submit(store, backoff_base=0)
    stale = store.claim("slow", LEASE)
    clock.advance(20)
    store.heartbeat("fast", LEASE)
    store.reap(worker_timeout=5)  # "slow" declared dead, task re-queued
    fresh = store.claim("fast", LEASE)
    assert fresh.id == tid and fresh.lease_token != stale.lease_token

    assert store.complete(tid, stale.lease_token, "stale result") is False
    assert store.fail(tid, stale.lease_token, "stale error") == "stale"
    assert store.get_task(tid).state == "running"  # untouched
    assert store.complete(tid, fresh.lease_token, "fresh result")
    assert store.get_task(tid).result == "fresh result"
    assert store.stats()["stale_completions"] == 2

def test_heartbeat_reports_lost_lease(store, clock):
    store.register_worker("w1")
    submit(store)
    t = store.claim("w1", LEASE)
    clock.advance(20)
    store.reap(worker_timeout=5)
    # The worker comes back: it is alive again but no longer holds the task.
    assert store.heartbeat("w1", LEASE) == set()
    assert store.get_task(t.id).state == "queued"

def test_reap_is_idempotent(store, clock):
    store.register_worker("w")
    tid = submit(store)
    store.claim("w", LEASE)
    clock.advance(60)
    assert store.reap(worker_timeout=5).retried == [tid]
    second = store.reap(worker_timeout=5)
    assert second.dead_workers == [] and second.retried == []
    assert store.get_task(tid).attempts == 1

# schedules
def test_interval_schedule_fires_and_advances(store, clock):
    store.add_schedule("tick", "m:f", [1], interval=10)
    assert store.fire_due_schedules() == []
    clock.advance(10)
    first = store.fire_due_schedules()
    assert len(first) == 1
    assert store.fire_due_schedules() == []  # not due again yet
    clock.advance(10)
    assert len(store.fire_due_schedules()) == 1
    task = store.get_task(first[0])
    assert task.args == [1] and task.schedule_id is not None

def test_interval_schedule_collapses_missed_runs(store, clock):
    store.add_schedule("tick", "m:f", interval=10)
    clock.advance(95)  # nine occurrences missed
    assert len(store.fire_due_schedules()) == 1
    assert store.schedules()[0]["next_run_at"] == clock.now + 10

def test_cron_schedule(store, clock):
    clock.now = 1_767_225_600.0  # 2026-01-01 00:00:00 UTC
    store.add_schedule("hourly", "m:f", cron="0 * * * *")
    assert store.schedules()[0]["next_run_at"] == clock.now + 3600
    clock.advance(3600)
    assert len(store.fire_due_schedules()) == 1
    assert store.schedules()[0]["next_run_at"] == clock.now + 3600

def test_two_coordinators_fire_a_schedule_once(db_path, clock):
    a = Store(db_path, clock=clock)
    b = Store(db_path, clock=clock)
    a.add_schedule("tick", "m:f", interval=5)
    clock.advance(5)
    fired = a.fire_due_schedules() + b.fire_due_schedules()
    assert len(fired) == 1
    assert len(a.list_tasks()) == 1

def test_schedule_validation(store):
    with pytest.raises(ValueError):
        store.add_schedule("x", "m:f")
    with pytest.raises(ValueError):
        store.add_schedule("x", "m:f", cron="* * * * *", interval=5)
    with pytest.raises(ValueError):
        store.add_schedule("x", "m:f", interval=0)

def test_remove_schedule(store):
    store.add_schedule("x", "m:f", interval=5)
    assert store.remove_schedule("x")
    assert not store.remove_schedule("x")

# stats
def test_stats(store, clock):
    store.register_worker("w")
    submit(store)  # will succeed
    submit(store, max_attempts=1)  # will dead-letter
    submit(store, delay=100)  # delayed
    t = store.claim("w", LEASE)
    clock.advance(0.5)
    store.complete(t.id, t.lease_token)
    t = store.claim("w", LEASE)
    store.fail(t.id, t.lease_token, "x", event="timeout")
    submit(store)  # ready
    s = store.stats()
    assert s["tasks"]["succeeded"] == 1
    assert s["tasks"]["dead_letter"] == 1
    assert s["queue_depth"] == 1
    assert s["delayed"] == 1
    assert s["timeouts"] == 1
    assert s["workers"]["alive"] == 1
    assert s["duration_p50"] == pytest.approx(0.5)
    assert s["throughput_per_min"] == 1.0

def test_list_tasks_rejects_unknown_state(store):
    with pytest.raises(ValueError):
        store.list_tasks("bogus")
