import json
import threading
import urllib.error
import urllib.request
import pytest

from dtsched import Store
from dtsched.api import make_server
from dtsched.cli import main


@pytest.fixture
def api(db_path):
    server = make_server(db_path, port=0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()

def get(url):
    with urllib.request.urlopen(url, timeout=5) as r:
        body = r.read().decode()
        return r.status, (json.loads(body) if "json" in r.headers["Content-Type"] else body)

def post(url, payload=None):
    data = json.dumps(payload or {}).encode()
    req = urllib.request.Request(url, data, {"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

# HTTP API

def test_dashboard_and_health(api):
    status, html = get(api + "/")
    assert status == 200 and "<title>dtsched dashboard</title>" in html
    assert get(api + "/healthz") == (200, {"ok": True})


def test_submit_and_inspect_via_api(api, db_path):
    status, body = post(
        api + "/api/tasks",
        {"name": "dtsched.examples:add", "args": [1, 2], "priority": 3, "idempotency_key": "k"},
    )
    assert status == 201 and body["created"]
    tid = body["task_id"]
    assert post(api + "/api/tasks", {"name": "dtsched.examples:add", "idempotency_key": "k"}) == (
        200,
        {"task_id": tid, "created": False},
    )
    status, task = get(f"{api}/api/tasks/{tid}")
    assert task["priority"] == 3 and task["state"] == "queued"
    assert "lease_token" not in task
    assert [e["event"] for e in task["events"]] == ["submitted"]
    _, tasks = get(api + "/api/tasks?state=queued")
    assert [t["id"] for t in tasks] == [tid]


def test_api_rejects_bad_submissions(api):
    assert post(api + "/api/tasks", {"args": []})[0] == 400
    assert post(api + "/api/tasks", {"name": "m:f", "evil": 1})[0] == 400
    assert post(api + "/api/tasks", {"name": "nocolon"})[0] == 400
    with pytest.raises(urllib.error.HTTPError) as e:
        get(api + "/api/tasks?state=bogus")
    assert e.value.code == 400


def test_stats_workers_and_metrics(api, db_path):
    store = Store(db_path)
    store.register_worker("w1")
    store.submit("m:f")
    _, stats = get(api + "/api/stats")
    assert stats["queue_depth"] == 1 and stats["workers"]["alive"] == 1
    _, workers = get(api + "/api/workers")
    assert workers[0]["id"] == "w1" and workers[0]["heartbeat_age"] >= 0
    _, metrics = get(api + "/metrics")
    assert 'dtsched_tasks{state="queued"} 1' in metrics
    assert "dtsched_queue_depth 1" in metrics
    assert "dtsched_retries_total 0" in metrics


def test_dead_letter_endpoints(api, db_path):
    store = Store(db_path)
    tid = store.submit("m:f", max_attempts=1).task_id
    t = store.claim("w", 10)
    store.fail(t.id, t.lease_token, "boom")
    _, dlq = get(api + "/api/dead-letter")
    assert [d["id"] for d in dlq] == [tid]
    assert post(f"{api}/api/dead-letter/{tid}/requeue") == (200, {"requeued": True})
    assert post(f"{api}/api/dead-letter/{tid}/requeue")[0] == 404
    assert store.get_task(tid).state == "queued"


def test_unknown_routes_404(api):
    for path in ("/nope", "/api/tasks/999"):
        with pytest.raises(urllib.error.HTTPError) as e:
            get(api + path)
        assert e.value.code == 404


# CLI
def run_cli(db_path, *args):
    return main(["--db", db_path, *args])


def test_cli_submit_tasks_and_show(db_path, capsys):
    assert run_cli(db_path, "submit", "dtsched.examples:add", "2", "3", "--priority", "5") == 0
    assert capsys.readouterr().out.strip() == "task 1"
    run_cli(db_path, "submit", "dtsched.examples:add", "--idempotency-key", "x")
    run_cli(db_path, "submit", "dtsched.examples:add", "--idempotency-key", "x")
    assert "already submitted" in capsys.readouterr().out
    task = Store(db_path).get_task(1)
    assert task.args == [2, 3] and task.priority == 5

    run_cli(db_path, "tasks")
    out = capsys.readouterr().out
    assert "dtsched.examples:add" in out and "queued" in out
    run_cli(db_path, "show", "1")
    assert "submitted" in capsys.readouterr().out
    assert run_cli(db_path, "show", "99") == 1


def test_cli_status(db_path, capsys):
    store = Store(db_path)
    store.register_worker("w1")
    store.submit("m:f")
    run_cli(db_path, "status")
    out = capsys.readouterr().out
    assert "Queue depth 1 ready" in out and "w1" in out and "alive" in out
    run_cli(db_path, "status", "--json")
    assert json.loads(capsys.readouterr().out)["stats"]["queue_depth"] == 1


def test_cli_dlq_and_cancel(db_path, capsys):
    store = Store(db_path)
    for _ in range(2):
        tid = store.submit("m:f", max_attempts=1).task_id
        t = store.claim("w", 10)
        store.fail(t.id, t.lease_token, "boom")
    run_cli(db_path, "dlq", "list")
    assert capsys.readouterr().out.count("dead_letter") == 2
    run_cli(db_path, "dlq", "requeue", "--all")
    assert "requeued 2" in capsys.readouterr().out
    assert run_cli(db_path, "cancel", str(tid)) == 0
    assert run_cli(db_path, "cancel", str(tid)) == 1


def test_cli_schedules(db_path, capsys):
    task = "dtsched.examples:add"
    run_cli(db_path, "schedule", "add", "nightly", task, "1", "2", "--cron", "0 3 * * *")
    run_cli(db_path, "schedule", "add", "often", task, "--every", "30")
    run_cli(db_path, "schedule", "list")
    out = capsys.readouterr().out
    assert "0 3 * * *" in out and "every 30s" in out
    assert run_cli(db_path, "schedule", "remove", "often") == 0
    assert run_cli(db_path, "schedule", "remove", "often") == 1
