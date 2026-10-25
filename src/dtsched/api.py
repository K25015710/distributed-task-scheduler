"""HTTP monitoring API and dashboard, using only the standard library.

Endpoints::

    GET  /                          HTML dashboard (polls the JSON API)
    GET  /healthz                   liveness probe
    GET  /metrics                   Prometheus text format
    GET  /api/stats                 queue depth, task counts, worker counts, latency
    GET  /api/workers               worker list with heartbeat age
    GET  /api/tasks?state=&limit=   recent tasks
    GET  /api/tasks/<id>            one task plus its event history
    GET  /api/dead-letter           the dead-letter queue
    POST /api/tasks                 submit {"name": ..., "args": [...], ...}
    POST /api/dead-letter/<id>/requeue
"""

from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .store import Store

_SUBMIT_FIELDS = {
    "args",
    "kwargs",
    "priority",
    "delay",
    "max_attempts",
    "timeout",
    "backoff_base",
    "backoff_max",
    "idempotency_key",
}


def prometheus(stats: dict[str, Any]) -> str:
    lines = [
        "# HELP dtsched_tasks Tasks by state.",
        "# TYPE dtsched_tasks gauge",
    ]
    lines += [f'dtsched_tasks{{state="{s}"}} {n}' for s, n in stats["tasks"].items()]
    lines += ["# HELP dtsched_workers Workers by state.", "# TYPE dtsched_workers gauge"]
    lines += [f'dtsched_workers{{state="{s}"}} {n}' for s, n in stats["workers"].items()]
    gauges = {
        "queue_depth": "Tasks ready to run now.",
        "delayed": "Queued tasks not yet due (delayed or backing off).",
        "throughput_per_min": "Tasks succeeded per minute over the last window.",
    }
    for key, help_text in gauges.items():
        lines += [
            f"# HELP dtsched_{key} {help_text}",
            f"# TYPE dtsched_{key} gauge",
            f"dtsched_{key} {stats[key]}",
        ]
    for key in ("retries", "timeouts", "workers_lost", "leases_expired", "stale_completions"):
        lines += [f"# TYPE dtsched_{key}_total counter", f"dtsched_{key}_total {stats[key]}"]
    return "\n".join(lines) + "\n"


def make_handler(store: Store) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "dtsched"

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 - quiet
            pass

        def _send(self, code: int, body: str | bytes, ctype: str) -> None:
            data = body.encode() if isinstance(body, str) else body
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _json(self, obj: Any, code: int = 200) -> None:
            self._send(code, json.dumps(obj, default=str), "application/json")

        def do_GET(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            q = parse_qs(url.query)
            path = url.path.rstrip("/") or "/"
            try:
                if path == "/":
                    self._send(200, DASHBOARD, "text/html; charset=utf-8")
                elif path == "/healthz":
                    self._json({"ok": True})
                elif path == "/metrics":
                    self._send(200, prometheus(store.stats()), "text/plain; version=0.0.4")
                elif path == "/api/stats":
                    self._json(store.stats())
                elif path == "/api/workers":
                    now = store.clock()
                    self._json(
                        [
                            {**w, "heartbeat_age": round(now - w["last_heartbeat"], 2)}
                            for w in store.workers()
                        ]
                    )
                elif path == "/api/tasks":
                    state = q.get("state", [None])[0]
                    limit = int(q.get("limit", ["50"])[0])
                    self._json([t.to_dict() for t in store.list_tasks(state, limit=limit)])
                elif path == "/api/dead-letter":
                    self._json([t.to_dict() for t in store.dead_letters()])
                elif m := re.fullmatch(r"/api/tasks/(\d+)", path):
                    task = store.get_task(int(m[1]))
                    if task is None:
                        self._json({"error": "not found"}, 404)
                    else:
                        self._json({**task.to_dict(), "events": store.events(task.id)})
                else:
                    self._json({"error": "not found"}, 404)
            except ValueError as exc:
                self._json({"error": str(exc)}, 400)

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.rstrip("/")
            try:
                if path == "/api/tasks":
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length) or b"{}")
                    unknown = set(body) - _SUBMIT_FIELDS - {"name"}
                    if "name" not in body or unknown:
                        raise ValueError(f"need 'name'; unknown fields: {sorted(unknown)}")
                    opts = {k: v for k, v in body.items() if k in _SUBMIT_FIELDS}
                    res = store.submit(body["name"], **opts)
                    self._json(
                        {"task_id": res.task_id, "created": res.created},
                        201 if res.created else 200,
                    )
                elif m := re.fullmatch(r"/api/dead-letter/(\d+)/requeue", path):
                    ok = store.requeue_dead_letter(int(m[1]))
                    self._json({"requeued": ok}, 200 if ok else 404)
                else:
                    self._json({"error": "not found"}, 404)
            except (ValueError, TypeError) as exc:
                self._json({"error": str(exc)}, 400)

    return Handler


def make_server(db_path: str, host: str = "127.0.0.1", port: int = 8080) -> ThreadingHTTPServer:
    """Build (but don't start) the server. Port 0 picks a free port."""
    return ThreadingHTTPServer((host, port), make_handler(Store(db_path)))


DASHBOARD = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>dtsched dashboard</title>
<style>
  :root { --bg:#fafafa; --fg:#1d1d1f; --muted:#6e6e73; --card:#fff; --line:#e5e5ea;
          --ok:#1a7f37; --bad:#c62828; --warn:#a15c00; }
  @media (prefers-color-scheme: dark) {
    :root { --bg:#121214; --fg:#f2f2f7; --muted:#98989f; --card:#1c1c1f; --line:#2c2c30;
            --ok:#4cc26b; --bad:#ff6b6b; --warn:#f0a73a; } }
  body { margin:0; padding:24px 16px; background:var(--bg); color:var(--fg);
         font:14px/1.45 system-ui, -apple-system, sans-serif; }
  main { max-width:1100px; margin:0 auto; }
  h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:15px; margin:28px 0 8px; }
  .sub { color:var(--muted); margin-bottom:20px; }
  .tiles { display:grid; grid-template-columns:repeat(auto-fill,minmax(140px,1fr)); gap:10px; }
  .tile { background:var(--card); border:1px solid var(--line); border-radius:8px; padding:12px; }
  .tile b { display:block; font-size:24px; font-variant-numeric:tabular-nums; }
  .tile span { color:var(--muted); font-size:12px; }
  .wrap { overflow-x:auto; }
  table { width:100%; border-collapse:collapse; background:var(--card);
          border:1px solid var(--line); border-radius:8px; }
  th, td { text-align:left; padding:6px 10px; border-bottom:1px solid var(--line);
           white-space:nowrap; font-variant-numeric:tabular-nums; }
  th { color:var(--muted); font-weight:500; font-size:12px; }
  .succeeded, .alive { color:var(--ok); } .dead_letter, .dead { color:var(--bad); }
  .running, .queued { color:var(--warn); }
  td.err { white-space:normal; color:var(--muted); max-width:380px; }
</style></head>
<body><main>
<h1>dtsched</h1><div class="sub" id="updated">loading...</div>
<div class="tiles" id="tiles"></div>
<h2>Workers</h2><div class="wrap"><table id="workers"></table></div>
<h2>Recent tasks</h2><div class="wrap"><table id="tasks"></table></div>
<script>
const ENT = {"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;"};
const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ENT[c]);
const fmt = v => v == null ? "-"
  : (typeof v === "number" && !Number.isInteger(v) ? v.toFixed(2) : v);
function table(el, cols, rows) {
  el.innerHTML = "<tr>" + cols.map(c => `<th>${c[0]}</th>`).join("") + "</tr>" +
    rows.map(r => "<tr>" + cols.map(c => c[1](r)).join("") + "</tr>").join("");
}
async function refresh() {
  try {
    const [s, w, t] = await Promise.all(["stats", "workers", "tasks?limit=25"].map(
      p => fetch("/api/" + p).then(r => r.json())));
    const tiles = [["Queue depth", s.queue_depth], ["Running", s.tasks.running],
      ["Succeeded", s.tasks.succeeded], ["Dead-lettered", s.tasks.dead_letter],
      ["Awaiting retry", s.awaiting_retry], ["Retries", s.retries], ["Timeouts", s.timeouts],
      ["Workers lost", s.workers_lost], ["Workers alive", s.workers.alive],
      ["p95 duration (s)", fmt(s.duration_p95)], ["Throughput / min", fmt(s.throughput_per_min)]];
    document.getElementById("tiles").innerHTML = tiles.map(
      ([k, v]) => `<div class="tile"><b>${esc(v)}</b><span>${esc(k)}</span></div>`).join("");
    table(document.getElementById("workers"), [
      ["Worker", r => `<td>${esc(r.id)}</td>`],
      ["State", r => `<td class="${esc(r.state)}">${esc(r.state)}</td>`],
      ["PID", r => `<td>${esc(r.pid)}</td>`],
      ["Heartbeat age (s)", r => `<td>${fmt(r.heartbeat_age)}</td>`],
      ["Current task", r => `<td>${fmt(r.current_task_id)}</td>`],
      ["Succeeded", r => `<td>${r.succeeded}</td>`], ["Failed", r => `<td>${r.failed}</td>`]], w);
    table(document.getElementById("tasks"), [
      ["ID", r => `<td>${r.id}</td>`], ["Task", r => `<td>${esc(r.name)}</td>`],
      ["Priority", r => `<td>${r.priority}</td>`],
      ["State", r => `<td class="${esc(r.state)}">${esc(r.state)}</td>`],
      ["Attempts", r => `<td>${r.attempts}/${r.max_attempts}</td>`],
      ["Worker", r => `<td>${esc(r.worker_id ?? "")}</td>`],
      ["Last error", r => `<td class="err">${esc((r.last_error ?? "").split("\\n")[0])}</td>`]], t);
    document.getElementById("updated").textContent = "Updated " + new Date().toLocaleTimeString();
  } catch (e) {
    document.getElementById("updated").textContent = "API unreachable: " + e;
  }
}
refresh(); setInterval(refresh, 2000);
</script></main></body></html>
"""
