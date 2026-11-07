"""Local, read-only browser for blocktrace SQLite databases."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import threading
from collections import defaultdict
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[2]
STATIC = Path(__file__).resolve().parent
MIB = 1 << 20
MAX_UPLOAD = 1024 * 1024 * 1024
MODES = {1: "Startup", 2: "ProbeBW", 3: "SlowDown", 4: "ProbeRTT", 5: "Choked"}


def open_db(path: Path) -> sqlite3.Connection:
    wal = Path(str(path) + "-wal")
    uri = path.as_uri() + ("?mode=ro" if wal.exists() and wal.stat().st_size else "?immutable=1")
    db = sqlite3.connect(uri, uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    required = {"torrent", "peer", "bw_sample", "block_event"}
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not required <= tables:
        db.close()
        raise ValueError("Not a Stardust blocktrace database")
    return db


def trace_info(path: Path) -> dict:
    with closing(open_db(path)) as db:
        count, start, end = db.execute(
            "SELECT count(*), min(ts_us), max(ts_us) FROM bw_sample"
        ).fetchone()
        return {"samples": count, "start_us": start, "end_us": end,
                "duration_s": round((end - start) / 1e6, 3) if start is not None else 0}


def peer_rows(path: Path) -> list[dict]:
    with closing(open_db(path)) as db:
        rows = db.execute("""
            SELECT p.id, p.torrent_id, p.addr, coalesce(t.name, '') torrent_name,
                   coalesce(r.bytes, 0) received_bytes, coalesce(r.blocks, 0) blocks,
                   coalesce(s.samples, 0) samples, s.first_us, s.last_us,
                   coalesce(conn.connections, 0) connections
              FROM peer p JOIN torrent t ON t.id=p.torrent_id
              LEFT JOIN (SELECT peer_id, sum(coalesce(bytes,16384)) bytes, count(*) blocks
                           FROM block_event WHERE kind=3 GROUP BY peer_id) r ON r.peer_id=p.id
              LEFT JOIN (SELECT peer_id, count(*) samples, min(ts_us) first_us,
                                max(ts_us) last_us FROM bw_sample GROUP BY peer_id) s ON s.peer_id=p.id
              LEFT JOIN (SELECT peer_id, count(*) connections FROM peer_state_event
                          WHERE kind=1 GROUP BY peer_id) conn ON conn.peer_id=p.id
             WHERE s.samples>0
             ORDER BY r.bytes DESC, s.samples DESC, p.id
        """).fetchall()
        return [{**dict(r), "received_mib": round(r["received_bytes"] / MIB, 3)} for r in rows]


def sample_rows(path: Path, peer_id: int, start_us: int | None, end_us: int | None,
                probe_only: bool, exclude_app_limited: bool) -> tuple[list[dict], dict]:
    clauses = ["peer_id=?", "inflight IS NOT NULL", "rtt_us IS NOT NULL", "rtt_us>0"]
    args: list[int] = [peer_id]
    if start_us is not None:
        clauses.append("ts_us>=?")
        args.append(start_us)
    if end_us is not None:
        clauses.append("ts_us<=?")
        args.append(end_us)
    if probe_only:
        clauses.append("mode=2")
    if exclude_app_limited:
        clauses.append("app_limited=0")
    where = " AND ".join(clauses)
    with closing(open_db(path)) as db:
        extent = db.execute(
            "SELECT min(ts_us), max(ts_us) FROM bw_sample WHERE peer_id=?", (peer_id,)
        ).fetchone()
        if extent[0] is None:
            return [], {"first_us": None, "last_us": None, "count": 0}
        rows = db.execute(
            f"SELECT id, ts_us, inflight, rtt_us, min_rtt_us, avg_bw, max_bw, "
            f"app_limited, mode FROM bw_sample WHERE {where} ORDER BY ts_us,id", args
        ).fetchall()
    data = [{"id": r["id"], "ts_us": r["ts_us"],
             "t": round((r["ts_us"] - extent[0]) / 1e6, 4),
             "inflight": r["inflight"], "rtt_ms": round(r["rtt_us"] / 1000, 4),
             "min_rtt_ms": round(r["min_rtt_us"] / 1000, 4) if r["min_rtt_us"] is not None else None,
             "bw_mib": round((r["avg_bw"] or 0) / MIB, 5),
             "max_bw_mib": round((r["max_bw"] or 0) / MIB, 5),
             "app_limited": bool(r["app_limited"]), "mode": MODES.get(r["mode"], "Unknown")}
            for r in rows]
    return data, {"first_us": extent[0], "last_us": extent[1], "count": len(data)}


def thin_time(rows: list[dict], limit: int) -> list[dict]:
    if len(rows) <= limit:
        return rows
    # First/last and each bucket's low/high RTT and bandwidth retain spikes.
    bucket_count = max(1, (limit - 2) // 4)
    picked = {0, len(rows) - 1}
    for bucket in range(bucket_count):
        lo = math.floor(bucket * len(rows) / bucket_count)
        hi = math.floor((bucket + 1) * len(rows) / bucket_count)
        part = range(lo, hi)
        for key in ("rtt_ms", "bw_mib"):
            picked.add(min(part, key=lambda i: rows[i][key]))
            picked.add(max(part, key=lambda i: rows[i][key]))
    return [rows[i] for i in sorted(picked)]


class TraceServer(ThreadingHTTPServer):
    def __init__(self, address):
        super().__init__(address, Handler)
        self.uploads = tempfile.TemporaryDirectory(prefix="stardust-trace-browser-")
        self.registry: dict[str, Path] = {}
        self.cache: dict[str, object] = {}
        self.lock = threading.Lock()
        for path in sorted(ROOT.glob("trace*.db")):
            self.registry[path.name] = path.resolve()

    def server_close(self):
        super().server_close()
        if hasattr(self, "uploads"):
            self.uploads.cleanup()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        print("%s %s" % (self.address_string(), format % args))

    def json_reply(self, value, status=200):
        body = json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def file_reply(self, name, mime):
        path = STATIC / name
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", mime)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def query_path(self, query):
        name = query.get("db", [None])[0]
        with self.server.lock:
            path = self.server.registry.get(name)
        if path is None:
            raise ValueError("Choose a trace database")
        return path

    def do_GET(self):
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        try:
            if parsed.path in ("/", "/index.html"):
                return self.file_reply("index.html", "text/html; charset=utf-8")
            if parsed.path == "/app.js":
                return self.file_reply("app.js", "text/javascript; charset=utf-8")
            if parsed.path == "/styles.css":
                return self.file_reply("styles.css", "text/css; charset=utf-8")
            if parsed.path == "/api/traces":
                with self.server.lock:
                    items = list(self.server.registry.items())
                result = []
                for name, path in items:
                    try:
                        result.append({"name": name, **trace_info(path)})
                    except (sqlite3.Error, ValueError):
                        continue
                result.sort(key=lambda r: ("exact" not in r["name"], r["name"]))
                return self.json_reply(result)
            if parsed.path == "/api/peers":
                path = self.query_path(query)
                with self.server.lock:
                    result = self.server.cache.get(str(path))
                if result is None:
                    result = peer_rows(path)
                    with self.server.lock:
                        self.server.cache[str(path)] = result
                return self.json_reply(result)
            if parsed.path == "/api/samples":
                path = self.query_path(query)
                peer = int(query.get("peer", [""])[0])
                start = int(query["start_us"][0]) if "start_us" in query else None
                end = int(query["end_us"][0]) if "end_us" in query else None
                limit = min(2000, max(80, int(query.get("limit", ["600"])[0])))
                data, meta = sample_rows(path, peer, start, end,
                                         query.get("probe", ["0"])[0] == "1",
                                         query.get("exclude_app", ["0"])[0] == "1")
                return self.json_reply({"samples": thin_time(data, limit), "meta": meta})
            self.send_error(404)
        except (ValueError, sqlite3.Error) as exc:
            self.json_reply({"error": str(exc)}, 400)

    def do_POST(self):
        if urlparse(self.path).path != "/api/upload":
            return self.send_error(404)
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= MAX_UPLOAD:
                raise ValueError("DB size must be between 1 byte and 1 GiB")
            raw_name = parse_qs(urlparse(self.path).query).get("name", ["trace.db"])[0]
            clean = Path(raw_name).name
            if not clean.endswith(".db"):
                raise ValueError("Choose a .db file")
            with self.server.lock:
                name = clean
                n = 2
                while name in self.server.registry:
                    name = f"{Path(clean).stem}-{n}.db"
                    n += 1
            path = Path(self.server.uploads.name) / name
            with path.open("wb") as out:
                remaining = length
                while remaining:
                    chunk = self.rfile.read(min(1 << 20, remaining))
                    if not chunk:
                        raise ValueError("Incomplete upload")
                    out.write(chunk)
                    remaining -= len(chunk)
            info = trace_info(path)
            with self.server.lock:
                self.server.registry[name] = path
            self.json_reply({"name": name, **info})
        except (ValueError, sqlite3.Error) as exc:
            self.json_reply({"error": str(exc)}, 400)


def main():
    parser = argparse.ArgumentParser(description="Browse Stardust blocktrace SQLite files")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()
    server = TraceServer((args.host, args.port))
    print(f"Trace browser: http://{args.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
