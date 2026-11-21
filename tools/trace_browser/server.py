"""Local, read-only browser for blocktrace SQLite databases."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import tempfile
import threading
from collections import defaultdict, deque
from contextlib import closing
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[2]
STATIC = Path(__file__).resolve().parent
MIB = 1 << 20
MAX_UPLOAD = 1024 * 1024 * 1024
MODES = {1: "Startup", 2: "ProbeBW", 3: "SlowDown", 4: "ProbeRTT", 5: "Choked", 6: "ProbeDown"}
UPLOAD_WINDOW_US = 2_000_000
OUR_STATES = {1: "Unknown", 2: "Unknown", 7: "OurChoke", 8: "OurUnchoke"}
# Both charts use one peer-relative clock, including peers that only upload.
ACTIVITY = """
    SELECT peer_id, ts_us FROM bw_sample
    UNION ALL SELECT peer_id, ts_us FROM block_event WHERE kind=10
    UNION ALL SELECT peer_id, ts_us FROM peer_state_event WHERE kind IN (1,2,7,8)
"""


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
            f"SELECT (SELECT count(*) FROM bw_sample), min(ts_us), max(ts_us) FROM ({ACTIVITY})"
        ).fetchone()
        return {"samples": count, "start_us": start, "end_us": end,
                "duration_s": round((end - start) / 1e6, 3) if start is not None else 0}


def peer_rows(path: Path) -> list[dict]:
    with closing(open_db(path)) as db:
        rows = db.execute(f"""
            WITH activity AS ({ACTIVITY})
            SELECT p.id, p.torrent_id, p.addr, coalesce(t.name, '') torrent_name,
                   coalesce(r.bytes, 0) received_bytes, coalesce(r.blocks, 0) blocks,
                   coalesce(u.bytes, 0) sent_bytes, coalesce(u.blocks, 0) sent_blocks,
                   coalesce(s.samples, 0) samples, a.first_us, a.last_us,
                   coalesce(conn.connections, 0) connections
              FROM peer p JOIN torrent t ON t.id=p.torrent_id
              LEFT JOIN (SELECT peer_id, sum(coalesce(bytes,16384)) bytes, count(*) blocks
                           FROM block_event WHERE kind=3 GROUP BY peer_id) r ON r.peer_id=p.id
              LEFT JOIN (SELECT peer_id, count(*) samples, min(ts_us) first_us,
                                max(ts_us) last_us FROM bw_sample GROUP BY peer_id) s ON s.peer_id=p.id
              LEFT JOIN (SELECT peer_id, sum(coalesce(bytes,16384)) bytes, count(*) blocks
                           FROM block_event WHERE kind=10 GROUP BY peer_id) u ON u.peer_id=p.id
              LEFT JOIN (SELECT peer_id, min(ts_us) first_us, max(ts_us) last_us
                           FROM activity GROUP BY peer_id) a ON a.peer_id=p.id
              LEFT JOIN (SELECT peer_id, count(*) connections FROM peer_state_event
                          WHERE kind=1 GROUP BY peer_id) conn ON conn.peer_id=p.id
             WHERE s.samples>0 OR u.blocks>0
             ORDER BY coalesce(r.bytes,0)+coalesce(u.bytes,0) DESC, s.samples DESC, p.id
        """).fetchall()
        return [{**dict(r), "received_mib": round(r["received_bytes"] / MIB, 3),
                 "sent_mib": round(r["sent_bytes"] / MIB, 3)} for r in rows]


def peer_extent(db: sqlite3.Connection, peer_id: int):
    return db.execute(
        f"SELECT min(ts_us), max(ts_us) FROM ({ACTIVITY}) WHERE peer_id=?", (peer_id,)
    ).fetchone()


def sample_rows(path: Path, peer_id: int, start_us: int | None, end_us: int | None,
                probe_only: bool, exclude_app_limited: bool,
                mode_id: int | None = None) -> tuple[list[dict], dict]:
    clauses = ["peer_id=?", "inflight IS NOT NULL", "rtt_us IS NOT NULL", "rtt_us>0"]
    args: list[int] = [peer_id]
    if start_us is not None:
        clauses.append("ts_us>=?")
        args.append(start_us)
    if end_us is not None:
        clauses.append("ts_us<=?")
        args.append(end_us)
    if probe_only:
        mode_id = 2
    if mode_id is not None:
        clauses.append("mode=?")
        args.append(mode_id)
    if exclude_app_limited:
        clauses.append("app_limited=0")
    where = " AND ".join(clauses)
    with closing(open_db(path)) as db:
        extent = peer_extent(db, peer_id)
        if extent[0] is None:
            return [], {"first_us": None, "last_us": None, "count": 0,
                        "mode_segments": []}
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
    return data, {"first_us": extent[0], "last_us": extent[1], "count": len(data),
                  "mode_segments": mode_segments(data)}


def upload_rows(path: Path, peer_id: int, start_us: int | None,
                end_us: int | None) -> tuple[list[dict], dict]:
    """Two-second rate of blocks handed to peer IO, with exact our-state transitions."""
    with closing(open_db(path)) as db:
        first, last = peer_extent(db, peer_id)
        if first is None:
            return [], {"count": 0, "mode_segments": []}
        start = max(first, start_us if start_us is not None else first)
        end = min(last, end_us if end_us is not None else last)
        if end < start:
            return [], {"count": 0, "mode_segments": []}
        blocks = db.execute(
            "SELECT ts_us, coalesce(bytes,16384) bytes FROM block_event "
            "WHERE peer_id=? AND kind=10 AND ts_us>? AND ts_us<=? ORDER BY ts_us,id",
            (peer_id, start - UPLOAD_WINDOW_US, end),
        ).fetchall()
        # Include state history before the range so a cropped view inherits it.
        states = db.execute(
            "SELECT ts_us, kind FROM peer_state_event WHERE peer_id=? "
            "AND kind IN (1,2,7,8) AND ts_us<=? ORDER BY ts_us,id", (peer_id, end),
        ).fetchall()

    times = {start, end}
    for block in blocks:
        for timestamp in (block["ts_us"], block["ts_us"] + UPLOAD_WINDOW_US):
            if start <= timestamp <= end:
                times.add(timestamp)
    times.update(row["ts_us"] for row in states if start <= row["ts_us"] <= end)
    pending = deque()
    total = block_index = state_index = 0
    our_state = "Unknown"
    rows = []
    for timestamp in sorted(times):
        while block_index < len(blocks) and blocks[block_index]["ts_us"] <= timestamp:
            block = blocks[block_index]
            pending.append(block)
            total += block["bytes"]
            block_index += 1
        while pending and pending[0]["ts_us"] <= timestamp - UPLOAD_WINDOW_US:
            total -= pending.popleft()["bytes"]
        while state_index < len(states) and states[state_index]["ts_us"] <= timestamp:
            our_state = OUR_STATES[states[state_index]["kind"]]
            state_index += 1
        rows.append({"ts_us": timestamp, "t": (timestamp - first) / 1e6,
                     "upload_bw_mib": total / (UPLOAD_WINDOW_US / 1e6) / MIB,
                     "mode": our_state})
    return rows, {"count": len(rows), "mode_segments": mode_segments(rows),
                  "start_s": (start - first) / 1e6, "end_s": (end - first) / 1e6,
                  "window_s": UPLOAD_WINDOW_US / 1e6}


def mode_key(row: dict) -> tuple[object, bool]:
    mode = row.get("mode")
    return mode, bool(row.get("app_limited")) if mode == "ProbeBW" else False


def mode_segments(rows: list[dict]) -> list[dict]:
    """Return every mode interval before metric samples are thinned."""
    if not rows:
        return []

    segments: list[dict] = []
    current = rows[0]
    start = rows[0]["t"]
    current_key = mode_key(current)
    for row in rows[1:]:
        row_key = mode_key(row)
        if row_key == current_key:
            continue
        segments.append({"mode": current["mode"],
                         "app_limited": mode_key(current)[1],
                         "start": start, "end": row["t"]})
        current = row
        current_key = row_key
        start = row["t"]
    segments.append({"mode": current["mode"],
                     "app_limited": mode_key(current)[1],
                     "start": start, "end": rows[-1]["t"]})
    return segments


def thin_time(rows: list[dict], limit: int,
              metric_keys=("rtt_ms", "bw_mib")) -> list[dict]:
    if len(rows) <= limit:
        return rows

    # Keep mode transitions before metric extrema. A short-lived mode may have
    # only one sample, and dropping that sample makes its timeline region
    # disappear even though the state was present in the trace.
    picked = {0, len(rows) - 1}
    transitions = [
        index for index in range(1, len(rows))
        if mode_key(rows[index - 1]) != mode_key(rows[index])
    ]
    transition_budget = max(0, limit - len(picked))
    if len(transitions) <= transition_budget:
        picked.update(transitions)
    elif transition_budget:
        for slot in range(transition_budget):
            transition = round(slot * (len(transitions) - 1) / max(1, transition_budget - 1))
            picked.add(transitions[transition])

    # Fill the remaining budget with each bucket's low/high RTT and bandwidth
    # so short-lived states stay visible without losing metric spikes.
    bucket_count = max(1, (limit - 2) // 4)
    for bucket in range(bucket_count):
        if len(picked) >= limit:
            break
        lo = math.floor(bucket * len(rows) / bucket_count)
        hi = math.floor((bucket + 1) * len(rows) / bucket_count)
        part = range(lo, hi)
        for key in metric_keys:
            if len(picked) >= limit:
                break
            picked.add(min(part, key=lambda i: rows[i][key]))
            if len(picked) < limit:
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
                                         query.get("exclude_app", ["0"])[0] == "1",
                                         6 if query.get("mode", [""])[0] == "probe-down" else None)
                uploads, upload_meta = upload_rows(path, peer, start, end)
                return self.json_reply({"samples": thin_time(data, limit), "meta": meta,
                                        "upload_samples": thin_time(uploads, limit, ("upload_bw_mib",)),
                                        "upload_meta": upload_meta})
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
