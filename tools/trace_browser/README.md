# Stardust Trace Explorer

Start the local browser UI from the repository root:

```bash
python3 tools/trace_browser/server.py --port 8765
```

Open `http://127.0.0.1:8765`. The picker lists `trace*.db` files in the repository root. You can also upload a completed `.db` file in the page; uploaded files remain in a temporary directory until the server exits.

The server uses Python's standard SQLite library and queries databases read-only. Files from the repository are opened with their WAL when present. Uploading a single DB file cannot include its WAL; checkpoint or create a SQLite backup before uploading an active trace.

The scatter plot uses `bw_sample.inflight`, smoothed `rtt_us`, and `avg_bw`; it does not use per-block RTT or assume that `inflight` is a cwnd. The default scatter view bins samples by inflight and plots median RTT and median bandwidth. The time chart preserves chronological order, with dashed `max_bw` and `min_rtt` references. Each line track has its own vertical scale. Both charts use the same selected peer, time range, mode and app-limited filter. Time-series thinning retains extrema within each time bucket.

The time chart shades its background by bandwidth-control mode (`Startup`, `ProbeBW`, `ProbeBW · app-limited`, `SlowDown`, `ProbeRTT`, and `Choked`) so transitions remain visible alongside the metric lines.
