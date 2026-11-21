# Stardust Trace Explorer

Start the local browser UI from the repository root:

```bash
python3 tools/trace_browser/server.py --port 8765
```

Open `http://127.0.0.1:8765`. The picker lists `trace*.db` files in the repository root. You can also upload a completed `.db` file in the page; uploaded files remain in a temporary directory until the server exits.

The server uses Python's standard SQLite library and queries databases read-only. Files from the repository are opened with their WAL when present. Uploading a single DB file cannot include its WAL; checkpoint or create a SQLite backup before uploading an active trace.

The scatter plot uses `bw_sample.inflight`, smoothed `rtt_us`, and `avg_bw`; it does not use per-block RTT or assume that `inflight` is a cwnd. The default scatter view bins samples by inflight and plots median RTT and median bandwidth. The time chart preserves chronological order, with dashed `max_bw` and `min_rtt` references. Each line track has its own vertical scale. Both charts use the same selected peer, time range, mode and app-limited filter. Time-series thinning retains extrema within each time bucket, while the mode background keeps every mode transition from the unthinned samples.

The time chart shades its background by bandwidth-control mode (`Startup`, `ProbeDown`, `ProbeBW`, `ProbeBW · app-limited`, `SlowDown`, `ProbeRTT`, and `Choked`) so transitions remain visible alongside the metric lines. Click the mode chips below the chart to select multiple modes; unselected mode regions are dimmed. The `全选` button restores all modes. When all modes are selected, clicking one mode consistently switches to that mode alone. The mode filter above the charts can select all samples, ProbeBW only, or ProbeDown only.

Each peer also has an upload-speed chart. It sums `send_block` event bytes in the trailing two seconds and divides by two, showing MiB/s on the same peer-relative time axis and time range as the download chart. These events record blocks handed to peer IO. No trace schema change is required. Peers with only uploads also appear in the list.

The upload chart uses `peer_state_event` OurChoke (orange) and OurUnchoke (teal) transitions to color both the curve and its background. Click the upload state chips to select one or multiple states; hidden intervals are not connected by the curve. `全选` restores every state. State before the selected time range is retained, and connection/disconnection resets to Unknown (gray) until a recorded our-choke transition. Old traces without these transitions show Unknown. Uploads share the time range and sample limit; download mode and app-limited filters apply to the download charts only.
