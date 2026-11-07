const $ = id => document.getElementById(id);
const state = { db: null, peers: [], peer: null, rows: [], meta: null, mode: 'all', view: 'binned', request: 0 };
const modeButtons = [...document.querySelectorAll('[data-mode]')];
const viewButtons = [...document.querySelectorAll('[data-view]')];
const fmt = (n, digits = 1) => Number.isFinite(n) ? n.toLocaleString('zh-CN', { maximumFractionDigits: digits }) : '—';
const median = values => {
  const sorted = [...values].sort((a, b) => a - b);
  const i = Math.floor(sorted.length / 2);
  return sorted.length ? (sorted[i] + sorted[sorted.length - i - 1]) / 2 : 0;
};

async function api(path, options) {
  const response = await fetch(path, options);
  let data;
  try { data = await response.json(); } catch { throw new Error(`HTTP ${response.status}`); }
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}
function status(text, error = false) { $('message').textContent = text; $('message').classList.toggle('error', error); }
function traceOptions(traces, selected) {
  $('dataset').replaceChildren(...traces.map(t => {
    const option = document.createElement('option'); option.value = t.name;
    option.textContent = `${t.name} · ${fmt(t.duration_s, 0)}s`; return option;
  }));
  $('dataset').value = selected;
}
async function loadTraces(selectName) {
  const traces = await api('/api/traces');
  if (!traces.length) { status('未找到 trace 数据库。可导入已 checkpoint 的 .db。'); return; }
  const preferred = selectName || traces.find(t => t.name === 'trace.local.exact.7.db')?.name || traces[0].name;
  traceOptions(traces, preferred);
  await loadDataset(preferred);
}
async function loadDataset(name) {
  state.db = name; state.peer = null; state.rows = [];
  status('正在读取 peer 列表…');
  try {
    state.peers = await api(`/api/peers?db=${encodeURIComponent(name)}`);
    renderPeers();
    if (state.peers.length) await choosePeer(state.peers[0]);
    else status('这份 trace 没有带宽采样。');
  } catch (err) { status(err.message, true); }
}
function renderPeers() {
  const q = $('peerSearch').value.trim().toLowerCase();
  const matched = state.peers.filter(p => q
    ? `${p.id} ${p.addr} ${p.torrent_name}`.toLowerCase().includes(q)
    : p.received_bytes > 0);
  $('peerCount').textContent = q ? `${matched.length} / ${state.peers.length}` : `${matched.length} 有接收`;
  const visible = matched.slice(0, 160);
  $('peerList').replaceChildren(...visible.map(peer => {
    const b = document.createElement('button'); b.type = 'button'; b.className = 'peer-row';
    if (peer.id === state.peer?.id) b.classList.add('active');
    const left = document.createElement('span');
    const id = document.createElement('span'); id.className = 'peer-id'; id.textContent = `#${peer.id} · ${peer.torrent_name || `Torrent ${peer.torrent_id}`}`;
    const addr = document.createElement('span'); addr.className = 'peer-addr'; addr.textContent = peer.addr; left.append(id, addr);
    const value = document.createElement('span'); value.className = 'peer-value'; value.textContent = `${fmt(peer.received_mib)} MiB`;
    b.append(left, value); b.addEventListener('click', () => choosePeer(peer)); return b;
  }));
}
async function choosePeer(peer) {
  state.peer = peer;
  $('peerTitle').textContent = `Peer #${peer.id}`;
  $('torrentName').textContent = peer.torrent_name || `Torrent ${peer.torrent_id}`;
  $('peerMeta').textContent = `${peer.addr} · Torrent ${peer.torrent_id}${peer.connections > 1 ? ` · ${peer.connections} 次连接` : ''}`;
  $('received').textContent = `${fmt(peer.received_mib)} MiB`;
  $('sampleCount').textContent = fmt(peer.samples, 0);
  const duration = Math.max(0, (peer.last_us - peer.first_us) / 1e6);
  $('duration').textContent = `${fmt(duration, 1)} s`;
  $('startSec').value = '0'; $('endSec').value = (Math.ceil(duration * 10) / 10).toFixed(1);
  renderPeers(); await loadSamples();
}
async function loadSamples() {
  if (!state.peer || !state.db) return;
  const request = ++state.request;
  const begin = Number($('startSec').value), end = Number($('endSec').value);
  if (!Number.isFinite(begin) || !Number.isFinite(end) || begin < 0 || end <= begin) { status('时间范围需要满足 0 ≤ 起点 < 终点。', true); return; }
  status('正在读取采样…');
  const params = new URLSearchParams({ db: state.db, peer: state.peer.id, start_us: Math.round(state.peer.first_us + begin * 1e6), end_us: Math.round(state.peer.first_us + end * 1e6), probe: state.mode === 'probe' ? 1 : 0, exclude_app: $('excludeApp').checked ? 1 : 0, limit: $('sampleLimit').value });
  try {
    const data = await api(`/api/samples?${params}`);
    if (request !== state.request) return;
    state.rows = data.samples; state.meta = data.meta; renderCharts();
    const note = data.meta.count > data.samples.length ? `显示 ${data.samples.length} / ${fmt(data.meta.count, 0)} 个时间采样（保留每段的带宽和RTT极值）` : `${data.samples.length} 个采样`;
    status(note);
  } catch (err) { if (request === state.request) status(err.message, true); }
}
function setPressed(buttons, selected, attr) {
  buttons.forEach(b => b.setAttribute('aria-pressed', String(b.dataset[attr] === selected)));
}

function binScatter(rows) {
  const clean = rows.filter(r => r.inflight > 0 && r.bw_mib > 0 && r.rtt_ms > 0).sort((a, b) => a.inflight - b.inflight);
  if (state.view === 'raw' || clean.length <= 14) return clean.map(r => ({ ...r, count: 1 }));
  const bins = Math.min(18, Math.max(8, Math.ceil(Math.sqrt(clean.length))));
  const out = [];
  for (let b = 0; b < bins; b++) {
    const group = clean.slice(Math.floor(b * clean.length / bins), Math.floor((b + 1) * clean.length / bins));
    if (!group.length) continue;
    out.push({ inflight: median(group.map(x => x.inflight)), rtt_ms: median(group.map(x => x.rtt_ms)),
      bw_mib: median(group.map(x => x.bw_mib)), max_bw_mib: median(group.map(x => x.max_bw_mib)),
      min_rtt_ms: median(group.filter(x => x.min_rtt_ms !== null).map(x => x.min_rtt_ms)),
      t: median(group.map(x => x.t)), mode: group[0].mode, count: group.length });
  }
  return out;
}
function canvasSetup(canvas) {
  const rect = canvas.getBoundingClientRect(), ratio = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.round(rect.width * ratio)); canvas.height = Math.max(1, Math.round(rect.height * ratio));
  const ctx = canvas.getContext('2d'); ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, rect.width, rect.height);
  return { ctx, w: rect.width, h: rect.height };
}
function grid(ctx, x, y, w, h, yMax, xMax, xUnit, yUnit) {
  ctx.font = '11px system-ui'; ctx.fillStyle = '#708189'; ctx.strokeStyle = '#e6ebed'; ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const xx = x + w * i / 4, yy = y + h * i / 4;
    ctx.beginPath(); ctx.moveTo(xx, y); ctx.lineTo(xx, y + h); ctx.moveTo(x, yy); ctx.lineTo(x + w, yy); ctx.stroke();
    ctx.textAlign = 'center'; ctx.fillText(fmt(xMax * i / 4, xMax < 10 ? 1 : 0), xx, y + h + 18);
    ctx.textAlign = 'right'; ctx.fillText(fmt(yMax * (4 - i) / 4, yMax < 10 ? 1 : 0), x - 8, yy + 4);
  }
  ctx.fillStyle = '#687880'; ctx.textAlign = 'left'; ctx.fillText(yUnit, x, y - 8);
  ctx.textAlign = 'right'; ctx.fillText(xUnit, x + w, y + h + 35);
}
const niceMax = value => {
  if (!value || value <= 0) return 1;
  const power = 10 ** Math.floor(Math.log10(value));
  return ([1, 2, 2.5, 4, 5, 8, 10].find(n => n * power >= value * 1.05) || 10) * power;
};
function shade(value, max) {
  const fraction = max > 0 ? Math.max(0, Math.min(1, Math.log1p(value) / Math.log1p(max))) : 0;
  const lightness = 77 - fraction * 52;
  return `hsl(176 62% ${lightness}%)`;
}
function showTip(canvas, tip, event, html) {
  const outer = canvas.parentElement.getBoundingClientRect();
  tip.innerHTML = html; tip.hidden = false;
  const px = event.clientX - outer.left, py = event.clientY - outer.top;
  tip.style.left = `${Math.min(px + 14, outer.width - tip.offsetWidth - 8)}px`;
  tip.style.top = `${Math.max(8, Math.min(py - tip.offsetHeight - 8, outer.height - tip.offsetHeight - 8))}px`;
}
function buildModeSegments(rows, start, end) {
  if (!rows.length || end <= start) return [];
  const trackAppLimited = rows.some(row => Object.prototype.hasOwnProperty.call(row, 'app_limited'));
  const appLimitedFor = row => trackAppLimited && row.mode === 'ProbeBW' && row.app_limited === true;
  const createSegment = (row, segmentStart, segmentEnd) => {
    const segment = { mode: row.mode, start: segmentStart, end: segmentEnd };
    if (trackAppLimited) segment.app_limited = appLimitedFor(row);
    return segment;
  };
  const segments = [];
  let current = null;
  for (const row of rows) {
    const t = Math.max(start, Math.min(end, row.t));
    const appLimited = appLimitedFor(row);
    if (!current) {
      current = createSegment(row, t, end);
    } else if (row.mode !== current.mode || (trackAppLimited && appLimited !== current.app_limited)) {
      current.end = t;
      if (current.end > current.start) segments.push(current);
      current = createSegment(row, t, end);
    }
  }
  if (current && current.end > current.start) segments.push(current);
  return segments;
}
function drawScatter() {
  const canvas = $('scatter'), { ctx, w, h } = canvasSetup(canvas);
  const points = binScatter(state.rows); $('scatterCount').textContent = `${points.length} 点`;
  canvas._points = [];
  if (!points.length) { emptyChart(ctx, w, h); return; }
  const margin = { left: 58, right: 23, top: 28, bottom: 51 };
  const plot = { x: margin.left, y: margin.top, w: w - margin.left - margin.right, h: h - margin.top - margin.bottom };
  const xMax = niceMax(Math.max(...points.map(p => p.inflight))), yMax = niceMax(Math.max(...points.map(p => p.rtt_ms)));
  const bwMax = Math.max(...points.map(p => p.bw_mib));
  $('bwHigh').textContent = `${fmt(bwMax, 2)} MiB/s`;
  grid(ctx, plot.x, plot.y, plot.w, plot.h, yMax, xMax, 'inflight / requests', 'RTT / ms');
  const sorted = [...points].sort((a, b) => a.inflight - b.inflight);
  if (state.view === 'binned' && sorted.length > 1) {
    ctx.strokeStyle = '#8bb6b3'; ctx.lineWidth = 1.5; ctx.beginPath();
    sorted.forEach((p, i) => { const x = plot.x + p.inflight / xMax * plot.w, y = plot.y + plot.h - p.rtt_ms / yMax * plot.h; if (i) ctx.lineTo(x, y); else ctx.moveTo(x, y); }); ctx.stroke();
  }
  for (const p of points) {
    const x = plot.x + p.inflight / xMax * plot.w, y = plot.y + plot.h - p.rtt_ms / yMax * plot.h;
    ctx.fillStyle = shade(p.bw_mib, bwMax); ctx.beginPath(); ctx.arc(x, y, state.view === 'raw' ? 3 : 5, 0, 2 * Math.PI); ctx.fill();
    canvas._points.push({ x, y, data: p });
  }
}
function emptyChart(ctx, w, h) { ctx.fillStyle = '#87969c'; ctx.font = '13px system-ui'; ctx.textAlign = 'center'; ctx.fillText('当前筛选范围没有采样', w / 2, h / 2); }
function drawTimeline() {
  const canvas = $('timeline'), { ctx, w, h } = canvasSetup(canvas), rows = state.rows;
  $('timeCount').textContent = `${rows.length} 点`;
  if (!rows.length) { emptyChart(ctx, w, h); return; }
  const left = 64, right = 22, top = 30, bottom = 42, laneGap = 24;
  const laneH = (h - top - bottom - 2 * laneGap) / 3;
  const start = rows[0].t, end = rows[rows.length - 1].t, span = Math.max(1, end - start);
  const maxContinuousGap = Math.max(2.5, span / Math.max(1, rows.length - 1) * 3);
  const x = r => left + (r.t - start) / span * (w - left - right);
  const plotBottom = top + 3 * laneH + 2 * laneGap;
  const modeColors = {
    Startup: 'rgba(226, 177, 55, 0.16)',
    ProbeBW: 'rgba(34, 151, 145, 0.16)',
    'ProbeBW:app_limited': 'rgba(89, 162, 37, 0.16)',
    SlowDown: 'rgba(224, 133, 45, 0.16)',
    ProbeRTT: 'rgba(125, 104, 185, 0.16)',
    Choked: 'rgba(112, 128, 137, 0.16)',
    Unknown: 'rgba(170, 181, 185, 0.13)'
  };
  const modeSegments = buildModeSegments(rows, start, start + span);
  ctx.save();
  for (const segment of modeSegments) {
    const sx = left + (segment.start - start) / span * (w - left - right);
    const ex = left + (segment.end - start) / span * (w - left - right);
    const colorKey = segment.mode === 'ProbeBW' && segment.app_limited ? 'ProbeBW:app_limited' : segment.mode;
    ctx.fillStyle = modeColors[colorKey] || modeColors.Unknown;
    ctx.fillRect(sx, top, Math.max(0, ex - sx), plotBottom - top);
  }
  ctx.restore();
  ctx.save();
  ctx.strokeStyle = 'rgba(129, 146, 153, 0.72)';
  ctx.lineWidth = 1;
  for (let i = 1; i < 3; i++) {
    const dividerY = top + i * laneH + (i - 0.5) * laneGap;
    ctx.beginPath(); ctx.moveTo(left, dividerY); ctx.lineTo(w - right, dividerY); ctx.stroke();
  }
  ctx.restore();
  const lanes = [
    { label: 'BW · MiB/s', key: 'bw_mib', secondary: 'max_bw_mib', color: '#16837e', max: niceMax(Math.max(...rows.map(r => r.max_bw_mib))) },
    { label: 'In-flight · requests', key: 'inflight', color: '#db8a35', max: niceMax(Math.max(...rows.map(r => r.inflight))) },
    { label: 'RTT · ms', key: 'rtt_ms', secondary: 'min_rtt_ms', color: '#3d79b4', max: niceMax(Math.max(...rows.map(r => r.rtt_ms))) }
  ];
  lanes.forEach((lane, i) => {
    const y0 = top + i * (laneH + laneGap), bottom = y0 + laneH;
    ctx.fillStyle = 'rgba(255, 255, 255, 0.94)';
    ctx.fillRect(left, y0 - 19, w - left - right, 22);
    ctx.fillStyle = '#6b7d84'; ctx.font = '11px system-ui'; ctx.textAlign = 'left'; ctx.fillText(lane.label, left, y0 - 7);
    const legendText = lane.key === 'bw_mib' ? 'avg_bw / max_bw' : lane.key === 'rtt_ms' ? 'RTT / min RTT' : 'inflight';
    ctx.font = '10px system-ui';
    const markerWidth = lane.secondary ? 28 : 14;
    const legendWidth = markerWidth + 6 + ctx.measureText(legendText).width;
    const legendX = Math.max(left + ctx.measureText(lane.label).width + 10, w - right - legendWidth);
    const legendY = y0 - 10;
    ctx.strokeStyle = lane.color; ctx.lineWidth = 2; ctx.globalAlpha = 1;
    ctx.beginPath(); ctx.moveTo(legendX, legendY); ctx.lineTo(legendX + 12, legendY); ctx.stroke();
    if (lane.secondary) {
      ctx.setLineDash([3, 3]); ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(legendX + 16, legendY); ctx.lineTo(legendX + 28, legendY); ctx.stroke();
      ctx.setLineDash([]);
    }
    ctx.fillStyle = '#6b7d84'; ctx.textAlign = 'left'; ctx.fillText(legendText, legendX + markerWidth + 6, y0 - 6);
    ctx.strokeStyle = '#e6ebed'; ctx.lineWidth = 1;
    for (let j = 0; j <= 2; j++) {
      const yy = y0 + laneH * j / 2;
      ctx.beginPath(); ctx.moveTo(left, yy); ctx.lineTo(w - right, yy); ctx.stroke();
      ctx.textAlign = 'right'; ctx.fillText(fmt(lane.max * (2 - j) / 2, lane.max < 10 ? 1 : 0), left - 8, yy + 4);
    }
    const drawLine = (key, dashed) => {
      ctx.beginPath(); ctx.strokeStyle = lane.color; ctx.lineWidth = dashed ? 1 : 2; ctx.globalAlpha = dashed ? 0.48 : 1; ctx.setLineDash(dashed ? [4, 4] : []);
      let started = false, previous = null;
      for (const r of rows) {
        if (r[key] === null || r[key] === undefined) { started = false; continue; }
        const yy = bottom - r[key] / lane.max * laneH;
        if (started && previous !== null && r.t - previous <= maxContinuousGap) ctx.lineTo(x(r), yy);
        else ctx.moveTo(x(r), yy);
        started = true; previous = r.t;
      }
      ctx.stroke(); ctx.setLineDash([]); ctx.globalAlpha = 1;
    };
    if (lane.secondary) drawLine(lane.secondary, true);
    drawLine(lane.key, false);
  });
  ctx.fillStyle = '#708189'; ctx.font = '11px system-ui'; ctx.textAlign = 'center';
  for (let i = 0; i <= 4; i++) ctx.fillText(`${fmt(start + span * i / 4, 1)}s`, left + (w - left - right) * i / 4, h - 8);
  canvas._time = { left, right, x, rows, top, laneH, laneGap, h, w, modeSegments };
}
function renderCharts() { drawScatter(); drawTimeline(); }
function setupHover() {
  const scatter = $('scatter'), tip = $('scatterTip');
  scatter.addEventListener('pointermove', event => {
    const rect = scatter.getBoundingClientRect(), x = event.clientX - rect.left, y = event.clientY - rect.top;
    let found = null, distance = 20 ** 2;
    for (const p of scatter._points || []) { const d = (x - p.x) ** 2 + (y - p.y) ** 2; if (d < distance) { found = p; distance = d; } }
    if (!found) { tip.hidden = true; return; }
    const p = found.data;
    showTip(scatter, tip, event, `inflight <b>${fmt(p.inflight, 1)}</b><br>RTT <b>${fmt(p.rtt_ms, 2)} ms</b><br>avg_bw <b>${fmt(p.bw_mib, 3)} MiB/s</b>${p.count > 1 ? `<br>${p.count} 个采样的中位值` : `<br>t = ${fmt(p.t, 2)} s · ${p.mode}`}`);
  });
  scatter.addEventListener('pointerleave', () => { tip.hidden = true; });
  const canvas = $('timeline'), timeTip = $('timeTip');
  canvas.addEventListener('pointermove', event => {
    const state = canvas._time; if (!state) return;
    const rect = canvas.getBoundingClientRect(), px = event.clientX - rect.left;
    let lo = 0, hi = state.rows.length - 1;
    while (lo < hi) { const mid = (lo + hi) >> 1; if (state.x(state.rows[mid]) < px) lo = mid + 1; else hi = mid; }
    const idx = lo > 0 && Math.abs(state.x(state.rows[lo - 1]) - px) < Math.abs(state.x(state.rows[lo]) - px) ? lo - 1 : lo;
    const r = state.rows[idx];
    showTip(canvas, timeTip, event, `<b>t = ${fmt(r.t, 2)} s</b> · ${r.mode}${r.app_limited ? ' · app-limited' : ''}<br>avg_bw ${fmt(r.bw_mib, 3)} / max_bw ${fmt(r.max_bw_mib, 3)} MiB/s<br>inflight ${fmt(r.inflight, 0)} · RTT ${fmt(r.rtt_ms, 2)} ms`);
  });
  canvas.addEventListener('pointerleave', () => { timeTip.hidden = true; });
}

modeButtons.forEach(b => b.addEventListener('click', () => { state.mode = b.dataset.mode; setPressed(modeButtons, state.mode, 'mode'); loadSamples(); }));
viewButtons.forEach(b => b.addEventListener('click', () => { state.view = b.dataset.view; setPressed(viewButtons, state.view, 'view'); drawScatter(); }));
$('dataset').addEventListener('change', event => loadDataset(event.target.value));
$('peerSearch').addEventListener('input', renderPeers);
$('excludeApp').addEventListener('change', loadSamples);
$('sampleLimit').addEventListener('change', loadSamples);
$('applyRange').addEventListener('click', loadSamples);
$('upload').addEventListener('change', async event => {
  const file = event.target.files?.[0]; if (!file) return;
  status(`正在导入 ${file.name}…`);
  try {
    const item = await api(`/api/upload?name=${encodeURIComponent(file.name)}`, { method: 'POST', headers: { 'Content-Type': 'application/octet-stream' }, body: file });
    await loadTraces(item.name);
  } catch (err) { status(err.message, true); }
  event.target.value = '';
});
setupHover();
new ResizeObserver(() => { if (state.rows.length) renderCharts(); }).observe($('scatter').parentElement);
new ResizeObserver(() => { if (state.rows.length) renderCharts(); }).observe($('timeline').parentElement);
loadTraces().catch(err => status(err.message, true));
