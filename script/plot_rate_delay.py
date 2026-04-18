import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import scipy.stats as stats
import re
import sys
import os
from datetime import datetime

# --- 页面配置 ---
st.set_page_config(page_title="Stardust CC Analyzer Pro", layout="wide")

if 'offset_val' not in st.session_state:
    st.session_state.offset_val = 0

# --- 分布拟合辅助函数 ---
def fit_distributions(delays):
    """拟合多种分布，返回拟合结果"""
    if len(delays) < 2:
        return None

    dist_names = ['norm', 'lognorm', 'gamma', 'expon']
    results = []

    for name in dist_names:
        try:
            dist = getattr(stats, name)
            if name in ['lognorm', 'gamma', 'expon']:
                params = dist.fit(delays, floc=0)
            else:
                params = dist.fit(delays)

            log_likelihood = np.sum(dist.logpdf(delays, *params))
            k_val = params[0] if name in ['gamma', 'lognorm'] else np.nan
            theta_val = params[-1]

            results.append({
                'Distribution': name,
                'Log-Likelihood': log_likelihood,
                'k (shape)': round(k_val, 4),
                'theta (scale)': round(theta_val, 4),
                'params': params,
                'dist': dist
            })
        except Exception:
            pass

    return sorted(results, key=lambda x: x['Log-Likelihood'], reverse=True) if results else None

# --- 核心解析逻辑 ---
@st.cache_data
def parse_log_content(lines):
    # 正则1: 原始采样 (add sample)
    sample_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+add bw sample Some\((?P<delay>[\d.]+)(?P<unit>ns|us|µs|ms|s)\), inflight [\d.]+ inflight when sent Some\((?P<inflight>\d+)\)"
    )
    # Regex for different modes
    # ProbeBW
    probe_re = re.compile(
        r"stardust::transmit_manager: src/transmit_manager\.rs:\d+: (?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+) ProbeBW mode cycle (?P<cycle>\d+) capacity (?P<capacity>\d+) min rtt (?P<min_rtt>[\d.]+)ms probe rtt (?P<probe_rtt>[\d.]+)ms avg bw (?P<bw>[\d.]+) avg_bw_10s (?P<bw10>[\d.]+) max_bw (?P<max_bw>[\d.]+) req in flight (?P<req_if>\d+)"
    )
    # Startup
    startup_re = re.compile(
        r"stardust::transmit_manager: src/transmit_manager\.rs:\d+: (?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+) in (?P<mode>Startup) mode, cwnd (?P<cwnd>\d+), inflight (?P<inflight>\d+) prev max bw (?P<prev_max_bw>[\d.]+), avg-bw (?P<bw>[\d.]+) avg_bw_10s (?P<bw10>[\d.]+) new max bw (?P<max_bw>[\d.]+), limit count (?P<limit_count>\d+)"
    )
    # Slowdown
    slowdown_re = re.compile(
        r"stardust::transmit_manager: src/transmit_manager\.rs:\d+: (?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+) (?P<mode>Slowdown) mode min rtt (?P<min_rtt>[\d.]+)(ms|µs|us|s|ns) avg bw (?P<bw>[\d.]+) avg_bw_10s (?P<bw10>[\d.]+) req in flight (?P<req_if>\d+)"
    )
    # ProbeRTT
    probe_rtt_re = re.compile(
        r"stardust::transmit_manager: src/transmit_manager\.rs:\d+: (?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+) (?P<mode>ProbeRTT) mode min rtt (?P<min_rtt>[\d.]+)(ms|µs|us|s|ns) avg bw (?P<bw>[\d.]+) avg_bw_10s (?P<bw10>[\d.]+) inflight_target (?P<inflight_target>\d+) req in flight (?P<req_if>\d+)"
    )
    # 状态转换
    change_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+change from \w+ to (?P<target_mode>\w+) mode"
    )
    # RTT & Variance
    rtt_var_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+rtt\s+(?P<rtt>[\d.]+)(?P<rtt_unit>ns|us|µs|ms|s)\s+var\s+(?P<var>[\d.]+)(?P<var_unit>ns|us|µs|ms|s)"
    )
    # queue_delay
    queue_delay_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+recv Piece.*from (?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+),\s+queue_delay\s+(?P<val>[\d.]+)(?P<unit>ns|us|µs|ms|s)"
    )
    # rush mode (from block_picker: "{peer} endgame {bool}, rush {bool}")
    rush_re = re.compile(
        r"block_picker\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+endgame (?:true|false), rush (?P<rush>true|false)"
    )

    # --- 核心解析逻辑 ---
    samples, autos, states, rtt_vars, queue_delays, rush_events = [], [], [], [], [], []
    for line in lines:
        try:
            parts = line.split()
            if len(parts) < 1: continue
            dt_str = parts[0].replace('Z', '+00:00')
            dt = datetime.fromisoformat(dt_str)
            ts = dt.timestamp()

            if "add bw sample" in line:
                match = sample_re.search(line)
                if match:
                    raw_delay = float(match.group('delay'))
                    unit = match.group('unit')
                    # Convert to milliseconds
                    if unit == 'µs':
                        delay_ms = raw_delay / 1000.0
                    elif unit == 'us':
                        delay_ms = raw_delay / 1000.0
                    elif unit == 'ns':
                        delay_ms = raw_delay / 1_000_000.0
                    elif unit == 's':
                        delay_ms = raw_delay * 1000.0
                    else:  # ms
                        delay_ms = raw_delay
                    samples.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'delay': delay_ms, 'inflight': int(match.group('inflight'))
                    })
            elif "queue_delay" in line:
                match = queue_delay_re.search(line)
                if match:
                    raw_val = float(match.group('val'))
                    unit = match.group('unit')
                    if unit in ('µs', 'us'):
                        qd_ms = raw_val / 1000.0
                    elif unit == 'ns':
                        qd_ms = raw_val / 1_000_000.0
                    elif unit == 's':
                        qd_ms = raw_val * 1000.0
                    else:
                        qd_ms = raw_val
                    queue_delays.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'queue_delay': qd_ms
                    })
            elif "endgame" in line and "rush" in line:
                match = rush_re.search(line)
                if match:
                    rush_events.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'rush': match.group('rush') == 'true'
                    })
            elif "change from" in line:
                match = change_re.search(line)
                if match:
                    new_mode = match.group('target_mode');
                    states.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'state': new_mode
                    })
            elif "rtt" in line and "var" in line:
                match = rtt_var_re.search(line)
                if match:
                    # RTT ms convert
                    r_val = float(match.group('rtt'))
                    r_unit = match.group('rtt_unit')
                    if r_unit == 's':
                        r_ms = r_val * 1000.0
                    elif r_unit in ('us', 'µs'):
                        r_ms = r_val / 1000.0
                    elif r_unit == 'ns':
                        r_ms = r_val / 1_000_000.0
                    else:
                        r_ms = r_val

                    # Var ms convert
                    v_val = float(match.group('var'))
                    v_unit = match.group('var_unit')
                    if v_unit == 's':
                        v_ms = v_val * 1000.0
                    elif v_unit in ('us', 'µs'):
                        v_ms = v_val / 1000.0
                    elif v_unit == 'ns':
                        v_ms = v_val / 1_000_000.0
                    else:
                        v_ms = v_val

                    rtt_vars.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'rtt': r_ms, 'var': v_ms
                    })
            elif "mode" in line:
                # Try each mode regex
                match = probe_re.search(line)
                mode = "ProbeBW"
                if not match:
                    match = startup_re.search(line)
                    mode = "Startup"
                if not match:
                    match = slowdown_re.search(line)
                    mode = "Slowdown"
                if not match:
                    match = probe_rtt_re.search(line)
                    mode = "ProbeRTT"

                if match:
                    bw_kb = float(match.group('bw')) / 1024.0
                    bw10_kb = float(match.group('bw10')) / 1024.0 if 'bw10' in match.groupdict() and match.group('bw10') else bw_kb
                    peer = match.group('ip_port')

                    data = {
                        'dt': dt, 'ts': ts, 'peer': peer,
                        'bw': bw_kb,
                        'bw10': bw10_kb,
                        'mode': mode
                    }
                    if 'max_bw' in match.groupdict():
                        data['max_bw'] = float(match.group('max_bw')) / 1024.0

                    if mode in ["ProbeBW", "Slowdown", "ProbeRTT"]:
                        data['min_rtt'] = float(match.group('min_rtt'))
                        data['req_if'] = int(match.group('req_if'))
                    if mode == "ProbeBW" and 'probe_rtt' in match.groupdict() and match.group('probe_rtt'):
                        data['probe_rtt'] = float(match.group('probe_rtt'))

                    autos.append(data)
        except Exception: continue

    def _df(rows, cols):
        return pd.DataFrame(rows) if rows else pd.DataFrame(columns=cols)

    return (
        _df(samples,      ['dt', 'ts', 'peer', 'delay', 'inflight']),
        _df(autos,        ['dt', 'ts', 'peer', 'bw', 'bw10', 'mode']),
        _df(states,       ['dt', 'ts', 'peer', 'state']),
        _df(rtt_vars,     ['dt', 'ts', 'peer', 'rtt', 'var']),
        _df(queue_delays, ['dt', 'ts', 'peer', 'queue_delay']),
        _df(rush_events,  ['dt', 'ts', 'peer', 'rush']),
    )

# --- 文件加载 ---
cmd_file = None
if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        if os.path.exists(arg) and arg.endswith(('.log', '.txt')):
            cmd_file = arg
            break

st.sidebar.title("🚀 Stardust 传输分析器")
uploaded_file = st.sidebar.file_uploader("1. 上传日志文件", type=['log', 'txt'])

lines = None
if uploaded_file:
    lines = uploaded_file.read().decode("utf-8").splitlines()
elif cmd_file:
    with open(cmd_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

if lines:
    df_samples, df_autos, df_states, df_rtt_vars, df_queue_delays, df_rush_events = parse_log_content(lines)

    if df_samples.empty and df_autos.empty and df_rtt_vars.empty:
        st.error("❌ 未能识别到有效数据。")
        st.stop()

    # 统计及选择 Peer
    sample_counts = df_queue_delays['peer'].value_counts().to_dict() if not df_queue_delays.empty else {}
    peer_set_s = set(df_samples['peer'].unique()) if not df_samples.empty else set()
    peer_set_a = set(df_autos['peer'].unique()) if not df_autos.empty else set()
    peer_set_r = set(df_rtt_vars['peer'].unique()) if not df_rtt_vars.empty else set()
    peer_set_q = set(df_queue_delays['peer'].unique()) if not df_queue_delays.empty else set()
    all_peers_list = sorted(list(peer_set_s.union(peer_set_a).union(peer_set_r).union(peer_set_q)))

    peer_labels = [f"{p} (Blocks: {sample_counts.get(p, 0)})" for p in all_peers_list]
    peer_labels.sort(key=lambda x: int(re.search(r'Blocks: (\d+)', x).group(1)), reverse=True)
    label_to_peer = {f"{p} (Blocks: {sample_counts.get(p,0)})": p for p in all_peers_list}

    selected_label = st.sidebar.selectbox("2. 选择 Peer", peer_labels)
    selected_peer = label_to_peer[selected_label]

    peer_df = df_samples[df_samples['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True)
    peer_auto = df_autos[df_autos['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True)
    peer_rtt_var = df_rtt_vars[df_rtt_vars['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True)
    peer_queue_delays = df_queue_delays[df_queue_delays['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True) if not df_queue_delays.empty else pd.DataFrame()
    peer_rush_events = df_rush_events[df_rush_events['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True) if not df_rush_events.empty else pd.DataFrame()

    # --- RTT 平滑处理与速率统计 ---
    if not peer_df.empty or not peer_rtt_var.empty:
        st.sidebar.markdown("---")
        st.sidebar.subheader("⚙️ 统计设置")
        # 1. RTT 平滑
        alpha = st.sidebar.slider("RTT 平滑因子 (Alpha)", 0.01, 0.50, 0.125, help="Alpha 越小越平滑")

        if not peer_df.empty:
            peer_df['srtt'] = peer_df['delay'].ewm(alpha=alpha, adjust=False).mean()

        if not peer_rtt_var.empty:
            peer_rtt_var['srtt'] = peer_rtt_var['rtt'].ewm(alpha=alpha, adjust=False).mean()
            peer_rtt_var['svar'] = peer_rtt_var['var'].ewm(alpha=alpha, adjust=False).mean()

        # 2. 采样速率统计 (图3,4)
        min_samples = st.sidebar.number_input("最少统计点数 (N)", value=30, min_value=1)
        min_secs = st.sidebar.number_input("最少时间片段 (秒)", value=1.0, step=0.1)
        sample_size_kb = st.sidebar.number_input("单样本大小 (KiB)", value=16.0)

        if not peer_df.empty:
            ts_vals = peer_df['ts'].values
            full_rates = np.zeros(len(ts_vals))
            last_idx = 0
            for i in range(1, len(ts_vals)):
                count = i - last_idx
                time_diff = ts_vals[i] - ts_vals[last_idx]
                if count >= min_samples or time_diff >= min_secs:
                    avg_rate = (count * sample_size_kb) / max(time_diff, 0.001)
                    full_rates[last_idx:i+1] = avg_rate
                    last_idx = i
            if last_idx < len(ts_vals):
                full_rates[last_idx:] = full_rates[max(0, last_idx-1)] if last_idx > 0 else 0
            peer_df['rate'] = full_rates

    # 视图控制
    st.sidebar.markdown("---")
    offset_pct = st.sidebar.slider("起始位置 (%)", 0, 100, key="offset_slider", value=st.session_state.offset_val)
    st.session_state.offset_val = offset_pct
    window_pct = st.sidebar.slider("展示窗口比例 (%)", 1, 100, 15)

    ref_df = peer_df if not peer_df.empty else (peer_auto if not peer_auto.empty else peer_rtt_var)
    total_len = len(ref_df)
    start_idx = int(total_len * (offset_pct / 100))
    end_idx = min(int(start_idx + total_len * (window_pct / 100)), total_len)

    t_min, t_max = ref_df.iloc[start_idx]['ts'], ref_df.iloc[end_idx-1]['ts']
    sub_df = peer_df[(peer_df['ts'] >= t_min) & (peer_df['ts'] <= t_max)]
    sub_auto = peer_auto[(peer_auto['ts'] >= t_min) & (peer_auto['ts'] <= t_max)]
    sub_rtt_var = peer_rtt_var[(peer_rtt_var['ts'] >= t_min) & (peer_rtt_var['ts'] <= t_max)]
    sub_queue_delays = peer_queue_delays[(peer_queue_delays['ts'] >= t_min) & (peer_queue_delays['ts'] <= t_max)] if not peer_queue_delays.empty else pd.DataFrame()
    sub_rush_events = peer_rush_events[(peer_rush_events['ts'] >= t_min) & (peer_rush_events['ts'] <= t_max)] if not peer_rush_events.empty else pd.DataFrame()

    st.title(f"📊 传输详情: {selected_peer}")

    # 第一部分：旧版四张图 (fig1)
    if not sub_df.empty:
        inf, dl, rt = sub_df['inflight'].values, sub_df['delay'].values, sub_df['rate'].values
        dts, tm = sub_df['dt'].values, np.linspace(0, 1, len(sub_df))
        fig1 = plt.figure(figsize=(16, 12))

        ax1 = fig1.add_subplot(2, 2, 1)
        inf_edges = np.linspace(inf.min(), inf.max(), 11) if inf.min() != inf.max() else [inf.min(), inf.min()+1]
        dl_edges = np.linspace(dl.min(), dl.max(), 11) if dl.min() != dl.max() else [dl.min(), dl.min()+1]
        agg_inf, agg_dl, agg_rt, agg_tm = [], [], [], []
        for i in range(10):
            for j in range(10):
                mask = (inf >= inf_edges[i]) & (inf <= inf_edges[i+1]) & (dl >= dl_edges[j]) & (dl <= dl_edges[j+1])
                if np.any(mask):
                    agg_inf.append(np.mean(inf[mask])); agg_dl.append(np.mean(dl[mask]))
                    agg_rt.append(np.mean(rt[mask])); agg_tm.append(np.mean(tm[mask]))
        if agg_inf:
            max_rt_v = max(agg_rt) if max(agg_rt) > 0 else 1
            ax1.scatter(agg_inf, agg_dl, s=[(r/max_rt_v*2000)+100 for r in agg_rt], c=agg_tm,
                        cmap='rainbow_r', alpha=0.6, edgecolors='black', vmin=0, vmax=1)
        ax1.set_title("1. Aggregate: Inflight-Delay (Size=Rate, Color=Time)"); ax1.set_ylabel("Delay (ms)"); ax1.grid(True, alpha=0.2)

        ax2 = fig1.add_subplot(2, 2, 2)
        ax2.scatter(inf, dl, c=tm, cmap='rainbow_r', s=40, alpha=0.6, edgecolors='black', linewidths=0.1, vmin=0, vmax=1)
        ax2.set_title("2. Raw Sample Distribution"); ax2.set_ylabel("Delay (ms)"); ax2.grid(True, alpha=0.2)

        ax3 = fig1.add_subplot(2, 2, 3)
        ax3.scatter(inf, rt, c=tm, cmap='rainbow_r', s=40, alpha=0.6, edgecolors='black', linewidths=0.1, vmin=0, vmax=1)
        ax3.set_title("3. Inflight vs Smooth Rate"); ax3.set_xlabel("Inflight"); ax3.set_ylabel("Rate (KB/s)"); ax3.grid(True, alpha=0.2)

        ax4 = fig1.add_subplot(2, 2, 4)
        ax4.plot(dts, rt, color='#1f77b4', linewidth=1.5, alpha=0.3)
        ax4.scatter(dts, rt, c=tm, cmap='rainbow_r', s=30, alpha=0.8, edgecolors='black', vmin=0, vmax=1)
        ax4.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.setp(ax4.get_xticklabels(), rotation=30, ha='right')
        ax4.set_title("4. Calculated Download Speed"); ax4.set_ylabel("Rate (KB/s)"); ax4.grid(True, alpha=0.5)

        plt.tight_layout()
        st.pyplot(fig1)

    # 第二部分：Auto Mode 及平滑 RTT (fig2)
    if not sub_auto.empty or not sub_df.empty:
        st.markdown("---")
        st.subheader("📈 算法决策与平滑 RTT 分析 (Auto Mode)")

        fig2 = plt.figure(figsize=(16, 18))

        # 图 5: Avg BW with Mode-based Coloring
        ax5 = fig2.add_subplot(3, 2, 1)
        if not sub_auto.empty:
            # Color mapping for modes
            mode_colors = {
                'Startup': '#1f77b4',  # Blue
                'Slowdown': '#ff7f0e', # Orange
                'ProbeBW': '#2ca02c',  # Green
                'ProbeRTT': '#d62728'  # Red
            }

            # Group by mode and plot colored segments
            # We use a loop to plot segments to show different colors for different modes
            # To handle continuous line with different colors, we can plot each point with its mode color
            # or segments. Segments are better for performance.

            current_mode = None
            start_idx = 0
            for i in range(len(sub_auto)):
                mode = sub_auto.iloc[i]['mode']
                if current_mode is None:
                    current_mode = mode
                    start_idx = i
                elif mode != current_mode:
                    # Plot previous segment
                    segment = sub_auto.iloc[start_idx:i+1] # Include the first point of next segment to connect
                    ax5.plot(segment['dt'], segment['bw'], color=mode_colors.get(current_mode, '#2ca02c'), linewidth=2, label=f"{current_mode} avg-bw" if f"{current_mode} avg-bw" not in [l.get_label() for l in ax5.get_lines()] else "")
                    if 'max_bw' in segment.columns and current_mode in ['Startup', 'ProbeBW']:
                        ax5.plot(segment['dt'], segment['max_bw'], color=mode_colors.get(current_mode, '#2ca02c'), linestyle=':', linewidth=1.5, alpha=0.8, label=f"{current_mode} max-bw" if f"{current_mode} max-bw" not in [l.get_label() for l in ax5.get_lines()] else "")
                    ax5.fill_between(segment['dt'], segment['bw'], color=mode_colors.get(current_mode, '#2ca02c'), alpha=0.1)
                    current_mode = mode
                    start_idx = i

            # Plot last segment
            segment = sub_auto.iloc[start_idx:]
            ax5.plot(segment['dt'], segment['bw'], color=mode_colors.get(current_mode, '#2ca02c'), linewidth=2, label=f"{current_mode} avg-bw" if f"{current_mode} avg-bw" not in [l.get_label() for l in ax5.get_lines()] else "")
            if 'max_bw' in segment.columns and current_mode in ['Startup', 'ProbeBW']:
                ax5.plot(segment['dt'], segment['max_bw'], color=mode_colors.get(current_mode, '#2ca02c'), linestyle=':', linewidth=1.5, alpha=0.8, label=f"{current_mode} max-bw" if f"{current_mode} max-bw" not in [l.get_label() for l in ax5.get_lines()] else "")
            ax5.fill_between(segment['dt'], segment['bw'], color=mode_colors.get(current_mode, '#2ca02c'), alpha=0.1)

            ax5.legend(loc='upper left', fontsize='small')

        # Overlay 10s avg bandwidth curve
        if not sub_auto.empty and 'bw10' in sub_auto.columns:
            ax5.plot(sub_auto['dt'], sub_auto['bw10'], color='black', linewidth=1.5,
                     linestyle='--', alpha=0.7, label='Avg BW 10s')
            ax5.legend(loc='upper left', fontsize='small')

        ax5.set_title("5. Logged Avg Bandwidth (KB/s) - Mode Colored"); ax5.set_ylabel("Rate (KB/s)"); ax5.grid(True, alpha=0.3)

        # 图 6: Inflight
        ax6 = fig2.add_subplot(3, 2, 2)
        if not sub_auto.empty:
            if 'opt_if' in sub_auto.columns:
                ax6.step(sub_auto['dt'], sub_auto['opt_if'], where='post', label='Optimum Inflight', color='#1f77b4')
            if 'req_if' in sub_auto.columns:
                ax6.step(sub_auto['dt'], sub_auto['req_if'], where='post', label='Req In Flight', color='#ff7f0e', linestyle='--')
        ax6.set_title("6. Optimum vs Requested Inflight"); ax6.legend(); ax6.grid(True, alpha=0.3)

        # 图 7: Min RTT
        ax7 = fig2.add_subplot(3, 2, 3)
        if not sub_auto.empty:
            ax7.plot(sub_auto['dt'], sub_auto['min_rtt'], color='#d62728')
        ax7.set_title("7. Logged Min RTT Trend"); ax7.set_ylabel("ms"); ax7.grid(True, alpha=0.3)

        # 图 8: 平滑 RTT (新增)
        ax8 = fig2.add_subplot(3, 2, 4)
        if not sub_df.empty:
            ax8.plot(sub_df['dt'], sub_df['delay'], color='gray', alpha=0.2, label='Raw Delay')
            ax8.plot(sub_df['dt'], sub_df['srtt'], color='#9467bd', linewidth=2, label='Smoothed RTT')
            if not sub_auto.empty:
                ax8.step(sub_auto['dt'], sub_auto['min_rtt'], where='post', color='#d62728', linestyle=':', alpha=0.7, label='Base Min RTT')
        ax8.set_title(f"8. Calculated Smoothed RTT (α={alpha})"); ax8.set_ylabel("ms"); ax8.legend(); ax8.grid(True, alpha=0.3)

        # 图 9: 状态转换 (新增)
        ax9 = fig2.add_subplot(3, 2, 5) # 放在第 5 个位置
        if not df_states.empty and 'peer' in df_states.columns:
            sub_states = df_states[(df_states['peer'] == selected_peer) & (df_states['ts'] >= t_min) & (df_states['ts'] <= t_max)].copy()
        else:
            sub_states = pd.DataFrame()
        if not sub_states.empty:
            # 将模式名映射为数值用于绘图
            modes = sorted(sub_states['state'].unique())
            mode_map = {mode: i for i, mode in enumerate(modes)}
            sub_states['state_val'] = sub_states['state'].map(mode_map)

            ax9.step(sub_states['dt'], sub_states['state_val'], where='post', marker='o', color='#7f7f7f', linewidth=2)
            ax9.set_yticks(list(mode_map.values()))
            ax9.set_yticklabels(list(mode_map.keys()))
            ax9.set_title("9. Transport Mode States")
        else:
            ax9.text(0.5, 0.5, "No State Changes in Window", ha='center')
        ax9.grid(True, alpha=0.3)

        for ax in [ax5, ax6, ax7, ax8, ax9]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

        plt.tight_layout()
        st.pyplot(fig2)

    # 第三部分：RTT 分布拟合分析 (fig3)
    if not sub_df.empty:
        st.markdown("---")
        st.subheader("📊 RTT 分布拟合分析")

        delays = sub_df['delay'].values
        fitted_results = fit_distributions(delays)

        if fitted_results and len(delays) > 1:
            fig3 = plt.figure(figsize=(14, 6))
            ax_dist = fig3.add_subplot(1, 1, 1)

            # 绘制直方图
            ax_dist.hist(delays, bins=50, density=True, alpha=0.3, color='gray', label='RTT Samples')

            # 绘制拟合曲线
            x = np.linspace(delays.min(), delays.max(), 1000)
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

            for idx, result in enumerate(fitted_results[:4]):
                name = result['Distribution']
                params = result['params']
                dist = result['dist']
                pdf = dist.pdf(x, *params)
                ll = result['Log-Likelihood']
                ax_dist.plot(x, pdf, label=f"{name} (LL: {ll:.2f})", linewidth=2, color=colors[idx % len(colors)])

            ax_dist.set_xlabel('RTT (ms)')
            ax_dist.set_ylabel('Density')
            ax_dist.set_title(f'RTT Distribution - {selected_peer}')
            ax_dist.legend(loc='upper right')
            ax_dist.grid(True, alpha=0.2)
            st.pyplot(fig3)

            # 显示拟合参数表
            st.write("**拟合分布参数:**")
            results_df = pd.DataFrame([
                {
                    'Distribution': r['Distribution'],
                    'Log-Likelihood': f"{r['Log-Likelihood']:.2f}",
                    'k (shape)': r['k (shape)'],
                    'theta (scale)': r['theta (scale)']
                }
                for r in fitted_results
            ])
            st.dataframe(results_df, width='stretch')

    # 第四部分：RTT 与 Variance 走势图 (来自 plot_rtt_var)
    if not sub_rtt_var.empty:
        st.markdown("---")
        st.subheader("📈 协议层 RTT 与 Variance 走势")

        fig4, (ax_r1, ax_r2) = plt.subplots(2, 1, figsize=(16, 10))

        dts_rtt = sub_rtt_var['dt']
        rtt_vals = sub_rtt_var['rtt']
        var_vals = sub_rtt_var['var']
        srtt_vals = sub_rtt_var['srtt']
        svar_vals = sub_rtt_var['svar']

        # 上图：RTT 原值与平滑值
        ax_r1.plot(dts_rtt, rtt_vals, marker='o', linestyle='-', linewidth=0.5,
                  markersize=3, alpha=0.5, label='Raw RTT', color='lightblue')
        ax_r1.plot(dts_rtt, srtt_vals, linestyle='-', linewidth=2,
                  label=f'Smoothed RTT (α={alpha})', color='darkblue')

        upper = srtt_vals + svar_vals
        lower = (srtt_vals - svar_vals).clip(lower=0)
        ax_r1.fill_between(dts_rtt, lower, upper, alpha=0.2, color='blue', label='±Variance')

        ax_r1.set_ylabel('RTT (ms)', fontsize=11)
        ax_r1.set_title(f'Round Trip Time for {selected_peer}', fontsize=12, fontweight='bold')
        ax_r1.grid(True, alpha=0.3)
        ax_r1.legend(loc='best')
        ax_r1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.setp(ax_r1.get_xticklabels(), rotation=30, ha='right')

        # 下图：Variance 原值与平滑值
        ax_r2.plot(dts_rtt, var_vals, marker='s', linestyle='-', linewidth=0.5,
                  markersize=3, alpha=0.5, label='Raw Variance', color='lightsalmon')
        ax_r2.plot(dts_rtt, svar_vals, linestyle='-', linewidth=2,
                  label=f'Smoothed Variance (α={alpha})', color='darkred')
        ax_r2.fill_between(dts_rtt, 0, svar_vals, alpha=0.2, color='red')
        ax_r2.set_ylabel('Variance (ms)', fontsize=11)
        ax_r2.set_xlabel('Time', fontsize=11)
        ax_r2.set_title('RTT Variance Tracker', fontsize=12, fontweight='bold')
        ax_r2.grid(True, alpha=0.3)
        ax_r2.legend(loc='best')
        ax_r2.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.setp(ax_r2.get_xticklabels(), rotation=30, ha='right')

        plt.tight_layout()
        st.pyplot(fig4)

    # 第五部分：Queue Delay & Rush Mode 关系图
    if not sub_queue_delays.empty:
        st.markdown("---")
        st.subheader("⏱️ Queue Delay vs Rush Mode")

        # Build rush mode intervals from events: find contiguous True spans
        def get_rush_intervals(rush_df):
            """Return list of (start_dt, end_dt) for rush=True spans."""
            intervals = []
            if rush_df.empty:
                return intervals
            in_rush = False
            start_dt = None
            for _, row in rush_df.iterrows():
                if row['rush'] and not in_rush:
                    in_rush = True
                    start_dt = row['dt']
                elif not row['rush'] and in_rush:
                    in_rush = False
                    intervals.append((start_dt, row['dt']))
            if in_rush and start_dt is not None:
                intervals.append((start_dt, rush_df.iloc[-1]['dt']))
            return intervals

        rush_intervals = get_rush_intervals(sub_rush_events) if not sub_rush_events.empty else []

        fig5, (ax_qd, ax_rush) = plt.subplots(2, 1, figsize=(16, 10), sharex=True)

        # Top: queue_delay over time
        qd_vals = sub_queue_delays['queue_delay'].values
        qd_dts = sub_queue_delays['dt'].values

        ax_qd.plot(qd_dts, qd_vals, color='gray', linewidth=0.5, alpha=0.4, label='Raw queue_delay')
        # EWMA smoothed
        qd_smooth = sub_queue_delays['queue_delay'].ewm(alpha=0.2, adjust=False).mean().values
        ax_qd.plot(qd_dts, qd_smooth, color='#e377c2', linewidth=2, label='Smoothed (α=0.2)')

        # Shade rush mode regions
        for (rs, re_) in rush_intervals:
            ax_qd.axvspan(rs, re_, color='#ff7f0e', alpha=0.15, label='Rush mode' if (rs, re_) == rush_intervals[0] else "")

        ax_qd.set_ylabel('Queue Delay (ms)')
        ax_qd.set_title('Queue Delay over Time (orange = rush mode active)')
        ax_qd.legend(loc='upper left', fontsize='small')
        ax_qd.grid(True, alpha=0.3)
        ax_qd.set_yscale('symlog', linthresh=1.0)

        # Bottom: rush mode as step plot (True=1, False=0) — shows when global picker is in rush
        if not sub_rush_events.empty:
            rush_y = sub_rush_events['rush'].astype(int).values
            rush_dts = sub_rush_events['dt'].values
            ax_rush.step(rush_dts, rush_y, where='post', color='#ff7f0e', linewidth=2)
            ax_rush.fill_between(rush_dts, rush_y, step='post', color='#ff7f0e', alpha=0.3)
            ax_rush.set_yticks([0, 1])
            ax_rush.set_yticklabels(['Normal', 'Rush'])
        else:
            ax_rush.text(0.5, 0.5, 'No rush mode events in window', ha='center', transform=ax_rush.transAxes)

        ax_rush.set_ylabel('Picker Mode')
        ax_rush.set_title('Rush Mode State (all peers)')
        ax_rush.grid(True, alpha=0.3)

        for ax in [ax_qd, ax_rush]:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

        plt.tight_layout()
        st.pyplot(fig5)

        # Scatter: queue_delay vs rush state — join on nearest timestamp
        if not sub_rush_events.empty:
            st.subheader("📊 Queue Delay Distribution: Rush vs Normal")
            # For each queue_delay point, find the most recent rush state
            qd_df2 = sub_queue_delays.copy()
            rush_sorted = sub_rush_events.sort_values('ts')
            def last_rush_state(ts_val):
                idx = rush_sorted['ts'].searchsorted(ts_val, side='right') - 1
                if idx < 0:
                    return False
                return bool(rush_sorted.iloc[idx]['rush'])
            qd_df2['in_rush'] = qd_df2['ts'].apply(last_rush_state)

            rush_qd = qd_df2[qd_df2['in_rush']]['queue_delay'].values
            normal_qd = qd_df2[~qd_df2['in_rush']]['queue_delay'].values

            fig6, axes6 = plt.subplots(1, 2, figsize=(14, 5))
            max_val = max(qd_df2['queue_delay'].max(), 1.0)
            bins = np.logspace(np.log10(0.01), np.log10(max_val + 1), 60)

            if len(rush_qd) > 0:
                axes6[0].hist(rush_qd, bins=bins, color='#ff7f0e', alpha=0.7, label=f'Rush (n={len(rush_qd)})')
            if len(normal_qd) > 0:
                axes6[0].hist(normal_qd, bins=bins, color='#1f77b4', alpha=0.7, label=f'Normal (n={len(normal_qd)})')
            axes6[0].set_xscale('log')
            axes6[0].set_xlabel('Queue Delay (ms, log scale)')
            axes6[0].set_ylabel('Count')
            axes6[0].set_title('Queue Delay Histogram by Mode')
            axes6[0].legend()
            axes6[0].grid(True, alpha=0.3)

            box_data = [d for d in [rush_qd, normal_qd] if len(d) > 0]
            box_labels = [lbl for lbl, d in zip(['Rush', 'Normal'], [rush_qd, normal_qd]) if len(d) > 0]
            if box_data:
                axes6[1].boxplot(box_data, tick_labels=box_labels, patch_artist=True,
                                 boxprops=dict(facecolor='#ff7f0e', alpha=0.6),
                                 notch=True)
                axes6[1].set_yscale('symlog', linthresh=1.0)
                axes6[1].set_ylabel('Queue Delay (ms, symlog)')
                axes6[1].set_title('Box Plot: Queue Delay Rush vs Normal')
                axes6[1].grid(True, alpha=0.3)
                rush_med = np.median(rush_qd) if len(rush_qd) > 0 else 0
                norm_med = np.median(normal_qd) if len(normal_qd) > 0 else 0
                st.info(f"Median queue_delay — Rush: **{rush_med:.2f} ms** | Normal: **{norm_med:.2f} ms**")

            plt.tight_layout()
            st.pyplot(fig6)

    # 底部指标卡
    st.markdown("---")
    c1, c2, c3, c4 = st.columns(4)
    if not sub_df.empty:
        c1.metric("计算平均延迟", f"{sub_df['delay'].mean():.1f} ms")
        c2.metric("计算峰值速率", f"{sub_df['rate'].max():.2f} KB/s")
    if not sub_auto.empty:
        c3.metric("日志 Avg BW", f"{sub_auto['bw'].iloc[-1]:.2f} KB/s")
        c4.metric("最新 Min RTT", f"{sub_auto['min_rtt'].iloc[-1]} ms")
else:
    st.info("👋 请上传 Stardust 日志文件开始分析。")