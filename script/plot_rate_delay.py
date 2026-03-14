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
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+add\s+sample\s+Some\((?P<delay>[\d.]+)(?P<unit>µs|ms|s)\),\s+inflight\s+when\s+sent\s+Some\((?P<inflight>\d+)\)"
    )
    # 正则2: 自动模式指标 (Auto mode)
    auto_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+Auto mode optimum inflight (?P<opt_if>\d+) min rtt (?P<min_rtt>[\d.]+)(?P<min_rtt_unit>µs|ms|s)? avg bw (?P<bw>\d+) req in flight (?P<req_if>\d+)"
    )
    # 正则3: 状态转换
    change_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+change from \w+ to (?P<target_mode>\w+) mode"
    )

    samples, autos, states = [], [], []
    for line in lines:
        try:
            parts = line.split()
            if len(parts) < 1: continue
            dt_str = parts[0].replace('Z', '+00:00')
            dt = datetime.fromisoformat(dt_str)
            ts = dt.timestamp()

            if "add sample" in line:
                match = sample_re.search(line)
                if match:
                    raw_delay = float(match.group('delay'))
                    unit = match.group('unit')
                    # Convert to milliseconds
                    if unit == 'µs':
                        delay_ms = raw_delay / 1000.0
                    elif unit == 's':
                        delay_ms = raw_delay * 1000.0
                    else:  # ms
                        delay_ms = raw_delay
                    samples.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'delay': delay_ms, 'inflight': int(match.group('inflight'))
                    })
            elif "change from" in line:
                match = change_re.search(line)
                if match:
                    new_mode = match.group('target_mode');
                    new_mode_f = 0.0
                    if new_mode == "Auto":
                        new_mode_f = 1.0
                    elif new_mode == "SlowDown":
                        new_mode_f = 2.0
                    elif new_mode == "Probe":
                        new_mode_f = 3.0
                    states.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'state': new_mode
                    })
            elif "Auto mode" in line:
                match = auto_re.search(line)
                if match:
                    bw_kb = float(match.group('bw')) / 1024.0
                    min_rtt_val = float(match.group('min_rtt'))
                    min_rtt_unit = match.group('min_rtt_unit') or 'ms'  # default to ms if not specified
                    # Convert to milliseconds
                    if min_rtt_unit == 'µs':
                        min_rtt_ms = min_rtt_val / 1000.0
                    elif min_rtt_unit == 's':
                        min_rtt_ms = min_rtt_val * 1000.0
                    else:  # ms
                        min_rtt_ms = min_rtt_val
                    autos.append({
                        'dt': dt, 'ts': ts, 'peer': match.group('ip_port'),
                        'opt_if': int(match.group('opt_if')),
                        'min_rtt': min_rtt_ms,
                        'bw': bw_kb,
                        'req_if': int(match.group('req_if'))
                    })
        except Exception: continue

    return pd.DataFrame(samples), pd.DataFrame(autos), pd.DataFrame(states)

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
    df_samples, df_autos, df_states = parse_log_content(lines)

    if df_samples.empty and df_autos.empty:
        st.error("❌ 未能识别到有效数据。")
        st.stop()

    # 统计及选择 Peer
    sample_counts = df_samples['peer'].value_counts().to_dict() if not df_samples.empty else {}
    peer_set_s = set(df_samples['peer'].unique()) if not df_samples.empty else set()
    peer_set_a = set(df_autos['peer'].unique()) if not df_autos.empty else set()
    all_peers_list = sorted(list(peer_set_s.union(peer_set_a)))

    peer_labels = [f"{p} (Blocks: {sample_counts.get(p, 0)})" for p in all_peers_list]
    peer_labels.sort(key=lambda x: int(re.search(r'Blocks: (\d+)', x).group(1)), reverse=True)
    label_to_peer = {f"{p} (Blocks: {sample_counts.get(p,0)})": p for p in all_peers_list}

    selected_label = st.sidebar.selectbox("2. 选择 Peer", peer_labels)
    selected_peer = label_to_peer[selected_label]

    peer_df = df_samples[df_samples['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True)
    peer_auto = df_autos[df_autos['peer'] == selected_peer].copy().sort_values('ts').reset_index(drop=True)

    # --- RTT 平滑处理与速率统计 ---
    if not peer_df.empty:
        st.sidebar.markdown("---")
        st.sidebar.subheader("⚙️ 统计设置")
        # 1. RTT 平滑
        alpha = st.sidebar.slider("RTT 平滑因子 (Alpha)", 0.01, 0.50, 0.125, help="Alpha 越小越平滑")
        peer_df['srtt'] = peer_df['delay'].ewm(alpha=alpha).mean()

        # 2. 采样速率统计 (图3,4)
        min_samples = st.sidebar.number_input("最少统计点数 (N)", value=30, min_value=1)
        min_secs = st.sidebar.number_input("最少时间片段 (秒)", value=1.0, step=0.1)
        sample_size_kb = st.sidebar.number_input("单样本大小 (KiB)", value=16.0)

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

    ref_df = peer_df if not peer_df.empty else peer_auto
    total_len = len(ref_df)
    start_idx = int(total_len * (offset_pct / 100))
    end_idx = min(int(start_idx + total_len * (window_pct / 100)), total_len)

    t_min, t_max = ref_df.iloc[start_idx]['ts'], ref_df.iloc[end_idx-1]['ts']
    sub_df = peer_df[(peer_df['ts'] >= t_min) & (peer_df['ts'] <= t_max)]
    sub_auto = peer_auto[(peer_auto['ts'] >= t_min) & (peer_auto['ts'] <= t_max)]

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

        # 图 5: Avg BW
        ax5 = fig2.add_subplot(3, 2, 1)
        if not sub_auto.empty:
            ax5.plot(sub_auto['dt'], sub_auto['bw'], color='#2ca02c', linewidth=2)
            ax5.fill_between(sub_auto['dt'], sub_auto['bw'], color='#2ca02c', alpha=0.1)
        ax5.set_title("5. Logged Avg Bandwidth (KB/s)"); ax5.set_ylabel("Rate (KB/s)"); ax5.grid(True, alpha=0.3)

        # 图 6: Inflight
        ax6 = fig2.add_subplot(3, 2, 2)
        if not sub_auto.empty:
            ax6.step(sub_auto['dt'], sub_auto['opt_if'], where='post', label='Optimum Inflight', color='#1f77b4')
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
        sub_states = df_states[(df_states['peer'] == selected_peer) & (df_states['ts'] >= t_min) & (df_states['ts'] <= t_max)].copy()
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