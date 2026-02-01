import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re
import sys
import os
from datetime import datetime

# --- 页面配置 ---
st.set_page_config(page_title="Stardust CC Analyzer", layout="wide")

# --- 核心解析逻辑 ---
@st.cache_data
def parse_log_content(lines):
    # 更新后的正则说明：
    # 1. (?P<ip_port>...) 捕获包含端口的完整标识
    # 2. Some\((?P<delay>[\d.]+)(?P<unit>ms|s)\) 同时兼容 ms 和 s
    sample_re = re.compile(
        r"src/transmit_manager\.rs:\d+:\s+(?P<ip_port>\[?[a-fA-F0-9:.]+\]?:\d+)\s+add sample Some\((?P<delay>[\d.]+)(?P<unit>ms|s)\),\s+inflight when sent Some\((?P<inflight>\d+)\)"
    )

    data = []
    for line in lines:
        match = sample_re.search(line)
        if match:
            try:
                # 提取 ISO 时间戳
                ts_str = line.split(' ')[0].replace('Z', '+00:00')
                ts = datetime.fromisoformat(ts_str).timestamp()

                # 延迟单位换算：统一转为 ms
                raw_delay = float(match.group('delay'))
                unit = match.group('unit')
                delay_ms = raw_delay * 1000.0 if unit == 's' else raw_delay

                data.append({
                    'ts': ts,
                    'peer': match.group('ip_port'), # 这里现在包含 IP:PORT
                    'delay': delay_ms,
                    'inflight': int(match.group('inflight'))
                })
            except Exception:
                continue
    return pd.DataFrame(data)

# --- 获取命令行输入 ---
cmd_file = None
if len(sys.argv) > 1:
    for arg in sys.argv[1:]:
        if os.path.exists(arg) and arg.endswith(('.log', '.txt')):
            cmd_file = arg
            break

# --- 侧边栏 ---
st.sidebar.title("🛠️ 控制面板")
uploaded_file = st.sidebar.file_uploader("1. 上传日志", type=['log', 'txt'])

lines = None
if uploaded_file:
    lines = uploaded_file.read().decode("utf-8").splitlines()
elif cmd_file:
    with open(cmd_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

if lines:
    df = parse_log_content(lines)
    if df.empty:
        st.error("❌ 匹配失败。请检查日志格式，确保包含 'add sample' 且延迟单位为 s 或 ms。")
        st.stop()

    df = df.sort_values('ts').reset_index(drop=True)

    # 速率计算 (16KiB per sample)
    window_pts = 20
    df['rate'] = (16.0 * window_pts) / (df['ts'].diff(window_pts).fillna(1.0))

    # --- 过滤器：Peer 选择 (包含端口) ---
    unique_peers = df['peer'].unique()
    selected_peer = st.sidebar.selectbox("2. 选择 Peer (IP:Port)", unique_peers)
    peer_df = df[df['peer'] == selected_peer].copy().reset_index(drop=True)

    # --- 交互滑动条 ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("3. 动态范围调整")
    total_pts = len(peer_df)

    offset_pct = st.sidebar.slider("起始位置 (Offset %)", 0, 100, 0)
    window_pct = st.sidebar.slider("展示比例 (Window %)", 1, 100, 10)

    start_idx = int(total_pts * (offset_pct / 100))
    end_idx = min(int(start_idx + total_pts * (window_pct / 100)), total_pts)

    sub_df = peer_df.iloc[start_idx:end_idx]

    # --- 绘图 ---
    st.title(f"🛰️ 传输分析: {selected_peer}")

    if not sub_df.empty:
        inf = sub_df['inflight'].values
        dl = sub_df['delay'].values
        rt = sub_df['rate'].values
        tm = np.linspace(0, 1, len(sub_df))

        fig = plt.figure(figsize=(16, 12))

        # 1. 10x10 聚合气泡图
        ax1 = fig.add_subplot(2, 2, 1)
        bins_x, bins_y = 10, 10
        # 增加容错，防止窗口内数据全等
        inf_range = [inf.min(), inf.max()] if inf.min() != inf.max() else [inf.min()-1, inf.min()+1]
        dl_range = [dl.min(), dl.max()] if dl.min() != dl.max() else [dl.min()-1, dl.min()+1]

        inf_edges = np.linspace(inf_range[0], inf_range[1], bins_x + 1)
        dl_edges = np.linspace(dl_range[0], dl_range[1], bins_y + 1)
        b_inf, b_dl, b_rt, b_tm = [], [], [], []

        for i in range(bins_x):
            for j in range(bins_y):
                mask = (inf >= inf_edges[i]) & (inf <= inf_edges[i+1]) & \
                       (dl >= dl_edges[j]) & (dl <= dl_edges[j+1])
                if np.any(mask):
                    b_inf.append(np.mean(inf[mask]))
                    b_dl.append(np.mean(dl[mask]))
                    b_rt.append(np.mean(rt[mask]))
                    b_tm.append(np.mean(tm[mask]))

        if b_rt:
            b_rt_arr = np.array(b_rt)
            denom = b_rt_arr.max() - b_rt_arr.min() + 0.1
            s_sizes = ((b_rt_arr - b_rt_arr.min()) / denom * 2500) + 150
            ax1.scatter(b_inf, b_dl, c=b_tm, s=s_sizes, cmap='rainbow_r', alpha=0.8, edgecolors='black')
        ax1.set_title("1. Performance Aggregation (10x10)", fontsize=14)
        ax1.set_xlabel("Inflight (Packets)")
        ax1.set_ylabel("Delay (ms)")
        ax1.grid(True, alpha=0.3)

        # 2. 原始分布图 (饱和度增强)
        ax2 = fig.add_subplot(2, 2, 2)
        ax2.scatter(inf, dl, c=tm, cmap='rainbow_r', s=45, alpha=0.7, edgecolors='black', linewidths=0.2)
        ax2.set_title("2. Raw Inflight vs Delay (ms)", fontsize=14)
        ax2.set_ylabel("Delay (ms)")
        ax2.grid(True, alpha=0.3)

        # 3. 原始速率图 (饱和度增强)
        ax3 = fig.add_subplot(2, 2, 3)
        ax3.scatter(inf, rt, c=tm, cmap='rainbow_r', s=45, alpha=0.7, edgecolors='black', linewidths=0.2)
        ax3.set_title("3. Raw Inflight vs Rate (KB/s)", fontsize=14)
        ax3.set_xlabel("Inflight")
        ax3.set_ylabel("Rate (KB/s)")
        ax3.grid(True, alpha=0.3)

        plt.tight_layout()
        st.pyplot(fig)

        st.markdown("---")
        c1, c2, c3 = st.columns(3)
        c1.metric("窗口样本量", len(sub_df))
        c2.metric("最高速率", f"{rt.max():.1f} KB/s")
        c3.metric("平均延迟", f"{dl.mean():.1f} ms")
    else:
        st.warning("当前范围内无有效数据。")
else:
    st.info("💡 请上传日志，或在启动命令后添加日志路径。")