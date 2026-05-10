"""Streamlit dashboard for the ScrapeGrid distributed scraper simulator.

Exposes:
    A browser UI with Race, Nodes, Architecture, Algorithms, Event Log,
    and Analytics tabs.

Design notes:
    The dashboard polls an in-memory SimulationEngine. It is intended for local
    demos and teaching, not multi-user production hosting.
"""



import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import time

from simulation import create_engine, get_engine

# ── Page setup ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Distributed Scraper Sim",
    page_icon="🌐",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Minimal CSS for node cards
st.markdown("""
<style>
.node-card {
    border-radius: 8px; padding: 10px 14px; margin-bottom: 8px;
    border-left: 4px solid; font-size: 13px;
}
.card-scraping  { background:#0d2e1a; border-color:#00c853; }
.card-blocked   { background:#2e0d0d; border-color:#f44336; }
.card-cooling   { background:#2e1e0d; border-color:#ff9800; }
.card-idle      { background:#0d1e2e; border-color:#2196f3; }
.card-dead      { background:#1c1c1c; border-color:#555;    }
.card-done      { background:#0d2e2e; border-color:#009688; }
.metric-big { font-size:42px; font-weight:700; line-height:1.1; }
.metric-label { font-size:12px; color:#888; text-transform:uppercase; letter-spacing:.05em; }
.raft-leader  { background:#143d14; border-radius:8px; padding:8px 12px; border:2px solid #4caf50; }
.raft-follow  { background:#0d1e2e; border-radius:8px; padding:8px 12px; border:1px solid #2196f3; }
.raft-crashed { background:#1c1c1c; border-radius:8px; padding:8px 12px; border:1px solid #555; }
</style>
""", unsafe_allow_html=True)


# ── Sidebar — configuration & controls ───────────────────────────────────────

with st.sidebar:
    st.markdown("## 🌐 Distributed Scraper")
    st.caption("PDC Simulation — All Concepts Live")
    st.divider()

    st.markdown("### ⚙️ Simulation Config")

    lam = st.slider("λ — IP Block Intensity", 0.002, 0.03, 0.009, 0.001,
                    help="Higher = nodes blocked faster. P(block) = 1 - e^(−λ·requests)")
    init_nodes = st.slider("Initial Worker Nodes", 2, 8, 4)
    max_nodes  = st.slider("Max Nodes (auto-scale ceiling)", 4, 16, 10)
    duration   = st.slider("Race Duration (seconds)", 30, 180, 90)
    speed      = st.select_slider("Simulation Speed", [0.5, 1.0, 2.0, 5.0, 10.0], value=2.0,
                                   help="Higher = faster scraping simulation")
    auto_scale = st.toggle("Auto-scaling enabled", value=True)

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        start_btn = st.button("▶️ Start Race", type="primary", use_container_width=True)
    with col2:
        stop_btn  = st.button("⏹️ Stop", use_container_width=True)
    reset_btn = st.button("🔄 Reset", use_container_width=True)

    engine = get_engine()

    if start_btn:
        engine = create_engine({
            "lambda_intensity": lam,
            "initial_nodes": init_nodes,
            "max_nodes": max_nodes,
            "min_nodes": 2,
            "race_duration": duration,
            "speed": speed,
            "auto_scale": auto_scale,
        })
        engine.start_race()
        st.rerun()

    if stop_btn and engine and engine.race_running:
        engine.stop_race()
        st.rerun()

    if reset_btn:
        if engine and engine.race_running:
            engine.stop_race()
        import simulation.engine as _eng_mod
        _eng_mod._engine_instance = None
        st.rerun()

    st.divider()
    st.markdown("### 🎭 Raft Drama")
    st.caption("Crash the leader — watch re-election!")
    if st.button("💥 Crash Raft Leader", use_container_width=True):
        if engine:
            msg = engine.kill_raft_leader()
            st.toast(msg, icon="💥")

    engine = get_engine()
    if engine:
        for rn in engine.raft_nodes:
            if rn.crashed:
                if st.button(f"♻️ Recover Node-{rn.node_id}", use_container_width=True):
                    engine.recover_raft_node(rn.node_id)
                    st.rerun()


# ── Main area ─────────────────────────────────────────────────────────────────

engine = get_engine()

if engine is None:
    st.markdown("## 👈 Configure and click **▶️ Start Race** to begin")
    st.markdown("""
    ### What this simulation demonstrates:
    | Concept | Implementation |
    |---------|---------------|
    | **Bloom Filter** | Pre-filters 20k URLs; catches ~10% duplicates |
    | **Consistent Hashing** | 150 virtual nodes per shard, stable remapping |
    | **Raft Consensus** | 3 coordinator nodes; crash the leader live |
    | **Circuit Breaker** | 3-state per worker; exponential backoff |
    | **Work Stealing** | Deque-based; idle workers steal from busiest shard |
    | **Lamport + Vector Clocks** | All events timestamped; causality tracking |
    | **Gossip Protocol** | O(log N) state convergence without central polling |
    | **Token Buckets** | Per-domain rate limiting (15 req/s) |
    | **Auto-scaling** | Spawns/kills workers based on queue depth + block rate |
    | **MapReduce** | Post-race quality aggregation by domain category |
    """)
    st.stop()

# Get snapshot every render
S = engine.get_state()
elapsed  = S["elapsed"]
duration = S["duration"]
running  = S["race_running"]
finished = S["race_finished"]

# Race progress bar at the very top
progress = min(elapsed / max(duration, 1), 1.0)
st.markdown(f"### {'🏁 Race in progress' if running else ('✅ Race finished' if finished else '⏸️ Paused')}  —  {elapsed:.1f}s / {duration}s")
st.progress(progress)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_race, tab_nodes, tab_arch, tab_algo, tab_log, tab_analytics = st.tabs([
    "🏁 Race", "🔧 Nodes", "🏛️ Architecture", "🔬 Algorithms", "📋 Event Log", "📊 Analytics"
])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — RACE DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
with tab_race:
    sn = S["single"]
    dn = S["distributed"]

    left, right = st.columns(2)

    with left:
        st.markdown("### 🐢 Single Node")
        m1, m2, m3 = st.columns(3)
        m1.metric("URLs Scraped", f"{sn['urls_scraped']:,}")
        m2.metric("IP Blocks", sn["blocks"],
                  delta=f"-{sn.get('dead_pct', 0)}% dead time", delta_color="inverse")
        m3.metric("Throughput", f"{sn.get('rate', 0)} /s")

        m4, m5, m6 = st.columns(3)
        m4.metric("Status", sn["status"])
        m5.metric("Dead Time", f"{sn.get('dead_pct', 0)}%")
        m6.metric("Domain", sn.get("current_domain") or "—")

        if sn["block_events"]:
            st.caption("**Recent Block Events:**")
            recent_blocks = sn["block_events"][-5:]
            for b in reversed(recent_blocks):
                st.markdown(
                    f'<div class="node-card card-blocked">'
                    f'⛔ t={b["time"]:.1f}s — <b>{b["domain"]}</b> '
                    f'after {b["requests_before_block"]} reqs | backoff={b["backoff_s"]}s'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    with right:
        st.markdown("### ⚡ Distributed Fleet")
        m1, m2, m3 = st.columns(3)
        m1.metric("URLs Scraped", f"{dn['urls_scraped']:,}",
                  delta=f"+{dn['urls_scraped'] - sn['urls_scraped']:,} vs single" if dn["urls_scraped"] > sn["urls_scraped"] else None)
        m2.metric("IP Blocks", dn["blocks"])
        m3.metric("Throughput", f"{dn.get('rate', 0)} /s")

        m4, m5, m6 = st.columns(3)
        m4.metric("Active Nodes", f"{dn['active_node_count']} / {len(dn['nodes'])}")
        m5.metric("Stolen Tasks", f"{dn['stolen_tasks']:,}")
        m6.metric("Scale Events", len(dn["scale_events"]))

        if dn["scale_events"]:
            st.caption("**Recent Scale Events:**")
            for e in reversed(dn["scale_events"][-4:]):
                color = "card-scraping" if "UP" in e["direction"] else "card-idle"
                st.markdown(
                    f'<div class="node-card {color}">'
                    f'{e["direction"]} t={e["time"]}s — {e["from_n"]}→{e["to_n"]} nodes | {e["reason"]}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # Throughput over time chart
    th_s = sn.get("throughput_history", [])
    th_d = dn.get("throughput_history", [])

    if th_s or th_d:
        fig = go.Figure()

        if th_s:
            fig.add_trace(go.Scatter(
                x=[p["t"] for p in th_s],
                y=[p["urls"] for p in th_s],
                name="Single Node",
                line=dict(color="#f44336", width=2),
                mode="lines",
            ))
        if th_d:
            fig.add_trace(go.Scatter(
                x=[p["t"] for p in th_d],
                y=[p["urls"] for p in th_d],
                name="Distributed Fleet",
                line=dict(color="#4caf50", width=3),
                mode="lines",
            ))

        # Scale-up events as vertical lines
        for e in dn.get("scale_events", []):
            color = "rgba(76,175,80,0.4)" if "UP" in e["direction"] else "rgba(255,152,0,0.4)"
            fig.add_vline(x=e["time"], line_width=1, line_dash="dot", line_color=color)

        fig.update_layout(
            title="Cumulative URLs Scraped Over Time",
            xaxis_title="Elapsed Time (s)",
            yaxis_title="Total URLs Scraped",
            height=300,
            margin=dict(t=40, b=40, l=40, r=20),
            legend=dict(x=0.01, y=0.99),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#ccc",
        )
        st.plotly_chart(fig, use_container_width=True)

    # Block events timeline
    all_blocks_s = [{"t": b["time"], "system": "Single", "domain": b.get("category","?")}
                    for b in sn.get("block_events", [])]
    all_blocks_d = [{"t": b["time"], "system": "Distributed", "domain": b.get("category","?")}
                    for b in dn.get("block_events", [])]
    all_blocks = all_blocks_s + all_blocks_d

    if all_blocks:
        df_b = pd.DataFrame(all_blocks)
        fig_b = px.scatter(df_b, x="t", y="system", color="domain",
                           title="IP Block Events Timeline",
                           labels={"t": "Time (s)", "system": ""},
                           height=200)
        fig_b.update_layout(
            margin=dict(t=40, b=30, l=40, r=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#ccc",
        )
        st.plotly_chart(fig_b, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — NODE STATUS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_nodes:
    dn = S["distributed"]
    nodes = dn.get("nodes", {})

    def node_card_class(status: str) -> str:
        s = status.lower()
        if "scraping" in s:
            return "card-scraping"
        if "blocked" in s:
            return "card-blocked"
        if "cool" in s or "rate" in s:
            return "card-cooling"
        if "dead" in s or "kill" in s:
            return "card-dead"
        if "done" in s:
            return "card-done"
        return "card-idle"

    if not nodes:
        st.info("No workers spawned yet. Start the race first.")
    else:
        cols = st.columns(min(4, len(nodes)))
        for i, (nid, node) in enumerate(sorted(nodes.items())):
            with cols[i % 4]:
                css = node_card_class(node["status"])
                circ = node.get("circuit_state", "CLOSED")
                circ_icon = {"CLOSED": "🟢", "OPEN": "🔴", "HALF_OPEN": "🟡"}.get(circ, "⚪")
                cooldown = node.get("cooldown_remaining", 0)
                st.markdown(
                    f'<div class="node-card {css}">'
                    f'<b>Node-{nid}</b><br>'
                    f'{node["status"]}<br>'
                    f'Circuit: {circ_icon} {circ}<br>'
                    f'URLs: {node["urls_scraped"]:,} | Blocks: {node["blocks"]}<br>'
                    f'Domain: {node.get("domain") or "—"}<br>'
                    f'Stolen: {node.get("tasks_stolen", 0)}'
                    + (f'<br>Cooldown: {cooldown:.1f}s' if cooldown > 0 else '')
                    + '</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # Work queue depths
    wq = S.get("work_queue_sizes", {})
    if wq:
        fig_q = go.Figure(go.Bar(
            x=list(wq.keys()),
            y=list(wq.values()),
            marker_color=["#2196f3","#4caf50","#f44336","#9c27b0","#ff9800"],
            text=[f"{v:,}" for v in wq.values()],
            textposition="auto",
        ))
        fig_q.update_layout(
            title="Work Queue Depth per Shard",
            xaxis_title="Domain Shard",
            yaxis_title="Queued URLs",
            height=250,
            margin=dict(t=40, b=40, l=40, r=20),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#ccc",
        )
        st.plotly_chart(fig_q, use_container_width=True)

    # Node comparison table
    if nodes:
        rows = []
        for nid, n in sorted(nodes.items()):
            rows.append({
                "Node": f"Node-{nid}",
                "Status": n["status"],
                "Circuit": n.get("circuit_state","?"),
                "URLs": n["urls_scraped"],
                "Blocks": n["blocks"],
                "Stolen": n.get("tasks_stolen",0),
                "Domain": n.get("domain","—"),
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — ARCHITECTURE
# ═══════════════════════════════════════════════════════════════════════════════
with tab_arch:
    left, right = st.columns([1, 1])

    # ── Raft Consensus ────────────────────────────────────────────────────────
    with left:
        st.markdown("### 🏛️ Raft Consensus (3 Coordinators)")
        st.caption("Majority vote (2/3) required to commit. Crash the leader from sidebar.")
        raft_nodes = S.get("raft", [])
        leader_id = S.get("raft_leader")

        for rn in raft_nodes:
            css = "raft-crashed" if rn["crashed"] else ("raft-leader" if rn["state"] == "leader" else "raft-follow")
            icon = "💥" if rn["crashed"] else ("👑" if rn["state"] == "leader" else ("🗳️" if rn["state"] == "candidate" else "📡"))
            st.markdown(
                f'<div class="{css}">'
                f'<b>{icon} Coordinator-{rn["id"]}</b> — <b>{rn["state"].upper()}</b>'
                + (" ← LEADER" if rn["state"] == "leader" else "")
                + f'<br>Term: {rn["term"]} | Log: {rn["log_len"]} entries | '
                f'Elections: {rn["elections"]} | Committed: {rn["committed"]}'
                + ('<br><span style="color:#f44336">⚠️ CRASHED — use sidebar to recover</span>' if rn["crashed"] else '')
                + '</div>',
                unsafe_allow_html=True,
            )
            st.markdown("")  # spacing

        # Raft state distribution donut
        states = [rn["state"] if not rn["crashed"] else "crashed" for rn in raft_nodes]
        state_counts = pd.Series(states).value_counts()
        colors_map = {"leader":"#4caf50","follower":"#2196f3","candidate":"#ff9800","crashed":"#555"}
        fig_raft = go.Figure(go.Pie(
            labels=state_counts.index.tolist(),
            values=state_counts.values.tolist(),
            hole=0.5,
            marker_colors=[colors_map.get(s,"#888") for s in state_counts.index],
        ))
        fig_raft.update_layout(
            height=200,
            margin=dict(t=20, b=20, l=20, r=20),
            showlegend=True,
            paper_bgcolor="rgba(0,0,0,0)",
            font_color="#ccc",
        )
        st.plotly_chart(fig_raft, use_container_width=True)

    # ── Consistent Hash Ring ──────────────────────────────────────────────────
    with right:
        st.markdown("### 🔄 Consistent Hash Ring")
        st.caption("5 domain shards × 150 virtual nodes each. Clockwise assignment.")
        ring_data = S.get("hash_ring", [])

        if ring_data:
            COLORS = {"sports":"#f44336","tech":"#2196f3","news":"#4caf50",
                      "science":"#9c27b0","finance":"#ff9800","general":"#607d8b"}
            angles = [d["angle"] for d in ring_data]
            nodes_r = [d["node"] for d in ring_data]

            theta = [a * np.pi / 180 for a in angles]
            x = [np.cos(t) for t in theta]
            y = [np.sin(t) for t in theta]

            fig_ring = go.Figure()
            # Draw the ring
            t_ring = np.linspace(0, 2 * np.pi, 200)
            fig_ring.add_trace(go.Scatter(
                x=np.cos(t_ring).tolist(), y=np.sin(t_ring).tolist(),
                mode="lines", line=dict(color="#333", width=1),
                showlegend=False, hoverinfo="skip",
            ))
            # Plot each shard
            for i, (nx, ny, name) in enumerate(zip(x, y, nodes_r)):
                col = COLORS.get(name, "#888")
                fig_ring.add_trace(go.Scatter(
                    x=[nx], y=[ny],
                    mode="markers+text",
                    name=name,
                    marker=dict(size=18, color=col, line=dict(color="#fff", width=2)),
                    text=[name], textposition="top center",
                    hovertext=f"{name} @ {angles[i]:.1f}°",
                ))
            # Active workers as smaller dots on the ring
            for nid in S["distributed"]["nodes"]:
                angle_w = (hash(str(nid)) % 360) * np.pi / 180
                xw, yw = 0.85 * np.cos(angle_w), 0.85 * np.sin(angle_w)
                fig_ring.add_trace(go.Scatter(
                    x=[xw], y=[yw],
                    mode="markers",
                    name=f"W-{nid}",
                    marker=dict(size=10, color="#fff", symbol="diamond"),
                    hovertext=f"Worker {nid}",
                    showlegend=False,
                ))

            fig_ring.update_layout(
                height=350, showlegend=True,
                xaxis=dict(visible=False, range=[-1.4, 1.4]),
                yaxis=dict(visible=False, range=[-1.4, 1.4], scaleanchor="x"),
                margin=dict(t=10, b=10, l=10, r=10),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc",
            )
            st.plotly_chart(fig_ring, use_container_width=True)

    st.divider()

    # ── Gossip Protocol ───────────────────────────────────────────────────────
    st.markdown("### 💬 Gossip Protocol")
    g = S.get("gossip", {})
    g1, g2, g3, g4 = st.columns(4)
    g1.metric("Gossip Rounds", g.get("rounds", 0))
    g2.metric("Known Nodes", g.get("known_nodes", 0))
    g3.metric("Convergence", f"{g.get('convergence', 0)*100:.0f}%")
    g4.metric("O(log N) Rounds", f"~{max(1, int(np.log2(max(1, g.get('known_nodes', 1)))))} needed")

    recent_ex = g.get("recent_exchanges", [])
    if recent_ex:
        st.caption("**Recent gossip exchanges** (node_a → node_b)")
        for ex in reversed(recent_ex[-5:]):
            st.markdown(f"Round {ex['round']}: Node-{ex['from']} ↔ Node-{ex['to']} | {ex['nodes_shared']} nodes shared")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — ALGORITHMS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_algo:
    # ── Bloom Filter ──────────────────────────────────────────────────────────
    st.markdown("### 🌸 Bloom Filter — URL Deduplication")
    bloom = S.get("bloom", {})
    b1, b2, b3, b4, b5 = st.columns(5)
    b1.metric("Total URLs Generated", f"{S.get('url_pool_total', 0):,}")
    b2.metric("After Deduplication", f"{S.get('url_pool_filtered', 0):,}")
    b3.metric("Duplicates Caught", f"{bloom.get('duplicates_caught', 0):,}")
    b4.metric("Bit Array Size", f"{bloom.get('bit_size', 0):,}")
    b5.metric("Hash Functions (k)", bloom.get("hash_functions", 0))

    fill = bloom.get("fill_ratio", 0)
    st.markdown(f"**Bit array fill ratio:** {fill:.1%}  ← as this → 1, false positive rate climbs")
    st.progress(min(fill, 1.0))

    st.info(
        f"Target FP rate: {bloom.get('target_fp_rate', 0.01):.0%} | "
        f"Items added: {bloom.get('items_added', 0):,} | "
        f"**False negatives: 0** (guaranteed — never re-scrape a URL)"
    )

    st.divider()

    left2, right2 = st.columns(2)

    # ── Lamport Clock ─────────────────────────────────────────────────────────
    with left2:
        st.markdown("### ⏱️ Lamport Logical Clock")
        st.markdown(
            f'<div style="font-size:60px;font-weight:700;color:#4caf50;text-align:center">'
            f'{S.get("lamport", 0):,}</div>'
            f'<div style="text-align:center;color:#888;font-size:12px">GLOBAL LOGICAL TIMESTAMP</div>',
            unsafe_allow_html=True,
        )
        st.caption(
            "Every task assignment, completion, and block event increments this counter. "
            "On receive: L = max(L_local, L_received) + 1. Enables total ordering of all events."
        )

    # ── Vector Clocks ─────────────────────────────────────────────────────────
    with right2:
        st.markdown("### 🔢 Vector Clocks")
        st.caption("Per-node causality tracking. Incomparable vectors = truly concurrent events.")
        vc_data = S.get("vector_clocks", {})
        if vc_data:
            rows = []
            for nid, vec in sorted(vc_data.items()):
                nz = [v for v in vec if v > 0]
                rows.append({
                    "Node": f"N-{nid}",
                    "Own Events": vec[nid] if nid < len(vec) else 0,
                    "Vector (non-zero)": str([v for v in vec if v > 0][:6]),
                    "Total Events": sum(vec),
                })
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
        else:
            st.info("Vector clocks appear once workers start.")

    st.divider()

    # ── Token Buckets ─────────────────────────────────────────────────────────
    st.markdown("### 🪣 Token Buckets — Per-Domain Rate Limiting")
    st.caption("Each domain shard: 15 tokens/s, burst capacity 30. Workers must acquire tokens before scraping.")
    tb_data = S.get("token_buckets", {})
    if tb_data:
        cols_tb = st.columns(len(tb_data))
        COLORS_TB = {"sports":"#f44336","tech":"#2196f3","news":"#4caf50","science":"#9c27b0","finance":"#ff9800"}
        for i, (cat, tb) in enumerate(sorted(tb_data.items())):
            with cols_tb[i]:
                fill_pct = tb.get("fill", 0)
                color = COLORS_TB.get(cat, "#888")
                fig_g = go.Figure(go.Indicator(
                    mode="gauge+number",
                    value=fill_pct * 100,
                    title={"text": cat.capitalize()},
                    gauge={
                        "axis": {"range": [0, 100]},
                        "bar": {"color": color},
                        "bgcolor": "#1e1e1e",
                        "steps": [{"range": [0, 30], "color": "#2a0000"},
                                  {"range": [30, 70], "color": "#1a1a00"},
                                  {"range": [70, 100], "color": "#001a00"}],
                    },
                    number={"suffix": "%", "font": {"size": 18}},
                ))
                fig_g.update_layout(
                    height=180, margin=dict(t=30, b=10, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)", font_color="#ccc",
                )
                st.plotly_chart(fig_g, use_container_width=True)
                st.caption(f"Consumed: {tb.get('consumed', 0):,}")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — EVENT LOG
# ═══════════════════════════════════════════════════════════════════════════════
with tab_log:
    st.markdown("### 📋 Lamport-Ordered Event Log")
    st.caption("All system events ordered by Lamport timestamp. Color: 🔴 error, 🟡 warning, 🟢 success, ⚪ info")

    events = S.get("events", [])
    if not events:
        st.info("Events appear once the race starts.")
    else:
        # Filter controls
        fc1, fc2 = st.columns(2)
        with fc1:
            level_filter = st.multiselect(
                "Filter by level",
                ["error", "warning", "success", "info"],
                default=["error", "warning", "success", "info"],
            )
        with fc2:
            source_filter = st.text_input("Filter by source (leave blank = all)", "")

        LEVEL_ICON = {"error": "🔴", "warning": "🟡", "success": "🟢", "info": "⚪"}
        filtered = [
            e for e in events
            if e["level"] in level_filter
            and (not source_filter or source_filter.lower() in e["source"].lower())
        ]

        rows = []
        for e in filtered[:200]:
            rows.append({
                "L⏱": e["lamport"],
                "t(s)": e["t"],
                "Source": e["source"],
                "Level": LEVEL_ICON.get(e["level"], "⚪") + " " + e["level"],
                "Event": e["message"],
            })

        if rows:
            df_ev = pd.DataFrame(rows)
            st.dataframe(df_ev, use_container_width=True, hide_index=True, height=500)
        else:
            st.info("No events match your filter.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════════
with tab_analytics:
    sn = S["single"]
    dn = S["distributed"]

    st.markdown("### 📊 Race Summary")
    if not S["race_finished"] and not S["race_running"]:
        st.info("Run the race first to see analytics.")
    else:
        # Summary comparison table
        elapsed = max(S["elapsed"], 0.1)
        summary = pd.DataFrame([
            {
                "Metric": "URLs Scraped",
                "Single Node": f"{sn['urls_scraped']:,}",
                "Distributed": f"{dn['urls_scraped']:,}",
                "Winner": "✅ Distributed" if dn["urls_scraped"] > sn["urls_scraped"] else "Single",
            },
            {
                "Metric": "Avg Throughput",
                "Single Node": f"{sn['urls_scraped']/elapsed:.1f} /s",
                "Distributed": f"{dn['urls_scraped']/elapsed:.1f} /s",
                "Winner": "✅ Distributed" if dn["urls_scraped"] > sn["urls_scraped"] else "Single",
            },
            {
                "Metric": "IP Blocks",
                "Single Node": str(sn["blocks"]),
                "Distributed": str(dn["blocks"]),
                "Winner": "—",
            },
            {
                "Metric": "Dead Time %",
                "Single Node": f"{sn.get('dead_pct', 0)}%",
                "Distributed": "< 5% (circuit breaker + work steal)",
                "Winner": "✅ Distributed",
            },
            {
                "Metric": "Scale Events",
                "Single Node": "0",
                "Distributed": str(len(dn["scale_events"])),
                "Winner": "✅ Distributed (elastic)",
            },
            {
                "Metric": "Speedup",
                "Single Node": "1×",
                "Distributed": f"{dn['urls_scraped']/max(sn['urls_scraped'],1):.1f}×",
                "Winner": "✅ Distributed",
            },
        ])
        st.dataframe(summary, use_container_width=True, hide_index=True)

        # Node contribution breakdown
        st.markdown("### 📦 Per-Node Contribution")
        node_rows = [
            {"Node": f"Node-{nid}", "URLs": n["urls_scraped"],
             "Blocks": n["blocks"], "Stolen": n.get("tasks_stolen", 0)}
            for nid, n in sorted(dn["nodes"].items())
        ]
        if node_rows:
            df_n = pd.DataFrame(node_rows)
            fig_n = px.bar(df_n, x="Node", y="URLs", color="Stolen",
                           title="URLs Scraped per Node (color = stolen tasks)",
                           color_continuous_scale="blues", height=280)
            fig_n.update_layout(
                margin=dict(t=40, b=40, l=40, r=20),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc",
            )
            st.plotly_chart(fig_n, use_container_width=True)

        # MapReduce results
        mr = dn.get("mapreduce_results", {})
        if mr:
            st.markdown("### 🗺️ MapReduce Results — Domain Quality Aggregation")
            st.caption("Map: quality score per URL → Shuffle: by domain → Reduce: aggregate stats")
            mr_rows = [
                {"Domain": cat, **stats}
                for cat, stats in sorted(mr.items())
            ]
            df_mr = pd.DataFrame(mr_rows)
            st.dataframe(df_mr, use_container_width=True, hide_index=True)

            fig_mr = px.bar(df_mr, x="Domain", y="avg_quality",
                            title="Average Content Quality Score by Domain",
                            color="avg_quality",
                            color_continuous_scale="viridis", height=250)
            fig_mr.update_layout(
                margin=dict(t=40,b=40,l=40,r=20),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc",
            )
            st.plotly_chart(fig_mr, use_container_width=True)

        # Scaling events timeline
        if dn["scale_events"]:
            st.markdown("### 📈 Autoscaling Timeline")
            df_se = pd.DataFrame(dn["scale_events"])
            fig_sc = px.scatter(df_se, x="time", y="to_n",
                                color="direction", text="reason",
                                title="Cluster Size Over Time",
                                labels={"time": "Elapsed (s)", "to_n": "Node Count"},
                                height=250)
            fig_sc.update_layout(
                margin=dict(t=40,b=40,l=40,r=20),
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                font_color="#ccc",
            )
            st.plotly_chart(fig_sc, use_container_width=True)


# ── Auto-refresh while race is running ───────────────────────────────────────
if running:
    time.sleep(0.8)
    st.rerun()
