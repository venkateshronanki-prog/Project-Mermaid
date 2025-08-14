import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import plotly.graph_objects as go
import os

# ================= PATHS =================
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "backend", "db", "project_mermaid.db")

# ================= PAGE =================
st.set_page_config(page_title="RBSF Dashboard — Insurance KPIs", layout="wide")

# ================= PALETTE & TYPOGRAPHY =================
PRIMARY = "#2563eb"
ACCENT  = "#0ea5e9"
GOOD    = "#10b981"
WARN    = "#f59e0b"
BAD     = "#ef4444"
NEUTRAL = "#475569"
PURPLE  = "#8b5cf6"
PINK    = "#ec4899"
TEAL    = "#14b8a6"
INDIGO  = "#4f46e5"
ORANGE  = "#fb923c"
CYAN    = "#22d3ee"
LIME    = "#84cc16"
AMBER   = "#f59e0b"
SLATE_DARK = "#0b1220"

# ================= HELPERS =================
def fy_label(y: int) -> str:
    return f"FY{y-1}–{str(y)[-2:]}"

@st.cache_data
def load_df():
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query("""
            SELECT i.year, i.source, ins.name, ins.type,
                   i.solvency_ratio, i.claims_ratio,
                   i.claim_settlement_ratio, i.gross_premium_total,
                   i.eom_ratio, i.commission_ratio, i.aum_total,
                   i.grievances_received, i.grievances_resolved,
                   i.grievances_pending, i.grievances_within_tat_percent
            FROM indicators i
            JOIN insurers ins ON i.insurer_id = ins.id
        """, conn)
    except Exception:
        df = pd.read_sql_query("""
            SELECT i.year, ins.name, ins.type,
                   i.solvency_ratio, i.claims_ratio,
                   i.claim_settlement_ratio, i.gross_premium_total,
                   i.eom_ratio, i.commission_ratio, i.aum_total,
                   i.grievances_received, i.grievances_resolved,
                   i.grievances_pending, i.grievances_within_tat_percent
            FROM indicators i
            JOIN insurers ins ON i.insurer_id = ins.id
        """, conn)
        df["source"] = "handbook"
    conn.close()
    for c in df.columns:
        if c not in ["year","name","type","source"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    return df

@st.cache_data
def list_unique(df: pd.DataFrame, col: str) -> list:
    if col not in df.columns:
        return []
    return sorted([x for x in df[col].dropna().unique().tolist()])

def safe_mean(series):
    if series is None or series.empty: return float("nan")
    s = pd.to_numeric(series, errors="coerce").dropna()
    return s.mean() if not s.empty else float("nan")

def metric_box(val, label, suffix=""):
    if pd.isna(val): st.metric(label, "NA")
    else: st.metric(label, f"{val:.2f}{suffix}")

# ================= STYLE SYSTEM =================
def background_style(choice: str, url: str = "", style_mode: str = "Classic") -> str:
    if choice == "None":
        base = "background: linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);"
    elif choice == "Soft Gradient":
        base = "background: radial-gradient(1200px 600px at 10% 0%, #eef2ff 0%, #ffffff 35%), radial-gradient(1000px 500px at 90% 0%, #e0f2fe 0%, #ffffff 40%);"
    elif choice == "Subtle Mesh":
        base = "background-image: radial-gradient(#e2e8f0 0.8px, transparent 0.8px), radial-gradient(#e2e8f0 0.8px, #ffffff 0.8px); background-size: 24px 24px; background-position: 0 0,12px 12px;"
    elif choice == "Bubbles":
        base = "background-image: radial-gradient(circle at 20% 20%, #e0f2fe 0 120px, transparent 121px), radial-gradient(circle at 80% 30%, #e9d5ff 0 160px, transparent 161px), radial-gradient(circle at 50% 90%, #dcfce7 0 140px, transparent 141px); background-color:#ffffff;"
    elif choice == "Aurora Wave":
        base = "background-image: radial-gradient(1200px 600px at 20% 10%, rgba(99,102,241,0.22) 0%, transparent 45%), radial-gradient(900px 500px at 80% 15%, rgba(6,182,212,0.22) 0%, transparent 50%), linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);"
    elif choice == "Prism Glass":
        base = "background-image: linear-gradient( 135deg, rgba(240,249,255,0.85) 0%, rgba(236,253,245,0.85) 100% ), url('data:image/svg+xml;utf8,<svg xmlns=%22http://www.w3.org/2000/svg%22 width=%22160%22 height=%22160%22><defs><linearGradient id=%22g%22 x1=%220%22 y1=%220%22 x2=%221%22 y2=%221%22><stop offset=%220%25%22 stop-color=%22%23e2e8f0%22/><stop offset=%22100%25%22 stop-color=%22%23f1f5f9%22/></linearGradient></defs><rect width=%22160%22 height=%22160%22 fill=%22url(%23g)%22/></svg>'); background-size: cover;"
    elif choice == "Neon Grid":
        base = "background-image: linear-gradient(180deg,#0b1220 0%, #0e1a2b 100%), linear-gradient(transparent 24px, rgba(255,255,255,0.06) 25px), linear-gradient(90deg, transparent 24px, rgba(255,255,255,0.06) 25px); background-size: auto, 50px 50px, 50px 50px; background-position: 0 0, -1px -1px, -1px -1px;"
    elif choice == "Elegant Fabric":
        base = "background-image: linear-gradient(180deg,#fafafa,#f4f5f7), radial-gradient(600px 300px at 20% -20%, #e0e7ff 0, transparent 60%), radial-gradient(600px 300px at 80% -20%, #cffafe 0, transparent 60%); background-blend-mode: normal, multiply, multiply;"
    elif choice == "Ocean Mist":
        base = "background-image: radial-gradient(1200px 600px at -10% 10%, rgba(14,165,233,0.18) 0%, transparent 50%), radial-gradient(1000px 600px at 110% 10%, rgba(59,130,246,0.18) 0%, transparent 55%), linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);"
    elif choice == "Pearl Sheen":
        base = "background-image: linear-gradient(120deg, rgba(236,253,245,0.6), rgba(239,246,255,0.6)), radial-gradient(800px 400px at 50% -10%, rgba(244,244,245,0.7) 0%, transparent 60%);"
    elif choice == "Custom Image URL" and url:
        base = f"background-image: url('{url}'); background-size: cover; background-position: center;"
    else:
        base = "background: linear-gradient(180deg,#ffffff 0%,#f8fafc 100%);"

    if style_mode == "Liquid Glass":
        glass = """
        --glass-bg: rgba(255,255,255,0.18);
        --glass-br: 20px;
        --glass-blur: blur(26px);
        --glass-border: 1px solid rgba(255,255,255,0.58);
        """
    elif style_mode == "Vibrant Cards":
        glass = """
        --glass-bg: rgba(255,255,255,0.90);
        --glass-br: 16px;
        --glass-blur: blur(8px);
        --glass-border: 1px solid rgba(229,231,235,0.9);
        """
    else:
        glass = """
        --glass-bg: rgba(255,255,255,0.94);
        --glass-br: 12px;
        --glass-blur: blur(2px);
        --glass-border: 1px solid rgba(229,231,235,0.8);
        """
    return base + glass

def inject_css(bg_style: str = "", style_mode: str = "Classic"):
    st.markdown(f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=Poppins:wght@400;600;700&display=swap');
      html, body, [class*="css"]  {{
        font-family: Inter, -apple-system, BlinkMacSystemFont, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol", sans-serif;
      }}
      .stApp {{
        {bg_style}
        background-attachment: fixed;
        background-size: cover;
      }}
      /* Fullscreen toggle button styling */
      .fs-btn {{
        position: fixed; top: 14px; right: 14px; z-index: 9999;
        padding: 8px 12px; border-radius: 10px; border:1px solid #e5e7eb;
        background: rgba(255,255,255,0.88);
        backdrop-filter: blur(8px);
        cursor: pointer; color:#0f172a; font-weight:600;
      }}
      .glass {{
        background: var(--glass-bg);
        backdrop-filter: var(--glass-blur);
        -webkit-backdrop-filter: var(--glass-blur);
        border-radius: var(--glass-br);
        border: var(--glass-border);
        box-shadow: 0 20px 46px rgba(2,6,23,.14);
      }}
      .rbsf-header {{
        display:flex;align-items:center;justify-content:space-between;
        padding:16px 20px;margin-bottom:16px;
      }}
      .rbsf-title h1 {{margin:0;font-weight:900;color:#0f172a;font-size:28px;letter-spacing:.2px; font-family:Poppins,Inter,sans-serif}}
      .rbsf-subtitle {{color:#475569;font-size:13px;margin-top:2px}}
      .rbsf-badge {{
        color:#0f172a;font-size:13px;opacity:.92;padding:8px 12px;border-radius:999px;
        border:1px solid #e5e7eb;background:linear-gradient(180deg,#fff,#f1f5f9);
        box-shadow:0 4px 12px rgba(2,6,23,.06) inset;
      }}
      .rbsf-logo svg {{
        filter:drop-shadow(0 12px 30px rgba(37,99,235,.38));
        transition:transform .22s ease, filter .22s ease;
      }}
      .rbsf-logo svg:hover {{
        transform:scale(1.06);
        filter:drop-shadow(0 18px 36px rgba(14,165,233,.50));
      }}
      .card {{ padding:16px; margin-bottom:14px; }}
      .pill {{
        display:inline-block;padding:6px 10px;border-radius:999px;border:1px solid #e5e7eb;
        background:#ffffff;margin:4px;color:#0f172a;font-size:12px
      }}
      .table-like th, .table-like td {{padding:8px 10px;border-bottom:1px solid #e5e7eb;font-size:13px}}
      .danger {{background:#fef2f2;border-color:#fee2e2}}
      .warn {{background:#fffbeb;border-color:#fde68a}}
      .ok {{background:#ecfdf5;border-color:#a7f3d0}}
      .wide-chart .js-plotly-plot, .wide-chart .plot-container {{ min-height: 480px; }}
      .xl-chart .js-plotly-plot,   .xl-chart .plot-container   {{ min-height: 600px; }}
      /* Dropdown panels fit viewport: clamp to 85vh with top & bottom scroll */
      div[data-baseweb="select"] > div > div {{
        max-height: 85vh !important;
        overflow-y: auto !important;
      }}
    </style>
    <script>
      function toggleFullScreen() {{
        if (!document.fullscreenElement) {{
          document.documentElement.requestFullscreen();
        }} else {{
          if (document.exitFullscreen) {{
            document.exitFullscreen();
          }}
        }}
      }}
    </script>
    """, unsafe_allow_html=True)

def header(style_mode: str):
    container_class = "glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""
    st.markdown('<div class="fs-btn" onclick="toggleFullScreen()">⛶ Fullscreen</div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div class="rbsf-header {container_class}">
      <div style="display:flex;align-items:center;gap:16px">
        <div class="rbsf-logo">
          <!-- Improved gradient mark -->
          <svg width="112" height="112" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="RBSF">
            <defs>
              <radialGradient id="rg" cx="30%" cy="30%" r="80%">
                <stop offset="0%" stop-color="{ACCENT}" stop-opacity="1"/>
                <stop offset="60%" stop-color="{PRIMARY}" stop-opacity="0.95"/>
                <stop offset="100%" stop-color="{INDIGO}" stop-opacity="0.9"/>
              </radialGradient>
            </defs>
            <circle cx="32" cy="32" r="28" fill="url(#rg)"/>
            <text x="50%" y="50%" text-anchor="middle" dominant-baseline="central"
                  font-size="18" font-family="Poppins,Arial" fill="#ffffff" font-weight="800">RBSF</text>
          </svg>
        </div>
        <div class="rbsf-title">
          <h1>RBSF Dashboard</h1>
          <div class="rbsf-subtitle">Insurance KPIs — Solvency control level 150%</div>
        </div>
      </div>
      <div class="rbsf-badge">Project Mermaid</div>
    </div>
    """, unsafe_allow_html=True)

# ================= RISK MATRIX =================
def risk_score(level: str) -> int:
    return {"low":1, "medium":2, "high":3}.get((level or "").strip().lower(), 2)

def risk_matrix_figure(mode: str = "Heatmap", theme_variant: str = "plotly"):
    rows = ["Low","Medium","High"]
    cols = ["Low","Medium","High"]
    z = [[risk_score(r)+risk_score(c) for c in cols] for r in rows]
    if mode == "Heatmap":
        fig = go.Figure(data=go.Heatmap(
            z=z, x=cols, y=rows,
            colorscale=[
                [0.00,"#e6fffa"], [0.25,"#c7f9cc"], [0.50,"#fffbeb"],
                [0.75,"#fde68a"], [1.00,"#fecaca"]
            ],
            showscale=False,
            hovertemplate="Likelihood: %{y}<br>Impact: %{x}<extra></extra>"
        ))
    else:
        xx, yy, sz = [], [], []
        for r in rows:
            for c in cols:
                xx.append(c); yy.append(r); sz.append((risk_score(r)+risk_score(c))*18)
        fig = px.scatter(x=xx, y=yy, size=sz, color=xx,
                         color_discrete_sequence=[PRIMARY, ACCENT, PINK],
                         title="Risk Matrix (Bubbles)", template=theme_variant)
        fig.update_traces(marker=dict(line=dict(width=1,color="#ffffff")))
    fig.update_layout(
        title=f"Risk Matrix — {mode}",
        xaxis_title="Impact", yaxis_title="Likelihood",
        template=theme_variant, margin=dict(t=60,b=50,l=60,r=40)
    )
    return fig

# ================= SUPERVISORY LADDER =================
def ladder_colors():
    # Distinct colors level-wise
    return {
        1: "#10b981",  # Green
        2: "#f59e0b",  # Amber
        3: "#f97316",  # Orange
        4: "#ef4444",  # Red
        5: "#7f1d1d"   # Dark Red (escalation)
    }

def ladder_bar(theme_variant="plotly"):
    levels = [
        "Normal Monitoring",
        "Enhanced Monitoring",
        "Intensified Supervision",
        "Corrective Action Plan",
        "Enforcement / Resolution"
    ]
    colors = ladder_colors()
    fig = go.Figure()
    for i, lbl in enumerate(levels, start=1):
        fig.add_trace(go.Bar(
            x=[1], y=[1], base=i-1, orientation="v",
            marker=dict(color=colors[i], line=dict(color="#ffffff", width=1)),
            name=lbl, hovertemplate=f"{lbl}<extra></extra>", showlegend=True
        ))
    fig.update_layout(
        barmode="stack", height=240, template=theme_variant,
        title="Intervention Ladder", xaxis=dict(visible=False),
        yaxis=dict(visible=False, range=[0,5]), legend=dict(orientation="h", y=-0.2)
    )
    return fig

# ================= DATA =================
df = load_df()

# ================= SIDEBAR (GLOBAL) =================
with st.sidebar:
    st.subheader("Global Settings")
    style_mode = st.selectbox("Style", ["Classic","Liquid Glass","Vibrant Cards"], index=1)
    wp_choice = st.selectbox(
        "Wallpaper",
        [
            "None","Soft Gradient","Subtle Mesh","Bubbles",
            "Aurora Wave","Prism Glass","Neon Grid",
            "Elegant Fabric","Ocean Mist","Pearl Sheen","Custom Image URL"
        ],
        index=8
    )
    custom_url = st.text_input("Custom Image URL", "", placeholder="https://...") if wp_choice == "Custom Image URL" else ""
    st.caption("Use a public image URL for custom wallpaper.")

# Apply CSS + Header
inject_css(background_style(wp_choice, custom_url, style_mode), style_mode)
header(style_mode)

# ================= GLOBAL CONTROLS =================
g1, g2, g3 = st.columns([1.2, 1.2, 1.6])
with g1:
    density = st.slider("View Density", 0, 100, 50, help="Adjusts spacing, chart sizing, and label density")
with g2:
    label_angle = st.slider("X‑Label Angle", 0, 60, 30)
with g3:
    theme_variant = st.selectbox("Chart Theme", ["plotly","plotly_white","plotly_dark"], index=0)

scale = 1.0 + (50 - density)/100.0
chart_height_class = "xl-chart" if scale > 1.1 else "wide-chart"

# ================= RBSF RESOURCE HUB (top, with placeholders) =================
with st.expander("RBSF Resource Hub — Guidance, Stages, Links, FAQs (India-first)", expanded=True):
    st.markdown("##### India RBSF Progress — Overview")
    st.markdown("""
- Full implementation: solvency control level (150%), board governance structures, capital adequacy regime.
- Semi implementation: advanced conduct analytics, thematic inspections, ORSA alignment.
- In process: cross-sector KRIs harmonization, data pipelines, integrated SupTech.
    """)
    st.markdown("##### Guidance and Sources (ordered by preference)")
    st.markdown("""
1. Core RBSF Manuals and Handbooks — Insurers (placeholder)
2. Circulars/Guidelines — Capital, Solvency, Conduct, Product, Intermediaries (placeholder)
3. ORSA, Stress Testing, Governance Codes (placeholder)
4. Sector Reviews, Thematic Inspection Reports (placeholder)
5. FAQs and Implementation Playbooks (placeholder)
    """)
    st.markdown("##### Quick Links (placeholders — replace with URLs)")
    st.markdown("""
- RBSF Core Manual (Insurers)
- Solvency & Capital Framework
- Conduct & Intermediary Guidelines
- ORSA / Stress Testing Templates
- FAQs — Implementation & Staging
    """)

st.divider()

# ================= TABS (Trends immediately after Market Overview) =================
tab1, tab3, tab2, tab4, tab5, tab6 = st.tabs([
    "Market Overview", "Trends", "Risk & Conduct", "RBSF KOBs", "Risk Matrix", "Supervisory Ladder"
])

# -------- Market Overview --------
with tab1:
    c1, c2, c3, c4, c5 = st.columns([1.1,1.1,1.4,1.0,1.0])
    with c1:
        years = sorted(list_unique(df,"year"), reverse=True) if not df.empty else [2024]
        year = st.selectbox("Year", years, index=0)
    with c2:
        srcs = ["all"] + (list_unique(df,"source") if not df.empty else [])
        source = st.selectbox("Source", srcs, index=0)
    with c3:
        types = list_unique(df,"type") if not df.empty else []
        sel_types = st.multiselect("Insurer Types", types, default=types)
    with c4:
        topn = st.slider("Top‑N", 5, 40, 20)
    with c5:
        chart_mode = st.selectbox("Chart Style", ["Bars","Lines","Donut","Histogram","Treemap","Sunburst"], index=0)

    st.markdown(f"### Overview — {fy_label(int(year))}")

    base = df.copy()
    if not base.empty:
        base = base[(base["year"]==year)]
        if sel_types: base = base[base["type"].isin(sel_types)]
        if source!="all": base = base[base["source"]==source]

    k1,k2,k3,k4 = st.columns(4)
    metric_box(safe_mean(base.get("solvency_ratio")) if not base.empty else float("nan"), "Avg Solvency Ratio")
    metric_box(safe_mean(base.get("claim_settlement_ratio")) if not base.empty else float("nan"), "Avg Claim Settlement Ratio")
    metric_box(safe_mean(base.get("claims_ratio")) if not base.empty else float("nan"), "Avg Claims Ratio (ICR)")
    metric_box(safe_mean(base.get("grievances_within_tat_percent")) if not base.empty else float("nan"), "Avg Grievances within TAT%", "%")

    st.divider()

    a,b = st.columns(2)
    with a:
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        if not base.empty and "gross_premium_total" in base and base["gross_premium_total"].notna().any():
            top = base.sort_values("gross_premium_total", ascending=False).head(topn)
            if chart_mode=="Bars":
                fig = px.bar(top, x="name", y="gross_premium_total", color="type",
                             title=f"Top {min(topn,len(top))} by Gross Premium — {fy_label(int(year))}",
                             template=theme_variant,
                             color_discrete_sequence=[PRIMARY, ACCENT, PURPLE, TEAL, PINK, INDIGO, ORANGE, CYAN, LIME, AMBER])
                fig.update_xaxes(tickangle=label_angle)
            elif chart_mode=="Lines":
                fig = px.line(top.sort_values("gross_premium_total"), x="name", y="gross_premium_total", color="type",
                              title=f"Gross Premium (Line) — {fy_label(int(year))}", template=theme_variant)
                fig.update_xaxes(tickangle=label_angle)
            elif chart_mode=="Donut":
                fig = px.pie(top, names="name", values="gross_premium_total", color="type",
                             title=f"Gross Premium Share — {fy_label(int(year))}", hole=0.45)
            elif chart_mode=="Treemap":
                fig = px.treemap(top, path=["type","name"], values="gross_premium_total",
                                 title=f"Premium Treemap — {fy_label(int(year))}", color="type",
                                 color_discrete_sequence=px.colors.qualitative.Pastel)
            elif chart_mode=="Sunburst":
                fig = px.sunburst(top, path=["type","name"], values="gross_premium_total",
                                  title=f"Premium Sunburst — {fy_label(int(year))}",
                                  color="type", color_discrete_sequence=px.colors.qualitative.Set3)
            else:
                fig = px.histogram(base.dropna(subset=["gross_premium_total"]), x="gross_premium_total", nbins=24,
                                   title=f"Premium Distribution — {fy_label(int(year))}", template=theme_variant)
            st.markdown(f'<div class="{"xl-chart" if chart_height_class=="xl-chart" else "wide-chart"}">', unsafe_allow_html=True)
            st.plotly_chart(fig, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("Premium not available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with b:
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        if not base.empty and "aum_total" in base and base["aum_total"].notna().any():
            top_aum = base.sort_values("aum_total", ascending=False).head(topn)
            if chart_mode=="Bars":
                fig2 = px.bar(top_aum, x="name", y="aum_total", color="type",
                              title=f"Top {min(topn,len(top_aum))} by AUM — {fy_label(int(year))}",
                              template=theme_variant,
                              color_discrete_sequence=[ACCENT, PRIMARY, PINK, TEAL, INDIGO, PURPLE, ORANGE, CYAN, LIME, AMBER])
                fig2.update_xaxes(tickangle=label_angle)
            elif chart_mode=="Lines":
                fig2 = px.line(top_aum.sort_values("aum_total"), x="name", y="aum_total", color="type",
                               title=f"AUM (Line) — {fy_label(int(year))}", template=theme_variant)
                fig2.update_xaxes(tickangle=label_angle)
            elif chart_mode=="Donut":
                fig2 = px.pie(top_aum, names="name", values="aum_total", color="type",
                              title=f"AUM Share — {fy_label(int(year))}", hole=0.45,
                              color_discrete_sequence=px.colors.qualitative.Dark24)
            elif chart_mode=="Treemap":
                fig2 = px.treemap(top_aum, path=["type","name"], values="aum_total",
                                  title=f"AUM Treemap — {fy_label(int(year))}",
                                  color="type", color_discrete_sequence=px.colors.qualitative.Set2)
            elif chart_mode=="Sunburst":
                fig2 = px.sunburst(top_aum, path=["type","name"], values="aum_total",
                                   title=f"AUM Sunburst — {fy_label(int(year))}",
                                   color="type", color_discrete_sequence=px.colors.qualitative.Set1)
            else:
                fig2 = px.histogram(base.dropna(subset=["aum_total"]), x="aum_total", nbins=24,
                                    title=f"AUM Distribution — {fy_label(int(year))}", template=theme_variant)
            st.markdown(f'<div class="{"xl-chart" if chart_height_class=="xl-chart" else "wide-chart"}">', unsafe_allow_html=True)
            st.plotly_chart(fig2, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("AUM not available.")
        st.markdown('</div>', unsafe_allow_html=True)

# -------- Trends --------
with tab3:
    trc1, trc2, trc3 = st.columns([1.2,1.2,1.6])
    with trc1:
        trend_metric = st.selectbox("Metric", ["solvency_ratio","claim_settlement_ratio","claims_ratio"], index=0)
    with trc2:
        color_scheme = st.selectbox("Palette", ["Set2","Pastel","Dark24"], index=0)
    with trc3:
        st.info("Tip: Add/remove insurers below to compare trend lines.")

    base_tr = df[df["year"].notna()]
    if 'sel_types' in locals() and sel_types:
        base_tr = base_tr[base_tr["type"].isin(sel_types)]
    if 'source' in locals() and source!="all":
        base_tr = base_tr[base_tr["source"]==source]

    ins = st.multiselect("Insurers", sorted(base_tr["name"].dropna().unique().tolist()),
                         default=sorted(base_tr["name"].dropna().unique().tolist())[:6])
    dt = base_tr[base_tr["name"].isin(ins)].copy() if ins else pd.DataFrame()

    if not dt.empty and trend_metric in dt and dt[trend_metric].notna().any():
        palette = getattr(px.colors.qualitative, color_scheme)
        fig_t = px.line(dt.sort_values("year"), x="year", y=trend_metric, color="name",
                        title=f"{trend_metric.replace('_',' ').title()} Trend", template=theme_variant,
                        color_discrete_sequence=palette)
        if trend_metric == "solvency_ratio":
            fig_t.add_hline(y=150, line_dash="dot", line_color=BAD)
        st.markdown(f'<div class="xl-chart">', unsafe_allow_html=True)
        st.plotly_chart(fig_t, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.info("Select at least one insurer with data for the chosen metric.")

# -------- Risk & Conduct --------
with tab2:
    rc1, rc2, rc3 = st.columns([1.0, 1.0, 1.0])
    with rc1:
        bubble_size_scale = st.slider("Bubble Size Scale", 0.5, 2.0, 1.0, 0.1)
    with rc2:
        y_ref_line = st.number_input("Solvency Ref Line", value=150.0, step=10.0)
    with rc3:
        dist_points = st.selectbox("Boxplot Points", ["outliers","all","suspectedoutliers","False"], index=0)

    c,dv = st.columns(2)
    base_rc = df[df["year"]==year].copy() if not df.empty else pd.DataFrame()
    if 'sel_types' in locals() and sel_types: base_rc = base_rc[base_rc["type"].isin(sel_types)]
    if 'source' in locals() and source!="all": base_rc = base_rc[base_rc["source"]==source]

    with c:
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        if not base_rc.empty and "solvency_ratio" in base_rc and base_rc["solvency_ratio"].notna().any():
            fig_s = px.box(base_rc, y="solvency_ratio", points=None if dist_points=="False" else dist_points,
                           title=f"Solvency Ratio Distribution — {fy_label(int(year))}", template=theme_variant)
            fig_s.add_hline(y=y_ref_line, line_dash="dash", line_color=BAD, annotation_text=f"Min {int(y_ref_line)}%")
            st.markdown(f'<div class="{chart_height_class}">', unsafe_allow_html=True)
            st.plotly_chart(fig_s, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("Solvency data not available.")
        st.markdown('</div>', unsafe_allow_html=True)

    with dv:
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        need = ["claims_ratio","solvency_ratio","gross_premium_total"]
        if not base_rc.empty and all(col in base_rc for col in need):
            bubble = base_rc.dropna(subset=need)
            if not bubble.empty:
                fig_b = px.scatter(bubble, x="claims_ratio", y="solvency_ratio",
                                   size=(bubble["gross_premium_total"]**0.5)*bubble_size_scale,
                                   color="type", hover_name="name",
                                   title=f"ICR vs Solvency (size=Premium) — {fy_label(int(year))}",
                                   template=theme_variant,
                                   color_discrete_sequence=[PRIMARY, ACCENT, PURPLE, TEAL, PINK, INDIGO, ORANGE, CYAN, LIME, AMBER])
                fig_b.add_hline(y=y_ref_line, line_dash="dot", line_color=BAD)
                st.markdown(f'<div class="{chart_height_class}">', unsafe_allow_html=True)
                st.plotly_chart(fig_b, use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.info("Need ICR, Solvency, and Premium.")
        else:
            st.info("Need ICR, Solvency, and Premium.")
        st.markdown('</div>', unsafe_allow_html=True)

# -------- RBSF KOBs (Catalog + Significant Activities) --------
with tab4:
    left, right = st.columns([1.25,0.75])

    master = {
        "Governance & Board Oversight": [
            "Board composition & skills review","Risk appetite refresh","Committee effectiveness","Fit & proper cycle"
        ],
        "Capital Adequacy & Solvency": [
            "ORSA & stress testing","Capital contingency plan","Solvency monitoring","Reinsurance credit risk review"
        ],
        "Underwriting & Pricing": [
            "UW manual updates","Pricing model validation","Product filing & approval","Segment loss ratio analysis"
        ],
        "Reinsurance & Risk Transfer": [
            "Treaty renewal strategy","Counterparty concentration","Cat cover adequacy","Retention vs cession review"
        ],
        "Claims Management": [
            "FNOL to settlement TAT","Repudiation governance","Leakage & SIU","Grievance/ombudsman outcomes"
        ],
        "Investment & ALM": [
            "SAA/TAA review","Duration & cashflow match","Credit limits","Concentration monitoring"
        ],
        "Distribution & Conduct": [
            "Banca & digital oversight","Mis-selling controls","Persistency & lapse","Complaint redressal"
        ],
        "Operations & IT/Cyber": [
            "Core PAS stability","Access & identity controls","DR/BCP drills","Endpoint & SOC monitoring"
        ],
        "Compliance & Regulatory": [
            "Reg returns timeliness","Reg inspections","Penalty RCA actions","Circular adherence"
        ],
        "Financial Reporting": [
            "Audit controls","Reserving & IBNR adequacy","Revenue recognition","IFRS/IndAS readiness"
        ]
    }

    with left:
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        st.subheader("KOB Catalog")
        kob_df = pd.DataFrame({
            "KOB": list(master.keys()),
            "Sample Activities": [", ".join(v[:2])+"..." for v in master.values()]
        })
        st.dataframe(kob_df, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Significant Activities mapping charts: category map (sunburst/treemap)
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        st.subheader("Significant Activities — Visual Map")
        map_mode = st.selectbox("Map Style", ["Sunburst","Treemap"], index=0, key="sig_map_mode")
        flat = []
        for k, acts in master.items():
            for a in acts:
                flat.append({"KOB":k, "Activity":a, "Count":1})
        flat_df = pd.DataFrame(flat)
        if map_mode == "Sunburst":
            figm = px.sunburst(flat_df, path=["KOB","Activity"], values="Count",
                               title="Activities by KOB — Sunburst", color="KOB",
                               color_discrete_sequence=px.colors.qualitative.Set3)
        else:
            figm = px.treemap(flat_df, path=["KOB","Activity"], values="Count",
                              title="Activities by KOB — Treemap", color="KOB",
                              color_discrete_sequence=px.colors.qualitative.Set2)
        st.plotly_chart(figm, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # UI Form
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        st.subheader("Significant Activities — UI Form")
        with st.form("kob_form"):
            sel_kob = st.selectbox("KOB", list(master.keys()), index=0)
            sel_activity = st.selectbox("Preset Activity", master[sel_kob], index=0)
            add_activity = st.text_area("Add/Override Activity (optional)", "")
            inherent = st.selectbox("Inherent Risk (UI only)", ["Low","Medium","High"], index=1)
            submitted = st.form_submit_button("Preview")
        shown_activity = add_activity if add_activity.strip() else sel_activity
        st.markdown(f"""
        <div style="background:rgba(255,255,255,0.7);border:1px solid #e5e7eb;border-radius:12px;padding:12px">
          <div><b>KOB:</b> {sel_kob}</div>
          <div><b>Activity:</b> {shown_activity}</div>
          <div><b>Inherent Risk:</b> <span class="pill">{inherent}</span></div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with right:
        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        st.subheader("Inherent Risk Register — UI Shell")
        reg_df = pd.DataFrame({
            "KOB": list(master.keys()),
            "Risk": ["Medium","High","Medium","Medium","High","Medium","High","High","Medium","Medium"]
        })
        st.dataframe(reg_df, use_container_width=True, hide_index=True)
        st.markdown('</div>', unsafe_allow_html=True)

        st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
        st.subheader("Quick KOB Tags")
        st.markdown("""
            <div>
              <span class="pill">Board Oversight</span>
              <span class="pill">Capital & Solvency</span>
              <span class="pill">Underwriting</span>
              <span class="pill">Reinsurance</span>
              <span class="pill">Claims</span>
              <span class="pill">Investments</span>
              <span class="pill">Conduct</span>
              <span class="pill">IT & Cyber</span>
              <span class="pill">Compliance</span>
              <span class="pill">Reporting</span>
            </div>
        """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

# -------- Risk Matrix (separate menu after KOBs) --------
with tab5:
    rm1, rm2, rm3 = st.columns([1.0, 1.0, 1.0])
    with rm1:
        risk_matrix_mode = st.selectbox("Risk Matrix View", ["Heatmap","Bubbles"], index=0)
    with rm2:
        rm_theme = st.selectbox("Matrix Theme", ["plotly","plotly_white","plotly_dark"], index=1)
    with rm3:
        st.caption("Likelihood × Impact view")

    st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""} {"xl-chart" if chart_height_class=="xl-chart" else "wide-chart"}">', unsafe_allow_html=True)
    st.plotly_chart(risk_matrix_figure(risk_matrix_mode, theme_variant=rm_theme), use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# -------- Supervisory Ladder (distinct colors each level) --------
with tab6:
    st.markdown(f'<div class="card {"glass" if style_mode in ["Liquid Glass","Vibrant Cards"] else ""}">', unsafe_allow_html=True)
    st.subheader("Supervisory Intervention Ladder — UI Model")

    ladder_levels = [
        "Normal Monitoring",
        "Enhanced Monitoring",
        "Intensified Supervision",
        "Corrective Action Plan",
        "Enforcement / Resolution"
    ]
    level = st.slider("Position on Ladder", 1, 5, 2, help="Visualize supervisory intensity and recommended focus")
    selected_label = ladder_levels[level-1]

    tone_map = {
        1: ("ok", "Stable — BAU monitoring, no material concerns"),
        2: ("warn", "Heightened watch — emerging risks, closer monitoring"),
        3: ("warn", "Intensified — targeted reviews, thematic exams"),
        4: ("danger", "Corrective Action — mandated actions, timelines, follow‑ups"),
        5: ("danger", "Enforcement/Resolution — penalties, restructuring, resolution")
    }
    css_class, summary = tone_map[level]

    st.markdown(f"""
    <div class="table-like {css_class}" style="border:1px solid #e5e7eb;border-radius:12px;padding:12px">
      <div><b>Current Ladder Position:</b> {selected_label}</div>
      <div style="margin-top:6px">{summary}</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("##### Focus Areas")
    focus = {
        1: [" BAU returns & KPIs", " Routine engagements", " Early warning indicators"],
        2: [" Additional MI & KRIs", " Thematic deep dives", " Early remediation signals"],
        3: [" Targeted inspections", " Capital/ICR reviews", " Governance enhancements"],
        4: [" Time‑bound CAP actions", " Board certifications", " Independent validation"],
        5: [" Enforcement processes", " Resolution planning", " Policyholder protection"]
    }
    st.markdown("<ul>" + "".join([f"<li>{x}</li>" for x in focus[level]]) + "</ul>", unsafe_allow_html=True)

    fig_ladder = ladder_bar(theme_variant)
    st.plotly_chart(fig_ladder, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ========== RUN HINT ==========
# Run from project root:
#   python3 -m pip install --upgrade pip
#   python3 -m pip install -r backend/requirements.txt
#   streamlit run backend/dashboard/app.py
