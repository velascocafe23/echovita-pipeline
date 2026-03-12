"""
dashboard.py — Dashboard de monitoreo del pipeline Echovita

Visualiza en tiempo real:
- Obituarios scrapeados del JSONL
- Resultados de la consolidación SCD
- Métricas del pipeline
"""

import json
import sys
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd
import streamlit as st

# ── Paths ─────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
JSONL_PATH = ROOT / "scraper" / "obituaries.jsonl"
sys.path.insert(0, str(ROOT))

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Echovita Pipeline",
    page_icon="🕊️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500&display=swap');

  html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
  }
  h1, h2, h3 {
    font-family: 'DM Serif Display', serif !important;
  }
  .main { background-color: #0f0f0f; }
  .stApp { background-color: #0f0f0f; color: #e8e0d5; }

  .metric-card {
    background: #1a1a1a;
    border: 1px solid #2a2a2a;
    border-left: 3px solid #c9a96e;
    padding: 1.2rem 1.5rem;
    border-radius: 4px;
    margin-bottom: 1rem;
  }
  .metric-value {
    font-family: 'DM Serif Display', serif;
    font-size: 2.5rem;
    color: #c9a96e;
    line-height: 1;
  }
  .metric-label {
    font-size: 0.75rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: #888;
    margin-top: 0.3rem;
  }
  .obituary-card {
    background: #161616;
    border: 1px solid #222;
    padding: 1.2rem 1.5rem;
    border-radius: 4px;
    margin-bottom: 0.75rem;
    transition: border-color 0.2s;
  }
  .obituary-card:hover { border-color: #c9a96e44; }
  .obit-name {
    font-family: 'DM Serif Display', serif;
    font-size: 1.15rem;
    color: #e8e0d5;
  }
  .obit-dates {
    font-size: 0.8rem;
    color: #c9a96e;
    margin: 0.2rem 0;
  }
  .obit-text {
    font-size: 0.82rem;
    color: #777;
    margin-top: 0.4rem;
    line-height: 1.6;
    display: -webkit-box;
    -webkit-line-clamp: 3;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }
  .status-ok {
    display: inline-block;
    background: #1a2e1a;
    color: #4caf50;
    border: 1px solid #2d4a2d;
    padding: 0.15rem 0.6rem;
    border-radius: 2px;
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  .status-warn {
    display: inline-block;
    background: #2e2a1a;
    color: #ffb74d;
    border: 1px solid #4a3d1a;
    padding: 0.15rem 0.6rem;
    border-radius: 2px;
    font-size: 0.7rem;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  div[data-testid="stSidebar"] {
    background-color: #111 !important;
    border-right: 1px solid #1e1e1e;
  }
  .sidebar-section {
    font-size: 0.7rem;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    color: #555;
    margin: 1.5rem 0 0.5rem;
  }
</style>
""", unsafe_allow_html=True)


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def load_obituaries() -> pd.DataFrame:
    """Carga obituarios desde el JSONL generado por el spider."""
    if not JSONL_PATH.exists():
        return pd.DataFrame()
    records = []
    with open(JSONL_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    return df


@st.cache_data(ttl=30)
def load_consolidation() -> pd.DataFrame:
    """Ejecuta la consolidación SCD y retorna el resultado."""
    try:
        from consolidation.consolidate import run_consolidation
        results = run_consolidation()
        return pd.DataFrame(results)
    except Exception as e:
        return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🕊️ Echovita")
    st.markdown("<div class='sidebar-section'>Pipeline Monitor</div>", unsafe_allow_html=True)

    jsonl_exists = JSONL_PATH.exists()
    if jsonl_exists:
        st.markdown("<span class='status-ok'>● JSONL Online</span>", unsafe_allow_html=True)
    else:
        st.markdown("<span class='status-warn'>● JSONL Not Found</span>", unsafe_allow_html=True)

    st.markdown("<div class='sidebar-section'>Navigation</div>", unsafe_allow_html=True)
    page = st.radio(
        "",
        ["📊 Overview", "📋 Obituaries", "🗂️ SCD Consolidation"],
        label_visibility="collapsed",
    )

    st.markdown("<div class='sidebar-section'>Actions</div>", unsafe_allow_html=True)
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown("---")
    st.markdown(
        "<div style='font-size:0.7rem;color:#444;'>Veritas Project · Echovita Integration</div>",
        unsafe_allow_html=True
    )


# ── Pages ─────────────────────────────────────────────────────────────────────

df_obits = load_obituaries()
df_scd   = load_consolidation()

# ── PAGE: Overview ────────────────────────────────────────────────────────────
if page == "📊 Overview":
    st.markdown("# Pipeline Overview")
    st.markdown(
        "<p style='color:#666;font-size:0.9rem;margin-top:-0.5rem;margin-bottom:2rem;'>"
        "Real-time monitoring of the Echovita data integration pipeline</p>",
        unsafe_allow_html=True
    )

    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    total_obits  = len(df_obits)
    with_dob     = int(df_obits["date_of_birth"].notna().sum()) if not df_obits.empty else 0
    with_text    = int(df_obits["obituary_text"].notna().sum()) if not df_obits.empty else 0
    persons_consolidated = len(df_scd)

    with col1:
        st.markdown(f"""
        <div class='metric-card'>
          <div class='metric-value'>{total_obits}</div>
          <div class='metric-label'>Obituaries Scraped</div>
        </div>""", unsafe_allow_html=True)

    with col2:
        pct = f"{with_dob/total_obits*100:.0f}%" if total_obits else "—"
        st.markdown(f"""
        <div class='metric-card'>
          <div class='metric-value'>{pct}</div>
          <div class='metric-label'>With Birth Date</div>
        </div>""", unsafe_allow_html=True)

    with col3:
        pct2 = f"{with_text/total_obits*100:.0f}%" if total_obits else "—"
        st.markdown(f"""
        <div class='metric-card'>
          <div class='metric-value'>{pct2}</div>
          <div class='metric-label'>With Obituary Text</div>
        </div>""", unsafe_allow_html=True)

    with col4:
        st.markdown(f"""
        <div class='metric-card'>
          <div class='metric-value'>{persons_consolidated}</div>
          <div class='metric-label'>Persons Consolidated</div>
        </div>""", unsafe_allow_html=True)

    # Pipeline status
    st.markdown("### Pipeline Status")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("**Component Health**")
        components = [
            ("Scrapy Spider",       jsonl_exists,          "Echovita obituaries scraper"),
            ("S3 Mock Pipeline",    jsonl_exists,          "AWS S3 upload simulation"),
            ("GCS Mock Pipeline",   jsonl_exists,          "GCP Storage upload simulation"),
            ("JSONL Export",        jsonl_exists,          f"{JSONL_PATH.name}"),
            ("DuckDB Consolidation",not df_scd.empty,      "SCD Type 2 consolidation"),
            ("Airflow DAG",         True,                  "Daily 08:00 UTC schedule"),
        ]
        for name, ok, desc in components:
            status = "✅" if ok else "⚠️"
            color  = "#4caf50" if ok else "#ffb74d"
            st.markdown(
                f"<div style='display:flex;justify-content:space-between;padding:0.4rem 0;"
                f"border-bottom:1px solid #1e1e1e;font-size:0.85rem;'>"
                f"<span>{status} {name}</span>"
                f"<span style='color:#555;font-size:0.75rem;'>{desc}</span></div>",
                unsafe_allow_html=True
            )

    with col_b:
        if not df_obits.empty:
            st.markdown("**Field Coverage**")
            fields = ["full_name", "date_of_birth", "date_of_death", "obituary_text"]
            labels = ["Full Name", "Date of Birth", "Date of Death", "Obituary Text"]
            for field, label in zip(fields, labels):
                pct = df_obits[field].notna().mean() * 100 if field in df_obits.columns else 0
                bar_w = int(pct)
                color = "#4caf50" if pct > 80 else "#ffb74d" if pct > 40 else "#ef5350"
                st.markdown(f"""
                <div style='margin-bottom:0.8rem;'>
                  <div style='display:flex;justify-content:space-between;font-size:0.8rem;margin-bottom:0.2rem;'>
                    <span>{label}</span><span style='color:{color};'>{pct:.0f}%</span>
                  </div>
                  <div style='background:#1e1e1e;border-radius:2px;height:4px;'>
                    <div style='background:{color};width:{bar_w}%;height:4px;border-radius:2px;'></div>
                  </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.info("Run the spider first to see field coverage.")

    if not df_obits.empty and "scraped_at" in df_obits.columns:
        last_run = df_obits["scraped_at"].max()
        st.markdown(
            f"<div style='color:#555;font-size:0.75rem;margin-top:1rem;'>Last scrape: {last_run}</div>",
            unsafe_allow_html=True
        )


# ── PAGE: Obituaries ──────────────────────────────────────────────────────────
elif page == "📋 Obituaries":
    st.markdown("# Obituaries")

    if df_obits.empty:
        st.warning("No obituaries found. Run the spider first: `cd scraper && scrapy crawl echovita`")
    else:
        # Search + filter
        col1, col2 = st.columns([3, 1])
        with col1:
            search = st.text_input("🔍 Search by name", placeholder="e.g. John")
        with col2:
            show_nulls = st.checkbox("Show incomplete records", value=True)

        filtered = df_obits.copy()
        if search:
            filtered = filtered[
                filtered["full_name"].str.contains(search, case=False, na=False)
            ]
        if not show_nulls:
            filtered = filtered[
                filtered["date_of_birth"].notna() &
                filtered["date_of_death"].notna() &
                filtered["obituary_text"].notna()
            ]

        st.markdown(
            f"<div style='color:#555;font-size:0.8rem;margin-bottom:1rem;'>"
            f"Showing {len(filtered)} of {len(df_obits)} records</div>",
            unsafe_allow_html=True
        )

        for _, row in filtered.iterrows():
            dob = row.get("date_of_birth") or "—"
            dod = row.get("date_of_death") or "—"
            text = row.get("obituary_text") or "No obituary text available."
            url  = row.get("source_url", "")
            text_preview = str(text)[:300] + "..." if len(str(text)) > 300 else str(text)

            st.markdown(f"""
            <div class='obituary-card'>
              <div class='obit-name'>{row.get('full_name', 'Unknown')}</div>
              <div class='obit-dates'>🕊️ {dob} — {dod}</div>
              <div class='obit-text'>{text_preview}</div>
              <div style='margin-top:0.5rem;'>
                <a href='{url}' target='_blank'
                   style='font-size:0.72rem;color:#c9a96e44;text-decoration:none;
                          letter-spacing:0.05em;'>VIEW SOURCE →</a>
              </div>
            </div>""", unsafe_allow_html=True)

        # Raw data toggle
        with st.expander("📥 View raw data"):
            st.dataframe(filtered, use_container_width=True)


# ── PAGE: SCD Consolidation ───────────────────────────────────────────────────
elif page == "🗂️ SCD Consolidation":
    st.markdown("# SCD Consolidation")
    st.markdown(
        "<p style='color:#666;font-size:0.9rem;margin-top:-0.5rem;margin-bottom:2rem;'>"
        "Slowly Changing Dimensions Type 2 — one row per person</p>",
        unsafe_allow_html=True
    )

    if df_scd.empty:
        st.warning("Consolidation results not available.")
    else:
        # Summary metrics
        cols = st.columns(3)
        with cols[0]:
            st.markdown(f"""
            <div class='metric-card'>
              <div class='metric-value'>{len(df_scd)}</div>
              <div class='metric-label'>Total Persons</div>
            </div>""", unsafe_allow_html=True)
        with cols[1]:
            avg = df_scd["distinct_cities"].mean()
            st.markdown(f"""
            <div class='metric-card'>
              <div class='metric-value'>{avg:.1f}</div>
              <div class='metric-label'>Avg Cities / Person</div>
            </div>""", unsafe_allow_html=True)
        with cols[2]:
            no_city = int((df_scd["last_non_null_city"].isna()).sum())
            st.markdown(f"""
            <div class='metric-card'>
              <div class='metric-value'>{no_city}</div>
              <div class='metric-label'>No City Data</div>
            </div>""", unsafe_allow_html=True)

        # Table
        st.markdown("### Consolidated Records")
        styled = df_scd.copy()
        styled.columns = [c.replace("_", " ").title() for c in styled.columns]
        st.dataframe(styled, use_container_width=True, hide_index=True)

        # SQL explanation
        with st.expander("🔍 View consolidation SQL"):
            from consolidation.consolidate import CONSOLIDATION_QUERY
            st.code(CONSOLIDATION_QUERY, language="sql")
