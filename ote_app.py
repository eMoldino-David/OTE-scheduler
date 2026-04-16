import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import hashlib
import json

st.set_page_config(layout="wide", page_title="OTE Dashboard v1.0", page_icon="🏭")

# ── Try importing RR engine ────────────────────────────────────────────────
try:
    import rr_utils_startup as rr_utils
    RR_ENGINE = True
except ImportError:
    try:
        import run_rate_utils as rr_utils
        RR_ENGINE = True
    except ImportError:
        RR_ENGINE = False

# ── Try importing Firebase ─────────────────────────────────────────────────
try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

APP_VERSION = "v1.0"

# ══════════════════════════════════════════════════════════════════════════
# AUTHENTICATION
# ══════════════════════════════════════════════════════════════════════════
def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if st.session_state.authenticated:
        return True
    st.markdown("""
    <div style="max-width:380px;margin:8rem auto 0;">
        <h2 style="margin-bottom:1.5rem;font-size:1.4rem;">
            🏭 OTE Dashboard <span style="font-size:0.8rem;opacity:0.5;">v1.0</span>
        </h2>
    </div>
    """, unsafe_allow_html=True)
    col_l, col_m, col_r = st.columns([1, 2, 1])
    with col_m:
        pw = st.text_input("Password", type="password", key="ote_pw")
        if st.button("Login", use_container_width=True):
            hashed = hashlib.sha256(pw.encode()).hexdigest()
            stored = st.secrets.get("OTE_PASSWORD_HASH", "")
            if not stored:
                plain = st.secrets.get("OTE_PASSWORD", "ote2024")
                if pw == plain:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Incorrect password.")
            elif hashed == stored:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password.")
    return False

# ══════════════════════════════════════════════════════════════════════════
# FIREBASE
# ══════════════════════════════════════════════════════════════════════════
@st.cache_resource
def init_firebase():
    if not FIREBASE_AVAILABLE:
        return None
    try:
        if not firebase_admin._apps:
            if "firebase" in st.secrets:
                cred_dict = dict(st.secrets["firebase"])
                cred = credentials.Certificate(cred_dict)
            else:
                cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception as e:
        return None

def fb_get_collection(db, collection):
    if db is None:
        return []
    try:
        return [doc.to_dict() | {"_id": doc.id}
                for doc in db.collection(collection).stream()]
    except Exception:
        return []

def fb_save(db, collection, doc_id, data):
    if db is None:
        return False
    try:
        db.collection(collection).document(doc_id).set(data, merge=True)
        return True
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════════════════
# REASON CODES
# ══════════════════════════════════════════════════════════════════════════
PLANNED_REASONS = [
    "Scheduled Maintenance", "Tooling Changeover", "Planned Trial / Sampling",
    "Bank Holiday / Plant Shutdown", "Lubrication / Cleaning",
    "Quality Inspection Hold", "Material Change",
]
UNPLANNED_REASONS = [
    "Broken / Damaged Tool Component", "Ejection Problem", "Material Issue",
    "Machine Fault", "Operator Issue", "Cooling Issue", "Hydraulic Fault",
    "Sensor / Control Fault", "Unknown / Under Investigation",
]
SCRAP_REASONS = [
    "Flash", "Short Shot", "Sink Mark", "Warpage / Deformation",
    "Burn Mark", "Dimensional Out of Spec", "Surface Defect",
    "Cold Weld / Flow Mark", "Contamination", "Unknown",
]

# ══════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ══════════════════════════════════════════════════════════════════════════
def _normalise_columns(df):
    """Map raw DB column names to internal names used by the RR engine."""
    col_map = {
        'TOOLING ID': 'tool_id', 'EQUIPMENT_CODE': 'tool_id', 'EQUIPMENT CODE': 'tool_id',
        'SHOT TIME': 'shot_time', 'LOCAL_SHOT_TIME': 'shot_time',
        'ACTUAL CT': 'actual_ct', 'CT': 'actual_ct',
        'APPROVED CT': 'approved_ct', 'APPROVED_CT': 'approved_ct',
        'WORKING CAVITIES': 'working_cavities', 'WORKING_CAVITIES': 'working_cavities',
        'SUPPLIER': 'supplier_id', 'SUPPLIER_ID': 'supplier_id',
        'TOOLING TYPE': 'tooling_type', 'TOOLING_TYPE': 'tooling_type',
        'PART': 'part_id', 'PART_ID': 'part_id', 'PART ID': 'part_id',
        'PLANT': 'plant_id', 'PLANT_ID': 'plant_id',
        'MATERIAL': 'material', 'PART NAME': 'part_name', 'PART_NAME': 'part_name',
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if 'shot_time' in df.columns:
        df['shot_time'] = pd.to_datetime(df['shot_time'], dayfirst=False, errors='coerce')
        df = df.dropna(subset=['shot_time'])
    if 'actual_ct' in df.columns:
        df['actual_ct'] = pd.to_numeric(df['actual_ct'], errors='coerce')
        df = df.dropna(subset=['actual_ct'])
    if 'tool_id' in df.columns:
        df['tool_id'] = df['tool_id'].astype(str)
    return df

@st.cache_data(show_spinner="Loading production data...")
def load_data(files, _cache_ver=APP_VERSION):
    if RR_ENGINE:
        df = rr_utils.load_all_data(files)
        if df.empty:
            # Fallback: load raw and normalise
            dfs = []
            for f in files:
                try:
                    raw = pd.read_excel(f) if str(f.name).endswith(('.xlsx','.xls')) else pd.read_csv(f)
                    dfs.append(_normalise_columns(raw))
                except Exception:
                    pass
            return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        return df
    dfs = []
    for f in files:
        try:
            raw = pd.read_excel(f) if str(f.name).endswith(('.xlsx','.xls')) else pd.read_csv(f)
            dfs.append(_normalise_columns(raw))
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ══════════════════════════════════════════════════════════════════════════
# MINIMAL STANDALONE ENGINE (no rr_utils dependency)
# ══════════════════════════════════════════════════════════════════════════
def _get_stable_mode(series):
    s = series.dropna()
    if s.empty: return np.nan
    rounded = s.round(2)
    modes = rounded.mode()
    return float(modes.iloc[0]) if not modes.empty else float(s.median())

def _process_tool_df(df, tolerance, downtime_gap, run_interval_hours):
    """Minimal RR-equivalent processing — produces stop_flag, run_id, mode_ct."""
    df = df.copy().sort_values('shot_time').reset_index(drop=True)
    df['actual_ct'] = pd.to_numeric(df['actual_ct'], errors='coerce')
    df = df.dropna(subset=['actual_ct','shot_time'])
    if len(df) < 2:
        return pd.DataFrame(), {}

    df['time_diff_sec'] = df['shot_time'].diff().dt.total_seconds().fillna(0)
    mask_first = pd.Series([True] + [False]*(len(df)-1), index=df.index)
    df.loc[mask_first, 'time_diff_sec'] = df.loc[mask_first, 'actual_ct']

    is_new_run = df['time_diff_sec'] > (run_interval_hours * 3600)
    df['run_id'] = (is_new_run | mask_first).cumsum()

    run_modes = (df[df['actual_ct'] < 1000]
                 .groupby('run_id')['actual_ct']
                 .apply(_get_stable_mode))
    df['mode_ct'] = df['run_id'].map(run_modes).fillna(df['actual_ct'].median())
    df['mode_lower'] = df['mode_ct'] * (1 - tolerance)
    df['mode_upper'] = df['mode_ct'] * (1 + tolerance)

    df['next_diff'] = df['time_diff_sec'].shift(-1).fillna(0)
    is_gap   = df['next_diff'] > (df['actual_ct'] + downtime_gap)
    is_abn   = (df['actual_ct'] < df['mode_lower']) | (df['actual_ct'] > df['mode_upper'])
    is_hard  = df['actual_ct'] >= 999.9
    df['stop_flag'] = np.where(is_gap | is_abn | is_hard, 1, 0)

    startup_ct_ok = df['actual_ct'] < (df['mode_ct'] * 5)
    df.loc[(mask_first | is_new_run) & startup_ct_ok, 'stop_flag'] = 0
    df['prev_stop'] = df['stop_flag'].shift(1, fill_value=0)
    df['stop_event'] = ((df['stop_flag'] == 1) & (df['prev_stop'] == 0)).astype(int)
    df['adj_ct_sec'] = df['actual_ct'].copy()
    df.loc[is_gap, 'adj_ct_sec'] = df.loc[is_gap, 'next_diff']

    # Metrics
    run_durations = []
    for _, rdf in df.groupby('run_id'):
        if rdf.empty: continue
        dur = (rdf['shot_time'].max() - rdf['shot_time'].min()).total_seconds() + float(rdf.iloc[-1]['actual_ct'])
        run_durations.append(dur)

    total_runtime = sum(run_durations)
    prod_df = df[df['stop_flag'] == 0]
    prod_time = float(prod_df['actual_ct'].sum())
    downtime = max(0, total_runtime - prod_time)
    total_shots = len(df)
    normal_shots = len(prod_df)
    stop_events = int(df['stop_event'].sum())

    res = {
        'processed_df': df,
        'total_runtime_sec': total_runtime,
        'production_time_sec': prod_time,
        'downtime_sec': downtime,
        'total_shots': total_shots,
        'normal_shots': normal_shots,
        'stops': stop_events,
    }
    return df, res

# ══════════════════════════════════════════════════════════════════════════
# OTE CALCULATIONS
# ══════════════════════════════════════════════════════════════════════════
def compute_ote(df_tool, tolerance, downtime_gap, run_interval,
                ct_mode="Mode CT", scrap_confirmed=0, planned_downtime_sec=0):
    """Compute OTE pillars for a single tool's processed df."""
    if df_tool.empty:
        return {}

    df_proc, res = _process_tool_df(df_tool, tolerance, downtime_gap, run_interval)

    if df_proc.empty:
        return {}

    total_runtime = res.get('total_runtime_sec', 0)
    prod_time     = res.get('production_time_sec', 0)
    downtime_sec  = res.get('downtime_sec', 0)
    total_shots   = res.get('total_shots', 0)
    normal_shots  = res.get('normal_shots', 0)
    stop_events   = res.get('stops', 0)

    # Adjusted availability — remove planned downtime from denominator
    planned = min(planned_downtime_sec, total_runtime)
    avail_denom = max(total_runtime - planned, 1)
    unplanned_down = max(downtime_sec - planned, 0)
    availability = max(0, min(100, (avail_denom - unplanned_down) / avail_denom * 100))

    # Performance — vs selected CT benchmark
    if ct_mode == "Mode CT" and 'mode_ct' in df_proc.columns:
        benchmark_ct = float(df_proc['mode_ct'].dropna().median()) if not df_proc['mode_ct'].dropna().empty else None
    elif ct_mode == "Approved CT" and 'approved_ct' in df_proc.columns:
        benchmark_ct = float(df_proc['approved_ct'].dropna().median()) if not df_proc['approved_ct'].dropna().empty else None
    else:
        benchmark_ct = None

    if benchmark_ct and benchmark_ct > 0 and prod_time > 0:
        expected_shots = prod_time / benchmark_ct
        performance = min(100, (normal_shots / expected_shots * 100)) if expected_shots > 0 else 0
    else:
        performance = (normal_shots / total_shots * 100) if total_shots > 0 else 0

    # Quality
    cavities = 1
    if 'working_cavities' in df_proc.columns:
        cavities = int(df_proc['working_cavities'].dropna().median()) if not df_proc['working_cavities'].dropna().empty else 1
    actual_output = normal_shots * cavities
    if scrap_confirmed > 0 and actual_output > 0:
        quality = max(0, (1 - scrap_confirmed / actual_output) * 100)
        quality_confirmed = True
    else:
        quality = 100.0
        quality_confirmed = False

    ote = (availability / 100) * (performance / 100) * (quality / 100) * 100

    # Flag consecutive stops for scrap confidence
    stop_flags = df_proc.get('stop_flag', pd.Series(dtype=int)) if 'stop_flag' in df_proc.columns else pd.Series(dtype=int)
    consecutive = 0
    if len(stop_flags) > 1:
        consecutive = int((stop_flags & stop_flags.shift(1, fill_value=0)).sum())

    return {
        'ote': ote,
        'availability': availability,
        'performance': performance,
        'quality': quality,
        'quality_confirmed': quality_confirmed,
        'total_runtime': total_runtime,
        'prod_time': prod_time,
        'downtime_sec': downtime_sec,
        'total_shots': total_shots,
        'normal_shots': normal_shots,
        'stop_events': stop_events,
        'actual_output': actual_output,
        'cavities': cavities,
        'benchmark_ct': benchmark_ct,
        'consecutive_stops': consecutive,
        'processed_df': df_proc,
    }

def ote_color(score):
    if score >= 70: return "#27AE60"
    if score >= 50: return "#F39C12"
    return "#E74C3C"

def ote_label(score):
    if score >= 70: return "🟢"
    if score >= 50: return "🟡"
    return "🔴"

def flag_scrap_events(df_proc, tool_id):
    """Return list of scrap flag dicts from consecutive stop events."""
    flags = []
    if df_proc.empty or 'stop_flag' not in df_proc.columns:
        return flags
    sf = df_proc['stop_flag'].values
    times = df_proc['shot_time'].values
    i = 0
    while i < len(sf):
        if sf[i] == 1:
            j = i
            while j < len(sf) and sf[j] == 1:
                j += 1
            count = j - i
            confidence = "high" if count >= 2 else "low"
            flags.append({
                'tool_id': tool_id,
                'timestamp_start': str(pd.Timestamp(times[i])),
                'timestamp_end': str(pd.Timestamp(times[j-1])),
                'stop_count': count,
                'confidence': confidence,
                'flagged_qty': count,
                'confirmed_qty': 0,
                'status': 'flagged',
                'reason': '',
                'confirmed_by': '',
                'confirmed_at': '',
            })
            i = j
        else:
            i += 1
    return flags

# ══════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ══════════════════════════════════════════════════════════════════════════
PC = {"green":"#77DD77","red":"#FF6961","orange":"#FFB347","blue":"#3498DB",
      "grey":"#95A5A6","purple":"#9B59B6"}

def score_gauge(value, title, color=None):
    c = color or ote_color(value)
    plot_val = max(0, min(value, 100))
    fig = go.Figure(data=[go.Pie(
        values=[plot_val, 100-plot_val], hole=0.75, sort=False,
        direction='clockwise', textinfo='none',
        marker=dict(colors=[c, '#2C2C2C']), hoverinfo='none'
    )])
    fig.update_layout(
        annotations=[dict(
            text=f"<b>{value:.1f}%</b>",
            x=0.5, y=0.5, font_size=22, showarrow=False,
            font=dict(color=c)
        )],
        showlegend=False,
        margin=dict(t=30, b=10, l=10, r=10),
        height=180,
        title=dict(text=title, x=0.5, font=dict(size=13)),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    return fig

def metric_chip(value, label, bg):
    return (f'<span style="background:{bg};color:#0E1117;padding:3px 8px;'
            f'border-radius:10px;font-size:0.8rem;font-weight:bold;">'
            f'{value} {label}</span>')

def fmt_dhm(sec):
    if RR_ENGINE:
        return rr_utils.format_duration(sec)
    sec = int(sec)
    d, r = divmod(sec, 86400)
    h, r = divmod(r, 3600)
    m = r // 60
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    parts.append(f"{m}m")
    return " ".join(parts)

# ══════════════════════════════════════════════════════════════════════════
# PAGE: PLANT OVERVIEW
# ══════════════════════════════════════════════════════════════════════════
def page_plant_overview(df_all, config, db):
    st.header("🏭 Plant Overview")

    if df_all.empty:
        st.info("Upload production data using the sidebar to begin.")
        return

    id_col = 'tool_id' if 'tool_id' in df_all.columns else df_all.columns[0]
    tool_ids = sorted([str(x) for x in df_all[id_col].unique()
                       if str(x).lower() not in ['nan','none','unknown']])

    # Cascading filters
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        plants = sorted(df_all['plant_id'].dropna().unique()) if 'plant_id' in df_all.columns else []
        sel_plant = st.multiselect("Plant", plants, default=[], key="ov_plant")
    with fc2:
        suppliers = sorted(df_all['supplier_id'].dropna().unique()) if 'supplier_id' in df_all.columns else []
        sel_sup = st.multiselect("Supplier / Line", suppliers, default=[], key="ov_sup")
    with fc3:
        sel_tools = st.multiselect("Tools", tool_ids, default=[], key="ov_tools",
                                   placeholder="All tools")

    df_f = df_all.copy()
    if sel_plant and 'plant_id' in df_f.columns:
        df_f = df_f[df_f['plant_id'].isin(sel_plant)]
    if sel_sup and 'supplier_id' in df_f.columns:
        df_f = df_f[df_f['supplier_id'].isin(sel_sup)]
    if sel_tools:
        df_f = df_f[df_f[id_col].astype(str).isin(sel_tools)]

    active_tools = sorted([str(x) for x in df_f[id_col].unique()
                           if str(x).lower() not in ['nan','none','unknown']])
    if not active_tools:
        st.warning("No tools match the current filters.")
        return

    # Load confirmed downtime from Firebase
    dt_records = {r['_id']: r for r in fb_get_collection(db, 'downtime_log')}
    sc_records = {r['_id']: r for r in fb_get_collection(db, 'scrap_log')}

    # Compute OTE per tool
    rows = []
    progress = st.progress(0, text="Computing OTE scores...")
    for i, tid in enumerate(active_tools):
        progress.progress((i+1)/len(active_tools), text=f"Processing {tid}...")
        t_df = df_f[df_f[id_col].astype(str) == tid]
        if t_df.empty:
            continue

        # Sum confirmed planned downtime for this tool
        planned_sec = sum(
            r.get('duration_sec', 0)
            for r in dt_records.values()
            if r.get('tool_id') == tid and r.get('confirmed_type') == 'planned'
        )
        confirmed_scrap = sum(
            r.get('confirmed_qty', 0)
            for r in sc_records.values()
            if r.get('tool_id') == tid and r.get('status') == 'confirmed'
        )
        unconfirmed_flags = sum(
            1 for r in sc_records.values()
            if r.get('tool_id') == tid and r.get('status') == 'flagged'
        )
        unconfirmed_dt = sum(
            1 for r in dt_records.values()
            if r.get('tool_id') == tid and r.get('status') == 'unconfirmed'
        )

        m = compute_ote(t_df, config['tolerance'], config['downtime_gap'],
                        config['run_interval'], config['ct_mode'],
                        confirmed_scrap, planned_sec)
        if not m:
            continue

        rows.append({
            'tool_id': tid,
            'plant_id': t_df['plant_id'].iloc[0] if 'plant_id' in t_df.columns else '—',
            'supplier': t_df['supplier_id'].iloc[0] if 'supplier_id' in t_df.columns else '—',
            'OTE (%)': round(m['ote'], 1),
            'Availability (%)': round(m['availability'], 1),
            'Performance (%)': round(m['performance'], 1),
            'Quality (%)': round(m['quality'], 1),
            'q_confirmed': m['quality_confirmed'],
            'Total Shots': m['total_shots'],
            'Stop Events': m['stop_events'],
            'Downtime': fmt_dhm(m['downtime_sec']),
            'Flagged Items': unconfirmed_flags + unconfirmed_dt,
            '_m': m,
        })
    progress.empty()

    if not rows:
        st.warning("Could not compute OTE for any tools.")
        return

    rank_df = pd.DataFrame(rows).sort_values('OTE (%)', ascending=True)

    # Fleet KPI bar
    st.markdown("---")
    k1, k2, k3, k4, k5 = st.columns(5)
    fleet_ote = rank_df['OTE (%)'].mean()
    k1.metric("Fleet OTE", f"{fleet_ote:.1f}%")
    k2.metric("Tools Analysed", len(rank_df))
    k3.metric("Total Shots", f"{rank_df['Total Shots'].sum():,.0f}")
    k4.metric("Total Stop Events", f"{rank_df['Stop Events'].sum():,.0f}")
    k5.metric("Flagged Items", f"{rank_df['Flagged Items'].sum():,.0f}")
    st.markdown("---")

    # Ranking table
    st.subheader("Tool Ranking")

    def style_score(val):
        c = ote_color(val)
        return f'background-color:{c}22;color:{c};font-weight:bold'

    display_df = rank_df[[
        'tool_id','plant_id','supplier','OTE (%)','Availability (%)','Performance (%)',
        'Quality (%)','Total Shots','Stop Events','Downtime','Flagged Items'
    ]].copy()
    display_df.insert(0, '', rank_df['OTE (%)'].apply(ote_label))

    styled = display_df.style\
        .applymap(style_score, subset=['OTE (%)','Availability (%)','Performance (%)'])\
        .format({'OTE (%)':'{:.1f}','Availability (%)':'{:.1f}',
                 'Performance (%)':'{:.1f}','Quality (%)':'{:.1f}',
                 'Total Shots':'{:,.0f}','Stop Events':'{:,.0f}'})

    st.dataframe(styled, use_container_width=True, hide_index=True)

    # Tool drill-down
    st.markdown("---")
    st.subheader("Tool Drill-down")
    sel_drill = st.selectbox("Select tool", [r['tool_id'] for r in rows],
                             key="ov_drill")
    drill = next((r for r in rows if r['tool_id'] == sel_drill), None)
    if drill:
        m = drill['_m']
        g1, g2, g3, g4 = st.columns(4)
        with g1:
            with st.container(border=True):
                st.plotly_chart(score_gauge(drill['OTE (%)'], "OTE Score"),
                                use_container_width=True, key="drill_ote")
        with g2:
            with st.container(border=True):
                st.plotly_chart(score_gauge(drill['Availability (%)'],
                                            "Availability", PC['blue']),
                                use_container_width=True, key="drill_av")
        with g3:
            with st.container(border=True):
                st.plotly_chart(score_gauge(drill['Performance (%)'],
                                            "Performance", PC['green']),
                                use_container_width=True, key="drill_pf")
        with g4:
            with st.container(border=True):
                st.plotly_chart(score_gauge(drill['Quality (%)'],
                                            "Quality",
                                            PC['green'] if drill['q_confirmed'] else PC['grey']),
                                use_container_width=True, key="drill_ql")
                if not drill['q_confirmed']:
                    st.caption("⚠️ Quality unconfirmed — awaiting scrap data")

        with st.container(border=True):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Run Duration", fmt_dhm(m['total_runtime']))
            c2.metric("Production Time", fmt_dhm(m['prod_time']))
            c3.metric("RR Downtime", fmt_dhm(m['downtime_sec']))
            c4.metric("Benchmark CT", f"{m['benchmark_ct']:.2f}s" if m['benchmark_ct'] else "N/A")

        col_dl, col_sc = st.columns(2)
        with col_dl:
            if st.button(f"📋 View Downtime Log → {sel_drill}", key="drill_dt_btn"):
                st.session_state['dt_filter_tool'] = sel_drill
                st.session_state['page'] = 'Downtime Log'
                st.rerun()
        with col_sc:
            if st.button(f"🗑️ View Scrap Log → {sel_drill}", key="drill_sc_btn"):
                st.session_state['sc_filter_tool'] = sel_drill
                st.session_state['page'] = 'Scrap / Reject Log'
                st.rerun()

# ══════════════════════════════════════════════════════════════════════════
# PAGE: PARTS PRODUCED
# ══════════════════════════════════════════════════════════════════════════
def page_parts_produced(df_all, config, db):
    st.header("📊 Parts Produced")

    if df_all.empty:
        st.info("Upload production data to view parts produced.")
        return

    id_col = 'tool_id' if 'tool_id' in df_all.columns else df_all.columns[0]

    # Controls
    cc1, cc2, cc3, cc4 = st.columns(4)
    with cc1:
        tool_ids = sorted([str(x) for x in df_all[id_col].unique()
                           if str(x).lower() not in ['nan','none','unknown']])
        sel_tool = st.selectbox("Tool", tool_ids, key="pp_tool")
    with cc2:
        dates = sorted(df_all['shot_time'].dt.date.unique()) if 'shot_time' in df_all.columns else []
        sel_date = st.selectbox("Date", dates, index=len(dates)-1 if dates else 0,
                                format_func=lambda x: x.strftime('%d %b %Y'),
                                key="pp_date")
    with cc3:
        color_by = st.selectbox("Colour by", ["Output vs Expected","OTE Score"], key="pp_color")
    with cc4:
        st.markdown("<br>", unsafe_allow_html=True)
        show_expected = st.checkbox("Show expected line", value=True, key="pp_exp")

    t_df = df_all[df_all[id_col].astype(str) == sel_tool].copy()
    if 'shot_time' in t_df.columns and sel_date:
        t_df = t_df[t_df['shot_time'].dt.date == sel_date]

    if t_df.empty:
        st.warning("No data for this tool/date.")
        return

    df_proc, _ = _process_tool_df(
        t_df, config['tolerance'], config['downtime_gap'], config['run_interval']
    )

    if df_proc.empty:
        st.warning("Could not process data.")
        return

    # Build hourly grid
    df_proc = df_proc.copy()
    df_proc['hour'] = df_proc['shot_time'].dt.floor('h')
    cavities = 1
    if 'working_cavities' in df_proc.columns:
        cavities = int(df_proc['working_cavities'].dropna().median()) or 1

    mode_ct = float(df_proc['mode_ct'].dropna().median()) \
        if 'mode_ct' in df_proc.columns and not df_proc['mode_ct'].dropna().empty else None
    approved_ct = float(df_proc['approved_ct'].dropna().median()) \
        if 'approved_ct' in df_proc.columns and not df_proc['approved_ct'].dropna().empty else None

    benchmark = None
    if config['ct_mode'] == "Mode CT" and mode_ct:
        benchmark = mode_ct
    elif config['ct_mode'] == "Approved CT" and approved_ct:
        benchmark = approved_ct

    hourly = df_proc.groupby('hour').agg(
        shots=('shot_time', 'count'),
        normal=('stop_flag', lambda x: (x == 0).sum()),
        stops=('stop_flag', 'sum'),
    ).reset_index()
    hourly['parts'] = hourly['normal'] * cavities
    if benchmark:
        hourly['expected'] = (3600 / benchmark) * cavities
        hourly['pct_of_expected'] = (hourly['parts'] / hourly['expected'].replace(0, np.nan) * 100).fillna(0)
    else:
        hourly['expected'] = 0
        hourly['pct_of_expected'] = 100.0

    def cell_color(pct):
        if pct >= 90: return PC['green']
        if pct >= 70: return PC['orange']
        return PC['red']

    hourly['color'] = hourly['pct_of_expected'].apply(cell_color)
    hourly['hour_lbl'] = hourly['hour'].dt.strftime('%H:%M')

    # Shift bands
    shift_starts = [int(x) for x in config.get('shift_hours', [6, 14, 22])]
    def get_shift(h):
        hour = h.hour
        for i in range(len(shift_starts)-1, -1, -1):
            if hour >= shift_starts[i]:
                return f"Shift {i+1}"
        return f"Shift {len(shift_starts)}"
    hourly['shift'] = hourly['hour'].apply(get_shift)

    # KPI row
    kc1, kc2, kc3, kc4 = st.columns(4)
    kc1.metric("Total Parts", f"{int(hourly['parts'].sum()):,}")
    if benchmark:
        kc2.metric("Expected Parts", f"{int(hourly['expected'].sum()):,}")
        kc3.metric("Achievement", f"{hourly['parts'].sum()/max(hourly['expected'].sum(),1)*100:.1f}%")
    kc4.metric(f"Benchmark CT ({config['ct_mode']})", f"{benchmark:.2f}s" if benchmark else "N/A")

    st.markdown("---")

    # Bar chart
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=hourly['hour_lbl'], y=hourly['parts'],
        marker_color=hourly['color'],
        name='Actual Parts',
        text=hourly['parts'].apply(lambda x: f"{int(x):,}"),
        textposition='outside',
        hovertemplate="<b>%{x}</b><br>Parts: %{y:,.0f}<extra></extra>"
    ))
    if show_expected and benchmark:
        fig.add_trace(go.Scatter(
            x=hourly['hour_lbl'], y=hourly['expected'],
            mode='lines', name=f'Expected ({config["ct_mode"]})',
            line=dict(color='white', width=1.5, dash='dash')
        ))
    fig.update_layout(
        title=f"Hourly Parts Produced — {sel_tool} — {sel_date}",
        xaxis_title="Hour", yaxis_title="Parts",
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        height=380, legend=dict(orientation='h', y=1.1)
    )
    st.plotly_chart(fig, use_container_width=True)

    # Hourly detail table
    with st.expander("View Hourly Detail Table", expanded=False):
        disp = hourly[['hour_lbl','shift','shots','normal','stops','parts']].copy()
        if benchmark:
            disp['expected'] = hourly['expected'].round(0).astype(int)
            disp['% of Expected'] = hourly['pct_of_expected'].round(1)
        disp.columns = ['Hour','Shift','Total Shots','Normal Shots',
                        'Stop Events','Parts Produced'] + \
                       (['Expected','% of Expected'] if benchmark else [])
        st.dataframe(disp, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════
# PAGE: DOWNTIME LOG
# ══════════════════════════════════════════════════════════════════════════
def page_downtime_log(df_all, config, db):
    st.header("⏱️ Downtime Log")

    if df_all.empty:
        st.info("Upload production data to view downtime events.")
        return

    id_col = 'tool_id' if 'tool_id' in df_all.columns else df_all.columns[0]
    tool_ids = sorted([str(x) for x in df_all[id_col].unique()
                       if str(x).lower() not in ['nan','none','unknown']])

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        default_tool = [st.session_state.pop('dt_filter_tool', None)]
        default_tool = [t for t in default_tool if t in tool_ids]
        sel_tools = st.multiselect("Filter by Tool", tool_ids,
                                   default=default_tool, key="dt_tools")
    with fc2:
        sel_status = st.multiselect("Status",
                                    ["unconfirmed","registered","confirmed"],
                                    default=["unconfirmed","registered"],
                                    key="dt_status")
    with fc3:
        sel_type = st.multiselect("Type", ["planned","unplanned","—"],
                                  default=[], key="dt_type")

    # Build stop events from processed data
    all_stops = []
    tools_to_process = sel_tools if sel_tools else tool_ids[:20]  # cap at 20 for performance
    for tid in tools_to_process:
        t_df = df_all[df_all[id_col].astype(str) == tid]
        if t_df.empty:
            continue
        df_proc, _ = _process_tool_df(
            t_df, config['tolerance'], config['downtime_gap'], config['run_interval']
        )
        if df_proc.empty or 'stop_event' not in df_proc.columns:
            continue
        stops = df_proc[df_proc['stop_event'] == 1].copy()
        for _, row in stops.iterrows():
            adj = float(row.get('adj_ct_sec', row.get('actual_ct', 0)))
            doc_id = f"{tid}_{row['shot_time'].strftime('%Y%m%d%H%M%S')}"
            all_stops.append({
                '_id': doc_id,
                'tool_id': tid,
                'start_time': row['shot_time'],
                'duration_sec': adj,
                'auto_type': 'unplanned',
            })

    # Merge with Firebase confirmed records
    fb_dt = {r['_id']: r for r in fb_get_collection(db, 'downtime_log')}
    merged = []
    for s in all_stops:
        fb = fb_dt.get(s['_id'], {})
        merged.append({
            '_id': s['_id'],
            'tool_id': s['tool_id'],
            'start_time': s['start_time'],
            'duration_sec': s['duration_sec'],
            'Duration': fmt_dhm(s['duration_sec']),
            'Auto Type': s['auto_type'],
            'Confirmed Type': fb.get('confirmed_type', '—'),
            'Reason': fb.get('reason', '—'),
            'Status': fb.get('status', 'unconfirmed'),
            'Confirmed By': fb.get('confirmed_by', '—'),
        })

    if not merged:
        st.info("No stop events found for the selected tools.")
        return

    df_stops = pd.DataFrame(merged)
    if sel_status:
        df_stops = df_stops[df_stops['Status'].isin(sel_status)]
    if sel_type and '—' not in sel_type:
        df_stops = df_stops[df_stops['Confirmed Type'].isin(sel_type)]

    st.caption(f"{len(df_stops)} stop events | "
               f"{len(df_stops[df_stops['Status']=='unconfirmed'])} unconfirmed")

    def style_status(val):
        if val == 'confirmed': return f'color:{PC["green"]};font-weight:bold'
        if val == 'registered': return f'color:{PC["orange"]};font-weight:bold'
        return f'color:{PC["red"]};font-weight:bold'

    display = df_stops[['tool_id','start_time','Duration','Auto Type',
                         'Confirmed Type','Reason','Status','Confirmed By']].copy()
    display['start_time'] = display['start_time'].dt.strftime('%Y-%m-%d %H:%M')
    st.dataframe(
        display.style.applymap(style_status, subset=['Status']),
        use_container_width=True, hide_index=True
    )

    # Confirm panel
    st.markdown("---")
    st.subheader("Confirm / Classify Stop Event")
    sel_id = st.selectbox("Select Stop Event ID", df_stops['_id'].tolist(),
                          key="dt_sel_id")
    sel_row = df_stops[df_stops['_id'] == sel_id].iloc[0] if sel_id else None
    if sel_row is not None:
        with st.form("dt_confirm_form"):
            fc1, fc2 = st.columns(2)
            with fc1:
                dt_type = st.radio("Downtime Type", ["unplanned","planned"],
                                   key="dt_confirm_type")
            with fc2:
                reasons = PLANNED_REASONS if dt_type == "planned" else UNPLANNED_REASONS
                dt_reason = st.selectbox("Reason", reasons, key="dt_confirm_reason")
            dt_by = st.text_input("Confirmed by", key="dt_confirm_by")
            submitted = st.form_submit_button("✅ Confirm", use_container_width=True)
            if submitted:
                data = {
                    'tool_id': sel_row['tool_id'],
                    'start_time': str(sel_row['start_time']),
                    'duration_sec': float(sel_row['duration_sec']),
                    'auto_type': 'unplanned',
                    'confirmed_type': dt_type,
                    'reason': dt_reason,
                    'confirmed_by': dt_by,
                    'confirmed_at': datetime.now().isoformat(),
                    'status': 'confirmed',
                }
                if fb_save(db, 'downtime_log', sel_id, data):
                    st.success("Saved to Firebase ✅")
                    st.rerun()
                else:
                    st.warning("Firebase not connected — record not saved.")

# ══════════════════════════════════════════════════════════════════════════
# PAGE: SCRAP / REJECT LOG
# ══════════════════════════════════════════════════════════════════════════
def page_scrap_log(df_all, config, db):
    st.header("🗑️ Scrap / Reject Log")

    if df_all.empty:
        st.info("Upload production data to view scrap flags.")
        return

    id_col = 'tool_id' if 'tool_id' in df_all.columns else df_all.columns[0]
    tool_ids = sorted([str(x) for x in df_all[id_col].unique()
                       if str(x).lower() not in ['nan','none','unknown']])

    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        default_tool = [st.session_state.pop('sc_filter_tool', None)]
        default_tool = [t for t in default_tool if t in tool_ids]
        sel_tools = st.multiselect("Filter by Tool", tool_ids,
                                   default=default_tool, key="sc_tools")
    with fc2:
        sel_status = st.multiselect("Status",
                                    ["flagged","confirmed","dismissed"],
                                    default=["flagged"], key="sc_status")
    with fc3:
        sel_conf = st.multiselect("Confidence", ["high","low"],
                                  default=[], key="sc_conf")

    # Auto-generate flags from stop data
    all_flags = []
    tools_to_process = sel_tools if sel_tools else tool_ids[:20]
    for tid in tools_to_process:
        t_df = df_all[df_all[id_col].astype(str) == tid]
        if t_df.empty:
            continue
        df_proc, _ = _process_tool_df(
            t_df, config['tolerance'], config['downtime_gap'], config['run_interval']
        )
        flags = flag_scrap_events(df_proc, tid)
        all_flags.extend(flags)

    # Merge with Firebase
    fb_sc = {r['_id']: r for r in fb_get_collection(db, 'scrap_log')}
    merged = []
    for f in all_flags:
        doc_id = f"{f['tool_id']}_{f['timestamp_start'].replace(' ','_').replace(':','')}"
        fb = fb_sc.get(doc_id, {})
        merged.append({
            '_id': doc_id,
            'tool_id': f['tool_id'],
            'Period Start': f['timestamp_start'],
            'Stop Count': f['stop_count'],
            'Confidence': f['confidence'],
            'Flagged Qty': f['flagged_qty'],
            'Confirmed Qty': fb.get('confirmed_qty', 0),
            'Reason': fb.get('reason', '—'),
            'Status': fb.get('status', 'flagged'),
            'Confirmed By': fb.get('confirmed_by', '—'),
            '_raw': f,
        })

    if not merged:
        st.info("No scrap flags for the selected tools.")
        return

    df_flags = pd.DataFrame(merged)
    if sel_status:
        df_flags = df_flags[df_flags['Status'].isin(sel_status)]
    if sel_conf:
        df_flags = df_flags[df_flags['Confidence'].isin(sel_conf)]

    st.caption(
        f"{len(df_flags)} flags | "
        f"{len(df_flags[df_flags['Confidence']=='high'])} high confidence | "
        f"{len(df_flags[df_flags['Status']=='confirmed'])} confirmed"
    )

    def style_confidence(val):
        if val == 'high': return f'color:{PC["red"]};font-weight:bold'
        return f'color:{PC["orange"]};font-weight:bold'

    def style_status(val):
        if val == 'confirmed': return f'color:{PC["green"]};font-weight:bold'
        if val == 'dismissed': return f'color:{PC["grey"]};font-weight:bold'
        return f'color:{PC["red"]};font-weight:bold'

    display = df_flags[['tool_id','Period Start','Stop Count','Confidence',
                         'Flagged Qty','Confirmed Qty','Reason',
                         'Status','Confirmed By']].copy()
    st.dataframe(
        display.style
            .applymap(style_confidence, subset=['Confidence'])
            .applymap(style_status, subset=['Status']),
        use_container_width=True, hide_index=True
    )

    # Confirm panel
    st.markdown("---")
    st.subheader("Confirm / Dismiss Scrap Event")
    sel_id = st.selectbox("Select Flag ID", df_flags['_id'].tolist(), key="sc_sel_id")
    sel_row = df_flags[df_flags['_id'] == sel_id].iloc[0] if sel_id else None
    if sel_row is not None:
        with st.form("sc_confirm_form"):
            fc1, fc2 = st.columns(2)
            with fc1:
                sc_action = st.radio("Action", ["confirm","dismiss"], key="sc_action")
                sc_qty = st.number_input("Confirmed Scrap Qty", min_value=0,
                                         value=int(sel_row['Flagged Qty']),
                                         key="sc_qty") if sc_action == "confirm" else 0
            with fc2:
                sc_reason = st.selectbox("Scrap Reason", SCRAP_REASONS,
                                         key="sc_reason") if sc_action == "confirm" else None
            sc_by = st.text_input("Confirmed by", key="sc_by")
            submitted = st.form_submit_button("✅ Submit", use_container_width=True)
            if submitted:
                data = {
                    'tool_id': sel_row['tool_id'],
                    'timestamp_start': str(sel_row['Period Start']),
                    'stop_count': int(sel_row['Stop Count']),
                    'confidence': sel_row['Confidence'],
                    'flagged_qty': int(sel_row['Flagged Qty']),
                    'confirmed_qty': int(sc_qty),
                    'reason': sc_reason or '—',
                    'confirmed_by': sc_by,
                    'confirmed_at': datetime.now().isoformat(),
                    'status': 'confirmed' if sc_action == 'confirm' else 'dismissed',
                }
                if fb_save(db, 'scrap_log', sel_id, data):
                    st.success("Saved to Firebase ✅")
                    st.rerun()
                else:
                    st.warning("Firebase not connected — record not saved.")

# ══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ══════════════════════════════════════════════════════════════════════════
def page_settings(config):
    st.header("⚙️ Settings")

    with st.expander("Analysis Parameters", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            config['tolerance'] = st.slider("Tolerance Band", 0.01, 0.50,
                                             config['tolerance'], 0.01,
                                             key="cfg_tol")
        with c2:
            config['downtime_gap'] = st.slider("Downtime Gap (sec)", 0.0, 5.0,
                                                config['downtime_gap'], 0.5,
                                                key="cfg_gap")
        with c3:
            config['run_interval'] = st.slider("Run Interval (hours)", 1, 24,
                                                config['run_interval'], 1,
                                                key="cfg_run")

    with st.expander("CT Benchmark", expanded=True):
        config['ct_mode'] = st.radio("Default CT Benchmark",
                                     ["Mode CT","Approved CT","WACT"],
                                     index=["Mode CT","Approved CT","WACT"].index(
                                         config.get('ct_mode','Mode CT')),
                                     horizontal=True, key="cfg_ct")
        if config['ct_mode'] == "WACT":
            st.info("WACT requires ≥ 30 days of data. If insufficient data is available, "
                    "the app will fall back to Mode CT with a warning.")

    with st.expander("OTE Thresholds", expanded=True):
        c1, c2 = st.columns(2)
        with c1:
            config['ote_green'] = st.slider("Green threshold (%)", 50, 95,
                                             config.get('ote_green', 70), 5,
                                             key="cfg_green")
        with c2:
            config['ote_amber'] = st.slider("Amber threshold (%)", 30, 80,
                                             config.get('ote_amber', 50), 5,
                                             key="cfg_amber")

    with st.expander("Shift Configuration", expanded=False):
        st.caption("Default shift start hours (24h format)")
        s1, s2, s3 = st.columns(3)
        shifts = config.get('shift_hours', [6, 14, 22])
        shifts[0] = s1.number_input("Shift 1 Start", 0, 23, shifts[0], key="sh1")
        shifts[1] = s2.number_input("Shift 2 Start", 0, 23, shifts[1], key="sh2")
        shifts[2] = s3.number_input("Shift 3 Start", 0, 23, shifts[2], key="sh3")
        config['shift_hours'] = shifts

    with st.expander("Reason Codes (read-only)", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.subheader("Planned Downtime")
            for r in PLANNED_REASONS: st.caption(f"• {r}")
        with c2:
            st.subheader("Unplanned Downtime")
            for r in UNPLANNED_REASONS: st.caption(f"• {r}")
        with c3:
            st.subheader("Scrap Reasons")
            for r in SCRAP_REASONS: st.caption(f"• {r}")

    st.session_state['ote_config'] = config

# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════
def main():
    if not check_password():
        return

    # Sidebar
    st.sidebar.markdown(
        f'<div style="font-size:0.75rem;opacity:0.5;">OTE Dashboard {APP_VERSION}</div>',
        unsafe_allow_html=True
    )
    st.sidebar.title("OTE Dashboard")
    st.sidebar.markdown("**Tooling Effectiveness Score**")
    st.sidebar.markdown("---")

    uploaded = st.sidebar.file_uploader(
        "Upload Production Data (Excel / CSV)",
        type=['xlsx','xls','csv'],
        accept_multiple_files=True,
        key="ote_upload"
    )

    st.sidebar.markdown("---")

    # Config defaults
    if 'ote_config' not in st.session_state:
        st.session_state['ote_config'] = {
            'tolerance': 0.05,
            'downtime_gap': 2.0,
            'run_interval': 8,
            'ct_mode': 'Mode CT',
            'ote_green': 70,
            'ote_amber': 50,
            'shift_hours': [6, 14, 22],
        }
    config = st.session_state['ote_config']

    # Firebase
    db = init_firebase() if FIREBASE_AVAILABLE else None
    if db:
        st.sidebar.success("🔥 Firebase connected")
    else:
        st.sidebar.warning("⚠️ Firebase not connected\nConfirmations won't persist")

    # Navigation
    if 'page' not in st.session_state:
        st.session_state['page'] = 'Plant Overview'

    pages = ['🏭 Plant Overview','📊 Parts Produced',
             '⏱️ Downtime Log','🗑️ Scrap / Reject Log','⚙️ Settings']
    page_keys = ['Plant Overview','Parts Produced',
                 'Downtime Log','Scrap / Reject Log','Settings']

    sel_page = st.sidebar.radio("Navigation", pages, key="nav_radio",
                                index=page_keys.index(
                                    st.session_state.get('page','Plant Overview')))
    st.session_state['page'] = page_keys[pages.index(sel_page)]

    # Load data
    df_all = pd.DataFrame()
    if uploaded:
        df_all = load_data(uploaded)
        if not df_all.empty and 'shot_time' in df_all.columns:
            _min = df_all['shot_time'].min().date()
            _max = df_all['shot_time'].max().date()
            st.sidebar.caption(f"📅 {_min} → {_max}")
            st.sidebar.caption(f"🔧 {df_all['tool_id'].nunique() if 'tool_id' in df_all.columns else '?'} tools")

    # Route
    page = st.session_state['page']
    if page == 'Plant Overview':
        page_plant_overview(df_all, config, db)
    elif page == 'Parts Produced':
        page_parts_produced(df_all, config, db)
    elif page == 'Downtime Log':
        page_downtime_log(df_all, config, db)
    elif page == 'Scrap / Reject Log':
        page_scrap_log(df_all, config, db)
    elif page == 'Settings':
        page_settings(config)

if __name__ == "__main__":
    main()
