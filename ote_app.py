import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime
import hashlib

st.set_page_config(layout="wide", page_title="OTE Dashboard v1.0", page_icon="🏭")

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_AVAILABLE = True
except ImportError:
    FIREBASE_AVAILABLE = False

APP_VERSION = "v1.0"

# ══════════════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════════════
def check_password():
    if st.session_state.get("authenticated"):
        return True
    col_l, col_m, col_r = st.columns([1,2,1])
    with col_m:
        st.markdown(f"### 🏭 OTE Dashboard {APP_VERSION}")
        pw = st.text_input("Password", type="password", key="ote_pw")
        if st.button("Login", use_container_width=True):
            stored = st.secrets.get("OTE_PASSWORD_HASH", "")
            plain  = st.secrets.get("OTE_PASSWORD", "")
            if stored:
                ok = hashlib.sha256(pw.encode()).hexdigest() == stored
            elif plain:
                ok = pw == plain
            else:
                st.error("No password configured in secrets.")
                ok = False
            if ok:
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
            cred = credentials.Certificate(dict(st.secrets["firebase"])) \
                if "firebase" in st.secrets else credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred)
        return firestore.client()
    except Exception:
        return None

def fb_get(db, collection):
    if db is None: return {}
    try:
        return {d.id: d.to_dict() for d in db.collection(collection).stream()}
    except Exception:
        return {}

def fb_set(db, collection, doc_id, data):
    if db is None: return False
    try:
        db.collection(collection).document(doc_id).set(data, merge=True)
        return True
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════════════════
# REASON CODES
# ══════════════════════════════════════════════════════════════════════════
PLANNED_REASONS   = ["Scheduled Maintenance","Tooling Changeover","Planned Trial / Sampling",
                     "Bank Holiday / Plant Shutdown","Lubrication / Cleaning","Quality Inspection Hold"]
UNPLANNED_REASONS = ["Broken / Damaged Tool Component","Ejection Problem","Material Issue",
                     "Machine Fault","Operator Issue","Cooling Issue","Unknown / Under Investigation"]
SCRAP_REASONS     = ["Flash","Short Shot","Sink Mark","Warpage / Deformation","Burn Mark",
                     "Dimensional Out of Spec","Surface Defect","Cold Weld / Flow Mark",
                     "Startup / Warmup","Contamination","Unknown"]

# ══════════════════════════════════════════════════════════════════════════
# COLUMN NORMALISATION
# ══════════════════════════════════════════════════════════════════════════
COL_MAP = {
    'TOOLING ID':'tool_id','EQUIPMENT_CODE':'tool_id','EQUIPMENT CODE':'tool_id',
    'SHOT TIME':'shot_time','LOCAL_SHOT_TIME':'shot_time',
    'ACTUAL CT':'actual_ct','CT':'actual_ct',
    'APPROVED CT':'approved_ct','APPROVED_CT':'approved_ct',
    'WORKING CAVITIES':'working_cavities','WORKING_CAVITIES':'working_cavities',
    'SUPPLIER':'supplier_id','SUPPLIER_ID':'supplier_id',
    'TOOLING TYPE':'tooling_type','TOOLING_TYPE':'tooling_type',
    'PART':'part_id','PART_ID':'part_id','PART ID':'part_id',
    'PLANT':'plant_id','PLANT_ID':'plant_id',
}

def normalise(df):
    df = df.rename(columns={k:v for k,v in COL_MAP.items() if k in df.columns})
    if 'shot_time' in df.columns:
        df['shot_time'] = pd.to_datetime(df['shot_time'], dayfirst=False, errors='coerce')
        df = df.dropna(subset=['shot_time'])
    if 'actual_ct' in df.columns:
        df['actual_ct'] = pd.to_numeric(df['actual_ct'], errors='coerce')
        df = df.dropna(subset=['actual_ct'])
    if 'tool_id' in df.columns:
        df['tool_id'] = df['tool_id'].astype(str)
    return df

@st.cache_data(show_spinner="Loading data...", ttl=300)
def load_data(files, _v=APP_VERSION):
    dfs = []
    for f in files:
        try:
            raw = pd.read_excel(f) if str(f.name).endswith(('.xlsx','.xls')) else pd.read_csv(f)
            dfs.append(normalise(raw))
        except Exception:
            pass
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# ══════════════════════════════════════════════════════════════════════════
# ENGINE
# ══════════════════════════════════════════════════════════════════════════
def _mode(series):
    s = series.dropna()
    if s.empty: return np.nan
    return float(s.round(2).mode().iloc[0])

def process_tool(df, tolerance, downtime_gap, run_interval_h, startup_count=0):
    df = df.copy().sort_values('shot_time').reset_index(drop=True)
    df['actual_ct'] = pd.to_numeric(df['actual_ct'], errors='coerce')
    df = df.dropna(subset=['actual_ct','shot_time'])
    if len(df) < 2:
        return pd.DataFrame()

    df['time_diff'] = df['shot_time'].diff().dt.total_seconds().fillna(0)
    first = pd.Series([True]+[False]*(len(df)-1), index=df.index)
    df.loc[first, 'time_diff'] = df.loc[first, 'actual_ct']

    is_new_run = df['time_diff'] > (run_interval_h * 3600)
    df['run_id'] = (is_new_run | first).cumsum()

    run_modes = df[df['actual_ct'] < 1000].groupby('run_id')['actual_ct'].apply(_mode)
    df['mode_ct'] = df['run_id'].map(run_modes).fillna(df['actual_ct'].median())
    df['mode_lower'] = df['mode_ct'] * (1 - tolerance)
    df['mode_upper'] = df['mode_ct'] * (1 + tolerance)

    df['next_diff'] = df['time_diff'].shift(-1).fillna(0)
    is_gap  = df['next_diff'] > (df['actual_ct'] + downtime_gap)
    is_abn  = (df['actual_ct'] < df['mode_lower']) | (df['actual_ct'] > df['mode_upper'])
    is_hard = df['actual_ct'] >= 999.9
    df['stop_flag'] = np.where(is_gap | is_abn | is_hard, 1, 0)

    ok = df['actual_ct'] < (df['mode_ct'] * 5)
    df.loc[(first | is_new_run) & ok, 'stop_flag'] = 0
    df['prev_stop'] = df['stop_flag'].shift(1, fill_value=0)
    df['stop_event'] = ((df['stop_flag']==1) & (df['prev_stop']==0)).astype(int)
    df['adj_ct'] = df['actual_ct'].copy()
    df.loc[is_gap, 'adj_ct'] = df.loc[is_gap, 'next_diff']

    # Consecutive stop count per shot
    vals = df['stop_flag'].values
    counts = np.zeros(len(vals), dtype=int)
    i = 0
    while i < len(vals):
        if vals[i] == 1:
            j = i
            while j < len(vals) and vals[j] == 1: j += 1
            for k in range(i, j): counts[k] = j - i
            i = j
        else:
            i += 1
    df['consec_count'] = counts

    # Startup
    if startup_count > 0:
        df['shot_seq'] = df.groupby('run_id').cumcount() + 1
        df['startup_flag'] = (df['shot_seq'] <= startup_count).astype(int)
        df['startup_event'] = ((df['shot_seq']==1) & (df['startup_flag']==1)).astype(int)
    else:
        df['startup_flag'] = 0
        df['startup_event'] = 0

    cavities = 1
    if 'working_cavities' in df.columns:
        c = pd.to_numeric(df['working_cavities'], errors='coerce').dropna()
        if not c.empty: cavities = int(c.median())
    df['cavities'] = cavities

    approved_ct = None
    if 'approved_ct' in df.columns:
        a = pd.to_numeric(df['approved_ct'], errors='coerce').dropna()
        if not a.empty: approved_ct = float(a.median())
    df['approved_ct_val'] = approved_ct

    return df

@st.cache_data(show_spinner="Processing tools...", ttl=300)
def process_all(df_json, tolerance, downtime_gap, run_interval, startup_count, _v=APP_VERSION):
    df_all = pd.read_json(df_json)
    df_all['shot_time'] = pd.to_datetime(df_all['shot_time'], unit='ms', errors='coerce')
    results = {}
    id_col = 'tool_id' if 'tool_id' in df_all.columns else df_all.columns[0]
    for tid in df_all[id_col].unique():
        t_df = df_all[df_all[id_col].astype(str) == str(tid)]
        proc = process_tool(t_df, tolerance, downtime_gap, run_interval, startup_count)
        if not proc.empty:
            results[str(tid)] = proc.to_json()
    return results

def get_df(processed, tid):
    df = pd.read_json(processed[tid])
    df['shot_time'] = pd.to_datetime(df['shot_time'])
    return df

def classify_stop(consec, micro_max):
    if consec <= 0: return 'normal'
    if consec == 1: return 'microstop'
    if consec <= micro_max: return 'consec_microstop'
    return 'major'

def dhm(sec):
    sec = int(max(0, sec))
    d,r = divmod(sec,86400); h,r = divmod(r,3600); m = r//60
    parts = []
    if d: parts.append(f"{d}d")
    if h: parts.append(f"{h}h")
    parts.append(f"{m}m")
    return " ".join(parts)

def ote_color(v):
    if v >= 70: return "#27AE60"
    if v >= 50: return "#F39C12"
    return "#E74C3C"

def compute_pillars(df, ct_mode, t2_scrap=0):
    total_shots  = len(df)
    normal_shots = int((df['stop_flag']==0).sum())
    cavities     = int(df['cavities'].iloc[0]) if 'cavities' in df.columns else 1
    mode_ct      = float(df['mode_ct'].dropna().median()) if 'mode_ct' in df.columns else None
    approved_ct  = df['approved_ct_val'].iloc[0] if 'approved_ct_val' in df.columns else None

    prod_time = float(df[df['stop_flag']==0]['actual_ct'].sum())
    total_runtime = 0.0
    for _, rdf in df.groupby('run_id'):
        if rdf.empty: continue
        total_runtime += (rdf['shot_time'].max()-rdf['shot_time'].min()).total_seconds() + float(rdf.iloc[-1]['actual_ct'])

    downtime = max(0, total_runtime - prod_time)
    avail = (prod_time / total_runtime * 100) if total_runtime > 0 else 0

    bench = mode_ct if ct_mode == 'Mode CT' else approved_ct
    if bench and bench > 0 and prod_time > 0:
        perf = min(100, normal_shots / (prod_time / bench) * 100)
    else:
        perf = (normal_shots / total_shots * 100) if total_shots > 0 else 0

    actual_output = normal_shots * cavities
    if t2_scrap > 0 and actual_output > 0:
        qual = max(0, (1 - t2_scrap / actual_output) * 100)
        q_conf = True
    else:
        qual = 100.0
        q_conf = False

    ote = (avail/100)*(perf/100)*(qual/100)*100
    return dict(ote=ote, avail=avail, perf=perf, qual=qual, q_conf=q_conf,
                total_shots=total_shots, normal_shots=normal_shots,
                cavities=cavities, prod_time=prod_time, total_runtime=total_runtime,
                downtime=downtime, mode_ct=mode_ct, approved_ct=approved_ct,
                actual_output=actual_output)

# ══════════════════════════════════════════════════════════════════════════
# PAGE: PLANT OVERVIEW
# ══════════════════════════════════════════════════════════════════════════
def page_overview(processed, config, db):
    st.header("🏭 Plant Overview")
    if not processed:
        st.info("Upload production data to begin.")
        return

    fb_t2 = fb_get(db, 'scrap_t2')
    rows = []

    for tid, df_json in processed.items():
        df = get_df(processed, tid)
        t2_scrap = sum(v.get('total_scrap',0) for v in fb_t2.values()
                       if v.get('tool_id') == tid)
        p = compute_pillars(df, config['ct_mode'], t2_scrap)

        stop_shots = int((df['stop_flag']==1).sum())
        scrap_rate = (t2_scrap / max(stop_shots * p['cavities'], 1) * 100) if stop_shots > 0 else 0

        rows.append({
            'tool_id': tid,
            'OTE (%)': round(p['ote'],1),
            'Availability (%)': round(p['avail'],1),
            'Performance (%)': round(p['perf'],1),
            'Quality (%)': round(p['qual'],1),
            'q_conf': p['q_conf'],
            'Total Shots': p['total_shots'],
            'Stop Events': int(df['stop_event'].sum()),
            'Downtime': dhm(p['downtime']),
            'Actual Output': p['actual_output'],
            'Scrap': t2_scrap,
            '% Stop → Scrap': round(scrap_rate,1),
        })

    if not rows:
        st.warning("No data could be computed.")
        return

    df_rank = pd.DataFrame(rows).sort_values('OTE (%)')

    k1,k2,k3,k4,k5 = st.columns(5)
    k1.metric("Fleet OTE", f"{df_rank['OTE (%)'].mean():.1f}%")
    k2.metric("Tools", len(df_rank))
    k3.metric("Total Shots", f"{df_rank['Total Shots'].sum():,.0f}")
    k4.metric("Stop Events", f"{df_rank['Stop Events'].sum():,.0f}")
    k5.metric("Confirmed Scrap", f"{df_rank['Scrap'].sum():,.0f}")
    st.markdown("---")

    display = df_rank.drop(columns=['q_conf'])

    def sty(v):
        c = ote_color(v); return f'background-color:{c}22;color:{c};font-weight:bold'

    st.dataframe(
        display.style
            .map(sty, subset=['OTE (%)','Availability (%)','Performance (%)'])
            .format({'OTE (%)':'{:.1f}','Availability (%)':'{:.1f}',
                     'Performance (%)':'{:.1f}','Quality (%)':'{:.1f}',
                     'Total Shots':'{:,.0f}','Stop Events':'{:,.0f}',
                     'Actual Output':'{:,.0f}','Scrap':'{:,.0f}',
                     '% Stop → Scrap':'{:.1f}%'}),
        use_container_width=True, hide_index=True
    )

    st.markdown("---")
    st.subheader("Tool Detail")
    sel = st.selectbox("Select tool", [r['tool_id'] for r in rows], key="ov_sel")
    row = next((r for r in rows if r['tool_id']==sel), None)
    if not row: return

    g1,g2,g3,g4 = st.columns(4)
    for col, title, key_suf, color in [
        (g1,"OTE","ote", ote_color(row['OTE (%)'])),
        (g2,"Availability","av","#3498DB"),
        (g3,"Performance","pf","#27AE60"),
        (g4,"Quality","ql","#27AE60" if row['q_conf'] else "#95A5A6"),
    ]:
        val_key = 'OTE (%)' if title=='OTE' else f'{title} (%)'
        val = row[val_key]
        with col:
            with st.container(border=True):
                fig = go.Figure(go.Pie(
                    values=[val,100-val], hole=0.75, sort=False, textinfo='none',
                    marker=dict(colors=[color,'#2C2C2C']), hoverinfo='none'
                ))
                fig.update_layout(
                    annotations=[dict(text=f"<b>{val:.1f}%</b>", x=0.5, y=0.5,
                                      font_size=20, showarrow=False, font=dict(color=color))],
                    showlegend=False, height=170,
                    title=dict(text=title, x=0.5, font=dict(size=13)),
                    margin=dict(t=30,b=5,l=5,r=5),
                    paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig, use_container_width=True, key=f"g_{key_suf}_{sel}")
                if title=="Quality" and not row['q_conf']:
                    st.caption("⚠️ Awaiting confirmation")

    c1,c2,c3 = st.columns(3)
    c1.metric("% Stop Shots → Scrap", f"{row['% Stop → Scrap']:.1f}%")
    c2.metric("Confirmed Scrap", f"{row['Scrap']:,.0f}")
    c3.metric("Actual Output", f"{row['Actual Output']:,.0f}")

# ══════════════════════════════════════════════════════════════════════════
# PAGE: PARTS PRODUCED MATRIX
# ══════════════════════════════════════════════════════════════════════════
def page_matrix(processed, config, db):
    st.header("📊 Parts Produced")
    if not processed:
        st.info("Upload production data to begin.")
        return

    tool_ids = list(processed.keys())
    cc1,cc2,cc3 = st.columns(3)
    with cc1:
        sel_tools = st.multiselect("Tools", tool_ids, default=tool_ids[:10], key="pp_tools")
    with cc2:
        color_by = st.radio("Colour by",["OTE Score","Output vs Expected"],
                            horizontal=True, key="pp_col")
    with cc3:
        all_dates = []
        for tid in (sel_tools or tool_ids[:1]):
            df = get_df(processed, tid)
            all_dates += list(df['shot_time'].dt.date.unique())
        dates = sorted(set(all_dates))
        sel_date = st.selectbox("Date", dates, index=len(dates)-1 if dates else 0,
                                format_func=lambda x: x.strftime('%d %b %Y'), key="pp_date")

    if not sel_tools:
        st.info("Select tools above.")
        return

    shift_starts = config.get('shift_hours',[6,14,22])

    def get_shift(h):
        for i in range(len(shift_starts)-1,-1,-1):
            if h >= shift_starts[i]: return f"Shift {i+1}"
        return f"Shift {len(shift_starts)}"

    all_hours = [f"{h:02d}:00" for h in range(24)]
    matrix = {}
    summary_rows = []

    for tid in sel_tools:
        df = get_df(processed, tid)
        df_day = df[df['shot_time'].dt.date == sel_date]
        cavities = int(df['cavities'].iloc[0]) if 'cavities' in df.columns else 1
        mode_ct = float(df['mode_ct'].dropna().median()) if 'mode_ct' in df.columns else None
        bench = mode_ct if config['ct_mode']=='Mode CT' else (df['approved_ct_val'].iloc[0] if 'approved_ct_val' in df.columns else None)

        if df_day.empty:
            matrix[tid] = {h:('','rgba(0,0,0,0)') for h in all_hours}
            continue

        p = compute_pillars(df_day, config['ct_mode'])
        ote_day = p['ote']

        hourly = df_day.groupby(df_day['shot_time'].dt.floor('h')).agg(
            normal=('stop_flag', lambda x: (x==0).sum()),
            stops=('stop_flag','sum'),
        ).reset_index()
        hourly['parts'] = hourly['normal'] * cavities
        if bench and bench > 0:
            hourly['expected'] = (3600/bench)*cavities
            hourly['pct'] = (hourly['parts']/hourly['expected'].replace(0,np.nan)*100).fillna(0)
        else:
            hourly['expected'] = 0
            hourly['pct'] = 100.0

        h_dict = {r['shot_time'].strftime('%H:00'): r for _,r in hourly.iterrows()}

        row_data = {}
        for h in all_hours:
            if h in h_dict:
                r = h_dict[h]
                val = int(r['parts'])
                score = r['pct'] if color_by=="Output vs Expected" else ote_day
                row_data[h] = (str(val), ote_color(score))
            else:
                row_data[h] = ('','rgba(0,0,0,0)')
        matrix[tid] = row_data

        total_parts = sum(int(v) for v,_ in row_data.values() if v)
        exp_total = int((3600/bench)*cavities*24) if bench else 0
        summary_rows.append({
            'Tool': tid,
            'Parts': total_parts,
            'Expected': exp_total,
            'Achievement': f"{total_parts/max(exp_total,1)*100:.1f}%" if exp_total else "—",
            'OTE (day)': f"{ote_day:.1f}%",
        })

    # Build HTML matrix table
    shift_groups = {}
    for h in all_hours:
        s = get_shift(int(h.split(':')[0]))
        shift_groups.setdefault(s, []).append(h)

    sh_headers = "".join(
        f'<th colspan="{len(hs)}" style="text-align:center;background:#2C3E50;'
        f'color:#ECF0F1;padding:4px 2px;font-size:0.75rem;">{sh}</th>'
        for sh,hs in shift_groups.items()
    )
    hr_headers = "".join(
        f'<th style="text-align:center;background:#1a252f;color:#7F8C8D;'
        f'padding:2px 1px;font-size:0.62rem;min-width:36px;">{h}</th>'
        for _,hs in shift_groups.items() for h in hs
    )

    body = ""
    for tid in sel_tools:
        cells = (f'<td style="padding:3px 8px;font-size:0.8rem;'
                 f'white-space:nowrap;font-weight:600;">{tid}</td>')
        for _,hs in shift_groups.items():
            for h in hs:
                val, col = matrix.get(tid,{}).get(h,('','rgba(0,0,0,0)'))
                if val:
                    cells += (f'<td style="text-align:center;background:{col}33;'
                              f'color:{col};font-weight:bold;font-size:0.72rem;'
                              f'padding:3px 1px;border:1px solid #222;">{val}</td>')
                else:
                    cells += '<td style="background:#111;border:1px solid #1a1a1a;"></td>'
        body += f'<tr>{cells}</tr>'

    st.markdown(
        f'<div style="overflow-x:auto;">'
        f'<p style="font-size:0.72rem;color:#888;margin-bottom:4px;">'
        f'🟢 ≥70%&nbsp;&nbsp;🟡 50–70%&nbsp;&nbsp;🔴 &lt;50%&nbsp;&nbsp;'
        f'Colour: <b>{color_by}</b></p>'
        f'<table style="border-collapse:collapse;width:100%;background:#0E1117;">'
        f'<thead>'
        f'<tr><th style="background:#0E1117;"></th>{sh_headers}</tr>'
        f'<tr><th style="background:#0E1117;text-align:left;padding:2px 8px;'
        f'font-size:0.72rem;color:#555;">Tool</th>{hr_headers}</tr>'
        f'</thead><tbody>{body}</tbody></table></div>',
        unsafe_allow_html=True
    )

    if summary_rows:
        st.markdown("---")
        st.subheader(f"Daily Summary — {sel_date}")
        st.dataframe(pd.DataFrame(summary_rows), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════
# PAGE: STOP LOG
# ══════════════════════════════════════════════════════════════════════════
def page_stop_log(processed, config, db):
    st.header("⏱️ Stop Log & Downtime")
    if not processed:
        st.info("Upload production data to begin.")
        return

    micro_max = config.get('micro_max', 5)
    tool_ids  = list(processed.keys())

    fc1,fc2,fc3 = st.columns(3)
    with fc1:
        sel_tools = st.multiselect("Tool", tool_ids, default=[], key="sl_tools",
                                   placeholder="All tools")
    with fc2:
        sel_type = st.multiselect("Stop Type",
                                  ["microstop","consec_microstop","major"],
                                  default=["major","consec_microstop"], key="sl_type")
    with fc3:
        sel_status = st.multiselect("Status", ["unconfirmed","confirmed"],
                                    default=["unconfirmed"], key="sl_status")

    fb_dt = fb_get(db, 'downtime_log')
    tools = sel_tools or tool_ids[:20]
    all_events = []

    for tid in tools:
        df = get_df(processed, tid)
        vals  = df['stop_flag'].values
        times = df['shot_time'].values
        adjs  = df['adj_ct'].values if 'adj_ct' in df.columns else df['actual_ct'].values
        rids  = df['run_id'].values
        i = 0
        while i < len(vals):
            if vals[i] == 1:
                j = i
                while j < len(vals) and vals[j] == 1: j += 1
                count = j - i
                stype = classify_stop(count, micro_max)
                t_start = pd.Timestamp(times[i])
                dur = float(sum(adjs[i:j]))
                doc_id = f"{tid}_{t_start.strftime('%Y%m%d%H%M%S')}"
                fb = fb_dt.get(doc_id, {})
                all_events.append({
                    '_id': doc_id, 'Tool': tid, 'Run': int(rids[i]),
                    'Time': t_start, 'Duration': dhm(dur), 'dur_sec': dur,
                    'Stop Count': count, 'Type': stype,
                    'DT Type': fb.get('confirmed_type','—'),
                    'DT Reason': fb.get('reason','—'),
                    'Status': fb.get('status','unconfirmed'),
                })
                i = j
            else:
                i += 1

    if not all_events:
        st.info("No stop events found.")
        return

    df_ev = pd.DataFrame(all_events)
    if sel_type:   df_ev = df_ev[df_ev['Type'].isin(sel_type)]
    if sel_status: df_ev = df_ev[df_ev['Status'].isin(sel_status)]

    type_col = {'microstop':'#F39C12','consec_microstop':'#E67E22','major':'#E74C3C'}

    def sty_t(v): c=type_col.get(v,'#888'); return f'color:{c};font-weight:bold'
    def sty_s(v): return 'color:#27AE60;font-weight:bold' if v=='confirmed' else 'color:#E74C3C;font-weight:bold'

    st.caption(f"{len(df_ev)} events | {len(df_ev[df_ev['Type']=='major'])} major | "
               f"{len(df_ev[df_ev['Status']=='unconfirmed'])} unconfirmed")

    disp = df_ev[['Tool','Run','Time','Duration','Stop Count','Type',
                  'DT Type','DT Reason','Status']].copy()
    disp['Time'] = disp['Time'].dt.strftime('%Y-%m-%d %H:%M')
    st.dataframe(
        disp.style.map(sty_t, subset=['Type']).map(sty_s, subset=['Status']),
        use_container_width=True, hide_index=True
    )

    st.markdown("---")
    st.subheader("Classify Stop Event")
    unconf = df_ev[df_ev['Status']=='unconfirmed']
    if unconf.empty:
        st.success("All events classified ✅")
        return

    sel_id = st.selectbox("Select event", unconf['_id'].tolist(),
                          format_func=lambda x: (
                              f"{unconf[unconf['_id']==x]['Tool'].iloc[0]} | "
                              f"Run {unconf[unconf['_id']==x]['Run'].iloc[0]} | "
                              f"{unconf[unconf['_id']==x]['Type'].iloc[0]} | "
                              f"{unconf[unconf['_id']==x]['Duration'].iloc[0]}"
                          ), key="sl_sel")

    sel_row = unconf[unconf['_id']==sel_id].iloc[0]
    st.info(f"**{sel_row['Tool']}** | Run {sel_row['Run']} | {sel_row['Time'].strftime('%Y-%m-%d %H:%M') if hasattr(sel_row['Time'],'strftime') else sel_row['Time']} | "
            f"Duration: **{sel_row['Duration']}** | Type: **{sel_row['Type']}**")

    with st.form("sl_form"):
        c1,c2,c3 = st.columns(3)
        dt_type = c1.radio("Downtime type", ["unplanned","planned"], key="sl_dt")
        all_reasons = ["— Unplanned —"] + UNPLANNED_REASONS + ["— Planned —"] + PLANNED_REASONS
        dt_reason = c2.selectbox("Reason", all_reasons, key="sl_reason")
        by = c3.text_input("Your name", key="sl_by")
        if st.form_submit_button("✅ Confirm", use_container_width=True):
            data = {
                'tool_id': sel_row['Tool'], 'run_id': int(sel_row['Run']),
                'dur_sec': float(sel_row['dur_sec']),
                'stop_count': int(sel_row['Stop Count']), 'stop_type': sel_row['Type'],
                'confirmed_type': dt_type, 'reason': dt_reason,
                'confirmed_by': by, 'confirmed_at': datetime.now().isoformat(),
                'status': 'confirmed',
            }
            if fb_set(db, 'downtime_log', sel_id, data):
                st.success("Saved ✅"); st.rerun()
            else:
                st.warning("Firebase not connected.")

# ══════════════════════════════════════════════════════════════════════════
# PAGE: SCRAP (Two-tier)
# ══════════════════════════════════════════════════════════════════════════
def page_scrap(processed, config, db):
    st.header("🗑️ Scrap / Quality Log")
    if not processed:
        st.info("Upload production data to begin.")
        return

    micro_max = config.get('micro_max', 5)
    tool_ids  = list(processed.keys())
    sel_tool  = st.selectbox("Tool", tool_ids, key="sc_tool")
    df = get_df(processed, sel_tool)
    cavities = int(df['cavities'].iloc[0]) if 'cavities' in df.columns else 1

    fb_t1 = fb_get(db, 'scrap_t1')
    fb_t2 = fb_get(db, 'scrap_t2')

    # ── TIER 1 ─────────────────────────────────────────────────────────
    st.subheader("Tier 1 — Issue Log")
    st.caption("Per-event scrap confirmation. Confirmed items auto-populate Tier 2.")

    t1_items = []

    # Startup batches
    if 'startup_event' in df.columns:
        for run_id, rdf in df.groupby('run_id'):
            su_shots = int(rdf['startup_flag'].sum()) if 'startup_flag' in rdf.columns else 0
            if su_shots == 0: continue
            doc_id = f"{sel_tool}_su_{int(run_id)}"
            fb = fb_t1.get(doc_id, {})
            t1_items.append({
                '_id': doc_id, 'Source': f'Run {int(run_id)} — Startup',
                'Time': rdf['shot_time'].min().strftime('%H:%M'),
                'Type': 'startup', 'Est. Scrap': su_shots * cavities,
                'Confirmed Qty': fb.get('confirmed_qty','—'),
                'Reason': fb.get('reason','—'),
                'Status': fb.get('status','flagged'), '_run': int(run_id),
            })

    # Stop events (consec + major only)
    vals = df['stop_flag'].values
    times = df['shot_time'].values
    adjs  = df['adj_ct'].values if 'adj_ct' in df.columns else df['actual_ct'].values
    rids  = df['run_id'].values
    i = 0
    while i < len(vals):
        if vals[i] == 1:
            j = i
            while j < len(vals) and vals[j] == 1: j += 1
            count = j - i
            stype = classify_stop(count, micro_max)
            if stype in ('consec_microstop','major'):
                t_start = pd.Timestamp(times[i])
                doc_id = f"{sel_tool}_stop_{t_start.strftime('%Y%m%d%H%M%S')}"
                fb = fb_t1.get(doc_id, {})
                t1_items.append({
                    '_id': doc_id, 'Source': f'Run {int(rids[i])} — Stop x{count}',
                    'Time': t_start.strftime('%H:%M'), 'Type': stype,
                    'Est. Scrap': count * cavities,
                    'Confirmed Qty': fb.get('confirmed_qty','—'),
                    'Reason': fb.get('reason','—'),
                    'Status': fb.get('status','flagged'), '_run': int(rids[i]),
                })
            i = j
        else:
            i += 1

    if t1_items:
        df_t1 = pd.DataFrame(t1_items)
        tc = {'startup':'#9B59B6','consec_microstop':'#E67E22','major':'#E74C3C'}
        def sty_t(v): c=tc.get(v,'#888'); return f'color:{c};font-weight:bold'
        def sty_s(v):
            if v=='confirmed': return 'color:#27AE60;font-weight:bold'
            if v=='dismissed': return 'color:#7F8C8D'
            return 'color:#E74C3C;font-weight:bold'

        st.dataframe(
            df_t1[['Source','Time','Type','Est. Scrap','Confirmed Qty','Reason','Status']]
                .style.map(sty_t, subset=['Type']).map(sty_s, subset=['Status']),
            use_container_width=True, hide_index=True
        )

        pending = df_t1[df_t1['Status']=='flagged']
        if not pending.empty:
            st.markdown("**Confirm:**")
            sel_t1 = st.selectbox("Select", pending['_id'].tolist(),
                                  format_func=lambda x: pending[pending['_id']==x]['Source'].iloc[0],
                                  key="t1_sel")
            t1r = pending[pending['_id']==sel_t1].iloc[0]
            with st.form("t1_form"):
                cc1,cc2,cc3,cc4 = st.columns([1,1,2,1])
                action = cc1.radio("Action", ["Confirm Scrap","No Scrap"], key="t1_act")
                qty = cc2.number_input("Qty", 0, value=int(t1r['Est. Scrap']), key="t1_qty")
                def_reason = "Startup / Warmup" if t1r['Type']=='startup' else "Unknown"
                reason_idx = SCRAP_REASONS.index(def_reason) if def_reason in SCRAP_REASONS else 0
                sc_reason = cc3.selectbox("Reason", SCRAP_REASONS, index=reason_idx, key="t1_reason")
                by = cc4.text_input("Name", key="t1_by")
                if st.form_submit_button("✅ Submit", use_container_width=True):
                    status = 'confirmed' if action=="Confirm Scrap" else 'dismissed'
                    data = {
                        'tool_id': sel_tool, 'run_id': int(t1r['_run']),
                        'source': t1r['Source'], 'type': t1r['Type'],
                        'est_qty': int(t1r['Est. Scrap']),
                        'confirmed_qty': int(qty) if status=='confirmed' else 0,
                        'reason': sc_reason if status=='confirmed' else '—',
                        'confirmed_by': by, 'confirmed_at': datetime.now().isoformat(),
                        'status': status,
                    }
                    if fb_set(db, 'scrap_t1', sel_t1, data):
                        st.success("Saved ✅"); st.rerun()
                    else:
                        st.warning("Firebase not connected.")
    else:
        st.info("No flagged events for this tool.")

    st.markdown("---")

    # ── TIER 2 ─────────────────────────────────────────────────────────
    st.subheader("Tier 2 — Run Scrap Totals (feeds OTE Quality %)")

    t1_confirmed = {k:v for k,v in fb_t1.items()
                    if v.get('tool_id')==sel_tool and v.get('status')=='confirmed'}

    runs = sorted(df['run_id'].unique())
    t2_rows = []
    for run_id in runs:
        rdf = df[df['run_id']==run_id]
        if rdf.empty: continue
        normal = int((rdf['stop_flag']==0).sum())
        output = normal * cavities
        t_start = rdf['shot_time'].min().strftime('%H:%M')
        t_end   = rdf['shot_time'].max().strftime('%H:%M')

        auto = sum(v.get('confirmed_qty',0) for v in t1_confirmed.values()
                   if v.get('run_id')==run_id)
        doc_id = f"{sel_tool}_run_{int(run_id)}"
        fb = fb_t2.get(doc_id, {})
        additional = fb.get('additional_scrap', 0)
        total = auto + additional
        quality = max(0, (1 - total/max(output,1))*100)

        t2_rows.append({
            '_id': doc_id, 'Run': int(run_id),
            'Period': f"{t_start} – {t_end}",
            'Parts Produced': output,
            'Auto (T1)': auto, 'Additional': additional,
            'Total Scrap': total, 'Quality %': round(quality,1),
        })

    if t2_rows:
        df_t2 = pd.DataFrame(t2_rows)
        def sty_q(v): c=ote_color(v); return f'color:{c};font-weight:bold'
        st.dataframe(
            df_t2[['Run','Period','Parts Produced','Auto (T1)','Additional','Total Scrap','Quality %']]
                .style.map(sty_q, subset=['Quality %'])
                .format({'Parts Produced':'{:,.0f}','Auto (T1)':'{:,.0f}',
                         'Additional':'{:,.0f}','Total Scrap':'{:,.0f}',
                         'Quality %':'{:.1f}%'}),
            use_container_width=True, hide_index=True
        )

        stop_shots = int((df['stop_flag']==1).sum())
        total_scrap = df_t2['Total Scrap'].sum()
        rate = total_scrap / max(stop_shots*cavities,1)*100
        st.markdown(
            f"**{rate:.1f}% of RR stop shots were scrap** "
            f"({int(total_scrap):,} confirmed scrap / {stop_shots*cavities:,} stop shot parts)"
        )

        st.markdown("---")
        st.markdown("**Add / update additional scrap for a run:**")
        run_opts = [r['_id'] for r in t2_rows]
        sel_run = st.selectbox("Run", run_opts,
                               format_func=lambda x: (
                                   f"Run {x.split('_')[-1]} | "
                                   + next(r['Period'] for r in t2_rows if r['_id']==x)
                               ), key="t2_sel")
        t2r = next(r for r in t2_rows if r['_id']==sel_run)

        with st.form("t2_form"):
            tc1,tc2,tc3 = st.columns(3)
            add_qty = tc1.number_input("Additional scrap (not in T1)", 0,
                                       value=int(t2r['Additional']), key="t2_qty")
            add_reason = tc2.selectbox("Reason", SCRAP_REASONS, key="t2_reason")
            add_by = tc3.text_input("Your name", key="t2_by")
            st.caption(f"T1 auto: {t2r['Auto (T1)']} parts | "
                       f"New total will be: **{t2r['Auto (T1)']+add_qty}**")
            if st.form_submit_button("✅ Save Run Total", use_container_width=True):
                data = {
                    'tool_id': sel_tool, 'run_id': int(t2r['Run']),
                    'period': t2r['Period'], 'parts_produced': int(t2r['Parts Produced']),
                    'auto_scrap': int(t2r['Auto (T1)']),
                    'additional_scrap': int(add_qty),
                    'total_scrap': int(t2r['Auto (T1)'])+int(add_qty),
                    'reason': add_reason, 'confirmed_by': add_by,
                    'confirmed_at': datetime.now().isoformat(), 'status': 'confirmed',
                }
                if fb_set(db, 'scrap_t2', sel_run, data):
                    st.success("Saved ✅"); st.rerun()
                else:
                    st.warning("Firebase not connected.")

# ══════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ══════════════════════════════════════════════════════════════════════════
def page_settings(config):
    st.header("⚙️ Settings")
    with st.expander("Analysis Parameters", expanded=True):
        c1,c2,c3 = st.columns(3)
        config['tolerance']    = c1.slider("Tolerance Band", 0.01,0.50,config['tolerance'],0.01,key="cfg_tol")
        config['downtime_gap'] = c2.slider("Downtime Gap (sec)",0.0,5.0,config['downtime_gap'],0.5,key="cfg_gap")
        config['run_interval'] = c3.slider("Run Interval (hours)",1,24,config['run_interval'],1,key="cfg_ri")
    with st.expander("Stop Classification", expanded=True):
        c1,c2 = st.columns(2)
        config['micro_max'] = c1.slider("Max stops = microstop",1,20,config.get('micro_max',5),1,key="cfg_mm")
        c2.markdown(f"- 1 stop → Microstop\n- 2–{config['micro_max']} → Consecutive Microstop\n- >{config['micro_max']} → Major Stoppage")
    with st.expander("CT Benchmark", expanded=True):
        config['ct_mode'] = st.radio("Benchmark",["Mode CT","Approved CT","WACT"],
                                      index=["Mode CT","Approved CT","WACT"].index(config.get('ct_mode','Mode CT')),
                                      horizontal=True, key="cfg_ct")
    with st.expander("OTE Thresholds", expanded=True):
        c1,c2 = st.columns(2)
        config['ote_green'] = c1.slider("🟢 Green (%)",50,95,config.get('ote_green',70),5,key="cfg_g")
        config['ote_amber'] = c2.slider("🟡 Amber (%)",30,80,config.get('ote_amber',50),5,key="cfg_a")
    with st.expander("Shift Configuration"):
        s1,s2,s3 = st.columns(3)
        sh = config.get('shift_hours',[6,14,22])
        sh[0]=s1.number_input("Shift 1",0,23,sh[0],key="sh1")
        sh[1]=s2.number_input("Shift 2",0,23,sh[1],key="sh2")
        sh[2]=s3.number_input("Shift 3",0,23,sh[2],key="sh3")
        config['shift_hours'] = sh
    with st.expander("Startup Shots"):
        config['startup_count'] = st.slider("Start-up shots per run",0,50,
                                             config.get('startup_count',5),1,key="cfg_su")
        st.caption("0 = disabled. Startup batches appear in Tier 1 scrap log.")
    st.session_state['ote_config'] = config

# ══════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════
def main():
    if not check_password():
        return

    st.sidebar.markdown(f'<div style="font-size:0.72rem;opacity:0.4;">OTE {APP_VERSION}</div>',
                        unsafe_allow_html=True)
    st.sidebar.title("🏭 OTE Dashboard")
    st.sidebar.caption("Tooling Effectiveness Score")
    st.sidebar.markdown("---")

    uploaded = st.sidebar.file_uploader("Upload Production Data",
                                        type=['xlsx','xls','csv'],
                                        accept_multiple_files=True, key="ote_up")

    if 'ote_config' not in st.session_state:
        st.session_state['ote_config'] = {
            'tolerance':0.05,'downtime_gap':2.0,'run_interval':8,
            'ct_mode':'Mode CT','ote_green':70,'ote_amber':50,
            'shift_hours':[6,14,22],'micro_max':5,'startup_count':5,
        }
    config = st.session_state['ote_config']

    db = init_firebase()
    st.sidebar.success("🔥 Firebase connected") if db else st.sidebar.warning("⚠️ Firebase not connected")

    st.sidebar.markdown("---")
    pages    = ['🏭 Plant Overview','📊 Parts Produced','⏱️ Stop Log','🗑️ Scrap / Quality','⚙️ Settings']
    page_map = ['Plant Overview','Parts Produced','Stop Log','Scrap / Quality','Settings']
    if 'page' not in st.session_state: st.session_state['page'] = 'Plant Overview'

    sel = st.sidebar.radio("Navigation", pages, key="nav",
                           index=page_map.index(st.session_state.get('page','Plant Overview')))
    st.session_state['page'] = page_map[pages.index(sel)]

    df_all = pd.DataFrame()
    processed = {}
    if uploaded:
        df_all = load_data(uploaded)
        if not df_all.empty and 'shot_time' in df_all.columns:
            id_col = 'tool_id' if 'tool_id' in df_all.columns else df_all.columns[0]
            mn,mx = df_all['shot_time'].min().date(), df_all['shot_time'].max().date()
            st.sidebar.caption(f"📅 {mn} → {mx}")
            st.sidebar.caption(f"🔧 {df_all[id_col].nunique()} tools")
            try:
                processed = process_all(
                    df_all.to_json(), config['tolerance'], config['downtime_gap'],
                    config['run_interval'], config.get('startup_count',5)
                )
            except Exception as e:
                st.sidebar.error(f"Processing error: {e}")

    page = st.session_state['page']
    if   page == 'Plant Overview': page_overview(processed, config, db)
    elif page == 'Parts Produced': page_matrix(processed, config, db)
    elif page == 'Stop Log':       page_stop_log(processed, config, db)
    elif page == 'Scrap / Quality':page_scrap(processed, config, db)
    elif page == 'Settings':       page_settings(config)

if __name__ == "__main__":
    main()
