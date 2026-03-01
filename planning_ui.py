#!/usr/bin/env python3
"""
Yellow Stag Services — Planning Intelligence Platform
Streamlit UI for extracting planning consent data from Irish county council portals.
Proprietary software © Yellow Stag Services. All rights reserved.
"""

import os
import sys
import subprocess
import threading
import time
import logging
import base64
from datetime import datetime, date, timedelta

import streamlit as st
import pandas as pd

# ─── Logo ────────────────────────────────────────────────────────────────────
_logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "UK-346-Charles-G-Logo-V14_01-Option-1920w.png")
if os.path.exists(_logo_path):
    with open(_logo_path, "rb") as _f:
        _logo_b64 = base64.b64encode(_f.read()).decode()
    LOGO_HTML_SIDEBAR = f'<img src="data:image/png;base64,{_logo_b64}" style="width:180px; background:#162044; border-radius:6px; padding:6px 10px;" alt="Yellow Stag">'
    LOGO_HTML_HEADER = f'<img src="data:image/png;base64,{_logo_b64}" style="height:48px; vertical-align:middle; margin-right:12px; background:#162044; border-radius:4px; padding:4px 8px;" alt="Yellow Stag">'
else:
    LOGO_HTML_SIDEBAR = '<span style="font-size:2.2rem;">🦌</span>'
    LOGO_HTML_HEADER = '🦌'

# ─── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Yellow Stag — Planning Intelligence",
    page_icon="🦌",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ───────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Root variables ── */
    :root {
        --ys-gold: #D4A843;
        --ys-gold-light: #E8C96A;
        --ys-dark: #1A1A2E;
        --ys-darker: #12121F;
        --ys-card: #1E1E32;
        --ys-text: #E8E8EC;
        --ys-muted: #8A8A9A;
        --ys-success: #2ECC71;
        --ys-danger: #E74C3C;
        --ys-border: #2A2A42;
    }

    /* ── Global ── */
    .stApp {
        background: linear-gradient(175deg, var(--ys-darker) 0%, var(--ys-dark) 100%);
    }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        background: var(--ys-darker) !important;
        border-right: 1px solid var(--ys-border);
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown li,
    section[data-testid="stSidebar"] label {
        color: var(--ys-text) !important;
    }

    /* ── Header banner ── */
    .ys-header {
        background: linear-gradient(135deg, var(--ys-dark) 0%, #222240 50%, var(--ys-dark) 100%);
        border: 1px solid var(--ys-border);
        border-left: 4px solid var(--ys-gold);
        border-radius: 8px;
        padding: 1.6rem 2rem;
        margin-bottom: 1.5rem;
    }
    .ys-header h1 {
        color: var(--ys-gold) !important;
        font-size: 1.75rem !important;
        font-weight: 700 !important;
        margin: 0 0 0.25rem 0 !important;
        letter-spacing: 0.5px;
    }
    .ys-header p {
        color: var(--ys-muted) !important;
        font-size: 0.85rem !important;
        margin: 0 !important;
    }

    /* ── Metric cards ── */
    .ys-metric-row {
        display: flex;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    .ys-metric {
        flex: 1;
        background: var(--ys-card);
        border: 1px solid var(--ys-border);
        border-radius: 8px;
        padding: 1.1rem 1.3rem;
        text-align: center;
    }
    .ys-metric .value {
        color: var(--ys-gold);
        font-size: 1.8rem;
        font-weight: 700;
        line-height: 1.2;
    }
    .ys-metric .label {
        color: var(--ys-muted);
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin-top: 0.3rem;
    }

    /* ── Status badge ── */
    .ys-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.5px;
    }
    .ys-badge-idle { background: #2A2A42; color: var(--ys-muted); }
    .ys-badge-running { background: #1A3A2E; color: var(--ys-success); }
    .ys-badge-error { background: #3A1A1E; color: var(--ys-danger); }
    .ys-badge-done { background: #2E3A1A; color: var(--ys-gold-light); }

    /* ── Card panels ── */
    .ys-card {
        background: var(--ys-card);
        border: 1px solid var(--ys-border);
        border-radius: 8px;
        padding: 1.4rem 1.6rem;
        margin-bottom: 1rem;
    }
    .ys-card h3 {
        color: var(--ys-gold) !important;
        font-size: 1rem !important;
        font-weight: 600 !important;
        margin-bottom: 0.75rem !important;
        border-bottom: 1px solid var(--ys-border);
        padding-bottom: 0.5rem;
    }
    .ys-card p, .ys-card li {
        color: var(--ys-text) !important;
        font-size: 0.85rem;
    }

    /* ── Log area ── */
    .ys-log {
        background: #0D0D18;
        border: 1px solid var(--ys-border);
        border-radius: 6px;
        padding: 1rem;
        font-family: 'JetBrains Mono', 'Fira Code', monospace;
        font-size: 0.72rem;
        color: #8A8A9A;
        max-height: 400px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-all;
    }

    /* ── Buttons ── */
    .stButton > button {
        background: linear-gradient(135deg, var(--ys-gold) 0%, #C49A35 100%) !important;
        color: var(--ys-darker) !important;
        font-weight: 700 !important;
        border: none !important;
        border-radius: 6px !important;
        padding: 0.6rem 2rem !important;
        letter-spacing: 0.5px;
        transition: all 0.2s ease;
    }
    .stButton > button:hover {
        filter: brightness(1.1);
        box-shadow: 0 4px 16px rgba(212, 168, 67, 0.3);
    }

    /* ── Selectbox / Date Input ── */
    div[data-baseweb="select"] {
        border-color: var(--ys-border) !important;
    }

    /* ── Dataframe ── */
    .stDataFrame {
        border: 1px solid var(--ys-border);
        border-radius: 8px;
        overflow: hidden;
    }

    /* ── Footer ── */
    .ys-footer {
        text-align: center;
        padding: 1.5rem 0 0.5rem 0;
        border-top: 1px solid var(--ys-border);
        margin-top: 2rem;
    }
    .ys-footer p {
        color: var(--ys-muted) !important;
        font-size: 0.7rem !important;
    }
    .ys-footer span.gold {
        color: var(--ys-gold);
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


# ─── Session State Defaults ───────────────────────────────────────────────────
for key, default in {
    "run_status": "idle",       # idle | running | done | error
    "log_output": "",
    "run_results": None,
    "last_council": None,
    "last_date_from": None,
    "last_date_to": None,
    "skipped_refs": [],
    "new_refs": 0,
    "total_scraped": 0,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default


# ─── Google Sheets Helper — read existing refs ───────────────────────────────
GSHEETS_SPREADSHEET_ID = os.getenv(
    "GSHEETS_SPREADSHEET_ID", "1HgfwWdNXRLcTRJ2fGNnORnWkjETRaHsqIN5V32r3VUM"
)
GSHEETS_TAB = os.getenv("GSHEETS_TAB_ALL_LEADS", "All Leads")
GDRIVE_CREDENTIALS_FILE = os.getenv("GDRIVE_CREDENTIALS_FILE", "").strip()

# Column indices in the Google Sheet (A=0): Reference=0, Area=1, Decision Date=5, Reg Date=6
COL_REFERENCE = 0
COL_AREA = 1
COL_REG_DATE = 6


def _get_gsheets_service():
    """Build and return a Google Sheets API service."""
    try:
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except ImportError:
        st.error("Google API libraries not installed. Run: pip install google-api-python-client google-auth-oauthlib")
        return None

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    _base = os.path.dirname(os.path.abspath(__file__))
    token_path = os.path.join(_base, "final_dlr_planning_consents", "gsheets_token.json")
    creds = None

    if not os.path.exists(token_path):
        st.warning(f"Token file not found at: `{token_path}`")
    else:
        try:
            creds = Credentials.from_authorized_user_file(token_path, scopes)
        except Exception as e:
            st.warning(f"Token file exists but could not be loaded: {e}")
            creds = None

    if creds and not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                with open(token_path, "w", encoding="utf-8") as f:
                    f.write(creds.to_json())
            except Exception as e:
                st.warning(f"Token expired and refresh failed: {e}")
                creds = None
        else:
            st.warning("Token is invalid but not refreshable.")
            creds = None

    if not creds:
        if GDRIVE_CREDENTIALS_FILE and os.path.exists(GDRIVE_CREDENTIALS_FILE):
            flow = InstalledAppFlow.from_client_secrets_file(GDRIVE_CREDENTIALS_FILE, scopes)
            creds = flow.run_local_server(port=0)
            os.makedirs(os.path.dirname(token_path), exist_ok=True)
            with open(token_path, "w", encoding="utf-8") as f:
                f.write(creds.to_json())
        else:
            return None

    return build("sheets", "v4", credentials=creds)


def fetch_existing_sheet_data():
    """Read all rows from the Google Sheet and return as a list of row-lists."""
    service = _get_gsheets_service()
    if service is None:
        st.warning("Could not authenticate with Google Sheets. Check credentials/token.")
        return []
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=GSHEETS_SPREADSHEET_ID,
            range=f"'{GSHEETS_TAB}'!A4:L20000",
        ).execute()
        return result.get("values", [])
    except Exception as e:
        st.warning(f"Could not read Google Sheet: {e}")
        return []


def get_existing_references(rows, council_slug=None):
    """Extract a set of reference strings already in the sheet, optionally filtered by council area."""
    refs = set()
    for row in rows:
        if len(row) > max(COL_REFERENCE, COL_AREA):
            ref = (row[COL_REFERENCE] or "").strip()
            if ref:
                refs.add(ref)
    return refs


# ─── Council Config ───────────────────────────────────────────────────────────
COUNCILS = {
    "Dún Laoghaire-Rathdown (DLR)": "dunlaoghaire",
    "Fingal County Council": "fingal",
    "South Dublin County Council": "southdublin",
}

COUNCIL_DESCRIPTIONS = {
    "Dún Laoghaire-Rathdown (DLR)": "Covers Blackrock, Dún Laoghaire, Dalkey, Stillorgan, Dundrum and surrounding areas",
    "Fingal County Council": "Covers Swords, Malahide, Howth, Blanchardstown, Balbriggan and surrounding areas",
    "South Dublin County Council": "Covers Tallaght, Lucan, Clondalkin, Templeogue, Rathfarnham and surrounding areas",
}


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="text-align:center; padding: 1rem 0 0.5rem 0;">
        {LOGO_HTML_SIDEBAR}<br/>
        <span style="color:#8A8A9A; font-size:0.65rem; letter-spacing:2px; text-transform:uppercase;">Planning Intelligence</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    st.markdown("##### Extract Configuration")

    council_name = st.selectbox(
        "Council Portal",
        options=list(COUNCILS.keys()),
        help="Select the county council planning portal to scrape.",
    )
    council_slug = COUNCILS[council_name]

    st.caption(COUNCIL_DESCRIPTIONS[council_name])

    st.markdown("")

    col_from, col_to = st.columns(2)
    with col_from:
        date_from = st.date_input(
            "From",
            value=date.today() - timedelta(days=30),
            max_value=date.today(),
            help="Start date for registration date filter",
        )
    with col_to:
        date_to = st.date_input(
            "To",
            value=date.today(),
            max_value=date.today(),
            help="End date for registration date filter",
        )

    if date_from > date_to:
        st.error("'From' date must be before 'To' date.")

    st.markdown("")

    headless = st.checkbox("Run headless (no browser window)", value=True)
    skip_pdf = st.checkbox("Skip PDF downloads (faster)", value=False)
    use_gsheets = st.checkbox("Sync to Google Sheets", value=True)

    st.markdown("---")

    st.markdown("""
    <div style="padding: 0.75rem; background: #1A1A2E; border: 1px solid #2A2A42; border-radius: 6px;">
        <p style="color:#D4A843; font-size:0.75rem; font-weight:600; margin:0 0 0.3rem 0;">⚡ Smart Deduplication</p>
        <p style="color:#8A8A9A; font-size:0.7rem; margin:0;">
            Before writing to Google Sheets, the system checks for existing records.
            Only <strong style="color:#E8E8EC;">new</strong> planning applications are added — duplicates are automatically skipped.
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")
    st.markdown("""
    <div style="text-align:center;">
        <p style="color:#3A3A52; font-size:0.6rem; margin:0;">v2.0 — Proprietary Software</p>
        <p style="color:#3A3A52; font-size:0.6rem; margin:0;">© Yellow Stag Services Ltd.</p>
    </div>
    """, unsafe_allow_html=True)


# ─── Header ───────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="ys-header">
    <h1>{LOGO_HTML_HEADER} Planning Intelligence Platform</h1>
    <p>Yellow Stag Services — Proprietary Lead Generation System &nbsp;|&nbsp; Automated extraction from Irish county council planning portals</p>
</div>
""", unsafe_allow_html=True)


# ─── Status Bar ───────────────────────────────────────────────────────────────
status = st.session_state.run_status
badge_class = {
    "idle": "ys-badge-idle",
    "running": "ys-badge-running",
    "done": "ys-badge-done",
    "error": "ys-badge-error",
}[status]
badge_text = {
    "idle": "READY",
    "running": "EXTRACTING",
    "done": "COMPLETE",
    "error": "ERROR",
}[status]

col_status, col_council, col_dates = st.columns([1, 1, 1])
with col_status:
    st.markdown(f'<div class="ys-card"><h3>System Status</h3><span class="ys-badge {badge_class}">{badge_text}</span></div>', unsafe_allow_html=True)
with col_council:
    st.markdown(f'<div class="ys-card"><h3>Target Portal</h3><p>{council_name}</p></div>', unsafe_allow_html=True)
with col_dates:
    st.markdown(f'<div class="ys-card"><h3>Date Range</h3><p>{date_from.strftime("%d %b %Y")} → {date_to.strftime("%d %b %Y")}</p></div>', unsafe_allow_html=True)


# ─── Run Extraction ──────────────────────────────────────────────────────────
st.markdown("")

def run_extraction():
    """Execute the scraper as a subprocess, capturing logs."""
    lookback_days = (date.today() - date_from).days
    if lookback_days < 1:
        lookback_days = 1

    # First, fetch existing references from Google Sheets
    existing_refs = set()
    if use_gsheets:
        with st.spinner("Checking Google Sheets for existing data..."):
            rows = fetch_existing_sheet_data()
            existing_refs = get_existing_references(rows)
            if existing_refs:
                st.info(f"Found **{len(existing_refs)}** existing records in Google Sheets. Duplicates will be skipped.")
            else:
                st.info("No existing records found in Google Sheets — all results will be synced.")

    st.session_state.run_status = "running"
    st.session_state.log_output = ""
    st.session_state.skipped_refs = []
    st.session_state.new_refs = 0
    st.session_state.total_scraped = 0

    env = os.environ.copy()
    env["COUNCIL_SLUG"] = council_slug
    env["LOOKBACK_DAYS"] = str(lookback_days)
    env["HEADLESS"] = "1" if headless else "0"
    env["SKIP_PDF_DOWNLOAD"] = "1" if skip_pdf else "0"
    env["USE_GSHEETS_API"] = "1" if use_gsheets else "0"
    env["GSHEETS_INCREMENTAL_SYNC"] = "1"
    # Don't clear on start — we handle dedup ourselves
    env["GSHEETS_CLEAR_ON_START"] = "0"
    env["RUN_ALL_AREAS"] = "1"
    env["MAX_APPS"] = "0"

    # Store existing refs as a comma-separated env var for the modified scraper logic
    if existing_refs:
        env["_YS_EXISTING_REFS"] = ",".join(existing_refs)

    script_path = os.path.join(os.path.dirname(__file__), "dlr_scraper.py")

    log_placeholder = st.empty()
    progress_bar = st.progress(0, text="Initialising extraction...")

    try:
        process = subprocess.Popen(
            [sys.executable, script_path],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        log_lines = []
        areas_done = 0
        total_areas_estimate = {"dunlaoghaire": 35, "fingal": 22, "southdublin": 21}.get(council_slug, 30)

        for line in iter(process.stdout.readline, ""):
            log_lines.append(line.rstrip())
            # Keep last 100 lines in display
            display_lines = log_lines[-100:]
            st.session_state.log_output = "\n".join(display_lines)
            log_placeholder.markdown(
                f'<div class="ys-log">{st.session_state.log_output}</div>',
                unsafe_allow_html=True,
            )

            # Track progress from area markers
            if "==========" in line and "Area" in line:
                areas_done += 1
                pct = min(areas_done / total_areas_estimate, 0.95)
                progress_bar.progress(pct, text=f"Processing area {areas_done}/{total_areas_estimate}...")

            # Count results
            if "Processing:" in line:
                st.session_state.total_scraped += 1

        process.wait()
        progress_bar.progress(1.0, text="Extraction complete.")

        if process.returncode == 0:
            st.session_state.run_status = "done"
        else:
            st.session_state.run_status = "error"

    except Exception as e:
        st.session_state.run_status = "error"
        st.session_state.log_output += f"\n\nFATAL ERROR: {e}"
        log_placeholder.markdown(
            f'<div class="ys-log">{st.session_state.log_output}</div>',
            unsafe_allow_html=True,
        )

    # Post-run: check dedup stats
    if use_gsheets and st.session_state.run_status == "done":
        new_rows = fetch_existing_sheet_data()
        new_refs_set = get_existing_references(new_rows)
        truly_new = new_refs_set - existing_refs
        st.session_state.new_refs = len(truly_new)
        st.session_state.skipped_refs = list(existing_refs & new_refs_set)


# ─── Action Buttons ───────────────────────────────────────────────────────────
col_run, col_check, col_spacer = st.columns([1, 1, 2])

with col_run:
    if st.button(
        "▶  Run Extraction" if status != "running" else "⏳  Running...",
        disabled=(status == "running" or date_from > date_to),
        use_container_width=True,
    ):
        run_extraction()
        st.rerun()

with col_check:
    if st.button("📊  Preview Sheet Data", use_container_width=True):
        with st.spinner("Fetching Google Sheets data..."):
            rows = fetch_existing_sheet_data()
        if rows:
            headers = ["Reference", "Area", "Site Address", "Applicant", "Proposal",
                       "Decision Date", "Reg Date", "Lead Score", "Architect",
                       "Architect Contact", "Drive Link", "Detail URL"]
            # Pad rows to match header length
            padded = [r + [""] * (len(headers) - len(r)) for r in rows]
            df = pd.DataFrame(padded, columns=headers)
            st.session_state.run_results = df
            st.rerun()
        else:
            st.session_state.run_results = None
            st.warning("No data found in Google Sheets. Check that the token at `final_dlr_planning_consents/gsheets_token.json` is valid and the spreadsheet ID is correct.")


# ─── Results Display ──────────────────────────────────────────────────────────
if st.session_state.run_status == "done":
    st.markdown("")
    st.markdown(f"""
    <div class="ys-metric-row">
        <div class="ys-metric">
            <div class="value">{st.session_state.total_scraped}</div>
            <div class="label">Applications Scraped</div>
        </div>
        <div class="ys-metric">
            <div class="value">{st.session_state.new_refs}</div>
            <div class="label">New Records Added</div>
        </div>
        <div class="ys-metric">
            <div class="value">{len(st.session_state.skipped_refs)}</div>
            <div class="label">Duplicates Skipped</div>
        </div>
        <div class="ys-metric">
            <div class="value">✓</div>
            <div class="label">Sheet Synced</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

if st.session_state.run_status == "error":
    st.error("Extraction encountered an error. Check the logs below for details.")

# Show log output if we have any
if st.session_state.log_output and st.session_state.run_status in ("done", "error"):
    with st.expander("📋 Extraction Logs", expanded=False):
        st.markdown(
            f'<div class="ys-log">{st.session_state.log_output}</div>',
            unsafe_allow_html=True,
        )

# Show data preview
if st.session_state.run_results is not None:
    st.markdown("")
    st.markdown('<div class="ys-card"><h3>Google Sheets Data Preview</h3></div>', unsafe_allow_html=True)

    df = st.session_state.run_results

    # Filters
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        area_filter = st.multiselect("Filter by Area", options=sorted(df["Area"].unique()), default=[])
    with filter_col2:
        search = st.text_input("Search references / addresses", "")
    with filter_col3:
        st.markdown("")  # spacer

    filtered = df.copy()
    if area_filter:
        filtered = filtered[filtered["Area"].isin(area_filter)]
    if search:
        mask = filtered.apply(lambda row: search.lower() in " ".join(row.astype(str)).lower(), axis=1)
        filtered = filtered[mask]

    st.dataframe(
        filtered,
        use_container_width=True,
        height=500,
        column_config={
            "Detail URL": st.column_config.LinkColumn("Detail URL", display_text="View"),
            "Drive Link": st.column_config.LinkColumn("Drive Link", display_text="Open"),
        },
    )

    st.caption(f"Showing {len(filtered)} of {len(df)} records")


# ─── Also check for local CSV outputs ────────────────────────────────────────
output_dir = os.path.join(os.getcwd(), "final_dlr_planning_consents")
if os.path.isdir(output_dir) and st.session_state.run_status == "done":
    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    if csv_files:
        with st.expander("📁 Local Output Files"):
            for f in sorted(csv_files):
                st.text(f"  {f}")


# ─── Footer ──────────────────────────────────────────────────────────────────
st.markdown("""
<div class="ys-footer">
    <p>
        <span class="gold">Yellow Stag Services</span> — Planning Intelligence Platform v2.0<br/>
        Proprietary lead generation system. Unauthorised use prohibited.<br/>
        © 2024–2026 Yellow Stag Services Ltd. All rights reserved.
    </p>
</div>
""", unsafe_allow_html=True)
