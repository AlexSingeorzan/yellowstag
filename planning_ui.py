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
    LOGO_HTML_SIDEBAR = f'<img src="data:image/png;base64,{_logo_b64}" style="width:180px; background:#162044; border-radius:8px; padding:8px 14px;" alt="Yellow Stag">'
    LOGO_HTML_HEADER = f'<img src="data:image/png;base64,{_logo_b64}" style="height:44px; vertical-align:middle; margin-right:14px; background:#162044; border-radius:6px; padding:4px 10px;" alt="Yellow Stag">'
else:
    _shield = '<svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#D4A843" stroke-width="1.5"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>'
    LOGO_HTML_SIDEBAR = f'<div style="text-align:center;">{_shield}</div>'
    LOGO_HTML_HEADER = _shield


# ─── SVG Icon Library ────────────────────────────────────────────────────────
SVG_ICONS = {
    "play": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polygon points="5 3 19 12 5 21 5 3"/></svg>',
    "sheet": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="18" height="18" rx="2"/><line x1="3" y1="9" x2="21" y2="9"/><line x1="3" y1="15" x2="21" y2="15"/><line x1="9" y1="3" x2="9" y2="21"/></svg>',
    "check": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M22 11.08V12a10 10 0 11-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>',
    "alert": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
    "zap": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2"/></svg>',
    "target": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/></svg>',
    "calendar": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="4" width="18" height="18" rx="2"/><line x1="16" y1="2" x2="16" y2="6"/><line x1="8" y1="2" x2="8" y2="6"/><line x1="3" y1="10" x2="21" y2="10"/></svg>',
    "users": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 00-4-4H5a4 4 0 00-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 00-3-3.87"/><path d="M16 3.13a4 4 0 010 7.75"/></svg>',
    "chart": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="20" x2="12" y2="10"/><line x1="18" y1="20" x2="18" y2="4"/><line x1="6" y1="20" x2="6" y2="16"/></svg>',
    "file": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z"/><polyline points="14 2 14 8 20 8"/><line x1="16" y1="13" x2="8" y2="13"/><line x1="16" y1="17" x2="8" y2="17"/></svg>',
    "clock": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>',
    "shield": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>',
    "folder": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg>',
    "link": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10 13a5 5 0 007.54.54l3-3a5 5 0 00-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 00-7.54-.54l-3 3a5 5 0 007.07 7.07l1.71-1.71"/></svg>',
    "skip": '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg>',
}


def icon(name, size=14, color="currentColor"):
    """Return an inline SVG icon string."""
    svg = SVG_ICONS.get(name, "")
    if size != 14:
        svg = svg.replace('width="14"', f'width="{size}"').replace('height="14"', f'height="{size}"')
    if color != "currentColor":
        svg = svg.replace('stroke="currentColor"', f'stroke="{color}"')
    return svg


# ─── Page Config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Yellow Stag \u2014 Planning Intelligence",
    page_icon="\u25C6",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─── CSS Design System ───────────────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Fonts ── */
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Sans:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

    /* ── Root Variables ── */
    :root {
        --ys-gold: #D4A843;
        --ys-gold-light: #E8C96A;
        --ys-gold-dim: #B8912E;
        --ys-dark: #1A1A2E;
        --ys-darker: #12121F;
        --ys-darkest: #0A0A16;
        --ys-card: #1E1E32;
        --ys-card-hover: #242442;
        --ys-text: #E8E8EC;
        --ys-text-secondary: #C0C0CC;
        --ys-muted: #8A8A9A;
        --ys-success: #2ECC71;
        --ys-success-bg: #1A3A2E;
        --ys-danger: #E74C3C;
        --ys-danger-bg: #3A1A1E;
        --ys-warning: #F39C12;
        --ys-warning-bg: #3A2E1A;
        --ys-info: #3498DB;
        --ys-info-bg: #1A2E3A;
        --ys-border: #2A2A42;
        --ys-border-light: #3A3A55;

        --font-display: 'Space Grotesk', sans-serif;
        --font-body: 'IBM Plex Sans', sans-serif;
        --font-mono: 'JetBrains Mono', monospace;

        --shadow-sm: 0 1px 3px rgba(0,0,0,0.3);
        --shadow-md: 0 4px 12px rgba(0,0,0,0.4);
        --shadow-lg: 0 8px 24px rgba(0,0,0,0.5);
        --shadow-gold: 0 4px 20px rgba(212,168,67,0.12);

        --glass-bg: rgba(30,30,50,0.55);
        --glass-border: rgba(255,255,255,0.06);

        --radius-sm: 4px;
        --radius-md: 8px;
        --radius-lg: 12px;
        --radius-full: 999px;

        --ease: cubic-bezier(0.4, 0, 0.2, 1);
    }

    /* ── Animations ── */
    @keyframes ys-fade-in {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes ys-pulse-dot {
        0%, 100% { transform: scale(1); box-shadow: 0 0 0 0 rgba(46,204,113,0.4); }
        50% { transform: scale(1.3); box-shadow: 0 0 0 8px rgba(46,204,113,0); }
    }
    @keyframes ys-shimmer {
        0% { background-position: -200% 0; }
        100% { background-position: 200% 0; }
    }

    /* ── Global ── */
    .stApp {
        background: linear-gradient(175deg, var(--ys-darkest) 0%, var(--ys-dark) 40%, #181830 100%);
        font-family: var(--font-body);
    }
    .stApp h1, .stApp h2, .stApp h3, .stApp h4, .stApp h5 {
        font-family: var(--font-display) !important;
    }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, var(--ys-darker) 0%, var(--ys-darkest) 100%) !important;
        border-right: 1px solid var(--ys-border);
    }
    section[data-testid="stSidebar"] .stMarkdown p,
    section[data-testid="stSidebar"] .stMarkdown li,
    section[data-testid="stSidebar"] label {
        color: var(--ys-text) !important;
        font-family: var(--font-body);
    }
    section[data-testid="stSidebar"] .stMarkdown h5 {
        font-family: var(--font-display) !important;
        color: var(--ys-gold) !important;
        font-size: 0.7rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 2px;
    }

    /* ── Sidebar section containers ── */
    .ys-sb-section {
        padding: 0.85rem;
        margin: 0.4rem 0;
        background: rgba(26,26,46,0.4);
        border: 1px solid var(--ys-border);
        border-radius: var(--radius-md);
    }
    .ys-sb-title {
        color: var(--ys-gold) !important;
        font-family: var(--font-display);
        font-size: 0.65rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 2px;
        margin-bottom: 0.3rem !important;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* ── Glass Card ── */
    .ys-glass {
        background: var(--glass-bg);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid var(--glass-border);
        border-radius: var(--radius-lg);
        padding: 1.3rem 1.5rem;
        transition: all 0.25s var(--ease);
        animation: ys-fade-in 0.5s var(--ease) both;
    }
    .ys-glass:hover {
        border-color: rgba(212,168,67,0.18);
        box-shadow: var(--shadow-gold);
        transform: translateY(-2px);
    }
    .ys-glass__icon-title {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 10px;
        color: var(--ys-gold);
        font-family: var(--font-display);
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1.5px;
    }
    .ys-glass__body {
        color: var(--ys-text);
        font-size: 0.9rem;
        font-weight: 500;
    }

    /* ── Header banner ── */
    .ys-header {
        background: linear-gradient(135deg, rgba(26,26,46,0.8) 0%, rgba(34,34,64,0.6) 50%, rgba(26,26,46,0.8) 100%);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border: 1px solid var(--glass-border);
        border-left: 4px solid var(--ys-gold);
        border-radius: var(--radius-lg);
        padding: 1.6rem 2rem;
        margin-bottom: 1.5rem;
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        animation: ys-fade-in 0.4s var(--ease) both;
    }
    .ys-header h1 {
        color: var(--ys-gold) !important;
        font-size: 1.6rem !important;
        font-weight: 700 !important;
        margin: 0 0 0.25rem 0 !important;
        letter-spacing: 0.3px;
        display: flex;
        align-items: center;
        gap: 14px;
    }
    .ys-header p {
        color: var(--ys-muted) !important;
        font-size: 0.8rem !important;
        font-family: var(--font-body);
        margin: 0 !important;
    }

    /* ── Status dot ── */
    .ys-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 6px;
        vertical-align: middle;
    }
    .ys-dot--idle { background: var(--ys-muted); }
    .ys-dot--running { background: var(--ys-success); animation: ys-pulse-dot 1.5s ease-in-out infinite; }
    .ys-dot--done { background: var(--ys-gold); }
    .ys-dot--error { background: var(--ys-danger); }

    /* ── Status badge ── */
    .ys-badge {
        display: inline-flex;
        align-items: center;
        padding: 0.3rem 0.85rem;
        border-radius: var(--radius-full);
        font-family: var(--font-display);
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.8px;
    }
    .ys-badge-idle { background: #2A2A42; color: var(--ys-muted); }
    .ys-badge-running { background: var(--ys-success-bg); color: var(--ys-success); }
    .ys-badge-error { background: var(--ys-danger-bg); color: var(--ys-danger); }
    .ys-badge-done { background: rgba(212,168,67,0.12); color: var(--ys-gold); }

    /* ── Connection indicator ── */
    .ys-conn {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 6px 12px;
        border-radius: var(--radius-md);
        font-family: var(--font-body);
        font-size: 0.72rem;
        font-weight: 500;
    }
    .ys-conn--ok { background: var(--ys-success-bg); border: 1px solid rgba(46,204,113,0.2); color: var(--ys-success); }
    .ys-conn--fail { background: var(--ys-danger-bg); border: 1px solid rgba(231,76,60,0.2); color: var(--ys-danger); }

    /* ── KPI Grid ── */
    .ys-kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    .ys-kpi {
        background: var(--glass-bg);
        backdrop-filter: blur(10px);
        -webkit-backdrop-filter: blur(10px);
        border: 1px solid var(--glass-border);
        border-radius: var(--radius-lg);
        padding: 1.3rem;
        position: relative;
        overflow: hidden;
        transition: all 0.25s var(--ease);
    }
    .ys-kpi::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 3px;
        background: linear-gradient(90deg, var(--ys-gold), var(--ys-gold-light));
        border-radius: var(--radius-lg) var(--radius-lg) 0 0;
    }
    .ys-kpi:hover {
        border-color: rgba(212,168,67,0.2);
        box-shadow: var(--shadow-gold);
        transform: translateY(-2px);
    }
    .ys-kpi__icon {
        width: 36px;
        height: 36px;
        border-radius: var(--radius-md);
        display: flex;
        align-items: center;
        justify-content: center;
        margin-bottom: 0.6rem;
    }
    .ys-kpi__value {
        font-family: var(--font-display);
        font-size: 2rem;
        font-weight: 800;
        color: var(--ys-text);
        line-height: 1;
        margin-bottom: 0.2rem;
    }
    .ys-kpi__label {
        font-family: var(--font-body);
        font-size: 0.68rem;
        color: var(--ys-muted);
        text-transform: uppercase;
        letter-spacing: 1.5px;
        font-weight: 500;
    }

    /* ── Data Panel ── */
    .ys-data-panel {
        background: var(--glass-bg);
        backdrop-filter: blur(10px);
        border: 1px solid var(--glass-border);
        border-radius: var(--radius-lg);
        padding: 1.4rem;
        animation: ys-fade-in 0.5s var(--ease) both;
    }
    .ys-data-panel__header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
        padding-bottom: 0.6rem;
        border-bottom: 1px solid var(--ys-border);
    }
    .ys-data-panel__title {
        color: var(--ys-gold) !important;
        font-family: var(--font-display) !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        margin: 0 !important;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .ys-data-panel__count {
        color: var(--ys-muted);
        font-family: var(--font-mono);
        font-size: 0.72rem;
    }

    /* ── Log area ── */
    .ys-log {
        background: var(--ys-darkest);
        border: 1px solid var(--ys-border);
        border-top: 2px solid var(--ys-gold-dim);
        border-radius: var(--radius-md);
        padding: 1rem;
        font-family: var(--font-mono);
        font-size: 0.68rem;
        color: var(--ys-muted);
        max-height: 420px;
        overflow-y: auto;
        white-space: pre-wrap;
        word-break: break-all;
        line-height: 1.7;
    }
    .ys-log::-webkit-scrollbar { width: 5px; }
    .ys-log::-webkit-scrollbar-track { background: var(--ys-darkest); }
    .ys-log::-webkit-scrollbar-thumb { background: var(--ys-border); border-radius: 3px; }

    /* ── Buttons ── */
    .stButton > button {
        background: linear-gradient(135deg, var(--ys-gold) 0%, var(--ys-gold-dim) 100%) !important;
        color: var(--ys-darkest) !important;
        font-family: var(--font-display) !important;
        font-weight: 700 !important;
        font-size: 0.8rem !important;
        border: none !important;
        border-radius: var(--radius-md) !important;
        padding: 0.65rem 1.8rem !important;
        letter-spacing: 0.3px;
        transition: all 0.25s var(--ease);
        box-shadow: var(--shadow-sm);
    }
    .stButton > button:hover {
        filter: brightness(1.12);
        box-shadow: var(--shadow-gold);
        transform: translateY(-1px);
    }
    .stButton > button:active {
        transform: translateY(0);
        filter: brightness(0.95);
    }
    .stButton > button:disabled {
        background: var(--ys-border) !important;
        color: var(--ys-muted) !important;
        box-shadow: none !important;
        transform: none !important;
    }

    /* ── Inputs ── */
    div[data-baseweb="select"] {
        border-color: var(--ys-border) !important;
    }
    .stTextInput input {
        font-family: var(--font-body) !important;
    }

    /* ── Dataframe ── */
    .stDataFrame {
        border: 1px solid var(--ys-border);
        border-radius: var(--radius-md);
        overflow: hidden;
    }

    /* ── Expander ── */
    .streamlit-expanderHeader {
        font-family: var(--font-display) !important;
        font-weight: 600 !important;
        color: var(--ys-text-secondary) !important;
    }

    /* ── Footer ── */
    .ys-footer {
        text-align: center;
        padding: 1.5rem 0 0.5rem 0;
        border-top: 1px solid var(--ys-border);
        margin-top: 2rem;
    }
    .ys-footer-row {
        display: flex;
        justify-content: center;
        align-items: center;
        gap: 18px;
        flex-wrap: wrap;
        font-family: var(--font-body);
    }
    .ys-footer-sep {
        color: var(--ys-border-light);
        font-size: 0.8rem;
    }

    /* ── Stagger animation delays for KPI cards ── */
    .ys-kpi:nth-child(1) { animation: ys-fade-in 0.4s var(--ease) 0.05s both; }
    .ys-kpi:nth-child(2) { animation: ys-fade-in 0.4s var(--ease) 0.12s both; }
    .ys-kpi:nth-child(3) { animation: ys-fade-in 0.4s var(--ease) 0.19s both; }
    .ys-kpi:nth-child(4) { animation: ys-fade-in 0.4s var(--ease) 0.26s both; }

    /* ── Subtle background noise texture via gradient ── */
    .stApp::before {
        content: '';
        position: fixed;
        top: 0; left: 0; right: 0; bottom: 0;
        background:
            radial-gradient(ellipse at 20% 50%, rgba(212,168,67,0.03) 0%, transparent 50%),
            radial-gradient(ellipse at 80% 20%, rgba(52,152,219,0.02) 0%, transparent 50%),
            radial-gradient(ellipse at 50% 80%, rgba(46,204,113,0.02) 0%, transparent 50%);
        pointer-events: none;
        z-index: 0;
    }
</style>
""", unsafe_allow_html=True)


# ─── Component Helpers ────────────────────────────────────────────────────────

def render_kpi_row(kpis):
    """Render a grid of KPI cards. Each kpi: {value, label, icon, icon_bg}."""
    cards_html = ""
    for kpi in kpis:
        ic = icon(kpi["icon"], 18, kpi.get("icon_color", "#D4A843"))
        cards_html += f'''
        <div class="ys-kpi">
            <div class="ys-kpi__icon" style="background:{kpi.get("icon_bg", "var(--ys-info-bg)")};">{ic}</div>
            <div class="ys-kpi__value">{kpi["value"]}</div>
            <div class="ys-kpi__label">{kpi["label"]}</div>
        </div>'''
    st.markdown(f'<div class="ys-kpi-grid">{cards_html}</div>', unsafe_allow_html=True)


def render_connection_badge(connected):
    """Render a Google Sheets connection status pill."""
    if connected:
        dot = '<span class="ys-dot ys-dot--done"></span>'
        return f'<span class="ys-conn ys-conn--ok">{dot} Sheets Connected</span>'
    else:
        dot = '<span class="ys-dot ys-dot--error"></span>'
        return f'<span class="ys-conn ys-conn--fail">{dot} Sheets Disconnected</span>'


# ─── Session State Defaults ───────────────────────────────────────────────────
for key, default in {
    "run_status": "idle",
    "log_output": "",
    "run_results": None,
    "last_council": None,
    "last_date_from": None,
    "last_date_to": None,
    "skipped_refs": [],
    "new_refs": 0,
    "total_scraped": 0,
    "gsheets_connected": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# Probe Google Sheets connection once per session
_base = os.path.dirname(os.path.abspath(__file__))
_token_path = os.path.join(_base, "final_dlr_planning_consents", "gsheets_token.json")
if st.session_state.gsheets_connected is None:
    st.session_state.gsheets_connected = os.path.exists(_token_path)
gsheets_ok = st.session_state.gsheets_connected


# ─── Google Sheets Helper — read existing refs ───────────────────────────────
GSHEETS_SPREADSHEET_ID = os.getenv(
    "GSHEETS_SPREADSHEET_ID", "1HgfwWdNXRLcTRJ2fGNnORnWkjETRaHsqIN5V32r3VUM"
)
GSHEETS_TAB = os.getenv("GSHEETS_TAB_ALL_LEADS", "All Leads")
GDRIVE_CREDENTIALS_FILE = os.getenv("GDRIVE_CREDENTIALS_FILE", "").strip()

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
    """Extract a set of reference strings already in the sheet."""
    refs = set()
    for row in rows:
        if len(row) > max(COL_REFERENCE, COL_AREA):
            ref = (row[COL_REFERENCE] or "").strip()
            if ref:
                refs.add(ref)
    return refs


# ─── Council Config ───────────────────────────────────────────────────────────
COUNCILS = {
    "D\u00fan Laoghaire-Rathdown (DLR)": "dunlaoghaire",
    "Fingal County Council": "fingal",
    "South Dublin County Council": "southdublin",
}

COUNCIL_DESCRIPTIONS = {
    "D\u00fan Laoghaire-Rathdown (DLR)": "Covers Blackrock, D\u00fan Laoghaire, Dalkey, Stillorgan, Dundrum and surrounding areas",
    "Fingal County Council": "Covers Swords, Malahide, Howth, Blanchardstown, Balbriggan and surrounding areas",
    "South Dublin County Council": "Covers Tallaght, Lucan, Clondalkin, Templeogue, Rathfarnham and surrounding areas",
}


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="text-align:center; padding:1.2rem 0 0.6rem 0;">
        {LOGO_HTML_SIDEBAR}<br/>
        <span style="color:var(--ys-muted); font-family:var(--font-display); font-size:0.6rem; letter-spacing:2.5px; text-transform:uppercase;">
            Planning Intelligence
        </span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    # -- Portal Configuration --
    st.markdown(f"""
    <div class="ys-sb-section">
        <div class="ys-sb-title">{icon("target", 12, "#D4A843")} Portal Configuration</div>
    </div>
    """, unsafe_allow_html=True)

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

    # -- Run Options --
    st.markdown(f"""
    <div class="ys-sb-section">
        <div class="ys-sb-title">{icon("shield", 12, "#D4A843")} Run Options</div>
    </div>
    """, unsafe_allow_html=True)

    headless = st.checkbox("Run headless (no browser window)", value=True)
    skip_pdf = st.checkbox("Skip PDF downloads (faster)", value=False)
    use_gsheets = st.checkbox("Sync to Google Sheets", value=True)

    st.markdown("")

    # -- Connection Status --
    conn_html = render_connection_badge(gsheets_ok)
    st.markdown(f'<div style="text-align:center; margin:0.4rem 0;">{conn_html}</div>', unsafe_allow_html=True)

    st.markdown("---")

    # -- Smart Dedup info --
    st.markdown(f"""
    <div class="ys-sb-section" style="border-left:2px solid var(--ys-gold);">
        <p style="color:var(--ys-gold); font-family:var(--font-display); font-size:0.72rem; font-weight:600; margin:0 0 4px 0; display:flex; align-items:center; gap:6px;">
            {icon("zap", 13, "#D4A843")} Smart Deduplication
        </p>
        <p style="color:var(--ys-muted); font-family:var(--font-body); font-size:0.68rem; margin:0; line-height:1.5;">
            Only <strong style="color:var(--ys-text);">new</strong> planning applications are added.
            Duplicates are automatically detected and skipped.
        </p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("")
    st.markdown(f"""
    <div style="text-align:center;">
        <p style="color:#2A2A42; font-family:var(--font-body); font-size:0.58rem; margin:0;">v3.0 \u2014 Proprietary Software</p>
        <p style="color:#2A2A42; font-family:var(--font-body); font-size:0.58rem; margin:0;">\u00A9 Yellow Stag Services Ltd.</p>
    </div>
    """, unsafe_allow_html=True)


# ─── Header ───────────────────────────────────────────────────────────────────
conn_badge = render_connection_badge(gsheets_ok)
st.markdown(f"""
<div class="ys-header">
    <div>
        <h1>{LOGO_HTML_HEADER} <span>Planning Intelligence Platform</span></h1>
        <p>Yellow Stag Services \u2014 Proprietary Lead Generation System
           <span style="color:var(--ys-border-light); margin:0 6px;">\u2502</span>
           Automated extraction from Irish county council planning portals
        </p>
    </div>
    <div style="margin-top:4px;">{conn_badge}</div>
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
dot_class = f"ys-dot--{status}"

col_status, col_council, col_dates = st.columns([1, 1, 1])
with col_status:
    st.markdown(f"""
    <div class="ys-glass">
        <div class="ys-glass__icon-title">{icon("shield", 15, "#D4A843")} System Status</div>
        <span class="ys-badge {badge_class}"><span class="ys-dot {dot_class}"></span>{badge_text}</span>
    </div>
    """, unsafe_allow_html=True)
with col_council:
    st.markdown(f"""
    <div class="ys-glass" style="animation-delay:0.08s;">
        <div class="ys-glass__icon-title">{icon("target", 15, "#D4A843")} Target Portal</div>
        <div class="ys-glass__body">{council_name}</div>
    </div>
    """, unsafe_allow_html=True)
with col_dates:
    st.markdown(f"""
    <div class="ys-glass" style="animation-delay:0.16s;">
        <div class="ys-glass__icon-title">{icon("calendar", 15, "#D4A843")} Date Range</div>
        <div class="ys-glass__body">{date_from.strftime("%d %b %Y")} \u2192 {date_to.strftime("%d %b %Y")}</div>
    </div>
    """, unsafe_allow_html=True)


# ─── Run Extraction ──────────────────────────────────────────────────────────
st.markdown("")


def run_extraction():
    """Execute the scraper as a subprocess, capturing logs."""
    lookback_days = (date.today() - date_from).days
    if lookback_days < 1:
        lookback_days = 1

    existing_refs = set()
    if use_gsheets:
        with st.spinner("Checking Google Sheets for existing data..."):
            rows = fetch_existing_sheet_data()
            existing_refs = get_existing_references(rows)
            if existing_refs:
                st.info(f"Found **{len(existing_refs)}** existing records in Google Sheets. Duplicates will be skipped.")
            else:
                st.info("No existing records found in Google Sheets \u2014 all results will be synced.")

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
    env["GSHEETS_CLEAR_ON_START"] = "0"
    env["RUN_ALL_AREAS"] = "1"
    env["MAX_APPS"] = "0"

    if existing_refs:
        env["_YS_EXISTING_REFS"] = ",".join(existing_refs)

    script_path = os.path.join(os.path.dirname(__file__), "dlr_scraper.py")

    log_placeholder = st.empty()
    stats_placeholder = st.empty()
    progress_bar = st.progress(0, text="Initialising extraction...")

    start_time = time.time()

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
            display_lines = log_lines[-100:]
            st.session_state.log_output = "\n".join(display_lines)
            log_placeholder.markdown(
                f'<div class="ys-log">{st.session_state.log_output}</div>',
                unsafe_allow_html=True,
            )

            if "==========" in line and "Area" in line:
                areas_done += 1
                pct = min(areas_done / total_areas_estimate, 0.95)
                progress_bar.progress(pct, text=f"Processing area {areas_done}/{total_areas_estimate}...")

                elapsed = time.time() - start_time
                stats_placeholder.markdown(f"""
                <div style="display:flex; gap:24px; color:var(--ys-muted); font-family:var(--font-mono); font-size:0.68rem; margin:4px 0 8px 0;">
                    <span>{icon("clock", 11, "#8A8A9A")} Elapsed: {int(elapsed//60)}m {int(elapsed%60)}s</span>
                    <span>{icon("file", 11, "#8A8A9A")} Records: {st.session_state.total_scraped}</span>
                    <span>{icon("target", 11, "#8A8A9A")} Areas: {areas_done}/{total_areas_estimate}</span>
                </div>
                """, unsafe_allow_html=True)

            if "Processing:" in line:
                st.session_state.total_scraped += 1

        process.wait()
        progress_bar.progress(1.0, text="Extraction complete.")
        stats_placeholder.empty()

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
        "\u25B6  Run Extraction" if status != "running" else "\u25CC  Running...",
        disabled=(status == "running" or date_from > date_to),
        use_container_width=True,
    ):
        run_extraction()
        st.rerun()

with col_check:
    if st.button("\u25A6  Preview Sheet Data", use_container_width=True):
        with st.spinner("Fetching Google Sheets data..."):
            rows = fetch_existing_sheet_data()
        if rows:
            headers = ["Reference", "Area", "Site Address", "Applicant", "Proposal",
                       "Decision Date", "Reg Date", "Lead Score", "Architect",
                       "Architect Contact", "Drive Link", "Detail URL"]
            padded = [r + [""] * (len(headers) - len(r)) for r in rows]
            df = pd.DataFrame(padded, columns=headers)
            st.session_state.run_results = df
            st.rerun()
        else:
            st.session_state.run_results = None
            st.warning("No data found in Google Sheets. Check that the token is valid and the spreadsheet ID is correct.")


# ─── Results Display ──────────────────────────────────────────────────────────
if st.session_state.run_status == "done":
    st.markdown("")
    render_kpi_row([
        {"value": st.session_state.total_scraped, "label": "Applications Scraped", "icon": "file", "icon_bg": "var(--ys-info-bg)", "icon_color": "#3498DB"},
        {"value": st.session_state.new_refs, "label": "New Records Added", "icon": "check", "icon_bg": "var(--ys-success-bg)", "icon_color": "#2ECC71"},
        {"value": len(st.session_state.skipped_refs), "label": "Duplicates Skipped", "icon": "skip", "icon_bg": "var(--ys-warning-bg)", "icon_color": "#F39C12"},
        {"value": "\u2713", "label": "Sheet Synced", "icon": "link", "icon_bg": "var(--ys-success-bg)", "icon_color": "#2ECC71"},
    ])

if st.session_state.run_status == "error":
    st.error("Extraction encountered an error. Check the logs below for details.")

# Show log output
if st.session_state.log_output and st.session_state.run_status in ("done", "error"):
    with st.expander("\u25B8 Extraction Logs", expanded=False):
        st.markdown(
            f'<div class="ys-log">{st.session_state.log_output}</div>',
            unsafe_allow_html=True,
        )

# ─── Data Preview ─────────────────────────────────────────────────────────────
if st.session_state.run_results is not None:
    df = st.session_state.run_results

    # Analytical KPIs from loaded data
    st.markdown("")
    scores = pd.to_numeric(df["Lead Score"], errors="coerce").fillna(0)
    avg_score = scores.mean() if len(scores) > 0 else 0
    tier1_count = int((scores >= 70).sum())
    top_area = df["Area"].value_counts().index[0] if len(df) > 0 and df["Area"].any() else "N/A"

    render_kpi_row([
        {"value": len(df), "label": "Total Leads in Sheet", "icon": "users", "icon_bg": "var(--ys-info-bg)", "icon_color": "#3498DB"},
        {"value": tier1_count, "label": "Tier 1 \u2014 Call This Week", "icon": "zap", "icon_bg": "var(--ys-danger-bg)", "icon_color": "#E74C3C"},
        {"value": f"{avg_score:.0f}", "label": "Avg Lead Score", "icon": "target", "icon_bg": "var(--ys-warning-bg)", "icon_color": "#F39C12"},
        {"value": top_area, "label": "Top Area by Volume", "icon": "chart", "icon_bg": "var(--ys-success-bg)", "icon_color": "#2ECC71"},
    ])

    # Data panel header
    st.markdown(f"""
    <div class="ys-data-panel">
        <div class="ys-data-panel__header">
            <h3 class="ys-data-panel__title">{icon("sheet", 17, "#D4A843")} Google Sheets Data Preview</h3>
            <span class="ys-data-panel__count">{len(df)} records</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Filters
    f1, f2, f3, f4 = st.columns([1.2, 1.2, 1, 0.6])
    with f1:
        area_filter = st.multiselect("Filter by Area", options=sorted(df["Area"].unique()), default=[])
    with f2:
        search = st.text_input("Search references / addresses", "", placeholder="Type to search...")
    with f3:
        score_range = st.slider("Lead Score Range", 0, 100, (0, 100))
    with f4:
        st.markdown("")

    filtered = df.copy()
    if area_filter:
        filtered = filtered[filtered["Area"].isin(area_filter)]
    if search:
        mask = filtered.apply(lambda row: search.lower() in " ".join(row.astype(str)).lower(), axis=1)
        filtered = filtered[mask]
    if score_range != (0, 100):
        s = pd.to_numeric(filtered["Lead Score"], errors="coerce").fillna(0)
        filtered = filtered[(s >= score_range[0]) & (s <= score_range[1])]

    st.dataframe(
        filtered,
        use_container_width=True,
        height=520,
        column_config={
            "Detail URL": st.column_config.LinkColumn("Detail URL", display_text="View"),
            "Drive Link": st.column_config.LinkColumn("Drive Link", display_text="Open"),
            "Lead Score": st.column_config.ProgressColumn(
                "Lead Score",
                min_value=0,
                max_value=100,
                format="%d",
            ),
            "Reference": st.column_config.TextColumn("Reference", width="medium"),
            "Area": st.column_config.TextColumn("Area", width="small"),
            "Proposal": st.column_config.TextColumn("Proposal", width="large"),
        },
    )

    st.markdown(f"""
    <div style="display:flex; justify-content:space-between; align-items:center; padding:6px 4px;">
        <span style="color:var(--ys-muted); font-family:var(--font-body); font-size:0.72rem;">
            Showing {len(filtered)} of {len(df)} records
        </span>
        <span style="color:var(--ys-muted); font-family:var(--font-mono); font-size:0.65rem;">
            {icon("clock", 11, "#8A8A9A")} Refreshed {datetime.now().strftime("%H:%M:%S")}
        </span>
    </div>
    """, unsafe_allow_html=True)


# ─── Local CSV outputs ────────────────────────────────────────────────────────
output_dir = os.path.join(_base, "final_dlr_planning_consents")
if os.path.isdir(output_dir) and st.session_state.run_status == "done":
    csv_files = [f for f in os.listdir(output_dir) if f.endswith(".csv")]
    if csv_files:
        with st.expander("\u25B8 Local Output Files"):
            for f in sorted(csv_files):
                st.markdown(f'<span style="color:var(--ys-text); font-family:var(--font-mono); font-size:0.72rem;">{icon("file", 12, "#8A8A9A")} {f}</span>', unsafe_allow_html=True)


# ─── Footer ──────────────────────────────────────────────────────────────────
st.markdown(f"""
<div class="ys-footer">
    <div class="ys-footer-row">
        <span style="color:var(--ys-gold); font-family:var(--font-display); font-weight:600; font-size:0.75rem;">Yellow Stag Services</span>
        <span class="ys-footer-sep">\u2502</span>
        <span style="color:var(--ys-muted); font-size:0.68rem;">Planning Intelligence Platform v3.0</span>
        <span class="ys-footer-sep">\u2502</span>
        <span style="font-size:0.68rem;">
            <span class="ys-dot ys-dot--{"done" if gsheets_ok else "error"}" style="width:6px; height:6px;"></span>
            <span style="color:var(--ys-muted);">Sheets {"Connected" if gsheets_ok else "Disconnected"}</span>
        </span>
        <span class="ys-footer-sep">\u2502</span>
        <span style="color:var(--ys-muted); font-size:0.68rem;">{icon("shield", 10, "#3A3A52")} Proprietary Software</span>
    </div>
    <p style="color:#2A2A42; font-family:var(--font-body); font-size:0.58rem; margin-top:8px;">\u00A9 2024\u20132026 Yellow Stag Services Ltd. All rights reserved.</p>
</div>
""", unsafe_allow_html=True)
