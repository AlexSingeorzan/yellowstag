

#!/usr/bin/env python3
"""
DLR Planning Scraper — Yellow Stag Services Edition
Lead generation scraper for Dún Laoghaire-Rathdown County Council planning portal.
Targets homeowners/developers who have recently received planning permission.
"""

import os
import re
import time
import logging
import json
import shutil
from datetime import datetime, date, timedelta
from urllib import request, error

import pandas as pd
import pdfplumber
import pytesseract
from pdf2image import convert_from_path
from PIL import Image
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except Exception:
    Credentials = None
    InstalledAppFlow = None
    Request = None
    build = None
    MediaFileUpload = None

# ===============================
# CONFIG
# ===============================
COUNCIL_SLUG = os.getenv("COUNCIL_SLUG", "dunlaoghaire").strip().lower()
if COUNCIL_SLUG not in ("dunlaoghaire", "southdublin", "fingal"):
    COUNCIL_SLUG = "dunlaoghaire"
SEARCH_URL = f"https://planning.agileapplications.ie/{COUNCIL_SLUG}/search-applications/"
DETAIL_URL_BASE = f"https://planning.agileapplications.ie/{COUNCIL_SLUG}/application-details"
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(os.getcwd(), "final_dlr_planning_consents"))
TEMP_DOWNLOAD_DIR = os.path.join(OUTPUT_DIR, "_temp_downloads")
DEBUG_DIR = os.path.join(OUTPUT_DIR, "dlr_debug")

AREA_NAME = os.getenv("AREA_NAME", "").strip()
RUN_ALL_AREAS = os.getenv("RUN_ALL_AREAS", "1").lower() not in ("0", "false", "no")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "90"))
MIN_LEAD_SCORE = int(os.getenv("MIN_LEAD_SCORE", "0"))
EXPORT_TIER1_ONLY = os.getenv("EXPORT_TIER1_ONLY", "0").lower() not in ("0", "false", "no")
SKIP_PDF_DOWNLOAD = os.getenv("SKIP_PDF_DOWNLOAD", "0").lower() not in ("0", "false", "no")
MAX_APPS = int(os.getenv("MAX_APPS", "0"))
HEADLESS = os.getenv("HEADLESS", "1").lower() not in ("0", "false", "no")
RESUME_FROM_AREA = os.getenv("RESUME_FROM_AREA", "").strip()
PDF_WAIT_TIMEOUT = int(os.getenv("PDF_WAIT_TIMEOUT", "20"))
POST_FILTER_WAIT_SECONDS = float(os.getenv("POST_FILTER_WAIT_SECONDS", "3"))
SEARCH_RESULTS_TIMEOUT = int(os.getenv("SEARCH_RESULTS_TIMEOUT", "75"))
MAX_SITE_LAYOUT_DOCS_PER_APP = int(os.getenv("MAX_SITE_LAYOUT_DOCS_PER_APP", "3"))
MIN_SITE_LAYOUT_PRIORITY = int(os.getenv("MIN_SITE_LAYOUT_PRIORITY", "4"))
USE_OLLAMA = os.getenv("USE_OLLAMA", "1").lower() not in ("0", "false", "no")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:0.5b")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/generate")
OLLAMA_TIMEOUT = int(os.getenv("OLLAMA_TIMEOUT", "90"))
USE_PADDLE_OCR = os.getenv("USE_PADDLE_OCR", "1").lower() not in ("0", "false", "no")
USE_RAPIDOCR = os.getenv("USE_RAPIDOCR", "1").lower() not in ("0", "false", "no")
DOWNLOAD_ALL_DOCUMENTS = os.getenv("DOWNLOAD_ALL_DOCUMENTS", "0").lower() not in ("0", "false", "no")
DELETE_PDFS_AFTER_ANALYSIS = os.getenv("DELETE_PDFS_AFTER_ANALYSIS", "1").lower() not in ("0", "false", "no")
USE_WEBDRIVER_MANAGER = os.getenv("USE_WEBDRIVER_MANAGER", "0").lower() not in ("0", "false", "no")
USE_GDRIVE_API = os.getenv("USE_GDRIVE_API", "0").lower() not in ("0", "false", "no")
GDRIVE_CREDENTIALS_FILE = os.getenv("GDRIVE_CREDENTIALS_FILE", "").strip()
GDRIVE_TOKEN_FILE = os.getenv("GDRIVE_TOKEN_FILE", os.path.join(OUTPUT_DIR, "gdrive_token.json"))
GDRIVE_ROOT_FOLDER_ID = os.getenv("GDRIVE_ROOT_FOLDER_ID", "").strip()
GDRIVE_ROOT_FOLDER_NAME = os.getenv("GDRIVE_ROOT_FOLDER_NAME", "final_dlr_planning_consents").strip()
GDRIVE_SHARE_ANYONE = os.getenv("GDRIVE_SHARE_ANYONE", "0").lower() not in ("0", "false", "no")
GDRIVE_WIPE_ROOT_BEFORE_RUN = os.getenv("GDRIVE_WIPE_ROOT_BEFORE_RUN", "0").lower() not in ("0", "false", "no")
USE_GSHEETS_API = os.getenv("USE_GSHEETS_API", "0").lower() not in ("0", "false", "no")
GSHEETS_SPREADSHEET_ID = os.getenv("GSHEETS_SPREADSHEET_ID", "1HgfwWdNXRLcTRJ2fGNnORnWkjETRaHsqIN5V32r3VUM").strip()
GSHEETS_TAB_ALL_LEADS = os.getenv("GSHEETS_TAB_ALL_LEADS", "All Leads").strip()
GSHEETS_TOKEN_FILE = os.getenv("GSHEETS_TOKEN_FILE", os.path.join(OUTPUT_DIR, "gsheets_token.json"))
GSHEETS_INCREMENTAL_SYNC = os.getenv("GSHEETS_INCREMENTAL_SYNC", "1").lower() not in ("0", "false", "no")
GSHEETS_CLEAR_ON_START = os.getenv("GSHEETS_CLEAR_ON_START", "1").lower() not in ("0", "false", "no")
ENFORCE_ROW_DATE_CUTOFF = os.getenv("ENFORCE_ROW_DATE_CUTOFF", "0").lower() not in ("0", "false", "no")

TODAY = date.today()

# ── UI dedup: refs already in Google Sheets (passed from Streamlit UI) ──
_YS_EXISTING_REFS = set()
_raw_existing = os.getenv("_YS_EXISTING_REFS", "").strip()
if _raw_existing:
    _YS_EXISTING_REFS = {r.strip() for r in _raw_existing.split(",") if r.strip()}

DLR_AREAS = [
    "Ballinteer", "Ballybrack", "Ballyogan", "Belfield", "Blackrock",
    "Booterstown", "Cabinteely", "Carrickmines", "Cherrywood", "Churchtown",
    "Clonskeagh", "Dalkey", "Deansgrange", "Dundrum", "Dun Laoghaire",
    "Foxrock", "Glasthule", "Glenageary", "Glencullen", "Goatstown",
    "Johnstown", "Killiney", "Kilmacud", "Kilternan", "Leopardstown",
    "Loughlinstown", "Monkstown", "Mount Merrion", "Rathfarnham",
    "Rathmichael", "Sallynoggin", "Sandycove", "Sandyford", "Sandymount",
    "Shankill", "Stepaside", "Stillorgan", "Ticknock", "Windy Arbour",
]

SOUTH_DUBLIN_AREAS = [
    "Ballyboden", "Ballymount", "Bohernabreena", "Brittas", "Citywest",
    "Clondalkin", "Edmondstown", "Firhouse", "Greenhills", "Jobstown",
    "Knocklyon", "Lucan", "Newcastle", "Oldbawn", "Palmerstown",
    "Rathcoole", "Rathfarnham", "Saggart", "Tallaght", "Templeogue",
    "Tymon",
]

FINGAL_AREAS = [
    "Balbriggan", "Baldoyle", "Blanchardstown", "Castleknock", "Clonsilla",
    "Corduff", "Donabate", "Dublin Airport", "Garristown", "Hollystown",
    "Howth", "Kinsealy", "Lusk", "Malahide", "Mulhuddart",
    "Naul", "Ongar", "Portmarnock", "Rush", "Skerries",
    "Sutton", "Swords",
]

SITE_LAYOUT_KEYWORDS = [
    "site layout", "site layout plan", "site plan", "layout plan",
    "proposed site", "existing site", "block plan", "general arrangement",
    "su-", "lp-",
]
DOCUMENT_NAME_KEYWORDS = ["drawing", "layout", "plan"]

EXCLUDE_DOC_KEYWORDS = [
    "newspaper", "report", "drainage", "flood", "bat survey", "ecology", "traffic",
]

# Proposal type classification — checked in priority order
PROPOSAL_TYPE_RULES = [
    ("new_build",      ["construction of", "new dwelling", "new house", "erection of", "new build", "ground-up"]),
    ("extension",      ["extension", "rear extension", "side extension", "first floor extension", "attic conversion", "dormer", "sunroom", "porch"]),
    ("renovation",     ["refurbishment", "renovation", "internal alterations", "change of use", "conversion", "retrofit", "upgrade"]),
    ("heritage",       ["protected structure", "conservation", "restoration", "listed building", "historic"]),
    ("demolition",     ["demolition", "demolish"]),
    ("commercial",     ["office", "retail", "shop", "commercial", "restaurant", "hotel", "industrial"]),
    ("subdivision",    ["subdivision", "divide", "partition"]),
    ("infrastructure", ["road", "drainage", "boundary wall", "driveway", "parking"]),
    ("other",          []),
]

# Regex patterns
ARCHITECT_LABEL_PATTERNS = [
    re.compile(r"architects?\s*[:\-]\s*(.+)", re.I),
    re.compile(r"prepared\s+by\s*[:\-]\s*(.+)", re.I),
    re.compile(r"designed?\s+by\s*[:\-]\s*(.+)", re.I),
    re.compile(r"drawing\s+by\s*[:\-]\s*(.+)", re.I),
]
PRACTICE_HINT = re.compile(
    r"\b([A-Z][A-Za-z&'\-., ]{2,}(?:Architects?|Architecture|Studio|Design|Consultants?|Engineers?|Ltd|Limited|LLP))\b",
    re.I,
)
UPPERCASE_COMPANY_HINT = re.compile(r"^[A-Z][A-Z&'\- ]{3,}$")
ARCHITECTS_WORD_PATTERN = re.compile(r"\b([A-Z][A-Za-z&'\- ]+Architects?)\b", re.I)
EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")
PHONE_PATTERN = re.compile(r"(?:\+353[\s(]?\d[\d\s()\-]{6,}|\b0\d[\d\s()\-]{7,}\d\b)")
WEB_PATTERN = re.compile(r"\b(?:https?://)?(?:www\.)?[A-Za-z0-9.\-]+\.[A-Za-z]{2,}(?:/[^\s]*)?\b", re.I)
CONTACT_LINE_HINT = re.compile(r"(^|\s)(w|e|t|p)\s*[:\-]\s*", re.I)
EXCLUDE_CONTACT_CONTEXT = re.compile(
    r"(site address|project|client|drawing|drawn by|checked by|scale|revision|plot date|application reference|proposal)",
    re.I,
)
GENERIC_ARCHITECT_BAD_PATTERNS = [
    re.compile(r"\bplanning consultant\b", re.I),
    re.compile(r"\barchitectural designer\b", re.I),
    re.compile(r"\bapplicant\b", re.I),
    re.compile(r"\bclient\b", re.I),
]
GENERIC_ARCHITECT_BAD_PATTERNS += [
    re.compile(r"\bonline planning service\b", re.I),
    re.compile(r"\bplanning service\b", re.I),
    re.compile(r"\bunknown\b", re.I),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("dlr_scraper.log")],
)
logger = logging.getLogger(__name__)

_PADDLE_OCR_ENGINE = None
_RAPIDOCR_ENGINE = None
_GDRIVE_SERVICE = None
_GSHEETS_SERVICE = None
_GDRIVE_FOLDER_CACHE = {}
_GSHEETS_CLEARED = False

# ===============================
# DRIVER
# ===============================
def make_driver():
    """Create and return a configured Selenium Chrome WebDriver instance."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(TEMP_DOWNLOAD_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)

    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {
        "download.default_directory": TEMP_DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)

    # First try local chromedriver to avoid long/hanging online lookups.
    local_driver = (
        os.getenv("CHROMEDRIVER_PATH", "").strip()
        or shutil.which("chromedriver")
    )
    if local_driver:
        try:
            logger.info("Starting Chrome with local chromedriver: %s", local_driver)
            service = Service(local_driver)
            driver = webdriver.Chrome(service=service, options=options)
            logger.info("Chrome started using local chromedriver.")
        except Exception as exc:
            logger.warning("Local chromedriver failed (%s). Will try webdriver-manager fallback.", exc)
            driver = None
    else:
        driver = None

    if driver is None and USE_WEBDRIVER_MANAGER:
        try:
            logger.info("Trying webdriver-manager (may be slow if network/DNS is blocked)...")
            candidate = ChromeDriverManager().install()
            logger.info("webdriver-manager candidate: %s", candidate)
            service = Service(candidate)
            driver = webdriver.Chrome(service=service, options=options)
            logger.info("Chrome started via webdriver-manager binary.")
        except Exception as exc:
            logger.warning("webdriver-manager startup failed (%s). Trying Selenium Manager fallback...", exc)
            driver = None
    elif driver is None:
        logger.info("Skipping webdriver-manager (USE_WEBDRIVER_MANAGER=0).")

    # Final fallback: let Selenium Manager resolve driver automatically.
    if driver is None:
        try:
            logger.info("Trying Selenium Manager fallback...")
            driver = webdriver.Chrome(options=options)
            logger.info("Chrome started via Selenium Manager.")
        except Exception as exc:
            logger.error("Could not start Chrome driver: %s", exc)
            logger.error(
                "Set CHROMEDRIVER_PATH to a known-good driver, or install chromedriver on PATH. "
                "Example: brew install --cask chromedriver"
            )
            raise

    driver.execute_cdp_cmd(
        "Browser.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": TEMP_DOWNLOAD_DIR, "eventsEnabled": True},
    )
    return driver


# ===============================
# FOLDER HELPERS
# ===============================
def _safe_slug(name):
    """Convert a name to a safe filesystem slug (letters, digits, underscore, hyphen)."""
    slug = re.sub(r"[^\w\s\-]", "", (name or "unknown").strip())
    slug = re.sub(r"\s+", "_", slug).strip("_")
    return slug or "unknown"


def get_area_dir(area_name):
    """Return (and create) the output folder for a DLR area."""
    d = os.path.join(OUTPUT_DIR, _safe_slug(area_name or "unknown"))
    os.makedirs(d, exist_ok=True)
    return d


def get_app_dir(area_name, reference):
    """Return (and create) the folder for a specific planning application."""
    d = os.path.join(get_area_dir(area_name), _safe_slug(reference or "unknown"))
    os.makedirs(d, exist_ok=True)
    return d


def set_download_dir(driver, path):
    """Update Chrome's download destination directory via CDP."""
    os.makedirs(path, exist_ok=True)
    driver.execute_cdp_cmd(
        "Browser.setDownloadBehavior",
        {"behavior": "allow", "downloadPath": os.path.abspath(path), "eventsEnabled": True},
    )


# ===============================
# GOOGLE DRIVE HELPERS
# ===============================
def _gdrive_enabled():
    """Return True when Google Drive API mode is configured and dependencies are available."""
    return bool(USE_GDRIVE_API and Credentials and InstalledAppFlow and build and MediaFileUpload)


def _get_gdrive_service():
    """Initialise and return a Google Drive API service client."""
    global _GDRIVE_SERVICE
    if _GDRIVE_SERVICE is not None:
        return _GDRIVE_SERVICE
    if not _gdrive_enabled():
        return None
    if not GDRIVE_CREDENTIALS_FILE or not os.path.exists(GDRIVE_CREDENTIALS_FILE):
        logger.warning("GDRIVE_CREDENTIALS_FILE missing or not found; disabling Google Drive uploads.")
        return None
    creds = None
    scopes = ["https://www.googleapis.com/auth/drive"]
    if os.path.exists(GDRIVE_TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(GDRIVE_TOKEN_FILE, scopes)
        except Exception:
            creds = None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(GDRIVE_CREDENTIALS_FILE, scopes)
            creds = flow.run_local_server(port=0)
        os.makedirs(os.path.dirname(GDRIVE_TOKEN_FILE), exist_ok=True)
        with open(GDRIVE_TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
    _GDRIVE_SERVICE = build("drive", "v3", credentials=creds)
    return _GDRIVE_SERVICE


def _gdrive_query_folder(name, parent_id):
    """Return folder id if a folder exists by name under parent_id."""
    service = _get_gdrive_service()
    if service is None:
        return ""
    safe_name = name.replace("'", "\\'")
    q = f"mimeType='application/vnd.google-apps.folder' and trashed=false and name='{safe_name}'"
    if parent_id:
        q += f" and '{parent_id}' in parents"
    resp = service.files().list(
        q=q,
        fields="files(id,name)",
        supportsAllDrives=False,
        pageSize=10,
    ).execute()
    files = resp.get("files", [])
    return files[0]["id"] if files else ""


def _gdrive_create_folder(name, parent_id=""):
    """Create and return a folder id in Google Drive."""
    service = _get_gdrive_service()
    if service is None:
        return ""
    body = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        body["parents"] = [parent_id]
    created = service.files().create(body=body, fields="id").execute()
    folder_id = created.get("id", "")
    if folder_id and GDRIVE_SHARE_ANYONE:
        try:
            service.permissions().create(
                fileId=folder_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()
        except Exception:
            pass
    return folder_id


def _gdrive_get_or_create_folder(name, parent_id=""):
    """Find or create a folder and return its id."""
    cache_key = (parent_id or "root", name)
    if cache_key in _GDRIVE_FOLDER_CACHE:
        return _GDRIVE_FOLDER_CACHE[cache_key]
    folder_id = _gdrive_query_folder(name, parent_id)
    if not folder_id:
        folder_id = _gdrive_create_folder(name, parent_id)
    _GDRIVE_FOLDER_CACHE[cache_key] = folder_id
    return folder_id


def get_gdrive_app_folder(area_name, reference):
    """Return (folder_id, web_link) for an application folder area/reference in Drive."""
    if not _gdrive_enabled():
        return "", ""
    root_id = GDRIVE_ROOT_FOLDER_ID or _gdrive_get_or_create_folder(GDRIVE_ROOT_FOLDER_NAME, "")
    if not root_id:
        return "", ""
    area_id = _gdrive_get_or_create_folder(_safe_slug(area_name or "unknown"), root_id)
    if not area_id:
        return "", ""
    ref_id = _gdrive_get_or_create_folder(_safe_slug(reference or "unknown"), area_id)
    if not ref_id:
        return "", ""
    return ref_id, f"https://drive.google.com/drive/folders/{ref_id}"


def upload_file_to_gdrive(local_path, parent_folder_id, remote_name=""):
    """Upload a local file to Google Drive folder; returns uploaded file id or empty string."""
    service = _get_gdrive_service()
    if service is None or not parent_folder_id or not os.path.exists(local_path):
        return ""
    body = {
        "name": remote_name or os.path.basename(local_path),
        "parents": [parent_folder_id],
    }
    media = MediaFileUpload(local_path, resumable=False)
    created = service.files().create(body=body, media_body=media, fields="id").execute()
    return created.get("id", "")


def wipe_gdrive_root_folder_contents():
    """Delete all direct children under configured Drive root folder."""
    if not _gdrive_enabled():
        return
    service = _get_gdrive_service()
    if service is None:
        return
    root_id = GDRIVE_ROOT_FOLDER_ID or _gdrive_get_or_create_folder(GDRIVE_ROOT_FOLDER_NAME, "")
    if not root_id:
        logger.warning("Could not resolve Drive root folder for wipe.")
        return
    page_token = None
    deleted = 0
    while True:
        resp = service.files().list(
            q=f"'{root_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
            pageSize=200,
            pageToken=page_token,
        ).execute()
        for f in resp.get("files", []):
            try:
                service.files().delete(fileId=f["id"]).execute()
                deleted += 1
            except Exception as exc:
                logger.warning("Could not delete Drive item %s: %s", f.get("name", ""), exc)
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    logger.info("Drive wipe complete: deleted %s item(s) from root folder.", deleted)


def _gsheets_enabled():
    """Return True when Google Sheets API mode is configured and dependencies are available."""
    return bool(USE_GSHEETS_API and Credentials and InstalledAppFlow and build and Request)


def _get_gsheets_service():
    """Initialise and return a Google Sheets API service client."""
    global _GSHEETS_SERVICE
    if _GSHEETS_SERVICE is not None:
        return _GSHEETS_SERVICE
    if not _gsheets_enabled():
        return None
    if not GDRIVE_CREDENTIALS_FILE or not os.path.exists(GDRIVE_CREDENTIALS_FILE):
        logger.warning("GDRIVE_CREDENTIALS_FILE missing or not found; disabling Google Sheets sync.")
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = None
    if os.path.exists(GSHEETS_TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(GSHEETS_TOKEN_FILE, scopes)
        except Exception:
            creds = None
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception:
                creds = None
        if not creds:
            flow = InstalledAppFlow.from_client_secrets_file(GDRIVE_CREDENTIALS_FILE, scopes)
            creds = flow.run_local_server(port=0)
        os.makedirs(os.path.dirname(GSHEETS_TOKEN_FILE), exist_ok=True)
        with open(GSHEETS_TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
    _GSHEETS_SERVICE = build("sheets", "v4", credentials=creds)
    return _GSHEETS_SERVICE


def sync_all_leads_to_google_sheet(df_all):
    """Replace data rows in the All Leads tab with current run output."""
    service = _get_gsheets_service()
    if service is None:
        return False
    if not GSHEETS_SPREADSHEET_ID:
        logger.warning("GSHEETS_SPREADSHEET_ID is empty; skipping Google Sheets sync.")
        return False

    # Final required columns (exact order):
    # Reference, Area, Site Address, Applicant Name, Proposal,
    # Decision Date, Registration Date, Lead Score,
    # Architect Name, Architect Contact, Files, Detail URL
    values = []
    for _, row in df_all.iterrows():
        values.append([
            str(row.get("reference", "") or ""),
            str(row.get("search_area", "") or ""),
            str(row.get("site_address", "") or ""),
            str(row.get("applicant_name", "") or ""),
            str(row.get("proposal", "") or ""),
            str(row.get("decision_date", "") or ""),
            str(row.get("registration_date", "") or ""),
            str(row.get("lead_score", "") or ""),
            str(row.get("architect_name", "") or ""),
            str(row.get("architect_contact_details", "") or ""),
            str(row.get("google_drive_folder_link", "") or ""),
            str(row.get("detail_url", "") or ""),
        ])

    clear_range = f"'{GSHEETS_TAB_ALL_LEADS}'!A4:L20000"
    write_range = f"'{GSHEETS_TAB_ALL_LEADS}'!A4"
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=GSHEETS_SPREADSHEET_ID,
            range=clear_range,
            body={},
        ).execute()
        if values:
            service.spreadsheets().values().update(
                spreadsheetId=GSHEETS_SPREADSHEET_ID,
                range=write_range,
                valueInputOption="RAW",
                body={"values": values},
            ).execute()
        logger.info("Google Sheets sync complete: %s rows -> %s (%s)", len(values), GSHEETS_TAB_ALL_LEADS, GSHEETS_SPREADSHEET_ID)
        return True
    except Exception as exc:
        logger.warning("Google Sheets sync failed: %s", exc)
        return False


def _rows_for_gsheet(df):
    """Map dataframe rows to the configured Google Sheet column order."""
    values = []
    for _, row in df.iterrows():
        values.append([
            str(row.get("reference", "") or ""),
            str(row.get("search_area", "") or ""),
            str(row.get("site_address", "") or ""),
            str(row.get("applicant_name", "") or ""),
            str(row.get("proposal", "") or ""),
            str(row.get("decision_date", "") or ""),
            str(row.get("registration_date", "") or ""),
            str(row.get("lead_score", "") or ""),
            str(row.get("architect_name", "") or ""),
            str(row.get("architect_contact_details", "") or ""),
            str(row.get("google_drive_folder_link", "") or ""),
            str(row.get("detail_url", "") or ""),
        ])
    return values


def clear_google_sheet_all_leads_data():
    """Clear data rows (not headers) in the All Leads tab."""
    global _GSHEETS_CLEARED
    service = _get_gsheets_service()
    if service is None or not GSHEETS_SPREADSHEET_ID:
        return False
    clear_range = f"'{GSHEETS_TAB_ALL_LEADS}'!A4:L20000"
    try:
        service.spreadsheets().values().clear(
            spreadsheetId=GSHEETS_SPREADSHEET_ID,
            range=clear_range,
            body={},
        ).execute()
        _GSHEETS_CLEARED = True
        logger.info("Google Sheets cleared: %s", clear_range)
        return True
    except Exception as exc:
        logger.warning("Google Sheets clear failed: %s", exc)
        return False


def append_rows_to_google_sheet(df):
    """Append rows to the All Leads tab, starting after existing content."""
    service = _get_gsheets_service()
    if service is None or not GSHEETS_SPREADSHEET_ID:
        return False
    values = _rows_for_gsheet(df)
    if not values:
        return True
    try:
        service.spreadsheets().values().append(
            spreadsheetId=GSHEETS_SPREADSHEET_ID,
            range=f"'{GSHEETS_TAB_ALL_LEADS}'!A4:L",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": values},
        ).execute()
        logger.info("Google Sheets append complete: %s rows -> %s", len(values), GSHEETS_TAB_ALL_LEADS)
        return True
    except Exception as exc:
        logger.warning("Google Sheets append failed: %s", exc)
        return False


# ===============================
# UTILITIES
# ===============================
def js_click(driver, elem):
    """Scroll element into view and click via JavaScript to bypass overlays."""
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
    time.sleep(0.2)
    driver.execute_script("arguments[0].click();", elem)


def click_clickable_with_retry(driver, wait, by, locator, label, attempts=5):
    """Click a locator with stale-element retries; returns True on success."""
    for attempt in range(1, attempts + 1):
        try:
            elem = wait.until(EC.element_to_be_clickable((by, locator)))
            js_click(driver, elem)
            return True
        except StaleElementReferenceException:
            logger.warning(
                "Stale element on %s click (attempt %s/%s). Retrying...",
                label, attempt, attempts
            )
            time.sleep(0.5)
        except Exception as exc:
            if attempt >= attempts:
                logger.warning(
                    "Could not click %s after %s attempts: %s",
                    label, attempts, exc
                )
                return False
            time.sleep(0.5)
    return False


def click_xpath_js_with_retry(driver, xpath, label, attempts=6):
    """Click an element resolved in-page by XPath each attempt to avoid stale references."""
    for attempt in range(1, attempts + 1):
        try:
            clicked = driver.execute_script(
                """
                const xp = arguments[0];
                const node = document.evaluate(
                    xp, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null
                ).singleNodeValue;
                if (!node) return false;
                node.scrollIntoView({block: "center"});
                node.click();
                return true;
                """,
                xpath,
            )
            if clicked:
                return True
        except Exception:
            pass
        logger.warning("Retrying %s click via JS XPath (attempt %s/%s)...", label, attempt, attempts)
        time.sleep(0.5)
    return False


def dump_debug(driver, name):
    """Save a debug screenshot and page HTML to the debug directory."""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        png = os.path.join(DEBUG_DIR, f"{ts}_{name}.png")
        html = os.path.join(DEBUG_DIR, f"{ts}_{name}.html")
        driver.save_screenshot(png)
        with open(html, "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        logger.info("Debug dump: %s", png)
    except Exception as exc:
        logger.warning("Debug dump failed at %s: %s", name, exc)


def dismiss_overlays(driver):
    """Dismiss cookie banners and modal overlays via button click and DOM removal."""
    try:
        for xpath in [
            "//button[contains(.,'Accept')]",
            "//button[contains(.,'OK')]",
            "//button[contains(.,'Agree')]",
        ]:
            buttons = driver.find_elements(By.XPATH, xpath)
            if buttons:
                js_click(driver, buttons[0])
                time.sleep(0.5)
                break
    except Exception:
        pass
    try:
        driver.execute_script(
            """
            document.querySelectorAll('.sas-cookie-consent-overlay,[class*="cookie"]').forEach(e => e.remove());
            document.body.style.overflow='auto';
            """
        )
    except Exception:
        pass


def clean_name(value):
    """Strip noise from extracted name strings and return a clean value."""
    value = re.sub(r"\s+", " ", value or "")
    value = re.sub(r"\b(scale|date|drawing no|rev|project)\b.*$", "", value, flags=re.I)
    value = value.strip(" :-;,.\t")
    if len(value) < 3:
        return ""
    if re.search(r"\d{4,}", value):
        return ""
    return value


def is_likely_architect_name(value):
    """Return True if the cleaned value looks like a legitimate architect/practice name."""
    val = clean_name(value)
    if not val:
        return False
    for pat in GENERIC_ARCHITECT_BAD_PATTERNS:
        if pat.search(val):
            return False
    return True


def wait_for_new_pdf(before_set, download_dir=None, timeout=75):
    """Poll download_dir until a new PDF appears and stabilises, then return its path."""
    dl_dir = download_dir or TEMP_DOWNLOAD_DIR
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = [f for f in os.listdir(dl_dir) if f.lower().endswith(".pdf")]
        active = [f for f in os.listdir(dl_dir) if f.endswith(".crdownload")]
        new_files = [f for f in files if f not in before_set]
        if new_files and not active:
            new_files.sort(key=lambda f: os.path.getmtime(os.path.join(dl_dir, f)))
            newest = os.path.join(dl_dir, new_files[-1])
            size1 = os.path.getsize(newest)
            time.sleep(1)
            size2 = os.path.getsize(newest)
            if size1 == size2 and size2 > 0:
                return newest
        time.sleep(0.5)
    return None


def cleanup_downloaded_docs(docs, reference=""):
    """Delete downloaded document files after analysis; returns number deleted."""
    deleted = 0
    seen = set()
    for doc_path, _ in docs or []:
        if not doc_path or doc_path in seen:
            continue
        seen.add(doc_path)
        try:
            if os.path.exists(doc_path):
                os.remove(doc_path)
                deleted += 1
        except Exception as exc:
            logger.warning("[%s] Could not delete analyzed PDF %s: %s", reference or "n/a", doc_path, exc)
    return deleted


# ===============================
# OCR ENGINE HELPERS
# ===============================
def _get_paddle_ocr_engine():
    """Lazily initialise and cache the PaddleOCR engine, returning None on failure."""
    global _PADDLE_OCR_ENGINE
    if not USE_PADDLE_OCR:
        return None
    if _PADDLE_OCR_ENGINE is not None:
        return _PADDLE_OCR_ENGINE
    try:
        from paddleocr import PaddleOCR
        _PADDLE_OCR_ENGINE = PaddleOCR(use_angle_cls=True, lang="en")
        return _PADDLE_OCR_ENGINE
    except Exception as exc:
        logger.warning("PaddleOCR init failed, using Tesseract fallback: %s", exc)
        _PADDLE_OCR_ENGINE = None
        return None


def _get_rapidocr_engine():
    """Lazily initialise and cache the RapidOCR engine, returning None on failure."""
    global _RAPIDOCR_ENGINE
    if not USE_RAPIDOCR:
        return None
    if _RAPIDOCR_ENGINE is not None:
        return _RAPIDOCR_ENGINE
    try:
        from rapidocr_onnxruntime import RapidOCR
        _RAPIDOCR_ENGINE = RapidOCR()
        return _RAPIDOCR_ENGINE
    except Exception as exc:
        logger.warning("RapidOCR init failed: %s", exc)
        _RAPIDOCR_ENGINE = None
        return None


def _ocr_image_with_paddle(img):
    """OCR a PIL image using RapidOCR -> PaddleOCR -> Tesseract fallback chain."""
    ocr = _get_rapidocr_engine()
    if ocr is not None:
        try:
            import numpy as np
            arr = np.array(img.convert("RGB"))
            result, _ = ocr(arr)
            if result:
                return "\n".join([str(x[1]) for x in result if len(x) >= 2])
        except Exception as exc:
            logger.warning("RapidOCR run failed, trying Paddle/Tesseract: %s", exc)

    ocr = _get_paddle_ocr_engine()
    if ocr is None:
        return ""
    try:
        import numpy as np
        arr = np.array(img.convert("RGB"))
        result = ocr.ocr(arr, cls=True)
        lines = []
        for block in result or []:
            for item in block or []:
                if isinstance(item, (list, tuple)) and len(item) >= 2:
                    text_part = item[1]
                    if isinstance(text_part, (list, tuple)) and text_part:
                        lines.append(str(text_part[0]))
        return "\n".join(lines)
    except Exception as exc:
        logger.warning("PaddleOCR run failed, using Tesseract fallback: %s", exc)
        return ""


def _extract_text_from_pdf(pdf_path):
    """Extract native digital text from the first page of a PDF via pdfplumber."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            if pdf.pages:
                return (pdf.pages[0].extract_text() or "").strip()
    except Exception:
        return ""
    return ""


def _ocr_title_block(pdf_path):
    """OCR the bottom-right title block region of the first PDF page."""
    try:
        pages = convert_from_path(pdf_path, dpi=230, first_page=1, last_page=1)
        if not pages:
            return ""
        img = pages[0]
        w, h = img.size
        crop = img.crop((int(w * 0.62), int(h * 0.58), w, h))
        paddle_text = _ocr_image_with_paddle(crop)
        if paddle_text.strip():
            return paddle_text
        return pytesseract.image_to_string(crop)
    except Exception:
        return ""


def _ocr_full_page(pdf_path):
    """OCR the entire first page of a PDF at reduced DPI."""
    try:
        pages = convert_from_path(pdf_path, dpi=180, first_page=1, last_page=1)
        if not pages:
            return ""
        img = pages[0]
        paddle_text = _ocr_image_with_paddle(img)
        if paddle_text.strip():
            return paddle_text
        return pytesseract.image_to_string(img)
    except Exception:
        return ""


# ===============================
# ARCHITECT EXTRACTION
# ===============================
def _parse_architect(text):
    """Parse an architect/practice name from text using label patterns and name heuristics."""
    if not text:
        return ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    for line in lines:
        for pattern in ARCHITECT_LABEL_PATTERNS:
            m = pattern.search(line)
            if m:
                candidate = clean_name(m.group(1))
                if is_likely_architect_name(candidate):
                    return candidate
    for line in lines:
        m = PRACTICE_HINT.search(line)
        if m:
            candidate = clean_name(m.group(1))
            if is_likely_architect_name(candidate):
                return candidate
        m2 = ARCHITECTS_WORD_PATTERN.search(line)
        if m2:
            candidate = clean_name(m2.group(1))
            if is_likely_architect_name(candidate):
                return candidate
    for idx, line in enumerate(lines):
        if UPPERCASE_COMPANY_HINT.match(line) and not re.search(r"(NORTH|LEGEND|SCALE|SITE)", line):
            candidate = clean_name(line.title())
            if is_likely_architect_name(candidate):
                return candidate
        if "architect" in line.lower() and idx + 1 < len(lines):
            candidate = clean_name(lines[idx + 1])
            if is_likely_architect_name(candidate):
                return candidate
    return ""


def _find_architect_line_index(lines, architect_name):
    """Return the index of the line most likely containing the architect name."""
    if not lines:
        return -1
    if architect_name:
        arch_lower = architect_name.lower()
        for i, line in enumerate(lines):
            if arch_lower in line.lower():
                return i
    for i, line in enumerate(lines):
        if "architect" in line.lower():
            return i
    return -1


def _extract_architect_contact_from_text(text, architect_name=""):
    """Extract email, phone, and website contact details near the architect name in text."""
    if not text:
        return ""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return ""
    idx = _find_architect_line_index(lines, architect_name)
    window = lines
    if idx >= 0:
        start = max(0, idx - 4)
        end = min(len(lines), idx + 10)
        window = lines[start:end]
    emails = []
    phones = []
    webs = []
    for line in window:
        if EXCLUDE_CONTACT_CONTEXT.search(line):
            continue
        if not (
            CONTACT_LINE_HINT.search(line)
            or EMAIL_PATTERN.search(line)
            or PHONE_PATTERN.search(line)
            or ("www." in line.lower())
            or ("http" in line.lower())
        ):
            continue
        for e in EMAIL_PATTERN.findall(line):
            if e not in emails:
                emails.append(e)
        for p in PHONE_PATTERN.findall(line):
            val = re.sub(r"\s+", " ", p).strip()
            if val not in phones:
                phones.append(val)
        for w in WEB_PATTERN.findall(line):
            lw = w.lower()
            if "@" in lw or lw.endswith(".pdf"):
                continue
            if lw not in [x.lower() for x in webs]:
                webs.append(w)
    parts = []
    if emails:
        parts.append("email=" + ", ".join(emails[:2]))
    if phones:
        parts.append("phone=" + ", ".join(phones[:2]))
    if webs:
        parts.append("web=" + ", ".join(webs[:2]))
    return " | ".join(parts)


def _appears_in_source(candidate, source_text):
    """Return True if a candidate string appears (loosely) in source text."""
    if not candidate or not source_text:
        return False
    c = candidate.lower().strip()
    s = source_text.lower()
    if c in s:
        return True
    c_alnum = re.sub(r"[^a-z0-9]", "", c)
    s_alnum = re.sub(r"[^a-z0-9]", "", s)
    return bool(c_alnum and c_alnum in s_alnum)


def _normalize_contact_json(data, source_text=""):
    """Validate and normalise architect contact data from an LLM JSON dict."""
    if not isinstance(data, dict):
        return "", ""
    arch = clean_name(str(data.get("architect_name", "") or ""))
    if arch and not is_likely_architect_name(arch):
        arch = ""
    email = ""
    phone = ""
    website = ""
    em = str(data.get("architect_email", "") or "")
    ph = str(data.get("architect_phone", "") or "")
    wb = str(data.get("architect_website", "") or "")
    m_em = EMAIL_PATTERN.search(em)
    m_ph = PHONE_PATTERN.search(ph)
    m_wb = WEB_PATTERN.search(wb)
    if m_em:
        cand = m_em.group(0)
        if _appears_in_source(cand, source_text):
            email = cand
    if m_ph:
        cand = re.sub(r"\s+", " ", m_ph.group(0)).strip()
        if _appears_in_source(cand, source_text):
            phone = cand
    if m_wb:
        w = m_wb.group(0)
        if "@" not in w and not w.lower().endswith(".pdf") and _appears_in_source(w, source_text):
            website = w
    parts = []
    if email:
        parts.append(f"email={email}")
    if phone:
        parts.append(f"phone={phone}")
    if website:
        parts.append(f"web={website}")
    return arch, " | ".join(parts)


def _extract_json_object(text):
    """Extract the first JSON object from a raw text string."""
    if not text:
        return None
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        snippet = text[start:end + 1]
        try:
            return json.loads(snippet)
        except Exception:
            return None
    return None


def _ollama_extract_architect(text):
    """Send text to Ollama LLM and return (architect_name, contact_string) from JSON response."""
    if not USE_OLLAMA:
        return "", ""
    if not text or len(text.strip()) < 60:
        return "", ""
    prompt = (
        "Extract architect details from this planning drawing text. "
        "Return JSON only with keys: architect_name, architect_email, architect_phone, architect_website, confidence. "
        "Use only the architect/practice block. Ignore project/site address/client/drawing metadata.\n\n"
        f"TEXT:\n{text[:12000]}"
    )
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.0},
    }
    try:
        req = request.Request(
            OLLAMA_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with request.urlopen(req, timeout=OLLAMA_TIMEOUT) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
        top = _extract_json_object(raw)
        if not isinstance(top, dict):
            return "", ""
        body = top.get("response", "")
        data = _extract_json_object(body) if isinstance(body, str) else body
        arch, contact = _normalize_contact_json(data if isinstance(data, dict) else {}, source_text=text)
        return arch, contact
    except error.URLError:
        return "", ""
    except Exception:
        return "", ""


def extract_architect_and_contact(pdf_path):
    """Extract architect name and contact from a PDF using text extraction, OCR, and LLM."""
    text = _extract_text_from_pdf(pdf_path)
    llm_arch, llm_contact = _ollama_extract_architect(text)
    arch = llm_arch or _parse_architect(text)
    contact = llm_contact or _extract_architect_contact_from_text(text, arch)
    if arch and contact:
        method = "ollama_pdf_text" if llm_arch or llm_contact else "pdf_text"
        return arch, contact, method
    ocr_text = _ocr_title_block(pdf_path)
    ocr_llm_arch, ocr_llm_contact = _ollama_extract_architect(ocr_text)
    ocr_arch = ocr_llm_arch or _parse_architect(ocr_text)
    ocr_contact = ocr_llm_contact or _extract_architect_contact_from_text(ocr_text, arch or ocr_arch)
    final_arch = arch or ocr_arch
    final_contact = contact or ocr_contact
    if final_arch and not final_contact:
        full_ocr_text = _ocr_full_page(pdf_path)
        full_ocr_llm_arch, full_ocr_llm_contact = _ollama_extract_architect(full_ocr_text)
        final_arch = final_arch or full_ocr_llm_arch
        final_contact = full_ocr_llm_contact or _extract_architect_contact_from_text(full_ocr_text, final_arch)
    if final_arch or final_contact:
        if llm_arch or llm_contact:
            method = "ollama_pdf_text"
        elif ocr_llm_arch or ocr_llm_contact:
            method = "ollama_ocr"
        else:
            method = "pdf_text" if arch else "ocr"
        return final_arch, final_contact, method
    return "", "", ""


# ===============================
# PROPOSAL CLASSIFICATION
# ===============================
def classify_proposal(proposal_text):
    """Classify a proposal into a type string and return matched keyword list."""
    text_lower = (proposal_text or "").lower()
    for ptype, keywords in PROPOSAL_TYPE_RULES:
        if ptype == "other":
            return "other", ""
        hits = [kw for kw in keywords if kw in text_lower]
        if hits:
            return ptype, ", ".join(hits)
    return "other", ""


def extract_scale(proposal_text):
    """Estimate project scale (Small/Medium/Large/Unknown) from m² mentions in proposal."""
    text = proposal_text or ""
    matches = re.findall(r"(\d+(?:\.\d+)?)\s*(?:sq\.?\s*m|m²|sqm|square\s+met)", text, re.I)
    if not matches:
        return "Unknown"
    total = sum(float(m) for m in matches)
    if total < 50:
        return "Small"
    elif total <= 200:
        return "Medium"
    else:
        return "Large"


def extract_num_units(proposal_text):
    """Extract the number of residential units mentioned in the proposal text."""
    text = proposal_text or ""
    patterns = [
        r"(\d+)\s*no\.?\s*(?:dwelling\s+units?|dwellings?|apartments?|units?)",
        r"(\d+)\s*(?:new\s+)?(?:dwelling\s+units?|dwellings?|apartments?)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return int(m.group(1))
    return 0


def extract_storeys(proposal_text):
    """Extract the number of storeys mentioned in the proposal text."""
    text = proposal_text or ""
    m = re.search(r"(\d+)[\s-]*(?:storey|story|floor|no\.?\s*storeys?)", text, re.I)
    if m:
        return int(m.group(1))
    word_map = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5}
    m2 = re.search(r"\b(one|two|three|four|five)[\s-]*storey\b", text, re.I)
    if m2:
        return word_map.get(m2.group(1).lower(), 0)
    return 0


def classify_area_type(proposal_text):
    """Return the area type (Residential/Commercial/Heritage/Mixed) from proposal text."""
    text = (proposal_text or "").lower()
    is_commercial = any(k in text for k in ["office", "retail", "shop", "commercial", "restaurant", "hotel", "industrial"])
    is_heritage = any(k in text for k in ["protected structure", "conservation", "listed", "historic"])
    is_residential = any(k in text for k in ["dwelling", "house", "apartment", "residential", "extension", "bedroom"])
    if is_heritage:
        return "Heritage/Protected Structure"
    if is_commercial and is_residential:
        return "Mixed"
    if is_commercial:
        return "Commercial"
    return "Residential"


def is_protected_structure(proposal_text):
    """Return True if the proposal mentions a protected structure or conservation area."""
    text = (proposal_text or "").lower()
    return any(k in text for k in ["protected structure", "conservation area", "listed building", "architectural conservation"])


def has_contractor_mentioned(combined_text):
    """Return True if the combined text suggests a contractor has already been identified."""
    text = (combined_text or "").lower()
    return bool(re.search(r"\b(contractor|main contractor|builder identified|appointed builder)\b", text))


# ===============================
# DATE UTILITIES
# ===============================
def _parse_date(date_value):
    """Parse a date string into a date object, trying multiple common formats."""
    if not date_value:
        return None
    for fmt in ("%d %b %Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%B %d, %Y"):
        try:
            return datetime.strptime(str(date_value).strip(), fmt).date()
        except Exception:
            continue
    return None


def _days_since(date_obj):
    """Return integer days between a date object and today (TODAY global)."""
    if not date_obj:
        return None
    try:
        return (TODAY - date_obj).days
    except Exception:
        return None


def _reference_family(ref):
    """Strip amendment suffixes (e.g. /C1, /C2) to get the base reference family."""
    ref = (ref or "").strip().upper()
    return re.sub(r"/C\d+$", "", ref)


def _amendment_rank(ref):
    """Return the amendment integer rank (0 for originals, 1+ for /C1 etc)."""
    ref = (ref or "").upper()
    m = re.search(r"/C(\d+)$", ref)
    if m:
        return int(m.group(1))
    return 0


# ===============================
# LEAD SCORING
# ===============================
def compute_lead_score(rec):
    """Compute a lead quality score from 0 to 100 based on application attributes."""
    score = 0
    days = rec.get("days_since_decision")
    proposal_type = rec.get("proposal_type", "")

    if isinstance(days, (int, float)):
        if 1 <= days <= 45:
            score += 15
        elif 46 <= days <= 90:
            score += 10
    if proposal_type in ("extension", "new_build", "renovation"):
        score += 20
    if proposal_type == "heritage":
        score += 15
    if rec.get("is_protected_structure"):
        score += 10
    num_units = rec.get("num_units", 0) or 0
    if num_units >= 2:
        score += 10
    if (rec.get("applicant_name") or "").strip():
        score += 5
    if rec.get("has_contractor_identified"):
        score -= 20

    return max(0, min(100, score))


def compute_lead_tier(score):
    """Return the lead tier label for a given 0-100 lead score."""
    if score >= 70:
        return "Tier 1 - Call This Week"
    elif score >= 45:
        return "Tier 2 - Send Letter"
    elif score >= 20:
        return "Tier 3 - Nurture"
    else:
        return "Skip"


def compute_outreach_channel(rec):
    """Recommend the best first-contact outreach channel based on available lead data."""
    tier = rec.get("lead_tier", "")
    has_address = bool((rec.get("applicant_address") or "").strip())
    has_arch_email = "email=" in (rec.get("architect_contact_details") or "")

    if has_address and "Tier 1" in tier:
        return "Phone + Letter"
    elif has_address:
        return "Letter"
    elif has_arch_email:
        return "Email via Architect"
    elif "Tier 1" in tier:
        return "Cold Visit"
    else:
        return "Letter"


def compute_talking_point(rec):
    """Generate a personalised one-sentence outreach opener for the salesperson."""
    ptype = rec.get("proposal_type", "other")
    site = rec.get("site_address") or rec.get("address", "the site")
    area = rec.get("search_area", "your area")
    templates = {
        "extension":      f"We saw your planning permission for the extension at {site} and we specialise in exactly this type of work in {area}.",
        "new_build":      f"Congratulations on your planning permission for {site} — we'd love to quote on the build.",
        "renovation":     f"We noticed your planning permission for works at {site} and have a strong track record on similar renovation projects in {area}.",
        "heritage":       f"We have specific experience with protected structures and conservation-compliant builds — we noticed your application at {site}.",
        "demolition":     f"We saw your planning permission at {site} and we handle full demolition-to-rebuild projects in {area}.",
        "commercial":     f"We noticed your commercial development application at {site} — Yellow Stag Services would be delighted to discuss the construction works.",
        "subdivision":    f"We saw your planning permission at {site} — we handle subdivision and multi-unit residential builds across {area}.",
        "infrastructure": f"We noticed your planning permission at {site} and we cover groundworks and infrastructure projects in {area}.",
        "other":          f"We noticed your recent planning permission at {site} and wanted to reach out about the construction works.",
    }
    return templates.get(ptype, templates["other"])


def compute_urgency_flag(rec):
    """Return an urgency label based on days elapsed since the decision date."""
    days = rec.get("days_since_decision")
    if not isinstance(days, (int, float)):
        return "Unknown"
    if days <= 29:
        return f"Active - {days} days since decision"
    elif days <= 45:
        remaining = 60 - days
        return f"URGENT: Act within {remaining} days"
    elif days <= 90:
        return f"Cooling - {days} days since grant"
    else:
        return f"Likely contracted - {days} days ago"


# ===============================
# DETAIL PAGE SCRAPING
# ===============================
def _load_detail_page_with_retry(driver, url, max_retries=3):
    """Load a URL with exponential backoff retry, returning True on success."""
    for attempt in range(1, max_retries + 1):
        try:
            driver.get(url)
            time.sleep(1.5)
            return True
        except Exception as exc:
            wait_time = 2 ** attempt
            logger.warning("Detail page load attempt %s failed (%s), retrying in %ss...", attempt, exc, wait_time)
            time.sleep(wait_time)
    logger.error("Failed to load detail page after %s retries: %s", max_retries, url)
    return False


def scrape_detail_page(driver, wait, detail_url, reference):
    """Scrape applicant name/address, decision status/date from the application detail page."""
    result = {
        "applicant_name": "",
        "applicant_address": "",
        "proposal_full": "",
        "decision_status": "",
        "decision_date": "",
        "has_contractor_identified": False,
    }

    if not _load_detail_page_with_retry(driver, detail_url):
        return result

    dump_debug(driver, f"detail_{reference.replace('/', '_')}")
    dismiss_overlays(driver)

    try:
        def _looks_like_label_text(v):
            s = re.sub(r"\s+", " ", (v or "").strip()).lower()
            if not s:
                return True
            label_bits = [
                "applicants name",
                "applicant name",
                "proposal description",
                "description of development",
            ]
            # Pure/near-pure labels like "Applicants name *"
            return any(bit in s for bit in label_bits) and len(s) <= 80

        def _read_sas_value(sas_id):
            try:
                return (
                    driver.execute_script(
                        """
                        const id = arguments[0];
                        // Prefer concrete controls, not wrapper custom elements.
                        const controls = document.querySelectorAll(`input[sas-id="${id}"], textarea[sas-id="${id}"]`);
                        for (const c of controls) {
                            const v = (c.value || c.getAttribute("value") || "").trim();
                            if (v) return v;
                        }

                        const wrappers = document.querySelectorAll(
                            `[sas-id="${id}"], sas-input-text[sas-id="${id}"], sas-textarea[sas-id="${id}"]`
                        );
                        for (const w of wrappers) {
                            // Some values are rendered as plain non-editable divs.
                            const divs = w.querySelectorAll('div[contenteditable="false"]');
                            for (const d of divs) {
                                const t = (d.textContent || "").trim();
                                if (t) return t;
                            }
                        }
                        return "";
                        """,
                        sas_id,
                    )
                    or ""
                ).strip()
            except Exception:
                return ""

        def _is_bad_name_value(v):
            s = (v or "").strip().lower()
            if not s or len(s) < 3:
                return True
            bad_tokens = [
                "registered date", "registration date", "decision date", "decision status",
                "proposal", "description", "site address", "application reference",
            ]
            if any(t in s for t in bad_tokens):
                return True
            return not bool(re.search(r"[a-z]", s))

        # Highest-priority extraction: exact known sas-id fields from this portal.
        direct_name = _read_sas_value("applicantSurname")
        if direct_name and not _is_bad_name_value(direct_name) and not _looks_like_label_text(direct_name):
            result["applicant_name"] = direct_name
        direct_proposal = _read_sas_value("fullProposal")
        if direct_proposal and len(direct_proposal) > 20 and not _looks_like_label_text(direct_proposal):
            result["proposal_full"] = re.sub(r"\s+", " ", direct_proposal).strip()
        direct_decision_date = _read_sas_value("decisionDate")
        if direct_decision_date and len(direct_decision_date) >= 6:
            result["decision_date"] = direct_decision_date

        # Attempt Angular scope extraction first
        try:
            scope_data = driver.execute_script(
                "try { var s = angular.element(document.querySelector('[ng-controller]')).scope(); "
                "return JSON.stringify(s.application || s.app || s.data || null); } catch(e) { return null; }"
            )
            if scope_data:
                data = json.loads(scope_data)
                if isinstance(data, dict):
                    for k, v in data.items():
                        kl = k.lower()
                        sval = str(v or "").strip()
                        if (
                            "applicant" in kl and "name" in kl
                            and not result["applicant_name"]
                            and not _is_bad_name_value(sval)
                            and not _looks_like_label_text(sval)
                        ):
                            result["applicant_name"] = str(v or "").strip()
                        elif "applicant" in kl and "address" in kl and not result["applicant_address"]:
                            result["applicant_address"] = str(v or "").strip()
                        elif any(x in kl for x in ["proposal", "description", "development"]) and not result["proposal_full"]:
                            if len(sval) > 20 and not _looks_like_label_text(sval):
                                result["proposal_full"] = sval
                        elif "decision" in kl and "status" in kl and not result["decision_status"]:
                            result["decision_status"] = str(v or "").strip()
                        elif "decision" in kl and "date" in kl and not result["decision_date"]:
                            result["decision_date"] = str(v or "").strip()
        except Exception:
            pass

        # Build full page text for regex fallback
        all_text = []
        page_pairs = {}
        page_pairs_multi = {}

        try:
            dts = driver.find_elements(By.XPATH, "//dt | //th | //label | //strong")
            for dt in dts:
                label = (dt.text or "").strip().lower()
                try:
                    sibling = dt.find_element(By.XPATH, "following-sibling::dd[1] | following-sibling::td[1]")
                    value = (sibling.text or "").strip()
                except Exception:
                    value = ""
                if value:
                    page_pairs[label] = value
                    page_pairs_multi.setdefault(label, []).append(value)
                    all_text.append(f"{dt.text}: {value}")
        except Exception:
            pass

        # Parse table rows directly (most reliable on this portal)
        try:
            for tr in driver.find_elements(By.XPATH, "//tr"):
                cells = tr.find_elements(By.XPATH, "./th|./td")
                if len(cells) < 2:
                    continue
                label = (cells[0].text or "").strip().lower()
                value = " ".join((cells[1].text or "").split()).strip()
                if not label or not value:
                    continue
                page_pairs[label] = value
                page_pairs_multi.setdefault(label, []).append(value)
                all_text.append(f"{cells[0].text}: {value}")
        except Exception:
            pass

        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text or ""
            all_text.append(body_text)
        except Exception:
            pass

        full_text = "\n".join(all_text)
        body_lines = [ln.strip() for ln in (full_text or "").splitlines() if ln.strip()]

        def _next_line_after_label(label_variants):
            low_lines = [x.lower() for x in body_lines]
            for i, ll in enumerate(low_lines):
                if any(v in ll for v in label_variants):
                    if i + 1 < len(body_lines):
                        candidate = body_lines[i + 1].strip()
                        if candidate and candidate.lower() not in label_variants:
                            return candidate
            return ""

        # Extract applicant name
        if not result["applicant_name"]:
            preferred_labels = [
                "applicant name", "name of applicant", "applicant",
            ]
            for pl in preferred_labels:
                for label, vals in page_pairs_multi.items():
                    if pl in label:
                        for val in vals:
                            if not _is_bad_name_value(val) and not _looks_like_label_text(val):
                                result["applicant_name"] = val.strip()
                                break
                    if result["applicant_name"]:
                        break
                if result["applicant_name"]:
                    break
        if not result["applicant_name"]:
            cand = _next_line_after_label(["applicant name", "applicant", "name of applicant", "owner"])
            if cand and len(cand) < 140 and not _is_bad_name_value(cand) and not _looks_like_label_text(cand):
                result["applicant_name"] = cand
        if not result["applicant_name"]:
            for pattern in [
                r"applicant\s*(?:name)?\s*[:\-]\s*(.+)",
                r"owner\s*[:\-]\s*(.+)",
            ]:
                m = re.search(pattern, full_text, re.I)
                if m:
                    candidate = m.group(1).split("\n")[0].strip()
                    if (
                        2 < len(candidate) < 120
                        and not _is_bad_name_value(candidate)
                        and not _looks_like_label_text(candidate)
                    ):
                        result["applicant_name"] = candidate
                        break

        # Extract applicant address
        if not result["applicant_address"]:
            for label, val in page_pairs.items():
                if "applicant" in label and "address" in label:
                    result["applicant_address"] = val
                    break
        if not result["applicant_address"]:
            cand = _next_line_after_label(["applicant address", "address"])
            if cand and len(cand) < 220:
                result["applicant_address"] = cand

        # Extract full proposal text from detail page
        if not result["proposal_full"]:
            best = ""
            for label, vals in page_pairs_multi.items():
                if any(x in label for x in ["proposal description", "description of development", "development description", "proposal"]):
                    for val in vals:
                        v = re.sub(r"\s+", " ", val or "").strip()
                        if len(v) > len(best) and not _looks_like_label_text(v):
                            best = v
            if len(best) > 20:
                result["proposal_full"] = best
        if not result["proposal_full"]:
            cand = _next_line_after_label(["description of development", "development description", "proposal"])
            if cand and len(cand) > 20:
                result["proposal_full"] = cand
        if not result["proposal_full"]:
            for pattern in [
                r"(?:description of development|proposal|development description)\s*[:\-]\s*(.+?)(?:\n[A-Z][A-Za-z ]{2,}:|\Z)",
            ]:
                m = re.search(pattern, full_text, re.I | re.S)
                if m:
                    candidate = re.sub(r"\s+", " ", m.group(1)).strip()
                    if len(candidate) > 20:
                        result["proposal_full"] = candidate
                        break
        if result["proposal_full"]:
            result["proposal_full"] = re.sub(r"\s+", " ", result["proposal_full"]).strip()

        # Decision status intentionally not used.
        result["decision_status"] = ""

        # Extract decision date
        if not result["decision_date"]:
            for label, val in page_pairs.items():
                if "decision" in label and "date" in label:
                    result["decision_date"] = val
                    break
        if not result["decision_date"]:
            for pattern in [
                r"decision\s*date\s*[:\-]\s*(\d{1,2}[\s/\-]\w+[\s/\-]\d{4}|\d{4}-\d{2}-\d{2})",
                r"date\s*of\s*decision\s*[:\-]\s*(\d{1,2}[\s/\-]\w+[\s/\-]\d{4})",
            ]:
                m = re.search(pattern, full_text, re.I)
                if m:
                    result["decision_date"] = m.group(1).strip()
                    break

        result["has_contractor_identified"] = has_contractor_mentioned(full_text)

    except Exception as exc:
        logger.warning("Detail page scrape error for %s: %s", reference, exc)

    return result


def extract_architect_from_detail_page(driver):
    """Extract architect/agent name and contact details from a loaded detail page."""
    lines = []
    for xpath in [
        "//*[contains(translate(text(),'AGENTARCHITECT','agentarchitect'),'agent') or contains(translate(text(),'AGENTARCHITECT','agentarchitect'),'architect')]",
        "//dt|//dd|//th|//td|//label|//p|//li|//span",
    ]:
        try:
            elems = driver.find_elements(By.XPATH, xpath)
            for e in elems[:350]:
                txt = (e.text or "").strip()
                if txt:
                    lines.append(txt)
        except Exception:
            continue

    architect_name = ""
    for i, line in enumerate(lines):
        lower = line.lower()
        if "agent name (company)" in lower:
            if i + 1 < len(lines):
                nxt = clean_name(lines[i + 1])
                if nxt and len(nxt.split()) >= 2 and is_likely_architect_name(nxt):
                    architect_name = nxt
                    break
        if "architect" in lower or "agent" in lower:
            after_colon = clean_name(line.split(":", 1)[1] if ":" in line else "")
            if after_colon and len(after_colon.split()) >= 2 and is_likely_architect_name(after_colon):
                architect_name = after_colon
                break
            if i + 1 < len(lines):
                nxt = clean_name(lines[i + 1])
                if nxt and len(nxt.split()) >= 2 and is_likely_architect_name(nxt):
                    architect_name = nxt
                    break

    contact = _extract_architect_contact_from_text("\n".join(lines), architect_name)
    return architect_name, contact


def open_documents_tab(driver, wait):
    """Click the Documents tab on a detail page and wait for it to load."""
    for xpath in [
        "//a[contains(.,'Documents')]",
        "//button[contains(.,'Documents')]",
        "//li[contains(.,'Documents')]//a",
    ]:
        tabs = driver.find_elements(By.XPATH, xpath)
        if tabs:
            js_click(driver, tabs[0])
            time.sleep(2)
            try:
                wait.until(EC.presence_of_all_elements_located((By.XPATH, "//a")))
            except Exception:
                pass
            return True
    return False


def _doc_is_site_layout(label):
    """Return True if a document label indicates it is a site layout drawing."""
    lower = (label or "").lower()
    has_layout_kw = any(k in lower for k in SITE_LAYOUT_KEYWORDS)
    if not has_layout_kw and re.search(r"\bsu[-_ ]?0?\d+\b", lower):
        has_layout_kw = True
    has_excluded_kw = any(k in lower for k in EXCLUDE_DOC_KEYWORDS)
    return has_layout_kw and not has_excluded_kw


def _site_layout_priority(text):
    """Score a document label by how likely it is to be the primary site layout plan."""
    t = (text or "").lower()
    score = 0
    if "site layout plan" in t:
        score += 8
    if "proposed site" in t:
        score += 5
    if "existing site" in t:
        score += 4
    if "su-02" in t or "pp-02" in t:
        score += 6
    if "site plan" in t or "layout plan" in t:
        score += 3
    return score


def _doc_matches_name_keywords(label_or_href):
    """Return True if a document name/href contains drawing/layout/plan."""
    t = (label_or_href or "").lower()
    return any(k in t for k in DOCUMENT_NAME_KEYWORDS)


def _download_from_external_dms_iframe(driver, reference, area_name):
    """Download matching files from external DMS iframe (South Dublin pattern)."""
    iframes = driver.find_elements(By.ID, "externalDMS")
    if not iframes:
        return [], False

    app_dir = get_app_dir(area_name, reference)
    set_download_dir(driver, app_dir)

    gdrive_app_folder_id = ""
    if _gdrive_enabled():
        try:
            gdrive_app_folder_id, _ = get_gdrive_app_folder(area_name, reference)
        except Exception as exc:
            logger.warning("[%s] Could not prepare Google Drive folder: %s", reference, exc)

    saved = []
    seen_hrefs = set()
    safe_ref = reference.replace("/", "_").replace(" ", "_")

    try:
        driver.switch_to.frame(iframes[0])
        time.sleep(2)

        anchors = driver.find_elements(By.XPATH, "//a[@href]")
        candidates = []
        for a in anchors:
            try:
                href = (a.get_attribute("href") or "").strip()
                text = " ".join((a.text or "").split()).strip()
                cell_text = ""
                try:
                    cell = a.find_element(By.XPATH, "./ancestor::td[1]")
                    cell_text = " ".join((cell.text or "").split()).strip()
                except Exception:
                    cell_text = ""
                label = cell_text or text or href
                if not href or href in seen_hrefs:
                    continue
                if _doc_matches_name_keywords(label) or _doc_matches_name_keywords(href):
                    seen_hrefs.add(href)
                    candidates.append((href, clean_name(label) or "document"))
            except Exception:
                continue

        doc_idx = 1
        for href, label in candidates:
            try:
                before = {f for f in os.listdir(app_dir) if f.lower().endswith(".pdf")}
                clicked = driver.execute_script(
                    """
                    const h = arguments[0];
                    const links = Array.from(document.querySelectorAll("a[href]"));
                    const el = links.find(x => (x.href || "") === h);
                    if (!el) return false;
                    el.scrollIntoView({block: "center"});
                    el.click();
                    return true;
                    """,
                    href,
                )
                if not clicked:
                    continue
                time.sleep(1.2)
                new_pdf = wait_for_new_pdf(before_set=before, download_dir=app_dir, timeout=PDF_WAIT_TIMEOUT)
                if not new_pdf:
                    continue
                ext = os.path.splitext(new_pdf)[1] or ".pdf"
                dest = os.path.join(app_dir, f"{safe_ref}_doc_{doc_idx:02d}{ext}")
                if os.path.abspath(new_pdf) != os.path.abspath(dest):
                    try:
                        if os.path.exists(dest):
                            os.remove(dest)
                        os.rename(new_pdf, dest)
                    except Exception:
                        dest = new_pdf
                saved.append((dest, label))
                if gdrive_app_folder_id:
                    try:
                        upload_file_to_gdrive(dest, gdrive_app_folder_id, os.path.basename(dest))
                    except Exception as exc:
                        logger.warning("[%s] Drive upload failed for %s: %s", reference, os.path.basename(dest), exc)
                doc_idx += 1
            except Exception:
                continue
    finally:
        try:
            driver.switch_to.default_content()
        except Exception:
            pass

    return saved, True


def download_all_documents(driver, wait, detail_url, reference, area_name):
    """Download documents for a planning application.

    Returns (saved_list, page_architect, page_contact) where saved_list is a list of
    (file_path, doc_label) tuples.
    """
    if SKIP_PDF_DOWNLOAD:
        if not _load_detail_page_with_retry(driver, detail_url):
            return [], "", ""
        dismiss_overlays(driver)
        arch, contact = extract_architect_from_detail_page(driver)
        return [], arch, contact

    if not _load_detail_page_with_retry(driver, detail_url):
        return [], "", ""

    safe_ref = reference.replace("/", "_").replace(" ", "_")
    dump_debug(driver, f"detail_{safe_ref}")
    dismiss_overlays(driver)
    architect_from_page, contact_from_page = extract_architect_from_detail_page(driver)

    if not open_documents_tab(driver, wait):
        dump_debug(driver, f"documents_tab_missing_{safe_ref}")
        return [], architect_from_page, contact_from_page

    dump_debug(driver, f"documents_open_{safe_ref}")

    # South Dublin uses an external iframe-based DMS.
    iframe_saved, iframe_present = _download_from_external_dms_iframe(driver, reference, area_name)
    if iframe_saved:
        set_download_dir(driver, TEMP_DOWNLOAD_DIR)
        logger.info("[%s] Downloaded %s document(s) via external DMS iframe", reference, len(iframe_saved))
        return iframe_saved, architect_from_page, contact_from_page
    if iframe_present:
        set_download_dir(driver, TEMP_DOWNLOAD_DIR)
        logger.info("[%s] External DMS iframe present but no matching drawing/layout/plan docs found.", reference)
        return [], architect_from_page, contact_from_page

    # Create folders lazily only when we are actually downloading a file.
    app_dir = None
    gdrive_app_folder_id = ""

    doc_rows = driver.find_elements(By.XPATH, "//tr[td]")
    saved = []
    candidates = []

    for row in doc_rows:
        try:
            row_text = " ".join((row.text or "").split())
            row_data = driver.execute_script(
                "try { return angular.element(arguments[0]).scope().row; } catch(e) { return null; }",
                row,
            )
            doc_label = row_text
            if isinstance(row_data, dict):
                doc_label = " ".join(
                    str(x).strip()
                    for x in [
                        row_data.get("name"),
                        row_data.get("description"),
                        row_data.get("documentType"),
                        row_data.get("receivedDate"),
                        row_text,
                    ]
                    if x
                )
            view_buttons = row.find_elements(
                By.XPATH,
                ".//button[contains(@aria-label,'View') or .//span[contains(@title,'View')]]",
            )
            if not view_buttons:
                continue
            if _doc_is_site_layout(doc_label):
                candidates.append({
                    "row": row,
                    "label": doc_label,
                    "priority": _site_layout_priority(doc_label),
                })
        except Exception:
            continue

    if DOWNLOAD_ALL_DOCUMENTS:
        doc_idx = 1
        for row in doc_rows:
            try:
                row_text = " ".join((row.text or "").split())
                row_data = driver.execute_script(
                    "try { return angular.element(arguments[0]).scope().row; } catch(e) { return null; }",
                    row,
                )
                doc_label = row_text
                if isinstance(row_data, dict):
                    doc_label = " ".join(
                        str(x).strip()
                        for x in [
                            row_data.get("name"),
                            row_data.get("description"),
                            row_data.get("documentType"),
                            row_data.get("receivedDate"),
                            row_text,
                        ]
                        if x
                    )
                view_buttons = row.find_elements(
                    By.XPATH,
                    ".//button[contains(@aria-label,'View') or .//span[contains(@title,'View')]]",
                )
                if not view_buttons:
                    continue
                if app_dir is None:
                    app_dir = get_app_dir(area_name, reference)
                    set_download_dir(driver, app_dir)
                    if _gdrive_enabled():
                        try:
                            gdrive_app_folder_id, _ = get_gdrive_app_folder(area_name, reference)
                        except Exception as exc:
                            logger.warning("[%s] Could not prepare Google Drive folder: %s", reference, exc)
                before = {f for f in os.listdir(app_dir) if f.lower().endswith(".pdf")}
                js_click(driver, view_buttons[0])
                time.sleep(1.2)
                new_pdf = wait_for_new_pdf(before_set=before, download_dir=app_dir, timeout=PDF_WAIT_TIMEOUT)
                if new_pdf:
                    ext = os.path.splitext(new_pdf)[1] or ".pdf"
                    dest = os.path.join(app_dir, f"{safe_ref}_doc_{doc_idx:02d}{ext}")
                    if os.path.abspath(new_pdf) != os.path.abspath(dest):
                        try:
                            if os.path.exists(dest):
                                os.remove(dest)
                            os.rename(new_pdf, dest)
                        except Exception:
                            dest = new_pdf
                    saved.append((dest, clean_name(doc_label) or f"document_{doc_idx}"))
                    if gdrive_app_folder_id:
                        try:
                            upload_file_to_gdrive(dest, gdrive_app_folder_id, os.path.basename(dest))
                        except Exception as exc:
                            logger.warning("[%s] Drive upload failed for %s: %s", reference, os.path.basename(dest), exc)
                    doc_idx += 1
            except Exception:
                continue
    else:
        candidates.sort(key=lambda x: x["priority"], reverse=True)
        best = candidates[0] if candidates else None
        if best:
            try:
                row = best["row"]
                view_buttons = row.find_elements(
                    By.XPATH,
                    ".//button[contains(@aria-label,'View') or .//span[contains(@title,'View')]]",
                )
                if view_buttons:
                    if app_dir is None:
                        app_dir = get_app_dir(area_name, reference)
                        set_download_dir(driver, app_dir)
                        if _gdrive_enabled():
                            try:
                                gdrive_app_folder_id, _ = get_gdrive_app_folder(area_name, reference)
                            except Exception as exc:
                                logger.warning("[%s] Could not prepare Google Drive folder: %s", reference, exc)
                    before = {f for f in os.listdir(app_dir) if f.lower().endswith(".pdf")}
                    js_click(driver, view_buttons[0])
                    time.sleep(1.2)
                    new_pdf = wait_for_new_pdf(before_set=before, download_dir=app_dir, timeout=PDF_WAIT_TIMEOUT)
                    if new_pdf:
                        ext = os.path.splitext(new_pdf)[1] or ".pdf"
                        dest = os.path.join(app_dir, f"{safe_ref}_site_layout_plan{ext}")
                        if os.path.abspath(new_pdf) != os.path.abspath(dest):
                            try:
                                if os.path.exists(dest):
                                    os.remove(dest)
                                os.rename(new_pdf, dest)
                            except Exception:
                                dest = new_pdf
                        saved.append((dest, clean_name(best["label"]) or "site_layout_plan"))
                        if gdrive_app_folder_id:
                            try:
                                upload_file_to_gdrive(dest, gdrive_app_folder_id, os.path.basename(dest))
                            except Exception as exc:
                                logger.warning("[%s] Drive upload failed for %s: %s", reference, os.path.basename(dest), exc)
            except Exception:
                pass

    # Reset Chrome downloads back to the temp dir
    set_download_dir(driver, TEMP_DOWNLOAD_DIR)

    logger.info("[%s] Downloaded %s document(s)%s", reference, len(saved), f" to {app_dir}" if app_dir else "")
    return saved, architect_from_page, contact_from_page


# ===============================
# SEARCH RESULTS COLLECTION
# ===============================
def collect_search_results(driver, wait, area_name):
    """Navigate the search page, paginate all results, and return raw application dicts."""
    driver.get(SEARCH_URL)
    time.sleep(2)
    dump_debug(driver, "search_loaded")
    dismiss_overlays(driver)

    location = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//input[contains(@placeholder,'location') or contains(@name,'location')]")
        )
    )
    location.clear()
    if area_name.strip():
        location.send_keys(area_name.strip())
        time.sleep(0.6)
        # The portal uses autocomplete; typing alone may not apply the area filter.
        # Prefer clicking the matching suggestion, then fallback to ENTER.
        picked = False
        try:
            suggestion_xpaths = [
                f"//li[contains(@class,'ui-menu-item')]//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{area_name.strip().lower()}')]",
                f"//ul[contains(@class,'ui-autocomplete')]//li//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{area_name.strip().lower()}')]",
                f"//*[contains(@class,'autocomplete')]//*[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{area_name.strip().lower()}')]",
            ]
            for sx in suggestion_xpaths:
                items = driver.find_elements(By.XPATH, sx)
                if items:
                    js_click(driver, items[0])
                    picked = True
                    time.sleep(0.4)
                    break
        except Exception:
            picked = False
        if not picked:
            try:
                location.send_keys(Keys.ENTER)
                time.sleep(0.3)
            except Exception:
                pass

    clicked_search = False
    for search_xpath in [
        "//button[@id='btnSearch' or @sas-id='btnSearch']",
        "//button[@id='btnSearchTop' or @sas-id='btnSearchTop']",
        "//button[contains(normalize-space(.),'Search') and not(contains(normalize-space(.),'Quick'))]",
        "//input[@value='Search']",
        "//button[@id='searchBtn' or @sas-id='searchBtn']",
    ]:
        if click_xpath_js_with_retry(driver, search_xpath, "search button", attempts=3):
            clicked_search = True
            break
    if not clicked_search:
        dump_debug(driver, f"search_click_failed_{_safe_slug(area_name)}")
        logger.warning("Area '%s': failed to click Search after retries. Skipping area.", area_name)
        return []
    dump_debug(driver, "search_clicked")
    try:
        WebDriverWait(driver, SEARCH_RESULTS_TIMEOUT).until(
            lambda d: (
                len(d.find_elements(By.XPATH, "//table//tr[td]")) > 0
                or len(d.find_elements(By.XPATH, "//p[contains(@class,'results-count')]")) > 0
                or len(d.find_elements(By.XPATH, "//ul[contains(@class,'pagination')]")) > 0
            )
        )
    except TimeoutException:
        dump_debug(driver, f"results_timeout_{_safe_slug(area_name)}")
        logger.warning("Area '%s': no results table appeared before timeout. Skipping area.", area_name)
        return []
    time.sleep(1)
    dump_debug(driver, "results_loaded")

    # Apply registration-date filter on page (last LOOKBACK_DAYS), then wait before pagination.
    try:
        start_dt = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).strftime("%d/%m/%Y")
        end_dt = datetime.now().strftime("%d/%m/%Y")
        range_text = f"{start_dt} - {end_dt}"
        first_ref_before_filter = ""
        pre_rows = driver.find_elements(By.XPATH, "//table//tr[td]")
        if pre_rows:
            try:
                first_ref_before_filter = pre_rows[0].find_elements(By.TAG_NAME, "td")[0].text.strip()
            except Exception:
                first_ref_before_filter = ""

        reg_inputs = driver.find_elements(
            By.XPATH,
            "//input[contains(@placeholder,'registration') or contains(@name,'registration') or contains(@id,'registration') or contains(@aria-label,'registration')]",
        )
        if not reg_inputs:
            reg_inputs = driver.find_elements(
                By.XPATH,
                "(//th[contains(.,'Registration date')])[1]/following::input[1]",
            )
        if reg_inputs:
            reg_input = reg_inputs[0]
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", reg_input)
            time.sleep(0.2)
            reg_input.send_keys(Keys.CONTROL, "a")
            reg_input.send_keys(range_text)
            reg_input.send_keys(Keys.ENTER)
            logger.info("Area '%s': applied registration date filter '%s'", area_name, range_text)
            try:
                wait.until(
                    lambda d: (
                        len(d.find_elements(By.XPATH, "//table//tr[td]")) > 0
                        and (
                            not first_ref_before_filter
                            or d.find_elements(By.XPATH, "//table//tr[td]")[0]
                            .find_elements(By.TAG_NAME, "td")[0]
                            .text.strip()
                            != first_ref_before_filter
                        )
                    )
                )
            except Exception:
                pass
            time.sleep(max(0, POST_FILTER_WAIT_SECONDS))
    except Exception as exc:
        logger.warning("Area '%s': could not apply on-page date filter: %s", area_name, exc)

    seen_refs = set()
    apps = []
    page = 1
    visited_page_numbers = set()
    date_cutoff = (datetime.now() - timedelta(days=LOOKBACK_DAYS)).date()

    def _read_total_results_and_page_size():
        """Return (total_results, page_size) parsed from 'results-count' text."""
        total_results = 0
        page_size = 10
        try:
            txt = ""
            elems = driver.find_elements(By.XPATH, "//p[contains(@class,'results-count')]")
            if elems:
                txt = " ".join((elems[0].text or "").split())
            nums = re.findall(r"\d+", txt)
            # Typical: "25 of 4549 results" => page_size=25, total=4549
            if len(nums) >= 2:
                page_size = int(nums[0]) if int(nums[0]) > 0 else 10
                total_results = int(nums[1])
            elif len(nums) == 1:
                total_results = int(nums[0])
                # Single-number form like "4432 results" does not include page size.
                page_size = 10
        except Exception:
            pass
        return total_results, max(1, page_size)

    def _get_numbered_candidates(current_page):
        links = driver.find_elements(
            By.XPATH,
            "//a[contains(@class,'page-link')][.//span[normalize-space(text())!='']]",
        )
        out = []
        for a in links:
            try:
                txt = (driver.execute_script("return (arguments[0].innerText || '').trim();", a) or "").strip()
                if not txt.isdigit():
                    continue
                num = int(txt)
                if num > current_page and num not in visited_page_numbers:
                    out.append((num, a))
            except Exception:
                continue
        out.sort(key=lambda x: x[0])
        return out

    def _active_ngtable_page():
        """Return active page number from ng-table pagination, or None."""
        xpaths = [
            "//ul[contains(@class,'ng-table-pagination')]//li[contains(@class,'active')]//span[normalize-space(text())!='']",
            "//ul[contains(@class,'ng-table-pagination')]//li[contains(@class,'active')]//a[normalize-space(text())!='']",
            "(//ul[contains(@class,'pagination')])[last()]//li[contains(@class,'active')]//*[self::a or self::span][1]",
        ]
        for xp in xpaths:
            try:
                el = driver.find_element(By.XPATH, xp)
                txt = (el.text or "").strip()
                if txt.isdigit():
                    return int(txt)
            except Exception:
                continue
        return None

    total_results, page_size = _read_total_results_and_page_size()
    expected_pages = (total_results + page_size - 1) // page_size if total_results > 0 else 1
    logger.info(
        "Area '%s': detected total_results=%s, page_size=%s, expected_pages=%s",
        area_name, total_results, page_size, expected_pages
    )

    def _click_bottom_page_number(target_page, first_ref_before):
        """Click explicit page number in bottom paginator and wait for table change."""
        def _active_page():
            try:
                el = driver.find_element(
                    By.XPATH,
                    "(//ul[contains(@class,'pagination')])[last()]//li[contains(@class,'active')]//*[self::a or self::span][1]",
                )
                t = (el.text or "").strip()
                return int(t) if t.isdigit() else None
            except Exception:
                return None

        active_before = _active_page()
        xpaths = [
            f"(//ul[contains(@class,'pagination')])[last()]//a[normalize-space(text())='{target_page}']",
            f"(//ul[contains(@class,'pagination')])[last()]//a[.//span[normalize-space(text())='{target_page}']]",
            f"//a[contains(@class,'page-link') and normalize-space(text())='{target_page}']",
            f"//a[contains(@class,'page-link') and .//span[normalize-space(text())='{target_page}']]",
        ]
        for xp in xpaths:
            elems = driver.find_elements(By.XPATH, xp)
            if not elems:
                continue
            el = elems[0]
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.2)
                js_click(driver, el)
            except Exception:
                continue
            try:
                wait.until(
                    lambda d: (
                        len(d.find_elements(By.XPATH, "//table//tr[td]")) > 0
                        and (
                            (
                                active_before is not None
                                and (
                                    (lambda t: int(t) if t.isdigit() else None)(
                                        (d.find_element(
                                            By.XPATH,
                                            "(//ul[contains(@class,'pagination')])[last()]//li[contains(@class,'active')]//*[self::a or self::span][1]",
                                        ).text or "").strip()
                                    ) == target_page
                                )
                            )
                            or (
                                not first_ref_before
                                or d.find_elements(By.XPATH, "//table//tr[td]")[0]
                                .find_elements(By.TAG_NAME, "td")[0]
                                .text.strip()
                                != first_ref_before
                            )
                        )
                    )
                )
                return True
            except Exception:
                # Retry with hard JS click once
                try:
                    driver.execute_script("arguments[0].click();", el)
                    wait.until(
                        lambda d: (
                            len(d.find_elements(By.XPATH, "//table//tr[td]")) > 0
                            and (
                                (
                                    active_before is not None
                                    and (
                                        (lambda t: int(t) if t.isdigit() else None)(
                                            (d.find_element(
                                                By.XPATH,
                                                "(//ul[contains(@class,'pagination')])[last()]//li[contains(@class,'active')]//*[self::a or self::span][1]",
                                            ).text or "").strip()
                                        ) == target_page
                                    )
                                )
                                or (
                                    not first_ref_before
                                    or d.find_elements(By.XPATH, "//table//tr[td]")[0]
                                    .find_elements(By.TAG_NAME, "td")[0]
                                    .text.strip()
                                    != first_ref_before
                                )
                            )
                        )
                    )
                    return True
                except Exception:
                    continue
        return False

    while True:
        rows = driver.find_elements(By.XPATH, "//table//tr[td]")
        if page == 1 and total_results > 0:
            # Use real first-page row count to determine pagination size.
            observed_page_size = max(1, len(rows))
            if observed_page_size != page_size:
                page_size = observed_page_size
                expected_pages = (total_results + page_size - 1) // page_size
                logger.info(
                    "Area '%s': adjusted page_size=%s from first page rows; expected_pages=%s",
                    area_name, page_size, expected_pages
                )
        logger.info("Page %s rows: %s", page, len(rows))

        for tr in rows:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if len(tds) < 3:
                continue
            try:
                row_data = driver.execute_script(
                    "try { return angular.element(arguments[0]).scope().row; } catch(e) { return null; }",
                    tr,
                )
                reference = ""
                detail_url = ""
                proposal = ""
                address = ""
                reg_date_raw = ""
                if isinstance(row_data, dict):
                    reference = str(row_data.get("reference", "")).strip()
                    proposal = str(row_data.get("proposalDesc", "")).strip()
                    address = str(row_data.get("location", "")).strip()
                    reg_date_raw = str(
                        row_data.get("applicationDate")
                        or row_data.get("registeredDate")
                        or row_data.get("registrationDate")
                        or ""
                    ).strip()
                    app_id = row_data.get("id")
                    if app_id:
                        detail_url = f"{DETAIL_URL_BASE}/{app_id}"

                if not reference:
                    reference = tds[0].text.strip()
                if not proposal and len(tds) > 1:
                    proposal = tds[1].text.strip()
                if not address and len(tds) > 2:
                    address = tds[2].text.strip()
                if not reg_date_raw and len(tds) > 3:
                    reg_date_raw = tds[3].text.strip()

                if not reference or "results" in reference.lower():
                    continue
                if reference in seen_refs:
                    continue

                if ENFORCE_ROW_DATE_CUTOFF:
                    reg_date = _parse_date(reg_date_raw)
                    if reg_date and reg_date < date_cutoff:
                        continue

                seen_refs.add(reference)
                apps.append({
                    "reference": reference,
                    "detail_url": detail_url,
                    "proposal": proposal,
                    "address": address,
                    "registration_date": reg_date_raw,
                    "search_area": area_name,
                })

                if MAX_APPS and len(apps) >= MAX_APPS:
                    logger.info("MAX_APPS=%s reached, stopping early.", MAX_APPS)
                    return apps

            except Exception:
                continue

        first_ref_before = ""
        if rows:
            try:
                first_ref_before = rows[0].find_elements(By.TAG_NAME, "td")[0].text.strip()
            except Exception:
                first_ref_before = ""
        active_page_before_click = _active_ngtable_page()

        visited_page_numbers.add(page)
        if expected_pages and page >= expected_pages:
            break
        next_btn = None
        for xpath in [
            "//ul[contains(@class,'ng-table-pagination')]//a[@ng-switch-when='next' and not(ancestor::li[contains(@class,'disabled')])]",
            "//ul[contains(@class,'ng-table-pagination')]//li[not(contains(@class,'disabled'))]//a[normalize-space(text())='»']",
            "//li[not(contains(@class,'disabled')) and contains(@class,'next')]//*[self::a or self::button][1]",
            "//a[not(contains(@class,'disabled')) and (contains(normalize-space(.),'Next') or contains(@aria-label,'Next') or @rel='next')]",
            "//button[not(@disabled) and (contains(normalize-space(.),'Next') or contains(@aria-label,'Next') or @title='Next')]",
            "//*[@role='button' and not(@disabled) and (contains(normalize-space(.),'Next') or contains(@aria-label,'Next'))]",
        ]:
            candidates = driver.find_elements(By.XPATH, xpath)
            if candidates:
                next_btn = candidates[0]
                break

        clicked = False
        target_page = page + 1

        # Primary: click exact page number from bottom paginator.
        clicked = _click_bottom_page_number(target_page, first_ref_before)

        # Secondary: generic numeric candidates.
        if not clicked:
            numbered_candidates = _get_numbered_candidates(page)
            if numbered_candidates:
                target_num, target_el = numbered_candidates[0]
                try:
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", target_el)
                    time.sleep(0.2)
                    js_click(driver, target_el)
                    target_page = target_num
                    clicked = True
                except Exception:
                    clicked = False

        # Last fallback: Next button.
        if not clicked and next_btn:
            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_btn)
                time.sleep(0.2)
                js_click(driver, next_btn)
                clicked = True
            except Exception:
                clicked = False

        # Final fallback: click any visible "Next" element in live DOM by JS (no stale refs).
        if not clicked:
            try:
                clicked = bool(
                    driver.execute_script(
                        """
                        const cands = Array.from(document.querySelectorAll("a,button,[role='button']"));
                        const next = cands.find(el => {
                          const t = (el.innerText || el.textContent || "").trim().toLowerCase();
                          const aria = (el.getAttribute("aria-label") || "").toLowerCase();
                          const rel = (el.getAttribute("rel") || "").toLowerCase();
                          const cls = (el.className || "").toLowerCase();
                          const disabled = el.disabled || cls.includes("disabled") || el.getAttribute("aria-disabled")==="true";
                          if (disabled) return false;
                          return t === "next" || t.startsWith("next ") || aria.includes("next") || rel === "next";
                        });
                        if (!next) return false;
                        next.scrollIntoView({block:'center'});
                        next.click();
                        return true;
                        """
                    )
                )
            except Exception:
                clicked = False

        if not clicked:
            break

        time.sleep(1.5)
        try:
            wait.until(
                lambda d: (
                    len(d.find_elements(By.XPATH, "//table//tr[td]")) > 0
                    and (
                        not first_ref_before
                        or d.find_elements(By.XPATH, "//table//tr[td]")[0]
                        .find_elements(By.TAG_NAME, "td")[0]
                        .text.strip()
                        != first_ref_before
                        or (
                            active_page_before_click is not None
                            and (lambda v: isinstance(v, int) and v != active_page_before_click)(_active_ngtable_page())
                        )
                    )
                )
            )
        except Exception:
            # If click failed to change page, retry numeric page navigation before stopping.
            numbered_candidates = _get_numbered_candidates(page)
            if numbered_candidates:
                target_num, target_el = numbered_candidates[0]
                try:
                    js_click(driver, target_el)
                    time.sleep(1.5)
                    wait.until(
                        lambda d: (
                            len(d.find_elements(By.XPATH, "//table//tr[td]")) > 0
                            and (
                                not first_ref_before
                                or d.find_elements(By.XPATH, "//table//tr[td]")[0]
                                .find_elements(By.TAG_NAME, "td")[0]
                                .text.strip()
                                != first_ref_before
                            )
                        )
                    )
                    page = target_num
                except Exception:
                    logger.info("No further pages detected; finishing results collection.")
                    break
            else:
                logger.info("No further pages detected; finishing results collection.")
                break

        current_active = _active_ngtable_page()
        page = current_active if isinstance(current_active, int) else target_page

    logger.info("Collected applications: %s", len(apps))
    return apps


# ===============================
# DEDUPLICATION
# ===============================
def deduplicate_records(records):
    """Keep the highest-amendment record per (normalised site_address, reference_family) group."""
    dedup = {}
    for rec in records:
        key = (
            (rec.get("site_address") or rec.get("address") or "").strip().lower(),
            _reference_family(rec.get("reference", "")),
        )
        prev = dedup.get(key)
        if not prev:
            dedup[key] = rec
            continue
        cur_rank = _amendment_rank(rec.get("reference", ""))
        prev_rank = _amendment_rank(prev.get("reference", ""))
        if cur_rank > prev_rank:
            dedup[key] = rec
        elif cur_rank == prev_rank:
            cur_date = _parse_date(rec.get("registration_date", ""))
            prev_date = _parse_date(prev.get("registration_date", ""))
            if cur_date and prev_date and cur_date > prev_date:
                dedup[key] = rec
    return list(dedup.values())


# ===============================
# RECORD ENRICHMENT
# ===============================
def enrich_record(rec):
    """Add all computed classification, scoring, and outreach fields to a raw record dict."""
    proposal = rec.get("proposal", "")

    ptype, pkeywords = classify_proposal(proposal)
    rec["proposal_type"] = ptype
    rec["proposal_keywords"] = pkeywords
    rec["area_type"] = classify_area_type(proposal)
    rec["is_protected_structure"] = is_protected_structure(proposal)
    rec["estimated_scale"] = extract_scale(proposal)
    rec["num_units"] = extract_num_units(proposal)
    rec["storeys"] = extract_storeys(proposal)

    rec["site_address"] = rec.get("site_address") or rec.get("address", "")
    rec["reference_family"] = _reference_family(rec.get("reference", ""))
    rec["amendment_rank"] = _amendment_rank(rec.get("reference", ""))

    reg_date = _parse_date(rec.get("registration_date", ""))
    dec_date = _parse_date(rec.get("decision_date", ""))
    rec["days_since_registration"] = _days_since(reg_date)
    rec["days_since_decision"] = _days_since(dec_date)

    rec["lead_score"] = compute_lead_score(rec)
    rec["lead_tier"] = compute_lead_tier(rec["lead_score"])
    rec["outreach_channel"] = compute_outreach_channel(rec)
    rec["outreach_talking_point"] = compute_talking_point(rec)
    rec["days_urgency_flag"] = compute_urgency_flag(rec)

    return rec


# ===============================
# OUTPUT COLUMNS
# ===============================
ALL_COLUMNS = [
    "reference", "reference_family", "amendment_rank",
    "applicant_name", "applicant_address", "site_address",
    "proposal", "proposal_type", "proposal_keywords",
    "registration_date", "decision_status", "decision_date",
    "days_since_decision", "days_since_registration",
    "area_type", "is_protected_structure", "estimated_scale",
    "num_units", "storeys", "search_area", "detail_url",
    "architect_name", "architect_contact_details", "architect_source",
    "has_contractor_identified", "documents_downloaded", "app_folder",
    "lead_score", "lead_tier", "outreach_channel",
    "outreach_talking_point", "days_urgency_flag",
]

TIER1_COLUMNS = [
    "reference", "site_address", "applicant_name", "applicant_address",
    "proposal", "proposal_type", "decision_date", "days_since_decision",
    "architect_name", "architect_contact_details",
    "lead_score", "outreach_talking_point", "outreach_channel", "days_urgency_flag",
    "documents_downloaded", "app_folder",
]

MASTER_SIMPLE_COLUMNS = [
    "reference",
    "applicant_name",
    "site_address",
    "proposal",
    "registration_date",
    "site_layout_plan_file",
    "google_drive_folder_link",
]


# ===============================
# OUTPUT GENERATION
# ===============================
def save_outputs(all_records):
    """Enrich all records, deduplicate, and write all output files to OUTPUT_DIR."""
    today_str = TODAY.strftime("%Y%m%d")

    if not all_records:
        logger.warning("No records to save.")
        return None

    enriched = [enrich_record(rec) for rec in all_records]

    if MIN_LEAD_SCORE > 0:
        enriched = [r for r in enriched if r.get("lead_score", 0) >= MIN_LEAD_SCORE]

    if EXPORT_TIER1_ONLY:
        enriched = [r for r in enriched if "Tier 1" in r.get("lead_tier", "")]

    enriched.sort(key=lambda r: r.get("lead_score", 0), reverse=True)

    df_all = pd.DataFrame(enriched)
    for col in ALL_COLUMNS:
        if col not in df_all.columns:
            df_all[col] = ""

    # Master CSV — simplified output required by user
    master_file = os.path.join(OUTPUT_DIR, f"master_all_leads_{today_str}.csv")
    for col in MASTER_SIMPLE_COLUMNS:
        if col not in df_all.columns:
            df_all[col] = ""
    df_all[MASTER_SIMPLE_COLUMNS].to_csv(master_file, index=False, encoding="utf-8-sig")
    logger.info("Master CSV: %s (%s records)", master_file, len(df_all))

    if _gsheets_enabled() and not GSHEETS_INCREMENTAL_SYNC:
        sync_all_leads_to_google_sheet(df_all)

    return df_all


def _write_summary(df, filepath, today_str):
    """Write the plain text weekly summary report to disk."""
    lines = []
    sep = "=" * 62
    lines.append(sep)
    lines.append("  DLR PLANNING SCRAPER — WEEKLY SUMMARY")
    lines.append("  Yellow Stag Services | yellowstagservices.com")
    lines.append(f"  Run date: {today_str}")
    lines.append(sep)
    lines.append("")
    lines.append(f"TOTAL APPLICATIONS SCRAPED:  {len(df)}")
    lines.append("")

    lines.append("BREAKDOWN BY PROPOSAL TYPE:")
    if not df.empty and "proposal_type" in df.columns:
        for ptype, count in df["proposal_type"].fillna("other").value_counts().items():
            lines.append(f"  {ptype:<35} {count}")
    lines.append("")

    lines.append("LEAD TIERS:")
    if not df.empty and "lead_tier" in df.columns:
        tc = df["lead_tier"].fillna("Unknown").value_counts()
        lines.append(f"  Tier 1 - Call This Week:   {tc.get('Tier 1 - Call This Week', 0)}")
        lines.append(f"  Tier 2 - Send Letter:      {tc.get('Tier 2 - Send Letter', 0)}")
        lines.append(f"  Tier 3 - Nurture:          {tc.get('Tier 3 - Nurture', 0)}")
        lines.append(f"  Skip:                      {tc.get('Skip', 0)}")
    lines.append("")

    lines.append("TOP 5 AREAS BY APPLICATION COUNT:")
    if not df.empty and "search_area" in df.columns:
        for area, count in df["search_area"].fillna("Unknown").value_counts().head(5).items():
            lines.append(f"  {area:<35} {count}")
    lines.append("")

    lines.append("TOP 5 ARCHITECTS BY APPLICATION COUNT:")
    if not df.empty and "architect_name" in df.columns:
        has_arch = df[df["architect_name"].fillna("").str.strip() != ""]
        for arch, count in has_arch["architect_name"].value_counts().head(5).items():
            lines.append(f"  {arch:<35} {count}")
    lines.append("")

    lines.append("TOP TIER 1 LEADS:")
    if not df.empty and "lead_tier" in df.columns:
        top = df[df["lead_tier"].str.contains("Tier 1", na=False)].head(10)
        for _, row in top.iterrows():
            score = row.get("lead_score", 0)
            addr = str(row.get("site_address", ""))[:45]
            urgency = str(row.get("days_urgency_flag", ""))[:28]
            lines.append(f"  [{score:3d}] {addr:<46} {urgency}")
    lines.append("")
    lines.append(sep)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    logger.info("Weekly summary written: %s", filepath)


def print_console_summary(df):
    """Print a formatted results summary table to the console at end of run."""
    sep = "=" * 70
    print(f"\n{sep}")
    print("  YELLOW STAG SERVICES — DLR PLANNING SCRAPER COMPLETE")
    print(sep)

    if df is None or df.empty:
        print("  No records found.")
        print(sep + "\n")
        return

    total = len(df)
    tier1 = len(df[df["lead_tier"].str.contains("Tier 1", na=False)]) if "lead_tier" in df.columns else 0
    tier2 = len(df[df["lead_tier"].str.contains("Tier 2", na=False)]) if "lead_tier" in df.columns else 0
    tier3 = len(df[df["lead_tier"].str.contains("Tier 3", na=False)]) if "lead_tier" in df.columns else 0
    arch_found = int((df["architect_name"].fillna("").str.strip() != "").sum()) if "architect_name" in df.columns else 0
    appl_found = int((df["applicant_name"].fillna("").str.strip() != "").sum()) if "applicant_name" in df.columns else 0

    print(f"  Total applications:      {total}")
    print(f"  Architect found:         {arch_found}")
    print(f"  Applicant name found:    {appl_found}")
    print()
    print(f"  TIER 1 HOT LEADS:        {tier1}  <-- Call these this week")
    print(f"  Tier 2 Send Letter:      {tier2}")
    print(f"  Tier 3 Nurture:          {tier3}")
    print()

    if tier1 > 0 and "lead_tier" in df.columns:
        print("  TOP TIER 1 LEADS:")
        top = df[df["lead_tier"].str.contains("Tier 1", na=False)].head(8)
        for _, row in top.iterrows():
            addr = str(row.get("site_address", ""))[:42]
            score = row.get("lead_score", 0)
            urgency = str(row.get("days_urgency_flag", ""))[:28]
            print(f"    [{score:3d}] {addr:<43} | {urgency}")
        print()

    print(f"  Output: {OUTPUT_DIR}")
    print(sep + "\n")


# ===============================
# MAIN
# ===============================
def main():
    """Scrape all configured DLR areas, enrich records, and write all output files."""
    driver = make_driver()
    wait = WebDriverWait(driver, 25)
    records = []
    seen_refs_global = set()

    try:
        if GDRIVE_WIPE_ROOT_BEFORE_RUN and _gdrive_enabled():
            wipe_gdrive_root_folder_contents()
        if _gsheets_enabled() and GSHEETS_INCREMENTAL_SYNC and GSHEETS_CLEAR_ON_START:
            clear_google_sheet_all_leads_data()

        if COUNCIL_SLUG == "southdublin":
            default_areas = SOUTH_DUBLIN_AREAS
        elif COUNCIL_SLUG == "fingal":
            default_areas = FINGAL_AREAS
        else:
            default_areas = DLR_AREAS

        if AREA_NAME:
            areas = [AREA_NAME]
        elif RUN_ALL_AREAS:
            areas = default_areas
        else:
            areas = [""]

        if RESUME_FROM_AREA:
            target = RESUME_FROM_AREA.strip().lower()
            if target:
                idx = next((i for i, a in enumerate(areas) if a.lower() == target), None)
                if idx is not None:
                    areas = areas[idx:]
                    logger.info("Resuming from area: %s (%s remaining)", RESUME_FROM_AREA, len(areas))
                else:
                    logger.warning("RESUME_FROM_AREA '%s' not found in area list; running full selection.", RESUME_FROM_AREA)

        for area_idx, area in enumerate(areas, start=1):
            logger.info("========== Area %s/%s: %s ==========", area_idx, len(areas), area)
            get_area_dir(area)  # create the area folder early
            try:
                apps = collect_search_results(driver, wait, area)
            except Exception as exc:
                logger.exception("Area '%s': search collection crashed; skipping area. Error: %s", area, exc)
                continue
            logger.info("Area '%s': %s applications in last %s days.", area, len(apps), LOOKBACK_DAYS)

            area_records = []

            for i, app in enumerate(apps, start=1):
                ref = app["reference"]
                if ref in seen_refs_global:
                    continue
                seen_refs_global.add(ref)
                logger.info("[%s/%s][%s] Processing: %s", i, len(apps), area, ref)

                # Scrape applicant + decision data from detail page
                detail_data = scrape_detail_page(driver, wait, app["detail_url"], ref)

                # Download ALL documents into area/reference folder and extract architect
                docs, page_architect, page_contact = download_all_documents(
                    driver, wait, app["detail_url"], ref, area
                )

                architect = ""
                architect_contact = ""
                method = ""

                # Try extracting architect from the first few downloaded PDFs
                docs_for_arch = docs[:]
                docs_for_arch.sort(key=lambda x: 0 if _doc_is_site_layout(x[1]) else 1)
                for pdf_path, _title in docs_for_arch[:5]:
                    if not pdf_path.lower().endswith(".pdf"):
                        continue
                    arch, contact, used_method = extract_architect_and_contact(pdf_path)
                    if arch or contact:
                        architect = arch
                        architect_contact = contact
                        method = used_method
                        break

                if not architect and page_architect:
                    architect = page_architect
                    method = "detail_page"
                if not architect_contact and page_contact:
                    architect_contact = page_contact

                if not is_likely_architect_name(architect):
                    architect = "No architect detected"
                    if not architect_contact:
                        architect_contact = "No architect detected"
                    method = method or "not_found"

                app_dir = get_app_dir(area, ref) if docs else ""
                site_layout_plan_file = ""
                if docs:
                    for doc_path, doc_label in docs:
                        if _doc_is_site_layout(doc_label):
                            site_layout_plan_file = doc_path
                            break
                    if not site_layout_plan_file:
                        site_layout_plan_file = docs[0][0]
                gdrive_link = ""
                if _gdrive_enabled():
                    try:
                        if docs:
                            _, gdrive_link = get_gdrive_app_folder(area, ref)
                        else:
                            gdrive_link = "No site layout plan"
                    except Exception:
                        gdrive_link = "No site layout plan"
                else:
                    gdrive_link = gdrive_link or ("No site layout plan" if not docs else "")

                if DELETE_PDFS_AFTER_ANALYSIS and docs:
                    deleted_count = cleanup_downloaded_docs(docs, reference=ref)
                    if deleted_count:
                        site_layout_plan_file = "Deleted after analysis"

                rec = {
                    "search_area": app.get("search_area", area),
                    "reference": ref,
                    "address": app["address"],
                    "site_address": app["address"],
                    "proposal": detail_data.get("proposal_full", "") or app["proposal"],
                    "registration_date": app.get("registration_date", ""),
                    "detail_url": app["detail_url"],
                    # Detail page data
                    "applicant_name": detail_data.get("applicant_name", ""),
                    "applicant_address": detail_data.get("applicant_address", ""),
                    "decision_status": detail_data.get("decision_status", ""),
                    "decision_date": detail_data.get("decision_date", ""),
                    "has_contractor_identified": detail_data.get("has_contractor_identified", False),
                    # Architect
                    "architect_name": architect,
                    "architect_contact_details": architect_contact,
                    "architect_source": method,
                    # Documents
                    "documents_downloaded": len(docs),
                    "app_folder": app_dir,
                    "site_layout_plan_file": site_layout_plan_file,
                    "google_drive_folder_link": gdrive_link,
                }

                area_records.append(rec)
                records.append(rec)

            if _gsheets_enabled() and GSHEETS_INCREMENTAL_SYNC and area_records:
                # Filter out records already in the sheet (passed from UI via env)
                if _YS_EXISTING_REFS:
                    new_area_records = [r for r in area_records if r.get("reference", "") not in _YS_EXISTING_REFS]
                    skipped = len(area_records) - len(new_area_records)
                    if skipped:
                        logger.info("UI dedup: skipped %s existing refs for area '%s'", skipped, area)
                    area_records_to_sync = new_area_records
                else:
                    area_records_to_sync = area_records
                if area_records_to_sync:
                    enriched_area = [enrich_record(dict(r)) for r in area_records_to_sync]
                    append_rows_to_google_sheet(pd.DataFrame(enriched_area))

    finally:
        driver.quit()

    # Global deduplication
    deduped_all = deduplicate_records(records)
    logger.info("Total records after global deduplication: %s", len(deduped_all))

    # Enrich + write all output files
    df = save_outputs(deduped_all)

    # Console summary
    print_console_summary(df)


if __name__ == "__main__":
    main()


# =============================================================
# requirements.txt
# =============================================================
# selenium>=4.0.0
# webdriver-manager>=4.0.0
# pandas>=2.0.0
# openpyxl>=3.1.0
# pdfplumber>=0.9.0
# pytesseract>=0.3.10
# pdf2image>=1.16.0
# Pillow>=10.0.0
#
# Optional (graceful fallback if unavailable):
# paddleocr>=2.7.0
# paddlepaddle>=2.5.0
# rapidocr-onnxruntime>=1.3.0
# numpy>=1.24.0
# =============================================================
