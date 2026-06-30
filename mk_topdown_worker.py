#!/usr/bin/env python3
"""
================================================================================
 MK TOP-DOWN WORKER  —  Python polling layer (replaces Apps Script triggers)
================================================================================

Runs on your laptop during market hours. Polls Fyers every minute using YOUR
IP (not Apps Script's shared egress pool — that's what caused the 1015 errors).
Writes results to the same Google Sheet, where the existing TD_* tabs and
conditional formatting render everything as before.

Architecture:
    Apps Script    = built the sheets, headers, CF rules (one-time only)
    Python (this)  = data plane (auth, fetch, analyze, write)
    No Apps Script triggers are installed anywhere.

This file ports the rotational refresh pattern from MK_TopDown.gs verbatim:
    - Broad + sector indices fetched every minute (~2 calls)
    - 1/5 of F&O universe per minute, rotating (~2 calls)
    - Total: ~4 API calls/min, 100% from your home IP — Cloudflare-safe.

USAGE
-----
    # one-time setup (see SETUP.md):
    pip install -r requirements.txt
    cp config.env.template config.env       # fill in credentials
    # place service_account.json in same folder
    # share the Google Sheet with the service-account email

    # daily use:
    python mk_topdown_worker.py             # leave running during market hours

Press Ctrl-C to stop. Logs to stdout and to worker.log.
================================================================================
"""

import os
import sys
import time
import json
import base64
import hashlib
import logging
import pickle
import threading
import queue
import datetime as dt
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# ── Force IST for all datetime.now() calls ──────────────────────────────
# GitHub Actions runners (and most cloud machines) run in UTC. The worker
# assumes IST everywhere (market hours, timestamps, day-roll logic). Rather
# than convert at every call site, we pin the process timezone to IST.
# On Linux/macOS this takes effect immediately via time.tzset(); on Windows
# tzset() doesn't exist, but local runs there are already in IST so it's moot.
os.environ["TZ"] = "Asia/Kolkata"
try:
    time.tzset()   # Linux/macOS only — applies the TZ env var process-wide
except AttributeError:
    pass           # Windows: no tzset(); local clock is already IST

# ============================================================================
#  IMPORTS — third party
# ============================================================================
try:
    import requests
    import pyotp
    import gspread
    from google.oauth2 import service_account
except ImportError as e:
    print(f"Missing dependency: {e.name}. Run:  pip install -r requirements.txt")
    sys.exit(1)

# python-dotenv is optional — only needed for LOCAL runs that read config.env.
# In GitHub Actions, credentials arrive as environment variables (from Secrets),
# so dotenv isn't required there.
try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*_a, **_kw):   # no-op fallback
        return False


# ============================================================================
#  CONFIG
# ============================================================================
ROOT = Path(__file__).parent

# Load config.env IF it exists (local runs). In GitHub Actions there is no
# config.env — credentials come straight from environment variables injected
# from GitHub Secrets. load_dotenv silently does nothing if the file is absent.
_CONFIG_ENV = ROOT / "config.env"
if _CONFIG_ENV.exists():
    load_dotenv(_CONFIG_ENV)

# Detect run environment for logging clarity
RUNNING_IN_CI = os.environ.get("GITHUB_ACTIONS", "").lower() == "true"

def _need(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        src = "GitHub Secrets / environment" if RUNNING_IN_CI else "config.env"
        print(f"ERROR: {key} not set in {src}")
        sys.exit(1)
    return v

FYERS_CLIENT_ID    = _need("FYERS_CLIENT_ID")
FYERS_SECRET_KEY   = _need("FYERS_SECRET_KEY")
FYERS_REDIRECT_URI = _need("FYERS_REDIRECT_URI")
FYERS_ID           = _need("FYERS_ID")
FYERS_TOTP_SECRET  = _need("FYERS_TOTP_SECRET")
FYERS_PIN          = _need("FYERS_PIN")
SHEET_ID           = _need("SHEET_ID")

# Service account: either a local file (local runs) OR a JSON string in an
# env var (GitHub Actions, from a Secret named GOOGLE_SERVICE_ACCOUNT_JSON).
SERVICE_ACCOUNT_FILE = ROOT / "service_account.json"
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")

CACHE_FILE  = ROOT / "state_cache.pkl"
TOKEN_FILE  = ROOT / "fyers_tokens.json"
LOG_FILE    = ROOT / "worker.log"

# Session end override (for GitHub Actions' 6-hour job cap — we split the
# trading day into two jobs). Format "HH:MM" in IST. Empty = run till market
# close (15:30) as normal. The morning CI job sets this to "12:30"; the
# afternoon job leaves it unset.
SESSION_END_OVERRIDE = os.environ.get("SESSION_END", "").strip()

ROTATION_SLICES   = 1     # was 5 — full F&O universe refreshed EVERY minute
POLL_INTERVAL_SEC = 60

# How many recent conviction readings to keep per stock (for stability check).
# With 1-slice rotation, each stock is re-evaluated every minute, so 10 readings
# spans the last ~10 minutes of activity for that name (tighter, intraday-grade).
CONVICTION_HISTORY_LEN = 10

# ── TradingAgents multi-LLM overlay (selective second opinion) ──────────────
# Heavy + slow framework — used only on Grade A/B setups, hard-capped per day.
# Set OVERLAY_ENABLED=False (default) until tradingagents is pip-installed
# and ANTHROPIC_API_KEY (or OPENAI/GOOGLE) is exported.
OVERLAY_ENABLED                 = False
OVERLAY_LLM_PROVIDER            = "anthropic"        # anthropic | openai | google
OVERLAY_DEEP_THINK_MODEL        = "claude-opus-4-7"
OVERLAY_QUICK_THINK_MODEL       = "claude-haiku-4-5"
OVERLAY_MAX_DEBATE_ROUNDS       = 2
OVERLAY_MAX_ANALYSES_PER_DAY    = 15                 # hard daily cap
OVERLAY_MIN_CONVICTION          = 80                 # only top-tier setups
OVERLAY_COOLDOWN_MIN_PER_TICKER = 240                # 4h cooldown per ticker

# Setup Quality weights — the COMPOSITE that turns a snapshot into a decision.
# Sum should be ~1.0. Time-decay multiplies the final score.
QUALITY_WEIGHTS = {
    "conviction":  0.35,   # how strong the textbook signal is right now
    "stability":   0.30,   # how consistently it's held — kills one-minute spikes
    "room":        0.25,   # how much potential move remains
    "sector":      0.10,   # sector confluence bonus
}

WEIGHTS = {
    "priceChange":      0.25,
    "oiBuildup":        0.30,
    "volumeSurge":      0.20,
    "relativeStrength": 0.15,
    "rangePosition":    0.10,
}

# Symbol lists. Fyers symbols verified against the master CSV at
# https://public.fyers.in/sym_details/NSE_CM.csv (authoritative — community
# posts can be outdated). Run `--list-indices` to verify each is live.
BROAD_INDICES = [
    ("NIFTY 50",           "NSE:NIFTY50-INDEX"),
    ("NIFTY BANK",         "NSE:NIFTYBANK-INDEX"),
    ("NIFTY NEXT 50",      "NSE:NIFTYNXT50-INDEX"),       # was NIFTYNEXT50 (silent fail)
    ("NIFTY 100",          "NSE:NIFTY100-INDEX"),
    ("NIFTY 200",          "NSE:NIFTY200-INDEX"),
    ("NIFTY 500",          "NSE:NIFTY500-INDEX"),
    ("NIFTY TOTAL MKT",    "NSE:NIFTYTOTALMKT-INDEX"),    # new — full equity market
    ("NIFTY LARGEMID 250", "NSE:NIFTYLARGEMID250-INDEX"), # new — top 250 by mcap
    ("NIFTY MIDCAP 50",    "NSE:NIFTYMIDCAP50-INDEX"),
    ("NIFTY MIDCAP 100",   "NSE:NIFTYMIDCAP100-INDEX"),
    ("NIFTY MIDCAP 150",   "NSE:NIFTYMIDCAP150-INDEX"),
    ("NIFTY SMALLCAP 50",  "NSE:NIFTYSMLCAP50-INDEX"),
    ("NIFTY SMALLCAP 100", "NSE:NIFTYSMLCAP100-INDEX"),
    ("NIFTY SMALLCAP 250", "NSE:NIFTYSMLCAP250-INDEX"),
    ("NIFTY MICROCAP 250", "NSE:NIFTYMICROCAP250-INDEX"), # new — smallest caps
    ("MIDCAP NIFTY",       "NSE:MIDCPNIFTY-INDEX"),
    ("FIN NIFTY",          "NSE:FINNIFTY-INDEX"),
    ("INDIA VIX",          "NSE:INDIAVIX-INDEX"),
]

SECTORAL_INDICES = [
    # ── Core 11 NSE sectoral indices ──
    ("NIFTY AUTO",         "NSE:NIFTYAUTO-INDEX"),
    ("NIFTY BANK",         "NSE:NIFTYBANK-INDEX"),
    ("NIFTY CONSR DURBL",  "NSE:NIFTYCONSRDURBL-INDEX"),   # was NIFTYCONSDUR (silent fail)
    ("NIFTY ENERGY",       "NSE:NIFTYENERGY-INDEX"),
    ("NIFTY FIN SERVICE",  "NSE:FINNIFTY-INDEX"),
    ("NIFTY FMCG",         "NSE:NIFTYFMCG-INDEX"),
    ("NIFTY HEALTHCARE",   "NSE:NIFTYHEALTHCARE-INDEX"),
    ("NIFTY IT",           "NSE:NIFTYIT-INDEX"),
    ("NIFTY MEDIA",        "NSE:NIFTYMEDIA-INDEX"),
    ("NIFTY METAL",        "NSE:NIFTYMETAL-INDEX"),
    ("NIFTY OIL & GAS",    "NSE:NIFTYOILANDGAS-INDEX"),    # was NIFTYOILGAS (silent fail)
    ("NIFTY PHARMA",       "NSE:NIFTYPHARMA-INDEX"),
    ("NIFTY PSU BANK",     "NSE:NIFTYPSUBANK-INDEX"),
    ("NIFTY PVT BANK",     "NSE:NIFTYPVTBANK-INDEX"),
    ("NIFTY REALTY",       "NSE:NIFTYREALTY-INDEX"),
    # ── Broader baskets ──
    ("NIFTY CONSUMPTION",  "NSE:NIFTYCONSUMPTION-INDEX"),
    ("NIFTY INFRA",        "NSE:NIFTYINFRA-INDEX"),
    ("NIFTY SERV SECTOR",  "NSE:NIFTYSERVSECTOR-INDEX"),  # was NIFTYSERVSEC (silent fail)
    ("NIFTY COMMODITIES",  "NSE:NIFTYCOMMODITIES-INDEX"),
    ("NIFTY MNC",          "NSE:NIFTYMNC-INDEX"),
    ("NIFTY PSE",          "NSE:NIFTYPSE-INDEX"),
    ("NIFTY CPSE",         "NSE:NIFTYCPSE-INDEX"),
    # ── Thematic indices (new) — hot themes worth tracking ──
    ("NIFTY DEFENCE",      "NSE:NIFTYINDDEFENCE-INDEX"),  # BEL, HAL, BDL, MAZAGON
    ("NIFTY EV",           "NSE:NIFTYEV-INDEX"),          # EV ecosystem
    ("NIFTY DIGITAL",      "NSE:NIFTYINDDIGITAL-INDEX"),  # digital-native cos
    ("NIFTY INDIA MFG",    "NSE:NIFTYINDIAMFG-INDEX"),    # Make-in-India theme
    ("NIFTY MOBILITY",     "NSE:NIFTYMOBILITY-INDEX"),    # auto + transport
    ("NIFTY TOURISM",      "NSE:NIFTYINDTOURISM-INDEX"),  # hospitality, airlines
    ("NIFTY HOUSING",      "NSE:NIFTYHOUSING-INDEX"),     # realty + building materials
    ("NIFTY CAPITAL MKT",  "NSE:NIFTYCAPITALMKT-INDEX"),  # brokers, exchanges, AMCs
    ("NIFTY TRANS & LOG",  "NSE:NIFTYTRANSLOGIS-INDEX"),  # logistics, ports, shipping
    ("NIFTY IPO",          "NSE:NIFTYIPO-INDEX"),         # recently-listed firms
]

# ──────────────────────────────────────────────────────────────────────
# INDEX WEIGHTS — for the Constituent Contribution tables on Market Overview
# ──────────────────────────────────────────────────────────────────────
# Weights are free-float market-cap-derived percentages. They drift daily
# but slowly; refresh from the NSE monthly factsheets when they look stale:
#   NIFTY 50:    https://www.nseindia.com/products-services/indices-nifty50-index
#   BANK NIFTY:  https://www.nseindia.com/products-services/indices-niftybank-index
# (Open "Factsheet" PDF; the constituent weights table is on page 2.)
#
# Values below: snapshot from late Jan 2026 NSE factsheets. If you see the
# table's "Coverage" row below 95% consistently, weights have drifted and
# need a refresh. Coverage of 98-102% is normal noise.
WEIGHTS_LAST_REFRESHED = "2026-01"

NIFTY50_WEIGHTS: Dict[str, float] = {
    # Top weights — these 10 stocks drive ~55% of NIFTY movement
    "HDFCBANK":   13.05,
    "ICICIBANK":   8.32,
    "RELIANCE":    8.04,
    "INFY":        5.42,
    "BHARTIARTL":  4.48,
    "ITC":         3.98,
    "LT":          3.85,
    "TCS":         3.61,
    "AXISBANK":    3.17,
    "KOTAKBANK":   2.86,
    # Middle weights
    "SBIN":        2.74,
    "HINDUNILVR":  2.21,
    "M&M":         2.18,
    "BAJFINANCE":  2.05,
    "SUNPHARMA":   1.78,
    "MARUTI":      1.71,
    "NTPC":        1.60,
    "HCLTECH":     1.59,
    "TITAN":       1.41,
    "ULTRACEMCO":  1.37,
    "ASIANPAINT":  1.30,
    "POWERGRID":   1.24,
    "TATAMOTORS":  1.22,
    "BAJAJFINSV":  1.05,
    "ONGC":        0.98,
    # Lower weights — still material on big moves
    "ADANIENT":    0.94,
    "WIPRO":       0.92,
    "JSWSTEEL":    0.89,
    "TATASTEEL":   0.86,
    "NESTLEIND":   0.84,
    "ADANIPORTS":  0.82,
    "GRASIM":      0.81,
    "COALINDIA":   0.79,
    "TECHM":       0.75,
    "INDUSINDBK":  0.71,
    "HINDALCO":    0.71,
    "EICHERMOT":   0.69,
    "BAJAJ-AUTO":  0.65,
    "TATACONSUM":  0.62,
    "DRREDDY":     0.60,
    "CIPLA":       0.59,
    "BRITANNIA":   0.57,
    "APOLLOHOSP":  0.56,
    "HEROMOTOCO":  0.54,
    "DIVISLAB":    0.51,
    "SHRIRAMFIN":  0.47,
    "BPCL":        0.42,
    "TRENT":       0.39,
    "SBILIFE":     0.38,
    "HDFCLIFE":    0.36,
    # Exactly 50 entries. Self-normalized to 100% at module load (below) so
    # contribution math is internally consistent regardless of approximations.
}

BANKNIFTY_WEIGHTS: Dict[str, float] = {
    # Top 4 banks drive ~80% of BANK NIFTY movement
    "HDFCBANK":    28.42,
    "ICICIBANK":   24.85,
    "SBIN":         9.91,
    "KOTAKBANK":    8.76,
    "AXISBANK":     8.30,
    # Mid-cap private + select PSUs
    "INDUSINDBK":   5.62,
    "BANKBARODA":   3.45,
    "PNB":          3.02,
    "FEDERALBNK":   2.78,
    "AUBANK":       2.10,
    "IDFCFIRSTB":   1.52,
    "BANDHANBNK":   1.27,
}


def _normalize_weights(weights: Dict[str, float], name: str) -> Dict[str, float]:
    """
    Force the weight dict to sum to exactly 100%. Necessary because:
      - Hardcoded weights are approximations of NSE's published values
      - NSE recalculates weights daily based on free-float market cap
      - Any drift in the hardcoded ratios will cascade into the contribution
        coverage check

    What this does:
      Each weight is scaled by (100 / current_sum), so ratios are preserved
      but the total becomes exactly 100.

    What this DOESN'T do:
      Fix ratio errors. If hardcoded HDFCBANK weight is 13.05 but real NSE
      is 12.30, this function still produces 13.05 (just normalized). For
      true accuracy, run `python mk_topdown_worker.py --refresh-weights`
      which prints a checklist for updating from the latest NSE factsheet.
    """
    total = sum(weights.values())
    n = len(weights)
    if total <= 0:
        return dict(weights)
    factor = 100.0 / total
    normalized = {k: round(v * factor, 4) for k, v in weights.items()}
    drift_pct = abs(total - 100.0)
    if drift_pct > 0.5:
        # Significant drift — print directly (log isn't initialized yet at
        # module load time). User sees this on every worker startup until
        # they refresh the weights from NSE.
        print(f"[weights] {name}: {n} entries, hardcoded sum = {total:.2f}%, "
              f"normalized to 100% (factor {factor:.4f}). "
              f"Drift of {drift_pct:.2f} points — consider running "
              f"--refresh-weights.")
    return normalized


# Apply at module load — these are what _compute_contributions actually uses.
# The raw NIFTY50_WEIGHTS / BANKNIFTY_WEIGHTS dicts above stay editable for
# the user; normalization happens here so the math is always consistent.
NIFTY50_WEIGHTS_NORM   = _normalize_weights(NIFTY50_WEIGHTS,   "NIFTY 50 weights")
BANKNIFTY_WEIGHTS_NORM = _normalize_weights(BANKNIFTY_WEIGHTS, "BANK NIFTY weights")


# ──────────────────────────────────────────────────────────────────────
# Sheet-based weight overrides
# ──────────────────────────────────────────────────────────────────────
# Why: every Indian financial site (Dhan, NSE, Moneycontrol, Yahoo) sits
# behind a Cloudflare WAF that blocks Python's HTTP libraries. Automated
# scraping is unreliable and brittle. The reliable path is letting the
# user paste weights into a Google Sheet tab — which the worker reads
# at runtime. This lets the user update weights monthly (or whenever
# they want) without code changes or fragile scraping.
#
# Flow:
#   1. setup_sheets() creates the "Index Weights" tab and pre-populates
#      it with the hardcoded defaults above.
#   2. User opens https://dhan.co/indices/nifty-50-companies/ in a browser,
#      copies the Ticker + Weight columns, pastes into the sheet.
#   3. Worker reads from the sheet every 5 minutes (cached) and uses
#      those values. If sheet is empty or unreachable, falls back to
#      hardcoded defaults.

# In-process cache for sheet-loaded weights. Reloaded every 5 minutes
# to pick up user edits without spamming the Sheets API.
_weights_cache: Dict[str, Any] = {
    "nifty": None,           # Dict[str, float] or None
    "bank":  None,
    "fetched_at": None,      # datetime of last successful load
}
_WEIGHTS_CACHE_TTL_SEC = 300


def load_weights_from_sheet(ss) -> Tuple[Dict[str, float], Dict[str, float]]:
    """
    Read NIFTY 50 + BANK NIFTY weights from the 'Index Weights' sheet.
    Returns (nifty_dict_normalized, banknifty_dict_normalized).
    Falls back to hardcoded NORM dicts on any failure.

    Cached for _WEIGHTS_CACHE_TTL_SEC so polling doesn't hammer Sheets.
    """
    now = dt.datetime.now()
    if _weights_cache["fetched_at"] is not None:
        age = (now - _weights_cache["fetched_at"]).total_seconds()
        if age < _WEIGHTS_CACHE_TTL_SEC and _weights_cache["nifty"] is not None:
            return _weights_cache["nifty"], _weights_cache["bank"]

    try:
        ws = ss.worksheet(SHEET_NAMES["WEIGHTS"])
        all_rows = ws.get_all_values()
    except Exception as e:
        log.warning(f"Could not read Index Weights sheet: {e}. "
                    f"Using hardcoded defaults.")
        return NIFTY50_WEIGHTS_NORM, BANKNIFTY_WEIGHTS_NORM

    # Skip header row (row 0)
    nifty_raw: Dict[str, float] = {}
    bank_raw:  Dict[str, float] = {}
    for r in all_rows[1:]:
        if len(r) < 3:
            continue
        # Cells can come back as strings (gspread default) or numbers if the
        # backend ever changes — be defensive about types.
        index_name = str(r[0] or "").strip().upper()
        ticker     = str(r[1] or "").strip().upper()
        weight_str = str(r[2]).strip().replace("%", "").strip() if r[2] != "" else ""
        if not ticker or not weight_str:
            continue
        try:
            weight = float(weight_str)
        except ValueError:
            continue
        if weight <= 0:
            continue
        if index_name == "NIFTY 50":
            nifty_raw[ticker] = weight
        elif index_name in ("BANK NIFTY", "NIFTY BANK"):
            bank_raw[ticker] = weight

    # Use what was found in the sheet; fall back per-index if empty
    if nifty_raw:
        nifty = _normalize_weights(nifty_raw, "NIFTY 50 (from sheet)")
    else:
        log.warning("No NIFTY 50 rows in Index Weights sheet. Using hardcoded.")
        nifty = NIFTY50_WEIGHTS_NORM

    if bank_raw:
        bank = _normalize_weights(bank_raw, "BANK NIFTY (from sheet)")
    else:
        log.warning("No BANK NIFTY rows in Index Weights sheet. Using hardcoded.")
        bank = BANKNIFTY_WEIGHTS_NORM

    _weights_cache["nifty"]      = nifty
    _weights_cache["bank"]       = bank
    _weights_cache["fetched_at"] = now
    return nifty, bank


def populate_weights_sheet(ss) -> None:
    """
    Pre-populate the Index Weights sheet with hardcoded defaults. Called
    from setup_sheets(). Safe to call repeatedly — only writes if the
    sheet is empty (i.e. first-time setup), so user edits aren't wiped.
    """
    ws = ss.worksheet(SHEET_NAMES["WEIGHTS"])
    existing = ws.get_all_values()
    # If sheet already has data beyond the header row, leave it alone
    if len(existing) > 1 and any(any(cell.strip() for cell in row)
                                  for row in existing[1:]):
        log.info(f"  {SHEET_NAMES['WEIGHTS']}: already populated, leaving as-is.")
        return

    rows: List[List[Any]] = []
    # Two helper / instruction rows just below the column header (row 1)
    rows.append(["— How to refresh weights:", "", ""])
    rows.append(["  1) Open Dhan's index page in a browser:", "", ""])
    rows.append(["     NIFTY 50:   https://dhan.co/indices/nifty-50-companies/", "", ""])
    rows.append(["     BANK NIFTY: https://dhan.co/indices/nifty-bank-share-price/", "", ""])
    rows.append(["  2) Copy the Ticker + Weight columns from the table.", "", ""])
    rows.append(["  3) Paste over the data below (column B + C). Index column stays.", "", ""])
    rows.append(["  4) Worker auto-picks up changes within 5 minutes.", "", ""])
    rows.append(["", "", ""])

    # NIFTY 50 block
    for ticker, weight in NIFTY50_WEIGHTS.items():
        rows.append(["NIFTY 50", ticker, weight])

    rows.append(["", "", ""])

    # BANK NIFTY block
    for ticker, weight in BANKNIFTY_WEIGHTS.items():
        rows.append(["BANK NIFTY", ticker, weight])

    ws.update(range_name=f"A2:C{len(rows) + 1}",
              values=rows, value_input_option="USER_ENTERED")
    log.info(f"  {SHEET_NAMES['WEIGHTS']}: populated with hardcoded defaults.")

# Stock-to-sector mapping for F&O Live. Each ticker maps to ONE primary sector
# label (matching SECTORAL_INDICES labels exactly). Stocks not listed default
# to "OTHER" and sort to the bottom of F&O Live.
#
# Rules used to assign:
#   - All banks (public + private) → NIFTY BANK (consolidated)
#   - Oil/gas producers/refiners → NIFTY OIL & GAS
#   - Power utilities + green energy → NIFTY ENERGY
#   - Hospital chains/diagnostics → NIFTY HEALTHCARE
#   - Pharma manufacturers → NIFTY PHARMA
#   - NBFCs/insurance/AMCs/capital markets → NIFTY FIN SERVICE
STOCK_SECTOR: Dict[str, str] = {
    # ─── NIFTY BANK (private + PSU consolidated) ───
    "HDFCBANK": "NIFTY BANK", "ICICIBANK": "NIFTY BANK",
    "KOTAKBANK": "NIFTY BANK", "AXISBANK": "NIFTY BANK",
    "INDUSINDBK": "NIFTY BANK", "FEDERALBNK": "NIFTY BANK",
    "BANDHANBNK": "NIFTY BANK", "IDFCFIRSTB": "NIFTY BANK",
    "AUBANK": "NIFTY BANK", "RBLBANK": "NIFTY BANK",
    "YESBANK": "NIFTY BANK", "CITYUNIONBANK": "NIFTY BANK",
    "DCBBANK": "NIFTY BANK", "KARURVYSYA": "NIFTY BANK",
    "SBIN": "NIFTY BANK", "PNB": "NIFTY BANK",
    "BANKBARODA": "NIFTY BANK", "CANBK": "NIFTY BANK",
    "UNIONBANK": "NIFTY BANK", "BANKINDIA": "NIFTY BANK",
    "INDIANB": "NIFTY BANK", "MAHABANK": "NIFTY BANK",
    "UCOBANK": "NIFTY BANK", "IOB": "NIFTY BANK",
    "CENTRALBK": "NIFTY BANK", "PSB": "NIFTY BANK",

    # ─── NIFTY IT ───
    "TCS": "NIFTY IT", "INFY": "NIFTY IT", "WIPRO": "NIFTY IT",
    "HCLTECH": "NIFTY IT", "TECHM": "NIFTY IT", "LTIM": "NIFTY IT",
    "COFORGE": "NIFTY IT", "PERSISTENT": "NIFTY IT",
    "MPHASIS": "NIFTY IT", "LTTS": "NIFTY IT", "OFSS": "NIFTY IT",
    "KPITTECH": "NIFTY IT", "BIRLASOFT": "NIFTY IT",
    "TATAELXSI": "NIFTY IT", "INTELLECT": "NIFTY IT",
    "ZENSARTECH": "NIFTY IT", "CYIENT": "NIFTY IT",
    "HAPPSTMNDS": "NIFTY IT", "TANLA": "NIFTY IT",
    "NEWGEN": "NIFTY IT", "ROUTE": "NIFTY IT",

    # ─── NIFTY AUTO ───
    "MARUTI": "NIFTY AUTO", "TATAMOTORS": "NIFTY AUTO",
    "M&M": "NIFTY AUTO", "BAJAJ-AUTO": "NIFTY AUTO",
    "HEROMOTOCO": "NIFTY AUTO", "EICHERMOT": "NIFTY AUTO",
    "TVSMOTOR": "NIFTY AUTO", "ASHOKLEY": "NIFTY AUTO",
    "BHARATFORG": "NIFTY AUTO", "MOTHERSON": "NIFTY AUTO",
    "BOSCHLTD": "NIFTY AUTO", "MRF": "NIFTY AUTO",
    "BALKRISIND": "NIFTY AUTO", "EXIDEIND": "NIFTY AUTO",
    "TIINDIA": "NIFTY AUTO", "APOLLOTYRE": "NIFTY AUTO",
    "ESCORTS": "NIFTY AUTO", "ENDURANCE": "NIFTY AUTO",
    "AMARAJABAT": "NIFTY AUTO", "SUNDARMFIN": "NIFTY AUTO",
    "SONACOMS": "NIFTY AUTO", "UNOMINDA": "NIFTY AUTO",
    "CEATLTD": "NIFTY AUTO", "JKTYRE": "NIFTY AUTO",

    # ─── NIFTY PHARMA ───
    "SUNPHARMA": "NIFTY PHARMA", "DRREDDY": "NIFTY PHARMA",
    "CIPLA": "NIFTY PHARMA", "LUPIN": "NIFTY PHARMA",
    "DIVISLAB": "NIFTY PHARMA", "AUROPHARMA": "NIFTY PHARMA",
    "TORNTPHARM": "NIFTY PHARMA", "BIOCON": "NIFTY PHARMA",
    "GLAND": "NIFTY PHARMA", "IPCALAB": "NIFTY PHARMA",
    "ZYDUSLIFE": "NIFTY PHARMA", "ALKEM": "NIFTY PHARMA",
    "GRANULES": "NIFTY PHARMA", "LAURUSLABS": "NIFTY PHARMA",
    "GLENMARK": "NIFTY PHARMA", "MANKIND": "NIFTY PHARMA",
    "ABBOTINDIA": "NIFTY PHARMA", "NATCOPHARM": "NIFTY PHARMA",
    "PFIZER": "NIFTY PHARMA", "AJANTPHARM": "NIFTY PHARMA",
    "ERIS": "NIFTY PHARMA", "EMCURE": "NIFTY PHARMA",
    "SANOFI": "NIFTY PHARMA", "JBCHEPHARM": "NIFTY PHARMA",
    "CAPLIPOINT": "NIFTY PHARMA", "WOCKPHARMA": "NIFTY PHARMA",

    # ─── NIFTY HEALTHCARE (hospitals + diagnostics) ───
    "APOLLOHOSP": "NIFTY HEALTHCARE", "MAXHEALTH": "NIFTY HEALTHCARE",
    "FORTIS": "NIFTY HEALTHCARE", "LALPATHLAB": "NIFTY HEALTHCARE",
    "METROPOLIS": "NIFTY HEALTHCARE", "MEDPLUSHEAL": "NIFTY HEALTHCARE",
    "NARAYANA": "NIFTY HEALTHCARE", "KIMS": "NIFTY HEALTHCARE",
    "HCG": "NIFTY HEALTHCARE", "ASTERDM": "NIFTY HEALTHCARE",
    "RAINBOW": "NIFTY HEALTHCARE",

    # ─── NIFTY METAL ───
    "TATASTEEL": "NIFTY METAL", "JSWSTEEL": "NIFTY METAL",
    "HINDALCO": "NIFTY METAL", "JINDALSTEL": "NIFTY METAL",
    "SAIL": "NIFTY METAL", "VEDL": "NIFTY METAL",
    "NMDC": "NIFTY METAL", "NATIONALUM": "NIFTY METAL",
    "HINDZINC": "NIFTY METAL", "JINDALSAW": "NIFTY METAL",
    "JSL": "NIFTY METAL", "APLAPOLLO": "NIFTY METAL",
    "RATNAMANI": "NIFTY METAL", "WELCORP": "NIFTY METAL",
    "COALINDIA": "NIFTY METAL", "MOIL": "NIFTY METAL",
    "GMDCLTD": "NIFTY METAL",

    # ─── NIFTY FMCG ───
    "HINDUNILVR": "NIFTY FMCG", "ITC": "NIFTY FMCG",
    "NESTLEIND": "NIFTY FMCG", "BRITANNIA": "NIFTY FMCG",
    "DABUR": "NIFTY FMCG", "COLPAL": "NIFTY FMCG",
    "MARICO": "NIFTY FMCG", "GODREJCP": "NIFTY FMCG",
    "TATACONSUM": "NIFTY FMCG", "UBL": "NIFTY FMCG",
    "VBL": "NIFTY FMCG", "EMAMILTD": "NIFTY FMCG",
    "PGHH": "NIFTY FMCG", "RADICO": "NIFTY FMCG",
    "MCDOWELL-N": "NIFTY FMCG", "BALRAMCHIN": "NIFTY FMCG",
    "PATANJALI": "NIFTY FMCG", "GILLETTE": "NIFTY FMCG",
    "VARUNBEV": "NIFTY FMCG", "JYOTHYLAB": "NIFTY FMCG",

    # ─── NIFTY ENERGY (power utilities + green) ───
    "POWERGRID": "NIFTY ENERGY", "NTPC": "NIFTY ENERGY",
    "TATAPOWER": "NIFTY ENERGY", "ADANIPOWER": "NIFTY ENERGY",
    "ADANIGREEN": "NIFTY ENERGY", "JSWENERGY": "NIFTY ENERGY",
    "NHPC": "NIFTY ENERGY", "SJVN": "NIFTY ENERGY",
    "TORNTPOWER": "NIFTY ENERGY", "CESC": "NIFTY ENERGY",
    "ADANIENSOL": "NIFTY ENERGY", "NTPCGREEN": "NIFTY ENERGY",
    "RPOWER": "NIFTY ENERGY",

    # ─── NIFTY OIL & GAS ───
    "RELIANCE": "NIFTY OIL & GAS", "ONGC": "NIFTY OIL & GAS",
    "BPCL": "NIFTY OIL & GAS", "IOC": "NIFTY OIL & GAS",
    "GAIL": "NIFTY OIL & GAS", "HINDPETRO": "NIFTY OIL & GAS",
    "OIL": "NIFTY OIL & GAS", "MGL": "NIFTY OIL & GAS",
    "IGL": "NIFTY OIL & GAS", "GUJGASLTD": "NIFTY OIL & GAS",
    "PETRONET": "NIFTY OIL & GAS", "AEGISLOG": "NIFTY OIL & GAS",
    "GSPL": "NIFTY OIL & GAS", "CASTROLIND": "NIFTY OIL & GAS",
    "GULFOILLUB": "NIFTY OIL & GAS",

    # ─── NIFTY REALTY ───
    "DLF": "NIFTY REALTY", "GODREJPROP": "NIFTY REALTY",
    "OBEROIRLTY": "NIFTY REALTY", "PRESTIGE": "NIFTY REALTY",
    "BRIGADE": "NIFTY REALTY", "PHOENIXLTD": "NIFTY REALTY",
    "LODHA": "NIFTY REALTY", "MAHLIFE": "NIFTY REALTY",
    "SOBHA": "NIFTY REALTY", "ANANTRAJ": "NIFTY REALTY",
    "SUNTECK": "NIFTY REALTY",

    # ─── NIFTY CONSR DURBL (consumer durables) ───
    "TITAN": "NIFTY CONSR DURBL", "VOLTAS": "NIFTY CONSR DURBL",
    "CROMPTON": "NIFTY CONSR DURBL", "HAVELLS": "NIFTY CONSR DURBL",
    "DIXON": "NIFTY CONSR DURBL", "KAJARIACER": "NIFTY CONSR DURBL",
    "BLUESTARCO": "NIFTY CONSR DURBL", "AMBER": "NIFTY CONSR DURBL",
    "WHIRLPOOL": "NIFTY CONSR DURBL", "BAJAJELEC": "NIFTY CONSR DURBL",
    "KALYANKJIL": "NIFTY CONSR DURBL", "POLYCAB": "NIFTY CONSR DURBL",
    "CERA": "NIFTY CONSR DURBL", "VGUARD": "NIFTY CONSR DURBL",
    "RAJESHEXPO": "NIFTY CONSR DURBL", "ORIENTELEC": "NIFTY CONSR DURBL",

    # ─── NIFTY MEDIA ───
    "ZEEL": "NIFTY MEDIA", "PVRINOX": "NIFTY MEDIA",
    "SUNTV": "NIFTY MEDIA", "NETWORK18": "NIFTY MEDIA",
    "NAZARA": "NIFTY MEDIA", "SAREGAMA": "NIFTY MEDIA",
    "TIPSMUSIC": "NIFTY MEDIA",

    # ─── NIFTY FIN SERVICE (NBFCs + insurance + AMCs + exchanges) ───
    "BAJFINANCE": "NIFTY FIN SERVICE", "BAJAJFINSV": "NIFTY FIN SERVICE",
    "HDFCLIFE": "NIFTY FIN SERVICE", "SBILIFE": "NIFTY FIN SERVICE",
    "ICICIPRULI": "NIFTY FIN SERVICE", "MUTHOOTFIN": "NIFTY FIN SERVICE",
    "CHOLAFIN": "NIFTY FIN SERVICE", "LICHSGFIN": "NIFTY FIN SERVICE",
    "PEL": "NIFTY FIN SERVICE", "PFC": "NIFTY FIN SERVICE",
    "RECLTD": "NIFTY FIN SERVICE", "CANFINHOME": "NIFTY FIN SERVICE",
    "M&MFIN": "NIFTY FIN SERVICE", "MANAPPURAM": "NIFTY FIN SERVICE",
    "ABCAPITAL": "NIFTY FIN SERVICE", "SHRIRAMFIN": "NIFTY FIN SERVICE",
    "IRFC": "NIFTY FIN SERVICE", "ICICIGI": "NIFTY FIN SERVICE",
    "HDFCAMC": "NIFTY FIN SERVICE", "NAM-INDIA": "NIFTY FIN SERVICE",
    "CDSL": "NIFTY FIN SERVICE", "BSE": "NIFTY FIN SERVICE",
    "MCX": "NIFTY FIN SERVICE", "ANGELONE": "NIFTY FIN SERVICE",
    "LIC": "NIFTY FIN SERVICE", "POONAWALLA": "NIFTY FIN SERVICE",
    "GICRE": "NIFTY FIN SERVICE", "STARHEALTH": "NIFTY FIN SERVICE",
    "NIVABUPA": "NIFTY FIN SERVICE", "GODREJFIN": "NIFTY FIN SERVICE",
    "PNBHOUSING": "NIFTY FIN SERVICE", "MOTILALOFS": "NIFTY FIN SERVICE",
    "IIFL": "NIFTY FIN SERVICE", "AAVAS": "NIFTY FIN SERVICE",
    "HOMEFIRST": "NIFTY FIN SERVICE", "FIVESTAR": "NIFTY FIN SERVICE",
    "JIOFIN": "NIFTY FIN SERVICE",
}

# F&O universe — loaded from sheet at startup
FNO_UNIVERSE: List[str] = []

# Globally cached resources
_SHEET = None       # gspread Spreadsheet
_FYERS_BASE = "https://api-t1.fyers.in"

# Sheet names — descriptive English instead of TD_* prefixes. The setup_sheets()
# function migrates from the old names to these. Polling writers use only these.
SHEET_NAMES = {
    "DASHBOARD":   "Dashboard",
    "MARKET":      "Market Overview",
    "SECTORS":     "Sector Rotation",
    "FNO":         "F&O Live",
    "OI_ACTIVITY": "OI Activity",      # consolidates 4 old buildup sheets
    "CONVICTION":  "Top Conviction",
    "ACTIVITY":    "Activity Log",
    "SNAPSHOTS":   "Snapshots Log",
    "PERFORMERS":  "Persistent Performers",   # 5-day rolling Grade A/B counts
    "WEIGHTS":     "Index Weights",           # user-editable index weights
    "GLOSSARY":    "Glossary",
    "CONFIG":      "Universe Config",
    "CACHE":       "_state_cache",     # hidden
}

# Map of legacy TD_* names to the new names. Used during one-time migration.
LEGACY_RENAMES = {
    "TD_Dashboard":      SHEET_NAMES["DASHBOARD"],
    "TD_Market":         SHEET_NAMES["MARKET"],
    "TD_Sectors":        SHEET_NAMES["SECTORS"],
    "TD_FnO_Stocks":     SHEET_NAMES["FNO"],
    "TD_Top_Conviction": SHEET_NAMES["CONVICTION"],
    "TD_Intraday_Log":   SHEET_NAMES["ACTIVITY"],
    "TD_Stock_Snapshots":SHEET_NAMES["SNAPSHOTS"],
    "TD_Config":         SHEET_NAMES["CONFIG"],
    "TD_State_Cache":    SHEET_NAMES["CACHE"],
}
# Old buildup sheets — removed by setup_sheets (replaced by OI Activity)
LEGACY_TO_DELETE = ["TD_LongBuildup", "TD_ShortBuildup",
                    "TD_ShortCovering", "TD_LongUnwinding"]

# Parallel depth fetch — Fyers /data/depth accepts only 1 symbol per call,
# but we can run multiple in parallel. With ROTATION_SLICES=1, the full F&O
# universe (~208 names) is fetched every minute. 6 workers @ ~250ms latency
# completes 208 calls in ~9s — well under POLL_INTERVAL_SEC.
# If Fyers returns 429s, drop this back to 4 and the per-call sleep
# inside fyers_fetch_quotes will absorb the slack.
DEPTH_PARALLEL_WORKERS = 6

# ============================================================================
#  LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger("mk_topdown")


# ============================================================================
#  FYERS AUTH  —  TOTP-based auto-login (mirrors MK_Fyers_Live.gs flow)
# ============================================================================

# Fyers auth endpoint constants — verified against working v3 implementations
# (FabTrader Nov 2025 reference, tkanhe/fyers-api-access-token-v3)
_VAGATOR_BASE = "https://api-t2.fyers.in/vagator/v2"
_API_V3_BASE  = "https://api-t1.fyers.in/api/v3"

def _save_tokens(access: str, refresh: str) -> None:
    TOKEN_FILE.write_text(json.dumps({
        "access_token":  access,
        "refresh_token": refresh,
        "saved_at": dt.datetime.now().isoformat(),
    }, indent=2))

def _load_tokens() -> Optional[Dict[str, str]]:
    if not TOKEN_FILE.exists():
        return None
    try:
        return json.loads(TOKEN_FILE.read_text())
    except Exception:
        return None

def _b64(s: str) -> str:
    """Fyers expects fy_id and PIN base64-encoded in vagator request bodies."""
    return base64.b64encode(str(s).encode("ascii")).decode("ascii")

def fyers_auto_login() -> Tuple[str, str]:
    """
    Five-step TOTP login flow. Returns (access_token, refresh_token).
    Endpoints verified against FabTrader's Nov 2025 working reference.
    """
    log.info("Running full TOTP login...")
    s = requests.Session()
    s.headers.update({
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
    })

    # ---- Step 1: send_login_otp_v2 ----
    r = s.post(f"{_VAGATOR_BASE}/send_login_otp_v2", json={
        "fy_id": _b64(FYERS_ID),
        "app_id": "2",
    }, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Step 1 send_login_otp_v2 failed {r.status_code}: {r.text[:300]}")
    request_key = r.json().get("request_key")
    if not request_key:
        raise RuntimeError(f"Step 1: no request_key in response: {r.text[:300]}")

    # ---- Step 2: verify_otp ----
    # Avoid the TOTP rollover window — if we're <=2s from the next 30s tick,
    # wait it out so the OTP we send is still valid when Fyers validates it.
    if dt.datetime.now().second % 30 >= 28:
        time.sleep(3)
    otp = pyotp.TOTP(FYERS_TOTP_SECRET).now()
    r = s.post(f"{_VAGATOR_BASE}/verify_otp", json={
        "request_key": request_key,
        "otp": otp,
    }, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Step 2 verify_otp failed {r.status_code}: {r.text[:300]}")
    request_key = r.json().get("request_key")
    if not request_key:
        raise RuntimeError(f"Step 2: no request_key in response: {r.text[:300]}")

    # ---- Step 3: verify_pin_v2 ----
    r = s.post(f"{_VAGATOR_BASE}/verify_pin_v2", json={
        "request_key":   request_key,
        "identity_type": "pin",
        "identifier":    _b64(FYERS_PIN),
    }, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Step 3 verify_pin_v2 failed {r.status_code}: {r.text[:300]}")
    jwt = r.json().get("data", {}).get("access_token")
    if not jwt:
        raise RuntimeError(f"Step 3: no JWT in response: {r.text[:300]}")

    # ---- Step 4: api/v3/token → returns redirect URL with auth_code ----
    # Fyers returns HTTP 308 (Permanent Redirect) here with the auth_code
    # embedded in the JSON body's "Url" field. allow_redirects=False so
    # `requests` doesn't try to follow to localhost (which would fail).
    s.headers.update({"Authorization": f"Bearer {jwt}"})
    if "-" in FYERS_CLIENT_ID:
        app_id_only, app_type = FYERS_CLIENT_ID.rsplit("-", 1)
    else:
        app_id_only, app_type = FYERS_CLIENT_ID, "100"
    r = s.post(f"{_API_V3_BASE}/token", json={
        "fyers_id":       FYERS_ID,
        "app_id":         app_id_only,
        "redirect_uri":   FYERS_REDIRECT_URI,
        "appType":        app_type,
        "code_challenge": "",
        "state":          "sample_state",
        "scope":          "",
        "nonce":          "",
        "response_type":  "code",
        "create_cookie":  True,
    }, timeout=15, allow_redirects=False)
    # 200 and 308 are both expected success statuses here
    if r.status_code not in (200, 308):
        raise RuntimeError(f"Step 4 token failed {r.status_code}: {r.text[:300]}")
    auth_url = r.json().get("Url", "")
    if "auth_code=" not in auth_url:
        raise RuntimeError(f"Step 4: no auth_code in Url: {r.text[:300]}")
    auth_code = auth_url.split("auth_code=")[1].split("&")[0]

    # ---- Step 5: validate-authcode → access_token + refresh_token ----
    app_hash = hashlib.sha256(
        f"{FYERS_CLIENT_ID}:{FYERS_SECRET_KEY}".encode()
    ).hexdigest()
    r = requests.post(f"{_API_V3_BASE}/validate-authcode", json={
        "grant_type":  "authorization_code",
        "appIdHash":   app_hash,
        "code":        auth_code,
    }, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"Step 5 validate-authcode failed {r.status_code}: {r.text[:300]}")
    j = r.json()
    access = j.get("access_token")
    refresh = j.get("refresh_token", "")
    if not access:
        raise RuntimeError(f"Step 5: no access_token in response: {r.text[:300]}")
    _save_tokens(access, refresh)
    log.info("Login OK. Tokens saved to fyers_tokens.json.")
    return access, refresh

def fyers_refresh_token(refresh: str) -> Optional[str]:
    """Try refresh-token flow. Returns new access_token or None on failure."""
    if not refresh:
        return None
    app_hash = hashlib.sha256(
        f"{FYERS_CLIENT_ID}:{FYERS_SECRET_KEY}".encode()
    ).hexdigest()
    try:
        r = requests.post(f"{_API_V3_BASE}/validate-refresh-token", json={
            "grant_type":    "refresh_token",
            "appIdHash":     app_hash,
            "refresh_token": refresh,
            "pin":           FYERS_PIN,
        }, timeout=15)
        if r.status_code != 200:
            return None
        return r.json().get("access_token")
    except Exception:
        return None

def fyers_ensure_valid_token(force: bool = False) -> str:
    """Return a valid access token. Refreshes or re-logs in as needed."""
    if not force:
        toks = _load_tokens()
        if toks and toks.get("access_token"):
            # Cheap validity probe — one quote call
            test = requests.get(
                f"{_FYERS_BASE}/data/quotes?symbols=NSE:NIFTY50-INDEX",
                headers={"Authorization": f"{FYERS_CLIENT_ID}:{toks['access_token']}"},
                timeout=10,
            )
            if test.status_code == 200:
                return toks["access_token"]
            # Try refresh
            new_token = fyers_refresh_token(toks.get("refresh_token", ""))
            if new_token:
                _save_tokens(new_token, toks.get("refresh_token", ""))
                return new_token
    # Full re-login
    access, _ = fyers_auto_login()
    return access


# ============================================================================
#  FYERS API  —  quotes and depth, with batching
# ============================================================================

def _fyers_get(token: str, url: str, retries: int = 2) -> dict:
    """GET wrapper with one 429 retry. Throws on persistent failure."""
    for attempt in range(retries):
        r = requests.get(url, headers={
            "Authorization": f"{FYERS_CLIENT_ID}:{token}"
        }, timeout=15)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429 and attempt < retries - 1:
            log.warning("429 from Fyers, retrying in 3s...")
            time.sleep(3)
            continue
        raise RuntimeError(f"Fyers {r.status_code}: {r.text[:300]}")
    raise RuntimeError("Fyers GET retries exhausted")

def fyers_fetch_quotes(token: str, symbols: List[str]) -> Dict[str, dict]:
    """Batch quote fetcher. /data/quotes accepts up to 50 symbols per call."""
    out: Dict[str, dict] = {}
    for i in range(0, len(symbols), 50):
        if i > 0:
            time.sleep(0.25)
        batch = symbols[i:i + 50]
        url = f"{_FYERS_BASE}/data/quotes?symbols={','.join(batch)}"
        j = _fyers_get(token, url)
        for item in j.get("d", []):
            if not item or "v" not in item:
                continue
            v = item["v"]
            out[item["n"]] = {
                "ltp":       float(v.get("lp", 0) or 0),
                "open":      float(v.get("open_price", 0) or 0),
                "high":      float(v.get("high_price", 0) or 0),
                "low":       float(v.get("low_price", 0) or 0),
                "prevClose": float(v.get("prev_close_price", 0) or 0),
                "chg":       float(v.get("ch", 0) or 0),
                "chgPct":    float(v.get("chp", 0) or 0),
                "vol":       int(v.get("volume", 0) or 0),
            }
    return out

def fyers_fetch_depth(token: str, symbols: List[str]) -> Dict[str, dict]:
    """
    Fetch OI data for futures symbols via /data/depth.

    Fyers's /data/depth accepts only ONE symbol per call. We parallelize
    across DEPTH_PARALLEL_WORKERS threads to bring a 30-symbol fetch from
    ~6 seconds (sequential) down to ~1.5 seconds.

    4 workers + ~250ms typical call time = peak ~16 req/sec averaged across
    bursts. Well under Fyers's 200 req/min limit, brushing the 10 req/sec
    momentary cap only during the burst — covered by the 429 retry.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _one(sym: str) -> Tuple[str, Optional[dict]]:
        url = f"{_FYERS_BASE}/data/depth?symbol={sym}&ohlcv_flag=1"
        try:
            j = _fyers_get(token, url)
        except RuntimeError as e:
            log.warning(f"depth fetch failed for {sym}: {e}")
            return sym, None
        d = j.get("d", {})
        if not d:
            return sym, None
        # Response keys back what we sent — take the first (only) entry
        for resp_sym, v in d.items():
            if not v:
                return sym, None
            oi = float(v.get("oi", 0) or 0)
            prev_oi = float(v.get("pdoi") or v.get("oiprevday")
                            or v.get("oi_previous") or v.get("prev_oi")
                            or v.get("oi_yest") or 0)
            oi_chg = oi - prev_oi
            oi_chg_pct = (oi_chg / prev_oi * 100) if prev_oi else 0.0
            return resp_sym, {
                "oi": oi, "prevOi": prev_oi,
                "oiChg": oi_chg, "oiChgPct": oi_chg_pct,
            }
        return sym, None

    out: Dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=DEPTH_PARALLEL_WORKERS) as ex:
        for fut in as_completed(ex.submit(_one, s) for s in symbols):
            sym, data = fut.result()
            if data:
                out[sym] = data
    return out

def current_fut_suffix() -> str:
    """NSE monthly expiry = last Tuesday of month (post Sep 2025 rule)."""
    months = ["JAN","FEB","MAR","APR","MAY","JUN",
              "JUL","AUG","SEP","OCT","NOV","DEC"]
    now = dt.datetime.now()
    y, m = now.year, now.month
    # Find last Tuesday of (y, m)
    import calendar
    last_day = calendar.monthrange(y, m)[1]
    last_tue = max(d for d in range(1, last_day + 1)
                   if dt.date(y, m, d).weekday() == 1)
    if now.day > last_tue:
        m += 1
        if m > 12:
            m = 1; y += 1
    return f"{str(y)[-2:]}{months[m-1]}FUT"

def fyers_fetch_fno_slice(token: str, tickers: List[str]) -> List[dict]:
    """Fetch spot + futures-depth for an arbitrary slice of tickers."""
    if not tickers:
        return []
    suffix = current_fut_suffix()
    spot_syms = [f"NSE:{t}-EQ" for t in tickers]
    fut_syms  = [f"NSE:{t}{suffix}" for t in tickers]
    spot = fyers_fetch_quotes(token, spot_syms)
    fut  = fyers_fetch_depth(token, fut_syms)
    out = []
    for t in tickers:
        s = spot.get(f"NSE:{t}-EQ", {})
        f = fut.get(f"NSE:{t}{suffix}", {})
        out.append({
            "ticker":    t,
            "ltp":       s.get("ltp", 0),
            "open":      s.get("open", 0),
            "high":      s.get("high", 0),
            "low":       s.get("low", 0),
            "prevClose": s.get("prevClose", 0),
            "chgPct":    s.get("chgPct", 0),
            "vol":       s.get("vol", 0),
            "oi":        f.get("oi", 0),
            "oiChgPct":  f.get("oiChgPct", 0),
        })
    return out


# ============================================================================
#  ROTATIONAL SLICE  &  LOCAL CACHE
# ============================================================================

def compute_current_slice(universe: List[str]) -> Tuple[List[str], int]:
    """Return (slice_tickers, slice_index) based on minute-of-day."""
    now = dt.datetime.now()
    minute_of_day = now.hour * 60 + now.minute
    idx = minute_of_day % ROTATION_SLICES
    size = -(-len(universe) // ROTATION_SLICES)  # ceil divide
    start = idx * size
    end = min(start + size, len(universe))
    return universe[start:end], idx

def _slim_cache_for_persist(cache: Dict[str, dict]) -> Dict[str, Any]:
    """
    Build a minimal cache containing ONLY what must survive between the
    morning and afternoon CI jobs:
      - per-ticker signal_history + conviction_history (for stability/reversal)
      - the 'performers' rolling state (for Persistent Performers)
    The full computed snapshot (ltp, grade, why, etc.) is regenerated every
    cycle anyway, so we don't persist it — keeps the blob small.
    """
    slim: Dict[str, Any] = {}
    for tk, v in cache.items():
        if tk == "performers":
            slim["performers"] = v
            continue
        if not isinstance(v, dict):
            continue
        sh = v.get("signal_history")
        ch = v.get("conviction_history")
        if sh or ch:
            slim[tk] = {"ticker": tk,
                        "signal_history": sh or [],
                        "conviction_history": ch or []}
    return slim


# How many sheet cells to split the cache blob across (A1, A2, A3...).
# Each cell holds <50k chars; 4 cells = up to ~196k chars of base64,
# enough for a full 200-ticker cache (~106k) with headroom.
_CACHE_CELL_COUNT = 4
_CACHE_CELL_SIZE  = 45000   # chars per cell, safely under the 50k cap


def read_cache() -> Dict[str, dict]:
    # Local file first (fast path, used on your laptop)
    if CACHE_FILE.exists():
        try:
            return pickle.load(open(CACHE_FILE, "rb"))
        except Exception:
            log.warning("Cache file unreadable, starting fresh.")
    # CI fallback: GitHub jobs are ephemeral (no shared disk between morning
    # and afternoon jobs), so read the cache from the hidden _state_cache
    # sheet, where it's stored as a base64 pickle blob split across A1:A{N}.
    if RUNNING_IN_CI:
        try:
            ss = get_sheet()
            ws = ss.worksheet(SHEET_NAMES["CACHE"])
            col = ws.col_values(1)   # all of column A, top to bottom
            blob = "".join(c for c in col[:_CACHE_CELL_COUNT] if c)
            if blob:
                raw = base64.b64decode(blob.encode("ascii"))
                cache = pickle.loads(raw)
                log.info(f"Cache restored from sheet "
                         f"({len([k for k in cache if k != 'performers'])} tickers).")
                return cache
        except Exception as e:
            log.warning(f"Could not restore cache from sheet: {e}. Starting fresh.")
    return {}


def write_cache(cache: Dict[str, dict]) -> None:
    # Always write the local file (harmless in CI, useful locally)
    try:
        pickle.dump(cache, open(CACHE_FILE, "wb"))
    except Exception as e:
        log.warning(f"Local cache write failed: {e}")
    # In CI, ALSO persist a SLIM cache to the hidden sheet so the next job
    # inherits the histories + performers state. Split across A1:A{N} cells
    # because a full cache exceeds the 50k single-cell limit.
    if RUNNING_IN_CI:
        try:
            slim = _slim_cache_for_persist(cache)
            raw = pickle.dumps(slim, protocol=pickle.HIGHEST_PROTOCOL)
            blob = base64.b64encode(raw).decode("ascii")
            chunks = [blob[i:i + _CACHE_CELL_SIZE]
                      for i in range(0, len(blob), _CACHE_CELL_SIZE)]
            if len(chunks) > _CACHE_CELL_COUNT:
                log.warning(f"Slim cache {len(blob)} chars needs "
                            f"{len(chunks)} cells (> {_CACHE_CELL_COUNT}). "
                            f"Truncating persist — increase _CACHE_CELL_COUNT.")
                chunks = chunks[:_CACHE_CELL_COUNT]
            # Pad to fixed cell count so stale lower cells get cleared
            while len(chunks) < _CACHE_CELL_COUNT:
                chunks.append("")
            ss = get_sheet()
            ws = ss.worksheet(SHEET_NAMES["CACHE"])
            ws.update(range_name=f"A1:A{_CACHE_CELL_COUNT}",
                      values=[[c] for c in chunks],
                      value_input_option="RAW")
        except Exception as e:
            log.warning(f"Could not persist cache to sheet: {e}")


# ============================================================================
#  ANALYSIS  —  port of tdAnalyzeFnoStocks_ logic
# ============================================================================

def minutes_since_open() -> int:
    """Indian market opens at 09:15 IST. Returns minutes since open, 0..375."""
    now = dt.datetime.now()
    open_t = now.replace(hour=9, minute=15, second=0, microsecond=0)
    delta = (now - open_t).total_seconds() / 60
    return max(0, min(375, int(delta)))

def _percentile(value: float, sorted_vals: List[float]) -> int:
    """0–100 percentile rank via binary search."""
    n = len(sorted_vals)
    if n == 0:
        return 50
    lo, hi = 0, n
    while lo < hi:
        mid = (lo + hi) // 2
        if sorted_vals[mid] < value:
            lo = mid + 1
        else:
            hi = mid
    return round(lo / n * 100)

def compute_market_regime(broad: Dict[str, dict]) -> dict:
    """Classify market into STRONG_BULL / BULL / RANGEBOUND / BEAR / STRONG_BEAR."""
    nifty = broad.get("NSE:NIFTY50-INDEX", {})
    vix = broad.get("NSE:INDIAVIX-INDEX", {})
    n_chg = nifty.get("chgPct", 0)
    v_chg = vix.get("chgPct", 0)
    if n_chg > 0.75 and v_chg < 0:
        tag = "STRONG_BULL"
    elif n_chg > 0.25:
        tag = "BULL"
    elif n_chg < -0.75 and v_chg > 0:
        tag = "STRONG_BEAR"
    elif n_chg < -0.25:
        tag = "BEAR"
    else:
        tag = "RANGEBOUND"
    return {"tag": tag, "niftyChg": n_chg, "vixChg": v_chg,
            "vixLevel": vix.get("ltp", 0)}

def rank_sectors(sectors: Dict[str, dict], broad: Dict[str, dict]) -> List[dict]:
    """Rank sectors by % change, tag as LEADER / FOLLOWER / LAGGARD / etc."""
    nifty_chg = broad.get("NSE:NIFTY50-INDEX", {}).get("chgPct", 0)
    rows = []
    for label, sym in SECTORAL_INDICES:
        s = sectors.get(sym, {})
        chg = s.get("chgPct", 0)
        rng = s.get("high", 0) - s.get("low", 0)
        rng_pos = ((s.get("ltp", 0) - s.get("low", 0)) / rng) if rng > 0 else 0.5
        rs = chg - nifty_chg
        if   rs > 0.3 and rng_pos > 0.7:   tag = "LEADER"
        elif rs > 0.1 and rng_pos > 0.5:   tag = "FOLLOWER"
        elif rs < -0.3 and rng_pos < 0.3:  tag = "LAGGARD"
        elif rs > 0.1 and rng_pos < 0.4:   tag = "REVERSING_UP"
        elif rs < -0.1 and rng_pos > 0.6:  tag = "REVERSING_DOWN"
        else: tag = "NEUTRAL"
        rows.append({"label": label, "symbol": sym, "chgPct": chg,
                     "rangePos": rng_pos, "rs": rs, "tag": tag,
                     "ltp": s.get("ltp", 0)})
    rows.sort(key=lambda r: r["chgPct"], reverse=True)
    return rows

def analyze_fno_stocks(stocks: List[dict], broad: Dict[str, dict]) -> List[dict]:
    """Tag with OI signal, compute conviction across full universe."""
    nifty_chg = broad.get("NSE:NIFTY50-INDEX", {}).get("chgPct", 0)
    tm = minutes_since_open()
    total_m = 6 * 60 + 15
    enriched = []
    for s in stocks:
        chg = s.get("chgPct", 0)
        oi_chg = s.get("oiChgPct", 0)
        if chg > 0 and oi_chg > 0:    signal = "LONG_BUILDUP"
        elif chg > 0 and oi_chg < 0:  signal = "SHORT_COVERING"
        elif chg < 0 and oi_chg > 0:  signal = "SHORT_BUILDUP"
        elif chg < 0 and oi_chg < 0:  signal = "LONG_UNWINDING"
        else:                          signal = "NEUTRAL"
        rng = s.get("high", 0) - s.get("low", 0)
        range_pos = ((s.get("ltp", 0) - s.get("low", 0)) / rng) if rng > 0 else 0.5
        rs = chg - nifty_chg
        # Volume surge: vol relative to expected pro-rated full-day volume.
        # Previously hardcoded to 1.0 in the first 30 minutes — which killed
        # the volume signal during the highest-information window of the day.
        # Fix: linearly blend from neutral (1.0) at the open to full surge
        # calculation at minute 30. After 30 min, use raw pro-rated surge.
        ef = max(tm / total_m, 0.01)
        raw_surge = (s.get("vol", 0) or 0) / ef
        if tm < 30:
            # Blend weight: 0.0 at the open, 1.0 at minute 30
            w = max(0.0, min(1.0, tm / 30.0))
            vol_surge = (1.0 - w) * 1.0 + w * raw_surge
        else:
            vol_surge = raw_surge
        if signal == "LONG_BUILDUP":
            pc, oc = chg, oi_chg
        elif signal == "SHORT_BUILDUP":
            pc, oc = -chg, oi_chg
        elif signal == "SHORT_COVERING":
            pc, oc = chg, -oi_chg
        elif signal == "LONG_UNWINDING":
            pc, oc = -chg, -oi_chg
        else:
            pc, oc = abs(chg), abs(oi_chg)
        rsc = -rs if signal in ("SHORT_BUILDUP", "LONG_UNWINDING") else rs
        rng_c = range_pos if signal in ("LONG_BUILDUP", "SHORT_COVERING") else (1 - range_pos)
        raw = (WEIGHTS["priceChange"] * pc + WEIGHTS["oiBuildup"] * oc +
               WEIGHTS["relativeStrength"] * rsc + WEIGHTS["rangePosition"] * rng_c * 5)
        e = dict(s)
        e.update({"signal": signal, "rs": rs, "rangePos": range_pos,
                  "volSurge": vol_surge, "rawScore": raw})
        enriched.append(e)
    # Percentile-normalize
    surge_vals = sorted(e["volSurge"] for e in enriched)
    score_vals = sorted(e["rawScore"] for e in enriched)
    for e in enriched:
        sp = _percentile(e["volSurge"], surge_vals)
        cp = _percentile(e["rawScore"], score_vals)
        e["volSurgePctile"] = sp
        e["conviction"] = round(0.7 * cp + 0.3 * sp)
    return enriched


def time_decay_factor() -> float:
    """
    Weight signals by how much trading time remains in the day.
    A LONG_BUILDUP at 9:30 has the whole day to play out → factor 1.0.
    The same signal at 14:30 has only 1 hour left → factor 0.45.
    """
    tm = minutes_since_open()
    if tm < 60:    return 1.00     # first hour: highest weight
    if tm < 150:   return 0.90     # mid-morning
    if tm < 240:   return 0.75     # lunch/post-lunch
    if tm < 330:   return 0.55     # afternoon
    return 0.35                     # last 45 min: signals can barely run


def compute_room_to_move(stock: dict) -> Tuple[int, str]:
    """
    How much room remains for this signal to play out?
    Returns (score 0-100, reason).

    For longs (LONG_BUILDUP / SHORT_COVERING):
        Already +3% = limited (score 20)
        Already +1.5% = some room (60)
        Below +0.75% = lots of room (95) — ideal early entry
    For shorts: same but on the negative side.
    """
    chg = stock.get("chgPct", 0)
    signal = stock.get("signal", "NEUTRAL")
    if signal in ("LONG_BUILDUP", "SHORT_COVERING"):
        if chg > 3.0:   return 15, "extended +3%"
        if chg > 2.0:   return 35, "+2% already in"
        if chg > 1.0:   return 65, "moderate move"
        if chg > 0.25:  return 90, "early — room to run"
        return 95, "flat — bottom-fishing zone"
    if signal in ("SHORT_BUILDUP", "LONG_UNWINDING"):
        if chg < -3.0:  return 15, "extended -3%"
        if chg < -2.0:  return 35, "-2% already in"
        if chg < -1.0:  return 65, "moderate move"
        if chg < -0.25: return 90, "early — room to fall"
        return 95, "flat — top-spotting zone"
    return 50, ""


def compute_stability(stock: dict) -> Tuple[int, str]:
    """
    How consistently has this stock's conviction held?
    Uses signal_history (list of {signal, conviction} dicts) attached during
    refresh — last N readings.
    Returns (score 0-100, reason). Also sets stock['reversal_flag'] when the
    current signal is a fresh flip vs the recent history — a high-quality
    intraday pattern that scoring alone misses.

    Insight: a stock that just spiked to 90 is noise; one that's held 70+
    across multiple readings is a real setup. A SHORT signal that follows
    several minutes of LONG signals is a reversal — that's actionable.
    """
    history = stock.get("signal_history", [])
    current_sig = stock.get("signal", "NEUTRAL")
    current_conv = stock.get("conviction", 0)

    # Backward compatibility: older cache entries might have just numbers
    if history and isinstance(history[0], (int, float)):
        # Legacy format — treat as conviction-only, no reversal detection
        avg = sum(history) / len(history)
        mn = min(history)
        if len(history) < 3:
            return 60, f"only {len(history)} readings"
        if mn >= 65:
            return 95, f"steady — all >65 over {len(history)} reads"
        if avg >= 65 and current_conv >= 65:
            return 80, f"avg {round(avg)} — holding"
        if current_conv > avg + 20:
            return 30, "just spiked — likely noise"
        if avg < 40:
            return 25, "weak history"
        return 55, "mixed"

    if not history:
        return 50, "new — no history yet"

    # New format — list of {signal, conviction}
    convs = [h.get("conviction", 0) for h in history]
    sigs  = [h.get("signal", "NEUTRAL") for h in history]
    avg = sum(convs) / len(convs)
    mn = min(convs)

    # ── Reversal detection ─────────────────────────────────────────
    # If current signal is OPPOSITE direction to majority of recent
    # readings AND current conviction is decent, that's a fresh reversal.
    LONG_SIGS  = ("LONG_BUILDUP", "SHORT_COVERING")
    SHORT_SIGS = ("SHORT_BUILDUP", "LONG_UNWINDING")

    def _direction(s: str) -> str:
        if s in LONG_SIGS:  return "LONG"
        if s in SHORT_SIGS: return "SHORT"
        return "NEUTRAL"

    if len(history) >= 4 and current_conv >= 50:
        recent_dirs = [_direction(s) for s in sigs[-4:]]
        cur_dir = _direction(current_sig)
        # At least 3 of last 4 readings were the OPPOSITE direction
        opposite = "SHORT" if cur_dir == "LONG" else ("LONG" if cur_dir == "SHORT" else None)
        if opposite and recent_dirs.count(opposite) >= 3 and cur_dir != "NEUTRAL":
            # Find how long ago the last opposite signal was
            n_back = next((i + 1 for i, d in enumerate(reversed(recent_dirs))
                           if d == opposite), len(recent_dirs))
            stock["reversal_flag"] = f"REVERSAL: was {opposite.lower()} {n_back} min ago"
            return 75, f"fresh reversal (was {opposite.lower()})"

    if len(history) < 3:
        return 60, f"only {len(history)} readings"
    if mn >= 65:
        return 95, f"steady — all >65 over {len(history)} reads"
    if avg >= 65 and current_conv >= 65:
        return 80, f"avg {round(avg)} — holding"
    if current_conv > avg + 20:
        return 30, "just spiked — likely noise"
    if avg < 40:
        return 25, "weak history"
    return 55, "mixed"


def _sector_score_for_signal(stock: dict, sector_ranked: List[dict]) -> Tuple[int, str]:
    """
    Sector confluence: a LONG signal in a Leading sector deserves a boost;
    a LONG signal in a Lagging sector deserves a penalty. Mirror for SHORTs.
    Returns (score 0-100, short reason).
    """
    ticker = stock.get("ticker", "")
    signal = stock.get("signal", "NEUTRAL")
    sector_label = STOCK_SECTOR.get(ticker)
    if not sector_label or not sector_ranked:
        return 50, ""

    # Find this stock's sector in the ranked list
    sec = next((r for r in sector_ranked if r["label"] == sector_label), None)
    if sec is None:
        return 50, ""

    tag = sec.get("tag", "NEUTRAL")
    direction_is_long = signal in ("LONG_BUILDUP", "SHORT_COVERING")
    direction_is_short = signal in ("SHORT_BUILDUP", "LONG_UNWINDING")

    # LONG in LEADER/FOLLOWER/REVERSING_UP = confluence; LONG in LAGGARD = headwind
    if direction_is_long:
        if tag == "LEADER":         return 95, f"long in leading {sector_label}"
        if tag == "FOLLOWER":       return 80, f"long in follower {sector_label}"
        if tag == "REVERSING_UP":   return 75, f"long in reversing-up {sector_label}"
        if tag == "NEUTRAL":        return 50, ""
        if tag == "REVERSING_DOWN": return 25, f"long in fading {sector_label}"
        if tag == "LAGGARD":        return 15, f"long in laggard {sector_label}"

    # SHORT in LAGGARD/REVERSING_DOWN = confluence; SHORT in LEADER = headwind
    if direction_is_short:
        if tag == "LAGGARD":        return 95, f"short in laggard {sector_label}"
        if tag == "REVERSING_DOWN": return 80, f"short in fading {sector_label}"
        if tag == "NEUTRAL":        return 50, ""
        if tag == "FOLLOWER":       return 30, f"short in follower {sector_label}"
        if tag == "REVERSING_UP":   return 25, f"short in reversing-up {sector_label}"
        if tag == "LEADER":         return 15, f"short in leading {sector_label}"

    return 50, ""


def compute_setup_quality(stock: dict,
                          sector_ranked: Optional[List[dict]] = None) -> dict:
    """
    The COMPOSITE that turns a snapshot into a decision.

    Combines: current conviction + multi-minute stability + how much room
    the signal still has + sector confluence + time-of-day weighting.
    Output is a grade A-D plus a numeric score 0-100, with a one-line 'why'.

    Sector confluence (NEW): a LONG in a Leading sector or a SHORT in a
    Laggard sector gets the full 10% sector weight; a counter-trend signal
    (LONG in laggard) gets penalized. Previously this was hardcoded to 50.
    """
    conv = stock.get("conviction", 0)
    if stock.get("signal", "NEUTRAL") == "NEUTRAL" or conv < 30:
        return {"quality": 0, "grade": "D", "direction": "—", "why": "no signal"}

    stab, stab_reason = compute_stability(stock)
    room, room_reason = compute_room_to_move(stock)
    sector, sector_reason = _sector_score_for_signal(stock, sector_ranked or [])
    decay = time_decay_factor()

    score = (
        QUALITY_WEIGHTS["conviction"] * conv +
        QUALITY_WEIGHTS["stability"]  * stab +
        QUALITY_WEIGHTS["room"]       * room +
        QUALITY_WEIGHTS["sector"]     * sector
    ) * decay

    quality = round(score)
    if   quality >= 70: grade = "A"
    elif quality >= 55: grade = "B"
    elif quality >= 40: grade = "C"
    else:               grade = "D"

    # Direction tag — what action does this signal suggest?
    if stock["signal"] in ("LONG_BUILDUP", "SHORT_COVERING"):
        direction = "LONG"
    elif stock["signal"] in ("SHORT_BUILDUP", "LONG_UNWINDING"):
        direction = "SHORT"
    else:
        direction = "—"

    why_parts = []
    if stab_reason and stab_reason != "mixed":
        why_parts.append(stab_reason)
    if room_reason:
        why_parts.append(room_reason)
    if sector_reason:
        why_parts.append(sector_reason)
    # Reversal flag (set by compute_stability) — surface prominently
    if stock.get("reversal_flag"):
        why_parts.insert(0, stock["reversal_flag"])
    if decay < 0.7:
        why_parts.append(f"late ({minutes_since_open()} min in)")
    why = "; ".join(why_parts) if why_parts else "—"

    return {"quality": quality, "grade": grade,
            "direction": direction, "why": why}


def attach_setup_quality(stocks: List[dict],
                         sector_ranked: Optional[List[dict]] = None) -> List[dict]:
    """Compute and attach Setup Quality to every stock in place."""
    for s in stocks:
        q = compute_setup_quality(s, sector_ranked)
        s.update(q)
    return stocks


# ============================================================================
#  GOOGLE SHEETS WRITER
# ============================================================================

def get_sheet():
    global _SHEET
    if _SHEET:
        return _SHEET
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Path 1 — JSON string in an env var (GitHub Actions, from a Secret).
    if SERVICE_ACCOUNT_JSON:
        try:
            info = json.loads(SERVICE_ACCOUNT_JSON)
        except json.JSONDecodeError as e:
            log.error(f"GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON: {e}")
            sys.exit(1)
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=scopes)
        _SHEET = gspread.authorize(creds).open_by_key(SHEET_ID)
        return _SHEET

    # Path 2 — local file (running on your laptop).
    if not SERVICE_ACCOUNT_FILE.exists():
        log.error(f"No credentials: set GOOGLE_SERVICE_ACCOUNT_JSON env var, "
                  f"or place service_account.json at {SERVICE_ACCOUNT_FILE}")
        sys.exit(1)
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE), scopes=scopes)
    _SHEET = gspread.authorize(creds).open_by_key(SHEET_ID)
    return _SHEET

def load_universe_from_sheet() -> List[str]:
    """Read F&O tickers from Universe Config column A (skip header)."""
    try:
        # Try new name first, fall back to legacy
        try:
            ws = get_sheet().worksheet(SHEET_NAMES["CONFIG"])
        except gspread.WorksheetNotFound:
            ws = get_sheet().worksheet("TD_Config")
        col = ws.col_values(1)
        return [t.strip() for t in col[1:] if t.strip()]
    except Exception as e:
        log.error(f"Could not read universe sheet: {e}")
        return []

def write_fno_sheet(ss, stocks: List[dict], sector_ranked: List[dict],
                    ts: dt.datetime):
    """
    F&O Live — all stocks with current state + Setup Quality grade + Sector.
    Sorting:
      1) By sector rank (best-performing sector first, worst last)
      2) "OTHER" sector stocks sorted to the bottom
      3) Within each sector, by Setup Quality (descending)
    Sector mapping comes from STOCK_SECTOR dict (see top of file).
    """
    ws = ss.worksheet(SHEET_NAMES["FNO"])

    # Build sector → rank lookup from the day's sector performance ordering.
    # sector_ranked is already sorted best → worst by rank_sectors().
    sector_rank = {r["label"]: i for i, r in enumerate(sector_ranked)}
    OTHER_RANK = 9999

    def sort_key(s: dict):
        sec = STOCK_SECTOR.get(s["ticker"], "OTHER")
        rank = sector_rank.get(sec, OTHER_RANK)
        return (rank, -s.get("quality", 0), s["ticker"])

    sorted_stocks = sorted(stocks, key=sort_key)

    rows = []
    for s in sorted_stocks:
        sec = STOCK_SECTOR.get(s["ticker"], "OTHER")
        rows.append([s["ticker"], sec, s["signal"], s.get("direction", ""),
                     round(s.get("ltp", 0), 2),
                     round(s.get("chgPct", 0) / 100, 4),
                     round(s.get("oiChgPct", 0) / 100, 4),
                     round(s.get("rs", 0) / 100, 4),
                     int(s.get("volSurgePctile", 0)),
                     round(s.get("rangePos", 0), 2),
                     int(s.get("conviction", 0)),
                     s.get("grade", ""),
                     int(s.get("quality", 0))])

    # Clear a WIDE range to wipe any orphan data from prior sheet structures
    ws.batch_clear(["A2:Z10000"])
    if rows:
        ws.update(range_name=f"A2:M{len(rows)+1}", values=rows,
                  value_input_option="USER_ENTERED")

def _compute_contributions(weights: Dict[str, float],
                           stocks_by_ticker: Dict[str, dict],
                           index_ltp: float, index_chg_pct: float
                           ) -> Tuple[List[List[Any]], float, float]:
    """
    Build a sorted list of constituent contribution rows for one index.

    Math: contribution_pts ≈ (stock_chg% / 100) × (weight% / 100) × index_prev_close
          where index_prev_close = index_ltp / (1 + index_chg_pct/100)

    This is the standard approximation every financial site uses. The
    actual NSE formula uses free-float mcap and an index divisor, but for
    weight × pct_change × prev_close the answer is within ~1% of NSE's
    published figures when weights are fresh.

    Returns (rows, total_pts, actual_pts) where:
        rows         = list of [Ticker, Weight%, LTP, Chg%, Pts] sorted by Pts desc
        total_pts    = sum of contributions we tracked
        actual_pts   = index's actual point move today (for coverage check)
    """
    if index_ltp <= 0 or not weights:
        return [], 0.0, 0.0

    # broad stores chgPct as percent (e.g. 0.45 = 0.45%, NOT 45%)
    prev_close = index_ltp / (1 + (index_chg_pct / 100.0))
    actual_pts = index_ltp - prev_close

    rows: List[List[Any]] = []
    total_pts = 0.0
    for ticker, weight in weights.items():
        s = stocks_by_ticker.get(ticker)
        if s is None:
            # Stock not in our universe — show row with N/A so user notices
            rows.append([ticker, round(weight, 2), "—", "—", "—"])
            continue
        ltp = s.get("ltp", 0)
        chg_pct = s.get("chgPct", 0)            # already in % form
        # contribution in index points
        pts = (chg_pct / 100.0) * (weight / 100.0) * prev_close
        total_pts += pts
        rows.append([
            ticker,
            round(weight, 2),
            round(ltp, 2),
            round(chg_pct / 100, 4),    # store as decimal so cell can be %-formatted
            round(pts, 2),
        ])

    # Sort: rows where contribution is a number (not "—"), by pts desc.
    # "—" rows go to the bottom so they're easy to spot.
    def _sort_key(r):
        pts = r[4]
        if isinstance(pts, (int, float)):
            return (0, -pts)   # numeric first, by descending
        return (1, 0)          # "—" last

    rows.sort(key=_sort_key)
    return rows, total_pts, actual_pts


def write_market_sheet(ss, broad: Dict[str, dict], regime: dict,
                       stocks: List[dict], ts: dt.datetime):
    """
    Market Overview, three sections (top to bottom):
      1. Broad indices               (columns A-E)
      2. + 3. Constituent contribs   (side-by-side: A-E = NIFTY 50, G-K = BANK NIFTY)

    Side-by-side layout means the user sees both contribution tables at a
    glance without scrolling, and can compare "what's moving NIFTY" vs
    "what's moving BANK NIFTY" with their eyes on the same screen.
    """
    ws = ss.worksheet(SHEET_NAMES["MARKET"])

    stocks_by_t: Dict[str, dict] = {s["ticker"]: s for s in stocks}

    # ── Block 1: Broad indices (unchanged, single column block) ─────
    top_rows: List[List[Any]] = []
    for label, sym in BROAD_INDICES:
        b = broad.get(sym, {})
        top_rows.append([label, round(b.get("ltp", 0), 2),
                         round(b.get("chgPct", 0) / 100, 4),
                         round(b.get("high", 0), 2),
                         round(b.get("low", 0), 2)])
    top_rows.append(["", "", "", "", ""])
    top_rows.append(["Regime", regime["tag"], "", "", ""])
    top_rows.append(["Last update", ts.strftime("%Y-%m-%d %H:%M:%S"), "", "", ""])
    top_rows.append(["", "", "", "", ""])      # blank gap before contributors

    # ── Compute contributions for both indices ──────────────────────
    # Loads weights from the 'Index Weights' sheet (cached 5 min), falling
    # back to hardcoded defaults if the sheet is empty/unreachable. This
    # lets the user paste fresh weights from Dhan/NSE without touching code.
    nifty_w, bank_w = load_weights_from_sheet(ss)

    nifty = broad.get("NSE:NIFTY50-INDEX", {})
    n_ltp = nifty.get("ltp", 0)
    n_chg = nifty.get("chgPct", 0)
    n_rows, n_total, n_actual = _compute_contributions(
        nifty_w, stocks_by_t, n_ltp, n_chg)
    n_coverage = (n_total / n_actual * 100) if abs(n_actual) > 0.01 else 0

    bnf = broad.get("NSE:NIFTYBANK-INDEX", {})
    b_ltp = bnf.get("ltp", 0)
    b_chg = bnf.get("chgPct", 0)
    b_rows, b_total, b_actual = _compute_contributions(
        bank_w, stocks_by_t, b_ltp, b_chg)
    b_coverage = (b_total / b_actual * 100) if abs(b_actual) > 0.01 else 0

    # ── Block 2 + 3: Side-by-side contribution tables ───────────────
    # Layout is an 11-column wide matrix:
    #   A-E (cols 0-4)  = NIFTY 50 contributions
    #   F   (col 5)     = blank separator column
    #   G-K (cols 6-10) = BANK NIFTY contributions
    EMPTY5 = ["", "", "", "", ""]
    PAD = ["", "", "", "", "", "", "", "", "", "", ""]

    def _combine(left5: List[Any], right5: List[Any]) -> List[Any]:
        """Stitch two 5-col rows into one 11-col row with a blank gap."""
        return list(left5) + [""] + list(right5)

    contrib_rows: List[List[Any]] = []

    # Section title row (both tables aligned)
    contrib_rows.append(_combine(
        [f"NIFTY 50 CONTRIBUTORS (weights: {WEIGHTS_LAST_REFRESHED})",
         "", "", "", ""],
        [f"BANK NIFTY CONTRIBUTORS (weights: {WEIGHTS_LAST_REFRESHED})",
         "", "", "", ""],
    ))

    # Column header row
    contrib_rows.append(_combine(
        ["Ticker", "Weight %", "LTP", "Chg %", "Pts Contributed"],
        ["Ticker", "Weight %", "LTP", "Chg %", "Pts Contributed"],
    ))

    # Interleave the two stock lists. NIFTY 50 has 50 rows, BANK NIFTY has 12 —
    # the shorter side gets blank padding so the table edges stay aligned.
    n_data_rows = len(n_rows)
    b_data_rows = len(b_rows)
    max_data = max(n_data_rows, b_data_rows)
    for i in range(max_data):
        left = n_rows[i] if i < n_data_rows else list(EMPTY5)
        right = b_rows[i] if i < b_data_rows else list(EMPTY5)
        contrib_rows.append(_combine(left, right))

    # Totals row — uses normalized weights so coverage always sums to 100%
    contrib_rows.append(_combine(
        ["— Total (tracked) —",
         round(sum(nifty_w.values()), 2), "",
         round(n_chg / 100, 4), round(n_total, 2)],
        ["— Total (tracked) —",
         round(sum(bank_w.values()), 2), "",
         round(b_chg / 100, 4), round(b_total, 2)],
    ))

    # Coverage row — explanatory, not alarmist
    def _coverage_label(cov: float) -> str:
        """Coverage interpretation. 95-105% is normal noise from price sync;
        outside that band suggests stale weight ratios (refresh) or
        constituent changes."""
        if cov == 0:                  return "(index flat — coverage N/A)"
        if 95 <= cov <= 105:          return "(weights look healthy)"
        if 90 <= cov < 95 or 105 < cov <= 110:
                                       return "(slight drift — fine for now)"
        return "(check weights — run --refresh-weights)"

    contrib_rows.append(_combine(
        [f"NIFTY move: {round(n_actual, 2)} pts | Coverage: "
         f"{round(n_coverage, 1)}% {_coverage_label(n_coverage)}",
         "", "", "", ""],
        [f"BANK NIFTY move: {round(b_actual, 2)} pts | Coverage: "
         f"{round(b_coverage, 1)}% {_coverage_label(b_coverage)}",
         "", "", "", ""],
    ))

    # ── Single batch write ──────────────────────────────────────────
    ws.batch_clear(["A2:Z300"])

    # Top block (broad indices): A2:E(2+n-1)
    top_n = len(top_rows)
    ws.update(range_name=f"A2:E{top_n + 1}",
              values=top_rows, value_input_option="USER_ENTERED")

    # Contributors block: starts immediately after top block, columns A-K
    contrib_start_row = 2 + top_n
    contrib_n = len(contrib_rows)
    contrib_end_row = contrib_start_row + contrib_n - 1
    ws.update(range_name=f"A{contrib_start_row}:K{contrib_end_row}",
              values=contrib_rows, value_input_option="USER_ENTERED")

    # ── Track row positions for formatting (1-indexed sheet rows) ───
    # Title row: contrib_start_row
    # Header row: contrib_start_row + 1
    # Data rows: contrib_start_row + 2 to contrib_start_row + 1 + max_data
    # Totals row: contrib_start_row + 2 + max_data
    # Coverage row: contrib_start_row + 3 + max_data
    title_row    = contrib_start_row
    header_row   = contrib_start_row + 1
    data_first   = contrib_start_row + 2
    data_last    = data_first + max_data - 1
    totals_row   = data_last + 1
    coverage_row = data_last + 2

    # ── Formatting — every dark fill gets explicit WHITE text ───────
    DARK_BG    = {"red": 0.10, "green": 0.10, "blue": 0.18}
    MID_BG     = {"red": 0.20, "green": 0.20, "blue": 0.28}
    WHITE_FG   = {"red": 1, "green": 1, "blue": 1}
    DARK_HEADER_STYLE = {
        "backgroundColor": DARK_BG,
        "textFormat": {"bold": True, "foregroundColor": WHITE_FG},
    }
    COLHEADER_STYLE = {
        "backgroundColor": MID_BG,
        "textFormat": {"bold": True, "foregroundColor": WHITE_FG},
    }

    # Title strip — both halves
    try:
        ws.format(f"A{title_row}:E{title_row}", DARK_HEADER_STYLE)
        ws.format(f"G{title_row}:K{title_row}", DARK_HEADER_STYLE)
    except Exception:
        pass

    # Column-header strip — both halves
    try:
        ws.format(f"A{header_row}:E{header_row}", COLHEADER_STYLE)
        ws.format(f"G{header_row}:K{header_row}", COLHEADER_STYLE)
    except Exception:
        pass

    # Totals row — subtle highlight (no dark fill, just bold)
    try:
        ws.format(f"A{totals_row}:E{totals_row}",
                  {"textFormat": {"bold": True}})
        ws.format(f"G{totals_row}:K{totals_row}",
                  {"textFormat": {"bold": True}})
    except Exception:
        pass

    # Coverage row — italic, slightly muted
    try:
        ws.format(f"A{coverage_row}:E{coverage_row}",
                  {"textFormat": {"italic": True}})
        ws.format(f"G{coverage_row}:K{coverage_row}",
                  {"textFormat": {"italic": True}})
    except Exception:
        pass

    # Percentage format on the Chg% columns: D in NIFTY block, J in BANK block.
    # Apply to data rows + totals row so both sides display correctly.
    pct_fmt = {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}}
    try:
        ws.format(f"D{data_first}:D{totals_row}", pct_fmt)
        ws.format(f"J{data_first}:J{totals_row}", pct_fmt)
    except Exception:
        pass

def write_sectors_sheet(ss, sector_ranked: List[dict], ts: dt.datetime):
    """Sector Rotation — full ranked list (detail view; Dashboard has summary)."""
    ws = ss.worksheet(SHEET_NAMES["SECTORS"])
    rows = [[i + 1, r["label"], r["tag"], round(r["chgPct"] / 100, 4),
             round(r["rs"] / 100, 4), round(r["rangePos"], 2)]
            for i, r in enumerate(sector_ranked)]
    ws.batch_clear(["A2:Z200"])
    ws.update(range_name=f"A2:F{len(rows)+1}", values=rows,
              value_input_option="USER_ENTERED")

def write_oi_activity_sheet(ss, stocks: List[dict], ts: dt.datetime):
    """OI Activity — all non-NEUTRAL stocks. Use Sheets' filter on Signal."""
    ws = ss.worksheet(SHEET_NAMES["OI_ACTIVITY"])
    signal_order = {"LONG_BUILDUP": 1, "SHORT_BUILDUP": 2,
                    "SHORT_COVERING": 3, "LONG_UNWINDING": 4}
    actionable = [s for s in stocks if s["signal"] in signal_order]
    actionable.sort(key=lambda s: (signal_order[s["signal"]],
                                    -s.get("quality", 0)))
    rows = [[s["signal"], s["ticker"], round(s.get("ltp", 0), 2),
             round(s.get("chgPct", 0) / 100, 4),
             round(s.get("oiChgPct", 0) / 100, 4),
             int(s.get("volSurgePctile", 0)),
             round(s.get("rangePos", 0), 2),
             int(s.get("conviction", 0)),
             s.get("grade", ""),
             int(s.get("quality", 0))]
            for s in actionable]
    ws.batch_clear(["A2:Z10000"])
    if rows:
        ws.update(range_name=f"A2:J{len(rows)+1}", values=rows,
                  value_input_option="USER_ENTERED")

def write_conviction_sheet(ss, stocks: List[dict], ts: dt.datetime):
    """
    Top Conviction — now filtered to Grade A and B setups only, sorted by
    Setup Quality (not raw conviction). This is the watchlist.
    """
    ws = ss.worksheet(SHEET_NAMES["CONVICTION"])
    candidates = [s for s in stocks
                  if s.get("grade") in ("A", "B")
                  and s.get("direction") in ("LONG", "SHORT")]
    candidates.sort(key=lambda x: x.get("quality", 0), reverse=True)
    candidates = candidates[:30]
    rows = [[i + 1, s["ticker"], s["direction"], s["grade"],
             int(s.get("quality", 0)), s["signal"],
             round(s.get("ltp", 0), 2),
             round(s.get("chgPct", 0) / 100, 4),
             round(s.get("oiChgPct", 0) / 100, 4),
             int(s.get("conviction", 0)),
             s.get("why", "")]
            for i, s in enumerate(candidates)]
    ws.batch_clear(["A2:Z10000"])
    if rows:
        ws.update(range_name=f"A2:K{len(rows)+1}", values=rows,
                  value_input_option="USER_ENTERED")

def write_dashboard(ss, regime: dict, sector_ranked: List[dict],
                    stocks: List[dict], ts: dt.datetime):
    """
    Comprehensive single-view Dashboard with FIXED-BLOCK LAYOUT.

    Why fixed-block: previously the Top LONG / Top SHORT setup rows expanded
    or shrank with each refresh (empty placeholder vs up to 5 rows each),
    which pushed OI counters down or up and broke formatting. Now every
    section sits at a hard-coded row range; missing setups are padded with
    "—" so the block height is constant.

    Also enforces INTEGER format on OI counter cells — fixes the 6000.00%
    display bug caused by stale percentage formatting on those cells.
    """
    ws = ss.worksheet(SHEET_NAMES["DASHBOARD"])

    # ── FIXED LAYOUT ANCHORS ─────────────────────────────────────────
    R_TITLE          = 1
    R_MARKET_HDR     = 3
    R_REGIME, R_NIFTY, R_VIX = 4, 5, 6
    R_LEADERS_HDR    = 8
    R_LEADERS_START  = 9          # 3 rows: 9,10,11
    R_LAGGARDS_HDR   = 13
    R_LAGGARDS_START = 14         # 3 rows: 14,15,16
    R_LONGS_HDR      = 18
    R_LONGS_START    = 19         # 5 rows reserved: 19–23
    LONGS_BLOCK_N    = 5
    R_SHORTS_HDR     = 25
    R_SHORTS_START   = 26         # 5 rows reserved: 26–30
    SHORTS_BLOCK_N   = 5
    R_COUNTERS_HDR   = 32
    R_COUNTERS_DATA  = 33         # 2x2 grid: rows 33, 34

    PAD_SETUP = ["—", "", "", "", ""]

    # ── Filter and sort setups ───────────────────────────────────────
    actionable = [s for s in stocks
                  if s.get("grade") in ("A", "B")
                  and s.get("direction") in ("LONG", "SHORT")]
    longs = sorted([s for s in actionable if s["direction"] == "LONG"],
                   key=lambda x: x.get("quality", 0), reverse=True)[:LONGS_BLOCK_N]
    shorts = sorted([s for s in actionable if s["direction"] == "SHORT"],
                    key=lambda x: x.get("quality", 0), reverse=True)[:SHORTS_BLOCK_N]

    # ── Activity counters ────────────────────────────────────────────
    cnt = {"LONG_BUILDUP": 0, "SHORT_BUILDUP": 0,
           "SHORT_COVERING": 0, "LONG_UNWINDING": 0}
    for s in stocks:
        if s["signal"] in cnt:
            cnt[s["signal"]] += 1

    # ── Helper: convert setup dict to 5-col row ──────────────────────
    def _setup_row(s):
        return [s["ticker"], s["grade"], int(s.get("quality", 0)),
                round(s.get("chgPct", 0) / 100, 4), s.get("why", "")]

    def _pad(rows, n):
        rows = rows[:n]
        while len(rows) < n:
            rows.append(PAD_SETUP)
        return rows

    # ── Clear ONLY the data ranges we control (preserves any user notes) ──
    ws.batch_clear([f"A{R_TITLE}:E{R_COUNTERS_DATA + 1}"])

    # ── Title strip + LIVE/STALE health indicator ────────────────────
    # Cell C1 is a Google Sheets formula that reads cell B1 (the refresh
    # timestamp) and compares with NOW(). Anything older than 3 minutes
    # shows '⚠ STALE — worker may be down'. Recalculates whenever the
    # sheet is viewed/edited, so as soon as you look, you see the truth.
    health_formula = (
        '=IF(NOW()-DATEVALUE(B1)-TIMEVALUE(MID(B1,12,8))>TIMEVALUE("00:03:00"),'
        '"⚠ STALE — check worker","✓ LIVE")'
    )
    ws.update(range_name=f"A{R_TITLE}:C{R_TITLE}",
              values=[["MK TOP-DOWN DASHBOARD",
                       ts.strftime("%Y-%m-%d %H:%M:%S"),
                       health_formula]],
              value_input_option="USER_ENTERED")

    # ── Market state ─────────────────────────────────────────────────
    ws.update(range_name=f"A{R_MARKET_HDR}:E{R_MARKET_HDR}",
              values=[["MARKET STATE", "", "", "", ""]],
              value_input_option="USER_ENTERED")
    ws.update(range_name=f"A{R_REGIME}:C{R_VIX}", values=[
        ["Regime",         regime["tag"],                              ""],
        ["NIFTY 50 % chg", round(regime["niftyChg"] / 100, 4),         ""],
        ["VIX level",      regime["vixLevel"],
         f"({'+' if regime['vixChg']>=0 else ''}{round(regime['vixChg'],2)}%)"],
    ], value_input_option="USER_ENTERED")

    # ── Sector leaders (always 3 rows) ───────────────────────────────
    leaders_rows = [[r["label"], round(r["chgPct"] / 100, 4),
                     round(r["rs"] / 100, 4), r["tag"]]
                    for r in sector_ranked[:3]]
    while len(leaders_rows) < 3:
        leaders_rows.append(["—", 0, 0, ""])
    ws.update(range_name=f"A{R_LEADERS_HDR}:D{R_LEADERS_HDR}",
              values=[["SECTOR LEADERS", "Chg %", "RS vs NIFTY", "Tag"]],
              value_input_option="USER_ENTERED")
    ws.update(range_name=f"A{R_LEADERS_START}:D{R_LEADERS_START + 2}",
              values=leaders_rows, value_input_option="USER_ENTERED")

    # ── Sector laggards (always 3 rows) ──────────────────────────────
    laggards_rows = [[r["label"], round(r["chgPct"] / 100, 4),
                      round(r["rs"] / 100, 4), r["tag"]]
                     for r in sector_ranked[-3:]]
    while len(laggards_rows) < 3:
        laggards_rows.append(["—", 0, 0, ""])
    ws.update(range_name=f"A{R_LAGGARDS_HDR}:D{R_LAGGARDS_HDR}",
              values=[["SECTOR LAGGARDS", "Chg %", "RS vs NIFTY", "Tag"]],
              value_input_option="USER_ENTERED")
    ws.update(range_name=f"A{R_LAGGARDS_START}:D{R_LAGGARDS_START + 2}",
              values=laggards_rows, value_input_option="USER_ENTERED")

    # ── Top LONG setups (FIXED 5 rows, padded) ───────────────────────
    longs_rows = _pad([_setup_row(s) for s in longs], LONGS_BLOCK_N)
    ws.update(range_name=f"A{R_LONGS_HDR}:E{R_LONGS_HDR}",
              values=[["TOP LONG SETUPS", "Grade", "Quality", "Chg %", "Why"]],
              value_input_option="USER_ENTERED")
    ws.update(range_name=f"A{R_LONGS_START}:E{R_LONGS_START + LONGS_BLOCK_N - 1}",
              values=longs_rows, value_input_option="USER_ENTERED")

    # ── Top SHORT setups (FIXED 5 rows, padded) ──────────────────────
    shorts_rows = _pad([_setup_row(s) for s in shorts], SHORTS_BLOCK_N)
    ws.update(range_name=f"A{R_SHORTS_HDR}:E{R_SHORTS_HDR}",
              values=[["TOP SHORT SETUPS", "Grade", "Quality", "Chg %", "Why"]],
              value_input_option="USER_ENTERED")
    ws.update(range_name=f"A{R_SHORTS_START}:E{R_SHORTS_START + SHORTS_BLOCK_N - 1}",
              values=shorts_rows, value_input_option="USER_ENTERED")

    # ── Activity counters — explicit integers, anchored row ──────────
    ws.update(range_name=f"A{R_COUNTERS_HDR}:E{R_COUNTERS_HDR}",
              values=[["OI ACTIVITY COUNTERS", "", "", "", ""]],
              value_input_option="USER_ENTERED")
    ws.update(range_name=f"A{R_COUNTERS_DATA}:D{R_COUNTERS_DATA + 1}", values=[
        ["Long Buildup",   int(cnt["LONG_BUILDUP"]),
         "Short Buildup",  int(cnt["SHORT_BUILDUP"])],
        ["Short Covering", int(cnt["SHORT_COVERING"]),
         "Long Unwinding", int(cnt["LONG_UNWINDING"])],
    ], value_input_option="USER_ENTERED")

    # ── Format section header bands ──────────────────────────────────
    header_rows = [R_TITLE, R_MARKET_HDR, R_LEADERS_HDR, R_LAGGARDS_HDR,
                   R_LONGS_HDR, R_SHORTS_HDR, R_COUNTERS_HDR]
    for r_idx in header_rows:
        try:
            ws.format(f"A{r_idx}:E{r_idx}", {
                "backgroundColor": {"red": 0.10, "green": 0.10, "blue": 0.18},
                "textFormat": {"bold": True,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            })
        except Exception:
            pass

    # ── KILL THE 6000% BUG: force integer format on counter cells ────
    # (Cells B33, D33, B34, D34 must be plain integers, NOT percentages.)
    try:
        ws.format(f"B{R_COUNTERS_DATA}:B{R_COUNTERS_DATA + 1}",
                  {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
        ws.format(f"D{R_COUNTERS_DATA}:D{R_COUNTERS_DATA + 1}",
                  {"numberFormat": {"type": "NUMBER", "pattern": "0"}})
    except Exception:
        pass

    # ── Percentage format on the numeric Chg% / RS columns ───────────
    try:
        ws.format(f"B{R_NIFTY}",
                  {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format(f"B{R_LEADERS_START}:C{R_LEADERS_START + 2}",
                  {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format(f"B{R_LAGGARDS_START}:C{R_LAGGARDS_START + 2}",
                  {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format(f"D{R_LONGS_START}:D{R_LONGS_START + LONGS_BLOCK_N - 1}",
                  {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
        ws.format(f"D{R_SHORTS_START}:D{R_SHORTS_START + SHORTS_BLOCK_N - 1}",
                  {"numberFormat": {"type": "PERCENT", "pattern": "0.00%"}})
    except Exception:
        pass


def append_activity_row(ss, broad: Dict[str, dict], sector_ranked: List[dict],
                        stocks: List[dict], ts: dt.datetime):
    """Append one row to Activity Log — market-level summary per minute."""
    ws = ss.worksheet(SHEET_NAMES["ACTIVITY"])
    cnt = {"LONG_BUILDUP": 0, "SHORT_BUILDUP": 0,
           "SHORT_COVERING": 0, "LONG_UNWINDING": 0}
    for s in stocks:
        if s["signal"] in cnt:
            cnt[s["signal"]] += 1
    top = max(stocks, key=lambda x: x.get("conviction", 0), default={})
    row = [
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        broad.get("NSE:NIFTY50-INDEX", {}).get("ltp", 0),
        broad.get("NSE:NIFTYBANK-INDEX", {}).get("ltp", 0),
        broad.get("NSE:INDIAVIX-INDEX", {}).get("ltp", 0),
        sector_ranked[0]["label"] if sector_ranked else "",
        sector_ranked[-1]["label"] if sector_ranked else "",
        cnt["LONG_BUILDUP"], cnt["SHORT_BUILDUP"],
        cnt["SHORT_COVERING"], cnt["LONG_UNWINDING"],
        top.get("ticker", ""), int(top.get("conviction", 0)),
    ]
    ws.append_row(row, value_input_option="USER_ENTERED")

def append_snapshots(ss, fresh_stocks: List[dict],
                     broad: Dict[str, dict], ts: dt.datetime):
    """Per-stock snapshots — only freshly-fetched stocks (clean backtest data).
    Now includes Grade/Setup Quality/Direction so the methodology audit can
    measure hit rates by grade and direction over time."""
    if not fresh_stocks:
        return
    ws = ss.worksheet(SHEET_NAMES["SNAPSHOTS"])
    nifty_chg = broad.get("NSE:NIFTY50-INDEX", {}).get("chgPct", 0)
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
    rows = [[ts_str, s["ticker"], round(s.get("ltp", 0), 2),
             round(s.get("chgPct", 0) / 100, 5),
             round(s.get("oiChgPct", 0) / 100, 5),
             int(s.get("vol", 0)), s["signal"], int(s.get("conviction", 0)),
             round(s.get("rangePos", 0), 3), round(nifty_chg / 100, 5),
             s.get("grade", "D"), int(s.get("quality", 0)),
             s.get("direction", "—")]
            for s in fresh_stocks]
    ws.append_rows(rows, value_input_option="USER_ENTERED")


# ============================================================================
#  PERSISTENT PERFORMERS  —  rolling 5-day Grade A/B counts per ticker
# ============================================================================
# State lives in the cache file under key 'performers' as:
#   {'YYYY-MM-DD': {'TICKER': {'minutes_AB': int, 'last_grade': str,
#                              'last_dir': str, 'last_seen': 'HH:MM:SS'}}}
# Each refresh increments minutes_AB for every Grade A/B ticker.
# On startup (and on day-roll), days older than 5 trading days are trimmed.

PERFORMERS_RETENTION_DAYS = 5

def _update_performers_state(cache: dict, analyzed: List[dict],
                             ts: dt.datetime) -> dict:
    """Update the rolling performers state. Returns the updated cache."""
    perf = cache.setdefault("performers", {})
    today_str = ts.strftime("%Y-%m-%d")
    today_perf = perf.setdefault(today_str, {})

    for s in analyzed:
        if s.get("grade") in ("A", "B") and s.get("direction") in ("LONG", "SHORT"):
            tk = s["ticker"]
            entry = today_perf.setdefault(tk, {"minutes_AB": 0,
                                                "last_grade": "",
                                                "last_dir": "",
                                                "last_seen": ""})
            entry["minutes_AB"] += 1
            entry["last_grade"] = s["grade"]
            entry["last_dir"]   = s["direction"]
            entry["last_seen"]  = ts.strftime("%H:%M:%S")

    # Trim: keep only the most recent N trading days
    sorted_days = sorted(perf.keys(), reverse=True)
    keep = set(sorted_days[:PERFORMERS_RETENTION_DAYS])
    for d in list(perf.keys()):
        if d not in keep:
            del perf[d]

    cache["performers"] = perf
    return cache


def write_performers_sheet(ss, cache: dict, ts: dt.datetime) -> None:
    """Render the 5-day rolling Grade A/B leaderboard to the dashboard tab."""
    perf = cache.get("performers", {})
    if not perf:
        return

    # Aggregate per-ticker across all days in window
    agg: Dict[str, dict] = {}
    for day, day_data in perf.items():
        for tk, info in day_data.items():
            a = agg.setdefault(tk, {"days": set(), "total_min": 0,
                                     "last_seen": "", "last_seen_day": "",
                                     "last_grade": "", "last_dir": ""})
            a["days"].add(day)
            a["total_min"] += info.get("minutes_AB", 0)
            # Keep the most recent last_seen across the window
            this_seen = f"{day} {info.get('last_seen', '')}"
            if this_seen > (a["last_seen_day"] + " " + a["last_seen"]):
                a["last_seen"] = info.get("last_seen", "")
                a["last_seen_day"] = day
                a["last_grade"] = info.get("last_grade", "")
                a["last_dir"]   = info.get("last_dir", "")

    # Build rows sorted by total minutes desc, then by days seen desc
    rows = []
    for tk, a in agg.items():
        rows.append([
            tk,
            STOCK_SECTOR.get(tk, "OTHER"),
            len(a["days"]),
            a["total_min"],
            f"{a['last_seen_day']} {a['last_seen']}".strip(),
            a["last_grade"],
            a["last_dir"],
        ])
    rows.sort(key=lambda r: (r[3], r[2]), reverse=True)

    ws = ss.worksheet(SHEET_NAMES["PERFORMERS"])
    ws.batch_clear(["A2:G1000"])
    if rows:
        ws.update(range_name=f"A2:G{len(rows) + 1}",
                  values=rows, value_input_option="USER_ENTERED")


# ============================================================================
#  METHODOLOGY AUDIT  —  hit-rate backtest on existing Snapshots Log
# ============================================================================
# Reads the Snapshots Log sheet directly. For each historical Grade A/B
# signal, looks up the same ticker's LTP 5, 15, 30, 60 minutes later
# (same day) and computes the realised move. Aggregates hit rates and
# average moves by grade, direction, signal type, and time-of-day bucket.
# Output: a new "Methodology Audit" sheet with the summary tables.
#
# Run from CLI:  python mk_topdown_worker.py --audit
# Safe to run any time — read-only on Snapshots, writes to its own sheet.

def run_methodology_audit():
    """Read Snapshots Log, compute hit rates by grade/direction/time bucket."""
    log.info("=" * 60)
    log.info("METHODOLOGY AUDIT — reading Snapshots Log")
    log.info("=" * 60)
    ss = get_sheet()
    try:
        ws = ss.worksheet(SHEET_NAMES["SNAPSHOTS"])
    except gspread.WorksheetNotFound:
        log.error("Snapshots Log sheet not found. Nothing to audit.")
        return

    rows = ws.get_all_values()
    if len(rows) < 2:
        log.error("Snapshots Log is empty. Need at least a few hours of data.")
        return

    header = rows[0]
    data = rows[1:]
    log.info(f"Snapshots: {len(data)} rows")

    # Column index helpers — tolerate the old schema (no Grade/Quality/Dir)
    def col(name: str, default: int = -1) -> int:
        return header.index(name) if name in header else default

    ts_i      = col("Timestamp")
    tk_i      = col("Ticker")
    ltp_i     = col("LTP")
    sig_i     = col("Signal")
    grade_i   = col("Grade")
    dir_i     = col("Direction")
    qual_i    = col("Setup Quality")

    if grade_i < 0:
        log.warning("Older snapshot schema — Grade/Direction not stored. "
                    "Audit will use conviction ≥ 70 as proxy for 'high quality'.")
    conv_i    = col("Conviction")

    # Parse into a list of dicts, index by (ticker, timestamp)
    parsed = []
    for r in data:
        try:
            ts = dt.datetime.strptime(r[ts_i], "%Y-%m-%d %H:%M:%S")
            parsed.append({
                "ts": ts,
                "ticker": r[tk_i],
                "ltp": float(r[ltp_i]) if r[ltp_i] else 0,
                "signal": r[sig_i],
                "conviction": int(r[conv_i]) if conv_i >= 0 and r[conv_i] else 0,
                "grade": r[grade_i] if grade_i >= 0 else "",
                "direction": r[dir_i] if dir_i >= 0 else "",
                "quality": int(r[qual_i]) if qual_i >= 0 and r[qual_i] else 0,
            })
        except (ValueError, IndexError):
            continue

    log.info(f"Parsed {len(parsed)} valid rows")

    # Build a fast price lookup: {(ticker, date): [(time, ltp), ...] sorted}
    price_idx: Dict[Tuple[str, dt.date], List[Tuple[dt.datetime, float]]] = {}
    for p in parsed:
        key = (p["ticker"], p["ts"].date())
        price_idx.setdefault(key, []).append((p["ts"], p["ltp"]))
    for v in price_idx.values():
        v.sort()

    def _ltp_at(ticker: str, target: dt.datetime) -> Optional[float]:
        """Find the first snapshot >= target for same ticker, same day."""
        prices = price_idx.get((ticker, target.date()))
        if not prices:
            return None
        for t, lt in prices:
            if t >= target and lt > 0:
                return lt
        return None

    # Compute forward returns for each "high quality" signal
    HORIZONS_MIN = [5, 15, 30, 60]
    results = []   # one row per qualifying signal
    for p in parsed:
        is_high_quality = (
            (p["grade"] in ("A", "B")) if grade_i >= 0
            else (p["conviction"] >= 70)
        )
        if not is_high_quality:
            continue
        direction = p["direction"] if dir_i >= 0 else (
            "LONG" if p["signal"] in ("LONG_BUILDUP", "SHORT_COVERING")
            else ("SHORT" if p["signal"] in ("SHORT_BUILDUP", "LONG_UNWINDING")
                  else "—")
        )
        if direction not in ("LONG", "SHORT") or p["ltp"] <= 0:
            continue

        moves = {}
        for h in HORIZONS_MIN:
            future_ltp = _ltp_at(p["ticker"], p["ts"] + dt.timedelta(minutes=h))
            if future_ltp is None:
                moves[h] = None
                continue
            raw_move = (future_ltp - p["ltp"]) / p["ltp"]
            # "Correct" direction: LONG wants positive, SHORT wants negative
            signed_move = raw_move if direction == "LONG" else -raw_move
            moves[h] = signed_move

        results.append({
            "ts": p["ts"],
            "ticker": p["ticker"],
            "signal": p["signal"],
            "direction": direction,
            "grade": p["grade"],
            "quality": p["quality"],
            "conviction": p["conviction"],
            "hour": p["ts"].hour,
            "moves": moves,
        })

    if not results:
        log.error("No qualifying signals found for backtest.")
        return
    log.info(f"Found {len(results)} qualifying signals to evaluate")

    # ── Aggregations ───────────────────────────────────────────────────
    def _summarize(subset: List[dict]) -> dict:
        out = {"n": len(subset)}
        for h in HORIZONS_MIN:
            vals = [r["moves"][h] for r in subset if r["moves"].get(h) is not None]
            if not vals:
                out[f"avg_{h}m"] = None
                out[f"hit_{h}m"] = None
                continue
            avg = sum(vals) / len(vals)
            hits = sum(1 for v in vals if v > 0)
            out[f"avg_{h}m"] = avg
            out[f"hit_{h}m"] = hits / len(vals)
        return out

    overall = _summarize(results)
    by_grade = {g: _summarize([r for r in results if r["grade"] == g])
                for g in ("A", "B") if any(r["grade"] == g for r in results)}
    by_dir = {d: _summarize([r for r in results if r["direction"] == d])
              for d in ("LONG", "SHORT")}
    by_signal = {sg: _summarize([r for r in results if r["signal"] == sg])
                 for sg in ("LONG_BUILDUP", "SHORT_BUILDUP",
                            "SHORT_COVERING", "LONG_UNWINDING")}
    by_hour = {h: _summarize([r for r in results if r["hour"] == h])
               for h in sorted({r["hour"] for r in results})}

    # ── Write audit sheet ──────────────────────────────────────────────
    audit_name = "Methodology Audit"
    try:
        aw = ss.worksheet(audit_name)
        aw.clear()
    except gspread.WorksheetNotFound:
        aw = ss.add_worksheet(title=audit_name, rows=200, cols=10)

    def _fmt(v):
        if v is None: return "—"
        if isinstance(v, float): return f"{v:.2%}"
        return v

    block_rows: List[List] = []
    block_rows.append([f"METHODOLOGY AUDIT — generated {dt.datetime.now().strftime('%Y-%m-%d %H:%M')}"])
    block_rows.append([f"Source: Snapshots Log ({len(parsed)} total rows; {len(results)} qualifying signals)"])
    block_rows.append([""])
    block_rows.append(["OVERALL", "Signals", "Hit% 5m", "Hit% 15m", "Hit% 30m", "Hit% 60m",
                       "Avg 5m", "Avg 15m", "Avg 30m", "Avg 60m"])
    block_rows.append(["All", overall["n"],
                       _fmt(overall["hit_5m"]), _fmt(overall["hit_15m"]),
                       _fmt(overall["hit_30m"]), _fmt(overall["hit_60m"]),
                       _fmt(overall["avg_5m"]), _fmt(overall["avg_15m"]),
                       _fmt(overall["avg_30m"]), _fmt(overall["avg_60m"])])
    block_rows.append([""])

    def _block(title: str, mapping: Dict[Any, dict], label: str = "Bucket"):
        block_rows.append([title, label, "Signals", "Hit% 5m", "Hit% 15m", "Hit% 30m", "Hit% 60m",
                           "Avg 15m", "Avg 30m", "Avg 60m"])
        for k, v in mapping.items():
            block_rows.append(["", str(k), v["n"],
                               _fmt(v["hit_5m"]), _fmt(v["hit_15m"]),
                               _fmt(v["hit_30m"]), _fmt(v["hit_60m"]),
                               _fmt(v["avg_15m"]), _fmt(v["avg_30m"]),
                               _fmt(v["avg_60m"])])
        block_rows.append([""])

    _block("BY GRADE", by_grade, "Grade")
    _block("BY DIRECTION", by_dir, "Direction")
    _block("BY SIGNAL TYPE", by_signal, "Signal")
    _block("BY HOUR (IST)", by_hour, "Hour")

    aw.update(range_name=f"A1:J{len(block_rows)}",
              values=block_rows, value_input_option="USER_ENTERED")
    # Header band
    aw.format("A1:J1", {
        "backgroundColor": {"red": 0.10, "green": 0.10, "blue": 0.18},
        "textFormat": {"bold": True,
                       "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
    })

    log.info(f"Audit written to '{audit_name}' sheet")
    log.info("=" * 60)
    print()
    print(f"Overall:  {overall['n']} signals  |  "
          f"Hit% 15m: {_fmt(overall['hit_15m'])}  |  "
          f"Hit% 30m: {_fmt(overall['hit_30m'])}  |  "
          f"Avg 30m: {_fmt(overall['avg_30m'])}")
    print()
    print("Open the 'Methodology Audit' sheet for full breakdown.")


# ============================================================================
#  BACKFILL FROM EXISTING SNAPSHOTS  —  warm-start instead of cold-start
# ============================================================================
# Walks Snapshots Log to reconstruct:
#   (a) The 5-day rolling Persistent Performers state
#   (b) The signal_history per ticker (last N readings) — so stability and
#       reversal detection work from minute 1 of the next live run instead of
#       needing 4-5 minutes of fresh data to warm up.
#
# For rows written before the schema upgrade (no Grade column), uses the
# legacy heuristic: conviction >= 70 + directional signal = treat as Grade B
# for performers counting. This matches what --audit does for old rows.
#
# Run from CLI:  python mk_topdown_worker.py --backfill-performers
# Safe to run any time — read-only on Snapshots, only writes the cache and
# the Persistent Performers sheet.

# Threshold used when a snapshot row has no Grade column (legacy data).
# Same value as the audit's "high quality proxy" — keeps the two consistent.
LEGACY_GRADE_AB_CONVICTION = 70


# ============================================================================
#  REFRESH WEIGHTS  —  pull current constituent list from NSE and report drift
# ============================================================================
# NSE doesn't expose daily index weights via a clean API — those live in
# monthly factsheet PDFs. But the *constituent list* is available at a
# reliable URL, so we can at least tell you:
#   - which stocks in our hardcoded dicts no longer belong to the index
#   - which stocks NSE lists that we're missing entirely
# That's the bulk of staleness. Exact weight ratios still need a monthly
# manual update from the factsheet — there's no honest way around that
# without paid data feeds.

def refresh_weights():
    """
    Inspect the 'Index Weights' sheet and report what's there vs the
    hardcoded defaults. Prints a concrete checklist for refreshing.

    NOTE: We deliberately do NOT auto-fetch from Dhan/NSE/Yahoo here.
    All three sit behind Cloudflare-style WAFs that reject Python's
    HTTP libraries. Pretending to work and then silently failing is
    worse than telling the user the truth: open the URL in a browser,
    copy, paste.
    """
    log.info("=" * 60)
    log.info("REFRESH WEIGHTS — checking Index Weights sheet vs hardcoded")
    log.info("=" * 60)

    ss = get_sheet()
    sheet_n, sheet_b = load_weights_from_sheet(ss)
    # Clear the cache so the next live refresh re-reads
    _weights_cache["fetched_at"] = None

    HARDCODED = {
        "NIFTY 50":   (NIFTY50_WEIGHTS,   NIFTY50_WEIGHTS_NORM,   sheet_n),
        "BANK NIFTY": (BANKNIFTY_WEIGHTS, BANKNIFTY_WEIGHTS_NORM, sheet_b),
    }
    URLS = {
        "NIFTY 50":   "https://dhan.co/indices/nifty-50-companies/",
        "BANK NIFTY": "https://dhan.co/indices/nifty-bank-share-price/",
    }

    for index_name, (raw, hardcoded_norm, sheet_norm) in HARDCODED.items():
        print()
        print(f"── {index_name} ──")
        in_sheet      = set(sheet_norm.keys())
        in_hardcoded  = set(hardcoded_norm.keys())

        sheet_sum_raw_set: List[float] = []   # for sum of raw sheet weights
        # We need raw sheet values. Re-read directly so we don't double-normalize.
        try:
            ws = ss.worksheet(SHEET_NAMES["WEIGHTS"])
            all_rows = ws.get_all_values()
            raw_sheet: Dict[str, float] = {}
            for r in all_rows[1:]:
                if len(r) < 3:
                    continue
                idx = str(r[0] or "").strip().upper()
                tkr = str(r[1] or "").strip().upper()
                wt_str = str(r[2]).strip().replace("%", "").strip() if r[2] != "" else ""
                if not tkr or not wt_str:
                    continue
                try:
                    wt = float(wt_str)
                except ValueError:
                    continue
                if (index_name == "NIFTY 50" and idx == "NIFTY 50") \
                   or (index_name == "BANK NIFTY" and idx in ("BANK NIFTY", "NIFTY BANK")):
                    raw_sheet[tkr] = wt
        except Exception as e:
            print(f"  Couldn't re-read sheet: {e}")
            raw_sheet = {}

        sheet_count   = len(raw_sheet)
        sheet_sum_raw = sum(raw_sheet.values()) if raw_sheet else 0

        print(f"  Source URL: {URLS[index_name]}")
        print(f"  Sheet:     {sheet_count} entries, raw sum {sheet_sum_raw:.2f}%")
        print(f"  Hardcoded: {len(raw)} entries, raw sum {sum(raw.values()):.2f}%")
        print()

        if raw_sheet:
            using = "SHEET (your edits)"
        else:
            using = "HARDCODED (sheet empty)"
        print(f"  Worker currently uses: {using}")
        print()

        # Differences: stocks in one but not the other
        only_in_sheet     = in_sheet - in_hardcoded
        only_in_hardcoded = in_hardcoded - in_sheet
        if only_in_sheet:
            print(f"  Stocks ONLY in sheet (not in hardcoded list):")
            for s in sorted(only_in_sheet):
                print(f"    + {s}  (weight {sheet_norm[s]:.2f}%)")
        if only_in_hardcoded:
            print(f"  Stocks ONLY in hardcoded list (not in sheet):")
            for s in sorted(only_in_hardcoded):
                print(f"    + {s}  (hardcoded {hardcoded_norm[s]:.2f}%)")
        if not only_in_sheet and not only_in_hardcoded:
            print(f"  ✓ Same constituent list in both.")

        # Coverage check on the source being used
        eff = sheet_norm if raw_sheet else hardcoded_norm
        eff_sum = sum(eff.values())
        print(f"  Effective (normalized) sum: {eff_sum:.4f}%  "
              f"(must be ~100%)")

    print()
    print("=" * 60)
    print("HOW TO UPDATE WEIGHTS")
    print("=" * 60)
    print("Why you need to do this manually:")
    print("  Dhan, NSE, Moneycontrol and Yahoo all block automated HTTP")
    print("  requests (Cloudflare WAF). Auto-scraping would be fragile.")
    print()
    print("Steps (~2 minutes per index, do quarterly or when coverage drifts):")
    print("  1. Open the Dhan URL above in your browser.")
    print("  2. On the page, find the constituents table.")
    print("  3. Select the Ticker and Weight columns (50 rows for NIFTY,")
    print("     12 rows for BANK NIFTY). Copy.")
    print(f"  4. Open the '{SHEET_NAMES['WEIGHTS']}' tab in your Google Sheet.")
    print("  5. Paste over column B (Ticker) and C (Weight %). Keep column A")
    print("     ('Index' value) intact — it tells the worker which index each")
    print("     row belongs to.")
    print("  6. Save. Worker picks up new values within 5 minutes (cached).")
    print()
    log.info("=" * 60)



    """
    Reconstruct the Persistent Performers state from Snapshots Log so the
    new sheet has the last 5 trading days of data instead of starting blank.
    Also seeds signal_history per ticker (last CONVICTION_HISTORY_LEN
    readings) so stability/reversal detection works from the next live run.
    """
    log.info("=" * 60)
    log.info("BACKFILL — rebuilding Persistent Performers + signal_history "
             "from Snapshots Log")
    log.info("=" * 60)
    ss = get_sheet()
    try:
        ws = ss.worksheet(SHEET_NAMES["SNAPSHOTS"])
    except gspread.WorksheetNotFound:
        log.error("Snapshots Log sheet not found. Nothing to backfill.")
        return

    rows = ws.get_all_values()
    if len(rows) < 2:
        log.error("Snapshots Log is empty — nothing to backfill.")
        return

    header = rows[0]
    data = rows[1:]
    log.info(f"Snapshots: {len(data)} rows")

    def col(name: str, default: int = -1) -> int:
        return header.index(name) if name in header else default

    ts_i    = col("Timestamp")
    tk_i    = col("Ticker")
    sig_i   = col("Signal")
    conv_i  = col("Conviction")
    grade_i = col("Grade")          # may be -1 on legacy rows
    dir_i   = col("Direction")      # may be -1 on legacy rows

    has_new_schema = grade_i >= 0
    if not has_new_schema:
        log.info(f"Legacy snapshots detected — using conviction >= "
                 f"{LEGACY_GRADE_AB_CONVICTION} + directional signal as "
                 f"Grade A/B proxy.")
    else:
        log.info("New-schema snapshots detected — using stored Grade column.")

    # ── Parse all rows ────────────────────────────────────────────────
    parsed = []   # list of dicts with ts, ticker, signal, conviction, [grade, direction]
    bad_rows = 0
    for r in data:
        try:
            ts = dt.datetime.strptime(r[ts_i], "%Y-%m-%d %H:%M:%S")
            ticker = r[tk_i]
            signal = r[sig_i]
            conviction = int(r[conv_i]) if r[conv_i] else 0
            entry = {"ts": ts, "ticker": ticker, "signal": signal,
                     "conviction": conviction}
            if has_new_schema:
                entry["grade"]     = r[grade_i] if grade_i < len(r) else ""
                entry["direction"] = r[dir_i] if dir_i < len(r) else ""
            parsed.append(entry)
        except (ValueError, IndexError):
            bad_rows += 1
            continue
    if bad_rows:
        log.warning(f"Skipped {bad_rows} malformed rows")
    log.info(f"Parsed {len(parsed)} valid rows")

    if not parsed:
        log.error("No parseable rows. Aborting backfill.")
        return

    # ── Identify the last 5 distinct trading days ─────────────────────
    distinct_days = sorted({p["ts"].date() for p in parsed}, reverse=True)
    keep_days = set(distinct_days[:PERFORMERS_RETENTION_DAYS])
    log.info(f"Keeping last {len(keep_days)} trading day(s): "
             f"{sorted(keep_days)}")

    # ── Direction helper ──────────────────────────────────────────────
    LONG_SIGS = ("LONG_BUILDUP", "SHORT_COVERING")
    SHORT_SIGS = ("SHORT_BUILDUP", "LONG_UNWINDING")

    def _direction_of(signal: str, stored: str = "") -> str:
        if stored in ("LONG", "SHORT"):
            return stored
        if signal in LONG_SIGS:  return "LONG"
        if signal in SHORT_SIGS: return "SHORT"
        return "—"

    def _is_grade_ab(entry: dict) -> bool:
        if has_new_schema:
            return entry.get("grade", "") in ("A", "B")
        # Legacy: proxy by conviction + directionality
        return (entry["conviction"] >= LEGACY_GRADE_AB_CONVICTION
                and entry["signal"] in LONG_SIGS + SHORT_SIGS)

    # ── Build the Performers state ────────────────────────────────────
    performers: Dict[str, Dict[str, dict]] = {}
    counted = 0
    for p in parsed:
        if p["ts"].date() not in keep_days:
            continue
        if not _is_grade_ab(p):
            continue
        day_str = p["ts"].strftime("%Y-%m-%d")
        day_data = performers.setdefault(day_str, {})
        tk = p["ticker"]
        entry = day_data.setdefault(tk, {"minutes_AB": 0, "last_grade": "",
                                          "last_dir": "", "last_seen": ""})
        entry["minutes_AB"] += 1
        entry["last_grade"] = p.get("grade") or ("B" if not has_new_schema else "")
        entry["last_dir"]   = _direction_of(p["signal"], p.get("direction", ""))
        # Keep the latest timestamp's HH:MM:SS as last_seen
        seen_str = p["ts"].strftime("%H:%M:%S")
        if seen_str > entry["last_seen"]:
            entry["last_seen"] = seen_str
        counted += 1
    log.info(f"Counted {counted} qualifying Grade A/B rows across "
             f"{len(performers)} day(s)")

    # ── Build signal_history per ticker (last N readings per ticker) ──
    by_ticker: Dict[str, List[dict]] = {}
    for p in parsed:
        by_ticker.setdefault(p["ticker"], []).append(p)
    histories: Dict[str, List[dict]] = {}
    conv_histories: Dict[str, List[int]] = {}
    for tk, plist in by_ticker.items():
        plist.sort(key=lambda x: x["ts"])
        tail = plist[-CONVICTION_HISTORY_LEN:]
        histories[tk] = [{"signal": q["signal"],
                          "conviction": q["conviction"]} for q in tail]
        conv_histories[tk] = [q["conviction"] for q in tail]
    log.info(f"Built signal_history for {len(histories)} tickers "
             f"(last {CONVICTION_HISTORY_LEN} readings each)")

    # ── Persist into cache ────────────────────────────────────────────
    cache = read_cache()
    cache["performers"] = performers
    for tk in by_ticker.keys():
        entry = cache.setdefault(tk, {})
        entry["signal_history"] = histories.get(tk, [])
        # Keep the legacy conviction_history in sync for back-compat
        entry["conviction_history"] = conv_histories.get(tk, [])
        cache[tk] = entry
    write_cache(cache)
    log.info("Cache updated.")

    # ── Render the Performers sheet immediately ───────────────────────
    # Make sure the sheet exists before writing.
    try:
        ss.worksheet(SHEET_NAMES["PERFORMERS"])
    except gspread.WorksheetNotFound:
        log.info("Persistent Performers tab missing — creating it.")
        new_ws = ss.add_worksheet(title=SHEET_NAMES["PERFORMERS"],
                                   rows=2000, cols=15)
        headers = SHEET_HEADERS[SHEET_NAMES["PERFORMERS"]]
        new_ws.update(range_name=f"A1:{chr(64 + len(headers))}1",
                      values=[headers], value_input_option="USER_ENTERED")
        new_ws.format(f"A1:{chr(64 + len(headers))}1", {
            "backgroundColor": {"red": 0.10, "green": 0.10, "blue": 0.18},
            "textFormat": {"bold": True,
                           "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        })
        new_ws.freeze(rows=1)

    write_performers_sheet(ss, cache, dt.datetime.now())
    log.info(f"'{SHEET_NAMES['PERFORMERS']}' sheet populated.")
    log.info("=" * 60)

    # ── Console summary ───────────────────────────────────────────────
    # Aggregate for display (matches what's now in the sheet)
    agg: Dict[str, dict] = {}
    for day, day_data in performers.items():
        for tk, info in day_data.items():
            a = agg.setdefault(tk, {"days": set(), "total_min": 0})
            a["days"].add(day)
            a["total_min"] += info["minutes_AB"]
    top10 = sorted(agg.items(),
                   key=lambda kv: (kv[1]["total_min"], len(kv[1]["days"])),
                   reverse=True)[:10]
    print()
    print(f"Backfill complete. {len(agg)} unique tickers across "
          f"{len(performers)} day(s).")
    print()
    print(f"Top 10 Persistent Performers (last {len(performers)} day(s)):")
    print(f"  {'Ticker':<14} {'Days':>5} {'Total Min A/B':>15}")
    for tk, a in top10:
        print(f"  {tk:<14} {len(a['days']):>5} {a['total_min']:>15}")
    print()
    print("Open the 'Persistent Performers' sheet for the full list.")


# ============================================================================
#  TRADINGAGENTS MULTI-LLM OVERLAY  —  selective second-opinion layer
# ============================================================================
# Why this design, not "TradingAgents runs everything":
#   1. TradingAgents takes 30-120s per ticker (multi-agent LLM debate).
#      Can't run on every 1-min bar × 208 tickers — that's 25k+ LLM calls
#      per minute and bankruptcy-level token spend.
#   2. TradingAgents is built around US equities + FinnHub. The Indian
#      data adapter below routes price lookups to yfinance (.NS suffix).
#   3. M-Score is the right primary engine — fast, deterministic, on-tape.
#      TradingAgents is the right SECOND opinion on a curated few.
#
# Triggers ONLY when: grade in {A, B} AND conviction ≥ OVERLAY_MIN_CONVICTION.
# Hard caps: max OVERLAY_MAX_ANALYSES_PER_DAY runs/day, per-ticker cooldown.
# Runs in background thread — never blocks the polling loop.
# Output: new "TradingAgents Overlay" sheet (auto-created on first write).
#
# To enable: set OVERLAY_ENABLED=True in CONFIG section + install:
#     pip install tradingagents yfinance
#     export ANTHROPIC_API_KEY=...
# ============================================================================

@dataclass
class OverlayRequest:
    timestamp: dt.datetime
    ticker: str
    direction: str               # "LONG" | "SHORT"
    worker_signal: str           # e.g. "LONG_BUILDUP"
    worker_grade: str            # "A" | "B"
    worker_conviction: int       # 0-100


@dataclass
class OverlayResult:
    timestamp: dt.datetime
    ticker: str
    worker_signal: str
    worker_grade: str
    worker_conviction: int
    agent_decision: str          # BUY | HOLD | SELL
    agent_confidence: float      # 0-1
    bull_thesis: str
    bear_thesis: str
    risk_flags: str
    time_taken_s: float


class _IndianMarketAdapter:
    """yfinance fallback for NSE tickers — TradingAgents' default FinnHub
    feed has thin Indian coverage. News lookup is a stub; wire to a real
    Indian feed (Moneycontrol RSS, Tijori, NSE corporate filings) before
    trusting the sentiment leg of the agent stack."""

    @staticmethod
    def to_yf_symbol(nse_ticker: str) -> str:
        u = nse_ticker.upper()
        if u == "NIFTY":     return "^NSEI"
        if u == "BANKNIFTY": return "^NSEBANK"
        return f"{nse_ticker}.NS"


class TradingAgentsOverlay:
    """Async overlay. Submitted requests run in a background thread; the
    polling loop never waits on LLM calls."""

    OUTPUT_SHEET = "TradingAgents Overlay"

    def __init__(self, ss_getter):
        self.ss_getter = ss_getter           # callable returning gspread Spreadsheet
        self._q: queue.Queue = queue.Queue()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._analyses_today = 0
        self._today = dt.date.today()
        self._last_run: Dict[str, dt.datetime] = {}
        self._thread: Optional[threading.Thread] = None
        if OVERLAY_ENABLED:
            self._thread = threading.Thread(
                target=self._loop, name="TAOverlay", daemon=True)
            self._thread.start()
            log.info("TradingAgents overlay started "
                     f"(provider={OVERLAY_LLM_PROVIDER}, "
                     f"daily cap={OVERLAY_MAX_ANALYSES_PER_DAY})")

    # ── Public API ────────────────────────────────────────────────────
    def submit(self, req: OverlayRequest) -> bool:
        if not OVERLAY_ENABLED:
            return False
        if req.worker_conviction < OVERLAY_MIN_CONVICTION:
            return False
        if req.worker_grade not in ("A", "B"):
            return False
        with self._lock:
            self._roll_day()
            if self._analyses_today >= OVERLAY_MAX_ANALYSES_PER_DAY:
                return False
            last = self._last_run.get(req.ticker)
            if last and (dt.datetime.now() - last).total_seconds() / 60 \
                    < OVERLAY_COOLDOWN_MIN_PER_TICKER:
                return False
            self._analyses_today += 1
            self._last_run[req.ticker] = dt.datetime.now()
        self._q.put(req)
        log.info(f"Overlay queued: {req.ticker} {req.direction} "
                 f"(grade={req.worker_grade}, conv={req.worker_conviction})")
        return True

    def shutdown(self, wait_seconds: float = 30.0) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=wait_seconds)

    # ── Internals ─────────────────────────────────────────────────────
    def _roll_day(self) -> None:
        today = dt.date.today()
        if today != self._today:
            self._today = today
            self._analyses_today = 0
            self._last_run.clear()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                req = self._q.get(timeout=1.0)
            except queue.Empty:
                continue
            try:
                result = self._run_one(req)
                if result:
                    self._write_result(result)
            except Exception as e:
                log.exception(f"Overlay run failed for {req.ticker}: {e}")

    def _run_one(self, req: OverlayRequest) -> Optional[OverlayResult]:
        start = time.time()
        try:
            from tradingagents.graph.trading_graph import TradingAgentsGraph
            from tradingagents.default_config import DEFAULT_CONFIG
        except ImportError:
            log.error("tradingagents not installed — "
                      "pip install tradingagents to enable overlay.")
            return None

        cfg = DEFAULT_CONFIG.copy()
        cfg["llm_provider"]      = OVERLAY_LLM_PROVIDER
        cfg["deep_think_llm"]    = OVERLAY_DEEP_THINK_MODEL
        cfg["quick_think_llm"]   = OVERLAY_QUICK_THINK_MODEL
        cfg["max_debate_rounds"] = OVERLAY_MAX_DEBATE_ROUNDS

        ta = TradingAgentsGraph(debug=False, config=cfg)
        analysis_date = dt.datetime.now().strftime("%Y-%m-%d")

        # TradingAgents takes the ticker string + date. The Indian adapter
        # is used implicitly via yfinance for any price lookup the agents
        # perform. (For deep integration, override TradingAgents' data
        # tool registrations to point at Fyers/Moneycontrol.)
        _, decision = ta.propagate(req.ticker, analysis_date)

        elapsed = time.time() - start
        return OverlayResult(
            timestamp=dt.datetime.now(),
            ticker=req.ticker,
            worker_signal=req.worker_signal,
            worker_grade=req.worker_grade,
            worker_conviction=req.worker_conviction,
            agent_decision=str(self._field(decision, "action", "HOLD")).upper(),
            agent_confidence=float(self._field(decision, "confidence", 0.5)),
            bull_thesis=str(self._field(decision, "bull_thesis", ""))[:500],
            bear_thesis=str(self._field(decision, "bear_thesis", ""))[:500],
            risk_flags=str(self._field(decision, "risk_flags", ""))[:300],
            time_taken_s=round(elapsed, 1),
        )

    @staticmethod
    def _field(obj: Any, name: str, default: Any) -> Any:
        if obj is None:
            return default
        if isinstance(obj, dict):
            return obj.get(name, default)
        return getattr(obj, name, default)

    def _write_result(self, r: OverlayResult) -> None:
        ss = self.ss_getter()
        try:
            ws = ss.worksheet(self.OUTPUT_SHEET)
        except gspread.WorksheetNotFound:
            ws = ss.add_worksheet(title=self.OUTPUT_SHEET, rows=2000, cols=12)
            ws.append_row([
                "Timestamp", "Ticker", "Worker Signal", "Worker Grade",
                "Worker Conv", "Agent Decision", "Agent Confidence",
                "Bull Thesis", "Bear Thesis", "Risk Flags", "Time Taken (s)",
            ], value_input_option="USER_ENTERED")
            ws.format("A1:K1", {
                "backgroundColor": {"red": 0.10, "green": 0.10, "blue": 0.18},
                "textFormat": {"bold": True,
                               "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            })
            ws.freeze(rows=1)

        ws.append_row([
            r.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            r.ticker, r.worker_signal, r.worker_grade, r.worker_conviction,
            r.agent_decision, f"{r.agent_confidence:.2f}",
            r.bull_thesis, r.bear_thesis, r.risk_flags, r.time_taken_s,
        ], value_input_option="USER_ENTERED")
        log.info(f"Overlay result: {r.ticker} → {r.agent_decision} "
                 f"(conf {r.agent_confidence:.2f}, {r.time_taken_s}s)")


# Module-level singleton — created once in main(), accessed from refresh_once().
_overlay: Optional[TradingAgentsOverlay] = None


# ============================================================================
#  MAIN REFRESH CYCLE
# ============================================================================

def refresh_once():
    ts = dt.datetime.now()
    token = fyers_ensure_valid_token()

    # Layer 1 + 2 — always fresh
    broad = fyers_fetch_quotes(token, [s for _, s in BROAD_INDICES])
    sectors = fyers_fetch_quotes(token, [s for _, s in SECTORAL_INDICES])

    # Layer 3 — rotational slice
    slice_tickers, slice_idx = compute_current_slice(FNO_UNIVERSE)
    fresh = fyers_fetch_fno_slice(token, slice_tickers)
    fresh_set = set(slice_tickers)
    log.info(f"Slice {slice_idx+1}/{ROTATION_SLICES}: "
             f"{len(slice_tickers)} fresh, "
             f"{len(FNO_UNIVERSE) - len(slice_tickers)} from cache")

    # Merge with cache. Preserve conviction_history + signal_history from
    # prior cache entries (signal_history is the newer paired format).
    cache = read_cache()
    by_t = {s["ticker"]: s for s in fresh}
    fno = []
    for t in FNO_UNIVERSE:
        prev = cache.get(t, {})
        prev_conv_history = prev.get("conviction_history", [])
        prev_sig_history  = prev.get("signal_history", [])
        if t in by_t:
            s = by_t[t]
            s["conviction_history"] = prev_conv_history  # carry forward (legacy)
            s["signal_history"]     = prev_sig_history   # carry forward (new)
            fno.append(s)
        elif t in cache:
            fno.append(cache[t])
        else:
            fno.append({"ticker": t, "ltp": 0, "chgPct": 0, "oiChgPct": 0,
                        "high": 0, "low": 0, "vol": 0, "oi": 0,
                        "open": 0, "prevClose": 0,
                        "conviction_history": [],
                        "signal_history": []})

    # Analysis (computes conviction)
    regime = compute_market_regime(broad)
    sector_ranked = rank_sectors(sectors, broad)
    analyzed = analyze_fno_stocks(fno, broad)

    # Append current reading to BOTH histories for FRESHLY-fetched stocks
    # only (cached stocks didn't get a new reading — their history shouldn't
    # grow).
    for s in analyzed:
        if s["ticker"] in fresh_set:
            # Legacy: conviction-only history (kept for backward compat)
            ch = s.get("conviction_history", [])
            ch = (ch + [s["conviction"]])[-CONVICTION_HISTORY_LEN:]
            s["conviction_history"] = ch
            # New: paired signal+conviction history (enables reversal detection)
            sh = s.get("signal_history", [])
            sh = (sh + [{"signal": s["signal"],
                         "conviction": s["conviction"]}])[-CONVICTION_HISTORY_LEN:]
            s["signal_history"] = sh

    # Compute Setup Quality grade (after history is up-to-date)
    # Pass sector_ranked so sector-confluence weighting can do its job.
    attach_setup_quality(analyzed, sector_ranked)

    # Persist back to cache (with updated history + computed fields)
    for s in analyzed:
        cache[s["ticker"]] = s

    # Update rolling 5-day Persistent Performers state in the cache
    cache = _update_performers_state(cache, analyzed, ts)

    write_cache(cache)

    # Sheet writes
    ss = get_sheet()
    write_dashboard(ss, regime, sector_ranked, analyzed, ts)
    write_market_sheet(ss, broad, regime, analyzed, ts)
    write_sectors_sheet(ss, sector_ranked, ts)
    write_fno_sheet(ss, analyzed, sector_ranked, ts)
    write_oi_activity_sheet(ss, analyzed, ts)
    write_conviction_sheet(ss, analyzed, ts)
    append_activity_row(ss, broad, sector_ranked, analyzed, ts)
    write_performers_sheet(ss, cache, ts)

    # Snapshots — only freshly-fetched stocks
    fresh_analyzed = [s for s in analyzed if s["ticker"] in fresh_set]
    append_snapshots(ss, fresh_analyzed, broad, ts)

    # ── TradingAgents overlay: fire-and-forget for Grade A/B setups ──
    # Non-blocking. submit() is internally throttled (daily cap + per-ticker
    # cooldown), so even if many A/B grades print, the LLM thread won't be
    # overwhelmed. Only fires when OVERLAY_ENABLED=True at module config.
    if _overlay is not None and OVERLAY_ENABLED:
        for s in analyzed:
            if s.get("grade") in ("A", "B") \
                    and s.get("direction") in ("LONG", "SHORT") \
                    and int(s.get("conviction", 0)) >= OVERLAY_MIN_CONVICTION:
                _overlay.submit(OverlayRequest(
                    timestamp=ts,
                    ticker=s["ticker"],
                    direction=s["direction"],
                    worker_signal=s["signal"],
                    worker_grade=s["grade"],
                    worker_conviction=int(s["conviction"]),
                ))


# ============================================================================
#  MAIN LOOP
# ============================================================================

def is_market_hours() -> bool:
    """09:15–15:30 IST on weekdays. If SESSION_END (HH:MM) is set via env
    (used by the morning GitHub Actions job to stop at 12:30 and stay under
    the 6-hour job cap), the window ends at that time instead of 15:30."""
    now = dt.datetime.now()
    if now.weekday() >= 5:  # Sat/Sun
        return False
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end   = now.replace(hour=15, minute=30, second=0, microsecond=0)
    if SESSION_END_OVERRIDE:
        try:
            eh, em = SESSION_END_OVERRIDE.split(":")
            end = now.replace(hour=int(eh), minute=int(em),
                              second=0, microsecond=0)
        except (ValueError, AttributeError):
            pass   # malformed override → fall back to 15:30
    return start <= now <= end

def test_auth_only():
    """Run the TOTP login flow once, print the result, and exit. Used to
    verify auth works before running the polling loop."""
    log.info("=" * 60)
    log.info("AUTH TEST ONLY - running TOTP login once")
    log.info("=" * 60)
    try:
        access, refresh = fyers_auto_login()
        log.info("[OK] Auth succeeded")
        log.info(f"  Access token: {access[:30]}... ({len(access)} chars)")
        log.info(f"  Refresh token: {refresh[:30] if refresh else '(none)'}...")
        # Probe one API call to confirm the token actually works
        r = requests.get(
            f"{_FYERS_BASE}/data/quotes?symbols=NSE:NIFTY50-INDEX",
            headers={"Authorization": f"{FYERS_CLIENT_ID}:{access}"},
            timeout=15,
        )
        if r.status_code == 200:
            ltp = r.json().get("d", [{}])[0].get("v", {}).get("lp", 0)
            log.info(f"[OK] Probe API call succeeded - NIFTY 50 LTP = {ltp}")
            log.info("Auth is fully working. Run without --test-auth to start polling.")
        else:
            log.error(f"[FAIL] Probe failed {r.status_code}: {r.text[:300]}")
    except Exception as e:
        log.exception(f"[FAIL] Auth failed: {e}")
        sys.exit(1)


# Known ticker renames. Extend as you discover more. The validator will
# auto-suggest replacements for any of these found in your TD_Config.
KNOWN_RENAMES = {
    "ZOMATO": "ETERNAL",   # Renamed April 9, 2025
}


def validate_universe():
    """
    --validate-universe mode. Probes every ticker in TD_Config against
    Fyers's quote endpoint (spot + futures). Auto-applies known renames,
    drops invalid tickers, updates TD_Config after user confirmation.

    Uses /data/quotes (50/batch) for both spot AND futures probing — far
    faster than /data/depth (1/call). For 181 tickers: ~3 seconds total.
    """
    log.info("=" * 60)
    log.info("UNIVERSE VALIDATOR")
    log.info("=" * 60)

    token = fyers_ensure_valid_token()
    universe = load_universe_from_sheet()
    if not universe:
        log.error("TD_Config is empty. Nothing to validate.")
        sys.exit(1)
    log.info(f"Loaded {len(universe)} tickers from TD_Config")

    # Probe set = current universe ∪ known-rename targets (so we can
    # verify replacements in the same pass)
    probe = sorted(set(universe + list(KNOWN_RENAMES.values())))
    suffix = current_fut_suffix()
    log.info(f"Probing {len(probe)} symbols against /data/quotes "
             f"(spot + {suffix} futures)...")

    spot_data = fyers_fetch_quotes(token, [f"NSE:{t}-EQ" for t in probe])
    fut_data  = fyers_fetch_quotes(token, [f"NSE:{t}{suffix}" for t in probe])

    def _has_data(d: dict, key: str) -> bool:
        v = d.get(key)
        if not v:
            return False
        return (v.get("ltp", 0) > 0) or (v.get("prevClose", 0) > 0)

    def _is_valid(ticker: str) -> Tuple[bool, str]:
        spot_ok = _has_data(spot_data, f"NSE:{ticker}-EQ")
        fut_ok  = _has_data(fut_data,  f"NSE:{ticker}{suffix}")
        if spot_ok and fut_ok:
            return True, ""
        missing = []
        if not spot_ok: missing.append("spot")
        if not fut_ok:  missing.append("futures")
        return False, " + ".join(missing) + " missing"

    valid: List[str] = []
    invalid: List[Tuple[str, str]] = []     # (ticker, reason)
    renames: List[Tuple[str, str]] = []     # (old, new)

    for t in universe:
        ok, reason = _is_valid(t)
        if ok:
            valid.append(t)
            continue
        if t in KNOWN_RENAMES:
            new = KNOWN_RENAMES[t]
            new_ok, _ = _is_valid(new)
            if new_ok:
                renames.append((t, new))
                continue
        invalid.append((t, reason))

    final_universe = sorted(set(valid + [new for _, new in renames]))

    # Report
    print()
    print(f"  Originally in TD_Config: {len(universe)}")
    print(f"  Valid:                   {len(valid)}")
    print(f"  Will be renamed:         {len(renames)}")
    print(f"  Will be removed:         {len(invalid)}")
    print(f"  Final count:             {len(final_universe)}")
    print()

    if renames:
        print("Renames to apply:")
        for old, new in renames:
            print(f"  {old:<15} ->  {new}")
        print()
    if invalid:
        print("Invalid tickers to remove:")
        for t, reason in invalid:
            print(f"  {t:<15} ({reason})")
        print()

    if not invalid and not renames:
        print("All tickers valid. No changes needed.")
        return

    print("Update TD_Config now? (yes/no): ", end="", flush=True)
    answer = input().strip().lower()
    if answer not in ("y", "yes"):
        print("Aborted. TD_Config unchanged.")
        return

    # Apply: clear column A below header, write final list
    ss = get_sheet()
    try:
        ws = ss.worksheet(SHEET_NAMES["CONFIG"])
    except gspread.WorksheetNotFound:
        ws = ss.worksheet("TD_Config")
    ws.batch_clear([f"A2:A{ws.row_count}"])
    if final_universe:
        rows = [[t] for t in final_universe]
        ws.update(range_name=f"A2:A{len(rows)+1}", values=rows,
                  value_input_option="USER_ENTERED")
    print(f"[OK] Universe Config updated. {len(final_universe)} tickers active.")
    print("Restart the worker (without --validate-universe) to use the new list.")


# ============================================================================
#  REFRESH UNIVERSE FROM FYERS MASTER  —  --refresh-universe
# ============================================================================

def refresh_universe_from_fyers():
    """
    Download the live Fyers NSE F&O symbol master and rebuild the universe
    with all currently-tradeable single-stock futures. Replaces the local
    seed list (which was stale; only 105 of 181 were valid).
    """
    log.info("=" * 60)
    log.info("REFRESH UNIVERSE FROM FYERS SYMBOL MASTER")
    log.info("=" * 60)
    MASTER_URL = "https://public.fyers.in/sym_details/NSE_FO.csv"
    log.info(f"Downloading {MASTER_URL}...")
    try:
        r = requests.get(MASTER_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        log.error(f"Failed to download symbol master: {e}")
        sys.exit(1)

    # The CSV has variable columns over time; we don't need a strict schema.
    # We're hunting for symbols ending in {current_suffix} that aren't options
    # (no CE/PE in name) and aren't indices.
    suffix = current_fut_suffix()
    log.info(f"Filtering for current-month futures (suffix: {suffix})...")
    INDEX_UNDERLYINGS = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY",
                         "NIFTYNXT50", "NIFTYIT", "BANKEX", "SENSEX"}
    underlyings = set()
    for line in r.text.splitlines():
        parts = line.split(",")
        for cell in parts:
            cell = cell.strip()
            # Looking for NSE:RELIANCE26MAYFUT pattern
            if cell.startswith("NSE:") and cell.endswith(suffix):
                # Strip prefix and suffix to get the underlying
                u = cell[len("NSE:"):-len(suffix)]
                # Skip indices and weird tokens
                if u in INDEX_UNDERLYINGS:
                    continue
                if not u or not u.replace("&", "").replace("-", "").isalnum():
                    continue
                underlyings.add(u)

    underlyings_sorted = sorted(underlyings)
    log.info(f"Found {len(underlyings_sorted)} F&O single-stock underlyings.")
    if len(underlyings_sorted) < 50:
        log.error("Sanity check failed — fewer than 50 underlyings found. "
                  "Fyers CSV format may have changed; not updating anything.")
        sys.exit(1)

    # Show summary
    print()
    print(f"  Found {len(underlyings_sorted)} current F&O underlyings.")
    print(f"  First 10: {', '.join(underlyings_sorted[:10])}")
    print(f"  Last 10:  {', '.join(underlyings_sorted[-10:])}")
    print()
    print("Replace Universe Config with this list? (yes/no): ", end="", flush=True)
    if input().strip().lower() not in ("y", "yes"):
        print("Aborted. No changes.")
        return

    ss = get_sheet()
    try:
        ws = ss.worksheet(SHEET_NAMES["CONFIG"])
    except gspread.WorksheetNotFound:
        ws = ss.worksheet("TD_Config")
    ws.batch_clear([f"A2:A{ws.row_count}"])
    rows = [[t] for t in underlyings_sorted]
    ws.update(range_name=f"A2:A{len(rows)+1}", values=rows,
              value_input_option="USER_ENTERED")
    print(f"[OK] Universe Config refreshed. {len(underlyings_sorted)} tickers.")
    print("Run --validate-universe to spot-check, then start the worker.")


# ============================================================================
#  LIST INDICES  —  --list-indices
# ============================================================================

def list_available_indices():
    """
    Diagnostic mode. Fetches every index symbol currently in Fyers's NSE Cash
    Market master, then attempts a live quote for each of the symbols in our
    BROAD_INDICES + SECTORAL_INDICES lists. Reports:
      - Which configured indices are working (returning live data)
      - Which configured indices are FAILING (so we can fix the symbol)
      - All -INDEX symbols Fyers exposes (so we can discover new ones)
    """
    log.info("=" * 60)
    log.info("INDEX DIAGNOSTIC — checking which symbols Fyers accepts")
    log.info("=" * 60)

    # 1. Get the live token (we need it for the quote-check step)
    token = fyers_ensure_valid_token()

    # 2. Combine all our configured indices into one probe set
    configured = list(BROAD_INDICES) + list(SECTORAL_INDICES)
    syms = sorted({sym for _, sym in configured})
    log.info(f"Probing {len(syms)} configured index symbols against /data/quotes...")

    quote_data = fyers_fetch_quotes(token, syms)

    working: List[Tuple[str, str, float]] = []     # (label, sym, ltp)
    failing: List[Tuple[str, str]] = []            # (label, sym)
    for label, sym in configured:
        v = quote_data.get(sym, {})
        ltp = float(v.get("ltp", 0) or 0)
        prev = float(v.get("prevClose", 0) or 0)
        if ltp > 0 or prev > 0:
            working.append((label, sym, ltp))
        else:
            failing.append((label, sym))

    print()
    print("=" * 70)
    print("CONFIGURED INDICES — STATUS")
    print("=" * 70)
    print(f"{'Status':<10} {'Label':<24} {'Symbol':<32} {'LTP':>10}")
    print("-" * 70)
    for label, sym, ltp in working:
        print(f"{'WORKING':<10} {label:<24} {sym:<32} {ltp:>10.2f}")
    for label, sym in failing:
        print(f"{'FAILING':<10} {label:<24} {sym:<32} {'—':>10}")

    print()
    print(f"Working: {len(working)} / {len(configured)}")
    if failing:
        print(f"Failing: {len(failing)} — these symbols are wrong or unsupported.")

    # 3. Also try to discover ALL index symbols from the master CSV
    MASTER_URL = "https://public.fyers.in/sym_details/NSE_CM.csv"
    print()
    print(f"Downloading {MASTER_URL} to discover ALL available index symbols...")
    try:
        r = requests.get(MASTER_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"  Could not download master CSV: {e}")
        return

    # Find every NSE:*-INDEX symbol in the file
    discovered = set()
    for line in r.text.splitlines():
        for cell in line.split(","):
            cell = cell.strip()
            if cell.startswith("NSE:") and cell.endswith("-INDEX"):
                discovered.add(cell)

    configured_syms = {sym for _, sym in configured}
    new_symbols = sorted(discovered - configured_syms)

    print()
    print("=" * 70)
    print(f"INDICES IN FYERS MASTER BUT NOT YET CONFIGURED ({len(new_symbols)})")
    print("=" * 70)
    if not new_symbols:
        print("  (none — your configuration covers everything Fyers exposes)")
    else:
        for s in new_symbols:
            print(f"  {s}")
        print()
        print("To add any of these, edit BROAD_INDICES or SECTORAL_INDICES")
        print("in mk_topdown_worker.py and add a (label, symbol) tuple.")
    print()


# ============================================================================
#  SETUP SHEETS  —  --setup-sheets
# ============================================================================

# Glossary content — explains every metric in plain English.
GLOSSARY_CONTENT = [
    ["Metric", "Definition", "How to interpret"],
    ["Sector (in F&O Live)",
     "The NSE sector index this stock belongs to. Maps each stock to one primary sector (e.g., HDFCBANK → NIFTY BANK).",
     "F&O Live is sorted by sector performance: stocks in the best-performing sector appear at the top, "
     "worst-performing sector at the bottom. 'OTHER' = stock not in any tracked sector index (cement, "
     "chemicals, telecom, etc.) — these sort to the bottom."],
    ["Direction",
     "LONG or SHORT — the trade action this signal suggests.",
     "LONG = consider buying (LONG_BUILDUP or SHORT_COVERING signal). "
     "SHORT = consider shorting/selling puts (SHORT_BUILDUP or LONG_UNWINDING)."],
    ["Setup Quality (0-100)",
     "Composite decision score: conviction (35%) + stability (30%) + room-to-move (25%) + sector (10%), multiplied by time-of-day decay.",
     "This is the field to RANK BY. Quality of 70+ deserves attention. "
     "Quality below 40 means the signal is weak, stale, late, or already extended."],
    ["Grade (A/B/C/D)",
     "Letter tier based on Setup Quality. A = 70+, B = 55-69, C = 40-54, D = below 40.",
     "Watchlist should only show A and B. C and D are noise. A pre-9:45 stock is rare and valuable."],
    ["Stability",
     "How consistently the conviction has held across the last several readings (rolling history).",
     "A spike from 30 to 90 in one minute = unstable = likely noise. A steady 70+ for 3+ readings = real."],
    ["Room to Move",
     "Inverse of how extended the price already is, in the direction of the signal.",
     "A LONG signal on a stock already +3% = limited room (low score). Same signal at +0.3% = lots of room (high score)."],
    ["Time Decay",
     "Multiplier that down-weights late-day signals. 1.0 in the first hour, 0.35 in the last 45 minutes.",
     "A signal at 9:30 has 6 hours to play out; at 14:45 only 45 minutes. Same signal, very different actionability."],
    ["Why (column)",
     "Human-readable rationale for the Setup Quality.",
     "Example: 'steady — all >65 over 4 reads; early — room to run' = a strong A-grade morning setup."],
    ["", "", ""],
    ["Signal",
     "OI matrix tag combining price direction and OI change.",
     "LONG_BUILDUP = fresh longs entering (bullish). SHORT_BUILDUP = fresh shorts (bearish). "
     "SHORT_COVERING = bears exiting (bullish but topping). LONG_UNWINDING = bulls exiting (bearish but bottoming)."],
    ["Conviction (0-100)",
     "Raw signal strength NOW. Blends price%, OI%, volume surge, RS, range position. Sign-adjusted by signal.",
     "Used as INPUT to Setup Quality, not as the standalone decision variable. "
     "A 90 conviction at midday on a stock that already moved 4% is descriptive, not predictive."],
    ["Chg %",
     "Stock's % change vs previous close.",
     "Positive = up today. Shown as percentage."],
    ["OI Chg %",
     "% change in Open Interest from previous day's close for current-month futures.",
     "Rising OI = new positions (confirming move). Falling OI = unwinding."],
    ["RS (Relative Strength)",
     "Stock's % change MINUS NIFTY 50's % change.",
     "Positive = outperforming today. Used to find leaders within the move."],
    ["Vol Surge %ile",
     "Percentile rank of volume pace today vs the F&O universe (0-100).",
     "100 = highest volume pace. >80 = unusual activity. <20 = quiet."],
    ["Range Position",
     "Where current price sits in the day's high-low range, 0 to 1.",
     "1.0 = at day's high (breakout). 0.0 = at day's low (breakdown). 0.5 = middle."],
    ["Sector Tag",
     "How a sector is behaving relative to NIFTY 50.",
     "LEADER = outperforming AND at day's highs. LAGGARD = underperforming AND at day's lows. "
     "REVERSING_UP/DOWN = price has reversed within the day."],
    ["Market Regime",
     "5-state classifier of the broader market today.",
     "STRONG_BULL: NIFTY >0.75% & VIX falling. BULL: NIFTY >0.25%. RANGEBOUND: |NIFTY|<0.25%. "
     "BEAR: NIFTY <-0.25%. STRONG_BEAR: NIFTY <-0.75% & VIX rising."],
    ["", "", ""],
    ["HOW TO USE THIS DASHBOARD",
     "Workflow:",
     "1) Check Market Regime — match your bias to it. "
     "2) Note Sector Leaders/Laggards — these define momentum direction. "
     "3) Open Dashboard's Top LONG / SHORT setups — Grade A first, then B. "
     "4) Read the 'Why' — discard anything saying 'extended' or 'late'. "
     "5) Confirm with your own technicals/chart before entering. "
     "6) After 14:00, be selective — time decay reduces actionability."],
    ["LIMITATIONS",
     "What this tool DOES NOT do.",
     "It does NOT predict prices. It RANKS stocks by how well the textbook OI-price setup is forming RIGHT NOW. "
     "It has no historical context beyond today, no news, no sentiment, no options-flow analysis. "
     "It's a discovery tool. Backtest before trusting any score with real capital."],
]

# Column headers for each sheet — descriptive names
SHEET_HEADERS = {
    SHEET_NAMES["DASHBOARD"]: ["", "", "", "", ""],   # custom-built per write
    SHEET_NAMES["MARKET"]:    ["Index", "LTP", "Chg %", "Day High", "Day Low"],
    SHEET_NAMES["SECTORS"]:   ["Rank", "Sector", "Tag", "Chg %", "RS vs NIFTY", "Range Pos"],
    SHEET_NAMES["FNO"]:       ["Ticker", "Sector", "Signal", "Direction", "LTP", "Chg %", "OI Chg %",
                               "RS", "Vol Surge %ile", "Range Pos", "Conviction",
                               "Grade", "Setup Quality"],
    SHEET_NAMES["OI_ACTIVITY"]: ["Signal", "Ticker", "LTP", "Chg %", "OI Chg %",
                                 "Vol Surge %ile", "Range Pos", "Conviction",
                                 "Grade", "Setup Quality"],
    SHEET_NAMES["CONVICTION"]: ["Rank", "Ticker", "Direction", "Grade", "Setup Quality",
                                "Signal", "LTP", "Chg %", "OI Chg %",
                                "Conviction", "Why"],
    SHEET_NAMES["ACTIVITY"]:  ["Timestamp", "NIFTY 50", "BANK NIFTY", "VIX",
                               "Top Sector", "Worst Sector",
                               "Long Buildup #", "Short Buildup #",
                               "Short Cover #", "Long Unwind #",
                               "Top Conv. Ticker", "Top Conv. Score"],
    SHEET_NAMES["SNAPSHOTS"]: ["Timestamp", "Ticker", "LTP", "Chg %", "OI Chg %",
                               "Volume", "Signal", "Conviction",
                               "Range Pos", "NIFTY Chg %",
                               "Grade", "Setup Quality", "Direction"],
    SHEET_NAMES["PERFORMERS"]: ["Ticker", "Sector", "Days Seen (5d)",
                                "Total Minutes A/B", "Last Seen",
                                "Last Grade", "Last Direction"],
    SHEET_NAMES["WEIGHTS"]:   ["Index", "Ticker", "Weight %"],
    SHEET_NAMES["GLOSSARY"]:  ["Metric", "Definition", "How to interpret"],
    SHEET_NAMES["CONFIG"]:    ["F&O Tickers (one per row, no -EQ suffix)"],
}


def setup_sheets():
    """
    One-time migration: rename legacy TD_* sheets to descriptive English names,
    delete obsolete buildup sheets, create the new consolidated OI Activity
    sheet, create the Glossary, and apply conditional formatting via the
    Sheets API batch_update.
    """
    log.info("=" * 60)
    log.info("SETUP SHEETS - migrating to descriptive names + formatting")
    log.info("=" * 60)
    ss = get_sheet()

    # ---- 1. Rename legacy sheets ----
    for old_name, new_name in LEGACY_RENAMES.items():
        try:
            ws = ss.worksheet(old_name)
            ws.update_title(new_name)
            log.info(f"Renamed: {old_name} -> {new_name}")
        except gspread.WorksheetNotFound:
            pass  # already migrated or never existed

    # ---- 2. Delete obsolete buildup sheets ----
    for old_name in LEGACY_TO_DELETE:
        try:
            ws = ss.worksheet(old_name)
            ss.del_worksheet(ws)
            log.info(f"Deleted obsolete: {old_name}")
        except gspread.WorksheetNotFound:
            pass

    # ---- 3. Ensure all new sheets exist with correct headers ----
    existing = {ws.title for ws in ss.worksheets()}
    for key, name in SHEET_NAMES.items():
        if name not in existing:
            ss.add_worksheet(title=name, rows=2000, cols=15)
            log.info(f"Created: {name}")
        ws = ss.worksheet(name)
        # Write headers
        if name in SHEET_HEADERS:
            headers = SHEET_HEADERS[name]
            ws.update(range_name=f"A1:{chr(64 + len(headers))}1",
                      values=[headers], value_input_option="USER_ENTERED")
            ws.format(f"A1:{chr(64 + len(headers))}1", {
                "backgroundColor": {"red": 0.10, "green": 0.10, "blue": 0.18},
                "textFormat": {"bold": True, "foregroundColor":
                               {"red": 1, "green": 1, "blue": 1}},
                "horizontalAlignment": "LEFT",
            })
            ws.freeze(rows=1)
        # Hide cache sheet
        if name == SHEET_NAMES["CACHE"]:
            try:
                ws.hide()
            except Exception:
                pass

    # ---- 4. Populate Glossary ----
    glossary_ws = ss.worksheet(SHEET_NAMES["GLOSSARY"])
    # Clear and write all rows (headers + content)
    glossary_ws.batch_clear(["A1:C200"])
    glossary_ws.update(range_name=f"A1:C{len(GLOSSARY_CONTENT)}",
                       values=GLOSSARY_CONTENT, value_input_option="USER_ENTERED")
    # Header formatting
    glossary_ws.format("A1:C1", {
        "backgroundColor": {"red": 0.10, "green": 0.10, "blue": 0.18},
        "textFormat": {"bold": True, "foregroundColor":
                       {"red": 1, "green": 1, "blue": 1}},
    })
    # Column widths via batch_update (gspread doesn't have direct API)
    glossary_ws.columns_auto_resize(0, 3)
    # Bold the IMPORTANT row
    for i, row in enumerate(GLOSSARY_CONTENT):
        if row and row[0] == "IMPORTANT":
            glossary_ws.format(f"A{i+1}:C{i+1}", {
                "backgroundColor": {"red": 1.0, "green": 0.95, "blue": 0.85},
                "textFormat": {"bold": True},
            })
    log.info("Glossary populated.")

    # ---- 4b. Populate Index Weights sheet (only if empty) ----
    try:
        populate_weights_sheet(ss)
    except Exception as e:
        log.warning(f"Could not populate Index Weights sheet: {e}")

    # ---- 5. Apply conditional formatting ----
    apply_conditional_formatting(ss)
    log.info("Conditional formatting applied.")

    log.info("=" * 60)
    log.info("SETUP COMPLETE")
    log.info("=" * 60)
    print()
    print("Your sheets are now:")
    for name in SHEET_NAMES.values():
        print(f"  - {name}")
    print()
    print("The Glossary sheet explains every metric.")
    print("Start the worker normally (python mk_topdown_worker.py) to begin polling.")


def apply_conditional_formatting(ss):
    """
    Apply conditional formatting rules via the Sheets API batch_update.
    Color-codes Chg %, signal types, conviction scores, sector tags.
    """
    sid_map = {ws.title: ws.id for ws in ss.worksheets()}
    requests_body = []

    def add_gradient(sheet_id, start_col, end_col, min_val, mid_val, max_val,
                     min_color, mid_color, max_color):
        requests_body.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1,
                                "endRowIndex": 5000,
                                "startColumnIndex": start_col,
                                "endColumnIndex": end_col}],
                    "gradientRule": {
                        "minpoint": {"color": min_color, "type": "NUMBER",
                                     "value": str(min_val)},
                        "midpoint": {"color": mid_color, "type": "NUMBER",
                                     "value": str(mid_val)},
                        "maxpoint": {"color": max_color, "type": "NUMBER",
                                     "value": str(max_val)},
                    }
                },
                "index": 0
            }
        })

    def add_text_color(sheet_id, start_col, end_col, text, bg_color, text_color=None):
        format_spec = {"backgroundColor": bg_color}
        if text_color:
            format_spec["textFormat"] = {"foregroundColor": text_color, "bold": True}
        requests_body.append({
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [{"sheetId": sheet_id, "startRowIndex": 1,
                                "endRowIndex": 5000,
                                "startColumnIndex": start_col,
                                "endColumnIndex": end_col}],
                    "booleanRule": {
                        "condition": {"type": "TEXT_EQ",
                                      "values": [{"userEnteredValue": text}]},
                        "format": format_spec
                    }
                },
                "index": 0
            }
        })

    RED   = {"red": 0.85, "green": 0.20, "blue": 0.15}
    GREEN = {"red": 0.10, "green": 0.65, "blue": 0.30}
    WHITE = {"red": 1, "green": 1, "blue": 1}
    LIGHT_RED   = {"red": 0.99, "green": 0.85, "blue": 0.85}
    LIGHT_GREEN = {"red": 0.85, "green": 0.96, "blue": 0.85}
    AMBER = {"red": 1.0, "green": 0.85, "blue": 0.40}

    # F&O Live columns: A=Ticker B=Sector C=Signal D=Direction E=LTP F=Chg%
    # G=OI Chg% H=RS I=VolSurge J=RangePos K=Conviction L=Grade M=Setup Quality
    if SHEET_NAMES["FNO"] in sid_map:
        sid = sid_map[SHEET_NAMES["FNO"]]
        # Chg%, OI Chg%, RS = cols 5,6,7
        add_gradient(sid, 5, 8, -0.02, 0, 0.02, RED, WHITE, GREEN)
        # Conviction col K = index 10
        add_gradient(sid, 10, 11, 0, 50, 100, RED, WHITE, GREEN)
        # Setup Quality col M = index 12
        add_gradient(sid, 12, 13, 0, 50, 100, RED, WHITE, GREEN)
        # Signal col C = index 2
        add_text_color(sid, 2, 3, "LONG_BUILDUP", LIGHT_GREEN)
        add_text_color(sid, 2, 3, "SHORT_BUILDUP", LIGHT_RED)
        add_text_color(sid, 2, 3, "SHORT_COVERING",
                       {"red": 0.85, "green": 0.95, "blue": 1.0})
        add_text_color(sid, 2, 3, "LONG_UNWINDING",
                       {"red": 1.0, "green": 0.92, "blue": 0.80})
        # Direction col D = index 3
        add_text_color(sid, 3, 4, "LONG", LIGHT_GREEN, GREEN)
        add_text_color(sid, 3, 4, "SHORT", LIGHT_RED, RED)
        # Grade col L = index 11
        add_text_color(sid, 11, 12, "A",
                       {"red": 0.70, "green": 0.95, "blue": 0.70}, GREEN)
        add_text_color(sid, 11, 12, "B",
                       {"red": 0.85, "green": 0.96, "blue": 0.85})
        add_text_color(sid, 11, 12, "D",
                       {"red": 0.95, "green": 0.85, "blue": 0.85})

    # OI Activity: same signal colors on col A (index 0), conviction (H = 7)
    if SHEET_NAMES["OI_ACTIVITY"] in sid_map:
        sid = sid_map[SHEET_NAMES["OI_ACTIVITY"]]
        add_text_color(sid, 0, 1, "LONG_BUILDUP", LIGHT_GREEN)
        add_text_color(sid, 0, 1, "SHORT_BUILDUP", LIGHT_RED)
        add_text_color(sid, 0, 1, "SHORT_COVERING",
                       {"red": 0.85, "green": 0.95, "blue": 1.0})
        add_text_color(sid, 0, 1, "LONG_UNWINDING",
                       {"red": 1.0, "green": 0.92, "blue": 0.80})
        add_gradient(sid, 3, 6, -0.02, 0, 0.02, RED, WHITE, GREEN)  # Chg, OI
        add_gradient(sid, 7, 8, 0, 50, 100, RED, WHITE, GREEN)       # Conviction

    # Top Conviction: Chg (E=4), OI (F=5), RS (G=6), Conviction (I=8)
    if SHEET_NAMES["CONVICTION"] in sid_map:
        sid = sid_map[SHEET_NAMES["CONVICTION"]]
        add_gradient(sid, 4, 7, -0.02, 0, 0.02, RED, WHITE, GREEN)
        add_gradient(sid, 8, 9, 0, 50, 100, RED, WHITE, GREEN)
        add_text_color(sid, 2, 3, "LONG_BUILDUP", LIGHT_GREEN)
        add_text_color(sid, 2, 3, "SHORT_BUILDUP", LIGHT_RED)

    # Market Overview: Chg % (C=2)
    if SHEET_NAMES["MARKET"] in sid_map:
        sid = sid_map[SHEET_NAMES["MARKET"]]
        add_gradient(sid, 2, 3, -0.02, 0, 0.02, RED, WHITE, GREEN)

    # Sector Rotation: tag (C=2), Chg (D=3), RS (E=4)
    if SHEET_NAMES["SECTORS"] in sid_map:
        sid = sid_map[SHEET_NAMES["SECTORS"]]
        add_gradient(sid, 3, 5, -0.02, 0, 0.02, RED, WHITE, GREEN)
        add_text_color(sid, 2, 3, "LEADER", LIGHT_GREEN, GREEN)
        add_text_color(sid, 2, 3, "LAGGARD", LIGHT_RED, RED)
        add_text_color(sid, 2, 3, "REVERSING_UP",
                       {"red": 0.85, "green": 0.95, "blue": 1.0})
        add_text_color(sid, 2, 3, "REVERSING_DOWN",
                       {"red": 1.0, "green": 0.92, "blue": 0.80})

    # Snapshots Log: Chg % (D=3), OI Chg % (E=4), Conviction (H=7)
    if SHEET_NAMES["SNAPSHOTS"] in sid_map:
        sid = sid_map[SHEET_NAMES["SNAPSHOTS"]]
        add_gradient(sid, 3, 5, -0.02, 0, 0.02, RED, WHITE, GREEN)
        add_gradient(sid, 7, 8, 0, 50, 100, RED, WHITE, GREEN)

    if requests_body:
        ss.batch_update({"requests": requests_body})


def force_one_poll():
    """
    Run exactly ONE refresh cycle, bypassing market-hours and weekday checks.
    Used for manually verifying sheet layout / formatting on weekends or
    outside trading hours.

    Caveats: on a weekend or pre-9:15 weekday, Fyers returns Friday's close
    prices and last-known OI. Signals/grades will reflect that stale state
    — useful only for confirming SHEET STRUCTURE, not for trading.
    """
    global FNO_UNIVERSE
    log.info("=" * 60)
    log.info("FORCED ONE-TIME POLL — bypassing market-hours check")
    log.info("=" * 60)
    now = dt.datetime.now()
    if not is_market_hours():
        if now.weekday() >= 5:
            log.warning("It's a weekend — data will be STALE (Friday's close).")
        else:
            log.warning(f"It's {now.strftime('%H:%M')} — outside 9:15-15:30 IST. "
                        "Data will be STALE (last trading session's close).")
        log.warning("Use this run only to verify sheet layout, not for signals.")

    FNO_UNIVERSE = load_universe_from_sheet()
    if not FNO_UNIVERSE:
        log.error("F&O universe is empty. Run --refresh-universe first.")
        sys.exit(1)
    log.info(f"Loaded {len(FNO_UNIVERSE)} F&O tickers")

    log.info("Running one refresh cycle...")
    t0 = time.time()
    refresh_once()
    elapsed = time.time() - t0
    log.info(f"Done in {elapsed:.1f}s. Check your Google Sheet now.")
    log.info("This was a ONE-TIME run. Worker is NOT polling. Exiting.")


def demo_loop(minutes: int):
    """
    Run the full refresh loop every 60s for `minutes` minutes, IGNORING the
    market-hours check. The GitHub equivalent of leaving the worker running
    locally for a demo outside trading hours.

    Outside market hours, Fyers returns last-traded prices, so the numbers
    won't move — but the dashboard, signals, contribution tables, performers,
    and sheet writes all populate and refresh exactly as they would live.
    Use it to demo the live behaviour any time.
    """
    global FNO_UNIVERSE
    log.info("=" * 60)
    log.info(f"DEMO LOOP — refreshing every 60s for {minutes} min "
             f"(market-hours check bypassed)")
    log.info("=" * 60)
    now = dt.datetime.now()
    if not is_market_hours():
        log.warning(f"It's {now.strftime('%H:%M')} {now.strftime('%a')} — "
                    "outside live hours. Prices will be STALE (last close). "
                    "Dashboard mechanics are real; the numbers just won't move.")

    FNO_UNIVERSE = load_universe_from_sheet()
    if not FNO_UNIVERSE:
        log.error("F&O universe is empty. Run --refresh-universe first.")
        sys.exit(1)
    log.info(f"Loaded {len(FNO_UNIVERSE)} F&O tickers")

    end_at = time.time() + minutes * 60
    cycle = 0
    while time.time() < end_at:
        cycle += 1
        t0 = time.time()
        try:
            refresh_once()
        except Exception as e:
            log.exception(f"Demo cycle {cycle} failed: {e}")
        elapsed = time.time() - t0
        log.info(f"Demo cycle {cycle}: refresh in {elapsed:.1f}s")
        # Sleep the remainder of the 60s window (unless time's up)
        if time.time() >= end_at:
            break
        sleep_for = max(POLL_INTERVAL_SEC - elapsed, 1)
        time.sleep(sleep_for)
    log.info(f"Demo loop finished after {cycle} cycle(s). Exiting.")


def main():
    global FNO_UNIVERSE
    # ── Utility modes ── these work any day, anytime (not gated by market hours)
    if "--test-auth" in sys.argv:
        test_auth_only()
        return
    if "--validate-universe" in sys.argv:
        validate_universe()
        return
    if "--refresh-universe" in sys.argv:
        refresh_universe_from_fyers()
        return
    if "--list-indices" in sys.argv:
        list_available_indices()
        return
    if "--setup-sheets" in sys.argv:
        setup_sheets()
        return
    if "--poll-now" in sys.argv:
        force_one_poll()
        return
    if "--demo-loop" in sys.argv:
        # Optional integer minutes after the flag; default 10
        mins = 10
        try:
            idx = sys.argv.index("--demo-loop")
            if idx + 1 < len(sys.argv):
                mins = int(sys.argv[idx + 1])
        except (ValueError, IndexError):
            log.warning("Could not parse minutes after --demo-loop; using 10.")
            mins = 10
        demo_loop(mins)
        return
    if "--audit" in sys.argv:
        run_methodology_audit()
        return
    if "--backfill-performers" in sys.argv:
        backfill_performers_from_snapshots()
        return
    if "--refresh-weights" in sys.argv:
        refresh_weights()
        return

    # ── Reject any unknown --flag instead of silently falling through to
    #    polling mode. This protects against typos like --list-indeces
    #    that would otherwise start the polling loop (and on weekends exit
    #    immediately with "Weekend — nothing to do").
    unknown_flags = [a for a in sys.argv[1:] if a.startswith("--")]
    if unknown_flags:
        print(f"ERROR: Unknown flag(s): {', '.join(unknown_flags)}")
        print()
        print("Available commands:")
        print("  python mk_topdown_worker.py                       Start polling (market hours only)")
        print("  python mk_topdown_worker.py --test-auth           Verify Fyers login")
        print("  python mk_topdown_worker.py --validate-universe   Clean up invalid tickers")
        print("  python mk_topdown_worker.py --refresh-universe    Download fresh F&O list from Fyers")
        print("  python mk_topdown_worker.py --list-indices        Diagnose which index symbols work")
        print("  python mk_topdown_worker.py --setup-sheets        Migrate/format Google sheets")
        print("  python mk_topdown_worker.py --poll-now            Run ONE refresh now (stale data on weekends)")
        print("  python mk_topdown_worker.py --demo-loop 10        Loop every 60s for 10 min, ignore market hours (demo)")
        print("  python mk_topdown_worker.py --audit               Backtest hit rates on Snapshots Log")
        print("  python mk_topdown_worker.py --backfill-performers Reconstruct Persistent Performers from existing snapshots")
        print("  python mk_topdown_worker.py --refresh-weights     Check NIFTY/BANK NIFTY constituents vs NSE; report drift")
        sys.exit(1)

    # ── Polling mode (only reached with no flags) ──
    log.info("=" * 60)
    log.info("MK Top-Down Worker starting")
    log.info("=" * 60)
    # Load universe from sheet once at startup
    FNO_UNIVERSE = load_universe_from_sheet()
    if not FNO_UNIVERSE:
        log.error("F&O universe is empty — populate TD_Config sheet and restart.")
        sys.exit(1)
    log.info(f"Loaded {len(FNO_UNIVERSE)} F&O tickers from TD_Config")
    log.info(f"Rotation: {ROTATION_SLICES} slice(s), "
             f"depth workers: {DEPTH_PARALLEL_WORKERS}, "
             f"poll interval: {POLL_INTERVAL_SEC}s")

    # ── TradingAgents overlay (optional; gated by OVERLAY_ENABLED) ──
    global _overlay
    if OVERLAY_ENABLED:
        _overlay = TradingAgentsOverlay(ss_getter=get_sheet)
    else:
        log.info("TradingAgents overlay: DISABLED "
                 "(set OVERLAY_ENABLED=True after pip install tradingagents)")

    # Tracks whether the market opened during this run. Used to exit cleanly
    # after close — pairs with Task Scheduler running us each weekday morning.
    market_opened_today = False

    while True:
        try:
            now = dt.datetime.now()
            if not is_market_hours():
                # Exit cleanly if market has CLOSED for today (was open earlier
                # in this session) — so Task Scheduler can launch a fresh run
                # next weekday morning.
                if market_opened_today:
                    log.info("Market closed for today. Exiting cleanly.")
                    if _overlay is not None:
                        _overlay.shutdown()
                    return
                # Weekend: nothing to do — exit so the task can sleep until Mon
                if now.weekday() >= 5:
                    log.info("Weekend — nothing to do. Exiting.")
                    if _overlay is not None:
                        _overlay.shutdown()
                    return
                # Edge case: launched AFTER market close on a weekday (e.g.
                # laptop opened at 16:00). Don't loop pointlessly until 9:15
                # tomorrow — exit so the scheduler can fire fresh next day.
                market_close = now.replace(hour=15, minute=30,
                                            second=0, microsecond=0)
                if now >= market_close:
                    log.info(f"Launched after today's close ({now.strftime('%H:%M')}). "
                             "Nothing to do until tomorrow. Exiting.")
                    if _overlay is not None:
                        _overlay.shutdown()
                    return
                # Pre-market: wait a few minutes and re-check
                log.info(f"Pre-market ({now.strftime('%H:%M')}), waiting 5 min...")
                time.sleep(300)
                continue
            market_opened_today = True
            t0 = time.time()
            refresh_once()
            elapsed = time.time() - t0
            sleep_for = max(POLL_INTERVAL_SEC - elapsed, 1)
            log.info(f"Refresh in {elapsed:.1f}s, sleeping {sleep_for:.1f}s")
            time.sleep(sleep_for)
        except KeyboardInterrupt:
            log.info("Stopped by user.")
            if _overlay is not None:
                log.info("Shutting down overlay...")
                _overlay.shutdown()
            break
        except Exception as e:
            log.exception(f"Refresh failed: {e}")
            time.sleep(10)


if __name__ == "__main__":
    main()
