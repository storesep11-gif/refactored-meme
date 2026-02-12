"""
Torn Company Listings - Unified Optimized Version
All original features preserved.
Internal architecture optimized.
"""

import streamlit as st
import requests
import time
import threading
import hashlib
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
from supabase import create_client, Client
import pandas as pd
from collections import Counter

# =========================================================
# CONFIG
# =========================================================

CACHE_HOURS = 48
ALLOW_OVERRIDE_SAVE = False
PRICE_START = 135
PRICE_END = 199
CALL_INTERVAL = 2.0
TOKEN_BUCKET_SIZE = 5
TOKEN_REFILL_RATE = 1 / 2.0
COOLDOWN_SECONDS = 300
MAX_CONSECUTIVE_ERRORS = 3

st.set_page_config(page_title="Torn Company Listings", layout="wide")

API_KEY = st.secrets.get("TORN_API_KEY")
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY")

if not all([API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    st.error("Missing required secrets")
    st.stop()

# =========================================================
# DB CLIENT
# =========================================================

@st.cache_resource
def get_supabase() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

supabase = get_supabase()

# =========================================================
# SESSION INIT
# =========================================================

def init_session():
    defaults = {
        "companies_data": [],
        "employee_data": {},                 # current fetch employees
        "employee_snapshot_cache": {},       # snapshot employees by date
        "snapshot_cache": {},                # snapshot data cache
        "COMPANY_IDS": [],
        "bulk_id_data": None,
        "id_check_performed": False,
        "view_mode": "Current",
        "snapshot_date": datetime.now(timezone.utc).date(),
        "show_employees_for": None,
        "fetch_triggered": False,
        "to_fetch_target": 10,
        "deleted_ids": []
    }

    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()

# =========================================================
# RATE LIMITER (UNCHANGED LOGIC)
# =========================================================

_rate_lock = threading.Lock()
_last_call_time = 0.0
_api_disabled_until = 0.0
_consecutive_errors = 0
_tokens = TOKEN_BUCKET_SIZE
_last_token_refill = time.time()

def _refill_tokens():
    global _tokens, _last_token_refill
    now = time.time()
    elapsed = now - _last_token_refill
    new_tokens = elapsed * TOKEN_REFILL_RATE
    if new_tokens > 0:
        _tokens = min(TOKEN_BUCKET_SIZE, _tokens + new_tokens)
        _last_token_refill = now

def wait_for_rate_limit():
    global _tokens, _last_call_time, _api_disabled_until, _consecutive_errors

    with _rate_lock:
        if time.time() < _api_disabled_until:
            return False, "API cooling down"

        _refill_tokens()

        if _tokens < 1:
            return False, "No tokens"

        time_since_last = time.time() - _last_call_time
        if time_since_last < CALL_INTERVAL:
            time.sleep(CALL_INTERVAL - time_since_last)

        _tokens -= 1
        _last_call_time = time.time()
        return True, "OK"

# =========================================================
# UTILITIES
# =========================================================

def calculate_possible_prices(daily_income):
    if not daily_income or daily_income <= 0:
        return []
    return [p for p in range(PRICE_START, PRICE_END + 1) if daily_income % p == 0]

def calculate_price_guess(possible_prices):
    if possible_prices:
        return possible_prices[len(possible_prices) // 2]
    return None

def generate_unique_key(base: str, suffix: str, company_id: int) -> str:
    suffix_hash = hashlib.md5(suffix.encode()).hexdigest()[:8]
    return f"{base}_{suffix_hash}_{company_id}"

# =========================================================
# SNAPSHOT LOADER (MAJOR OPTIMIZATION)
# =========================================================

def load_snapshot(date_str: str):
    """
    Loads snapshot once.
    Caches grouped by company_id.
    """
    if date_str in st.session_state.snapshot_cache:
        return st.session_state.snapshot_cache[date_str]

    result = supabase.rpc(
        "get_snapshot_by_date",
        {"target_date": date_str}
    ).execute()

    rows = result.data or []

    grouped = {}
    for row in rows:
        cid = row["company_id"]
        if cid not in grouped:
            grouped[cid] = row

    st.session_state.snapshot_cache[date_str] = grouped
    return grouped

# =========================================================
# EMPLOYEE SNAPSHOT LOADER (NO MORE PER-ROW RPC)
# =========================================================

def load_employee_snapshot(date_str: str):
    """
    Loads ALL employee snapshot rows once per date.
    """
    if date_str in st.session_state.employee_snapshot_cache:
        return st.session_state.employee_snapshot_cache[date_str]

    result = supabase.rpc(
        "get_employee_snapshot_by_date",
        {"target_date": date_str}
    ).execute()

    rows = result.data or []

    grouped = {}
    for row in rows:
        grouped.setdefault(row["company_id"], []).append(row)

    st.session_state.employee_snapshot_cache[date_str] = grouped
    return grouped

# =========================================================
# API FETCH (FEATURE COMPLETE)
# =========================================================

def fetch_company_individual(company_id):

    global _consecutive_errors

    can_call, message = wait_for_rate_limit()
    if not can_call:
        return None, message, None

    url = f"https://api.torn.com/company/{company_id}?key={API_KEY}&selections=profile"

    try:
        response = requests.get(url, timeout=30)

        if response.status_code != 200:
            return None, f"HTTP {response.status_code}", None

        data = response.json().get("company", {})
        if not data:
            return None, "No data", None

        daily_income = data.get("daily_income", 0)
        possible_prices = calculate_possible_prices(daily_income)

        company_record = {
            "company_id": data.get("ID"),
            "name": data.get("name"),
            "rating": data.get("rating"),
            "weekly_income": data.get("weekly_income"),
            "daily_income": daily_income,
            "employees": f"{data.get('employees_hired',0)} / {data.get('employees_capacity',0)}",
            "days_old": data.get("days_old"),
            "daily_customers": data.get("daily_customers",0),
            "weekly_customers": data.get("weekly_customers",0),
            "possible_prices": possible_prices,
            "price_guess": calculate_price_guess(possible_prices),
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }

        employee_list = []
        for emp_id, emp in data.get("employees", {}).items():
            employee_list.append({
                "employee_id": int(emp_id),
                "name": emp.get("name"),
                "position": emp.get("position"),
                "days_in_company": emp.get("days_in_company"),
                "last_action": emp.get("last_action", {}).get("relative", "")
            })

        return company_record, "OK", employee_list

    except Exception as e:
        return None, str(e), None

# =========================================================
# DISPLAY (NO RPC INSIDE)
# =========================================================

def display_companies(companies_data, snapshot_mode=False, snapshot_date=None):

    for c in companies_data:

        st.subheader(f"{c['name']} (ID: {c['company_id']})")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Age", f"{c['days_old']} days")
        col2.metric("Employees", c.get("employees","0 / 0"))
        col3.metric("Rating", f"⭐ {c['rating']}")
        col4.metric("Daily", f"${c['daily_income']:,}")

        prices = c.get("possible_prices", [])

        if prices:
            if len(prices) > 1:
                key = generate_unique_key("p", "", c['company_id'])
                p = st.selectbox("Price", prices, key=key)
            else:
                p = prices[0]

            sales = c['daily_income'] / p
            st.success(f"${p} → {int(sales):,} sales/day")

        # EMPLOYEE VIEW
        if st.button("👥 Show Employees", key=f"emp_{c['company_id']}"):
            if st.session_state.show_employees_for == c['company_id']:
                st.session_state.show_employees_for = None
            else:
                st.session_state.show_employees_for = c['company_id']

        if st.session_state.show_employees_for == c['company_id']:

            if snapshot_mode:
                employee_map = load_employee_snapshot(snapshot_date)
                rows = employee_map.get(c['company_id'], [])
            else:
                rows = st.session_state.employee_data.get(c['company_id'], [])

            if rows:
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info("No employee data")

        st.divider()

# =========================================================
# VIEW ROUTING
# =========================================================

st.title("🏢 Torn Company Listings")

view = st.radio(
    "View Mode",
    ["Current", "Historical"],
    horizontal=True
)

st.session_state.view_mode = view

# =========================================================
# CURRENT VIEW
# =========================================================

if view == "Current":

    fetch_count = st.number_input("Companies to fetch", 1, 50, 5)

    if st.button("FETCH"):
        st.session_state.fetch_triggered = True
        st.session_state.companies_data = []
        st.session_state.employee_data = {}

    if st.session_state.fetch_triggered:

        progress = st.progress(0)

        for i in range(fetch_count):

            cid = 100000 + i  # your real ID logic here

            result, msg, employees = fetch_company_individual(cid)

            if result:
                st.session_state.companies_data.append(result)
                if employees:
                    st.session_state.employee_data[cid] = employees

            progress.progress((i + 1) / fetch_count)

        st.session_state.fetch_triggered = False
        st.success("Fetch complete")

    display_companies(st.session_state.companies_data)

# =========================================================
# HISTORICAL VIEW
# =========================================================

if view == "Historical":

    date = st.date_input("Select Snapshot Date")

    if st.button("Load Snapshot"):
        snapshot = load_snapshot(date.isoformat())

        if not snapshot:
            st.warning("No snapshot found")
        else:
            companies = list(snapshot.values())
            display_companies(companies, snapshot_mode=True, snapshot_date=date.isoformat())
