"""
Torn Company Listings - Solo API Reworked Version
Changes: 
- Uses bulk endpoint for ID validation only
- Pre-fetch check with sidebar notices
- Manual individual API calls for data
- Fixed daily income chart
- Added comprehensive debug features
- Added employee data extraction and display
"""

import streamlit as st
import requests
import time
import threading
import hashlib
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any
from supabase import create_client, Client
import pandas as pd
from collections import Counter

# Configuration
CACHE_HOURS = 48
ALLOW_OVERRIDE_SAVE = False
PRICE_START = 135
PRICE_END = 199
CALL_INTERVAL = 2.0
TOKEN_BUCKET_SIZE = 5
TOKEN_REFILL_RATE = 1 / 2.0
COOLDOWN_SECONDS = 300
MAX_CONSECUTIVE_ERRORS = 3

st.set_page_config(page_title="Torn Company Listings - Solo", layout="wide", initial_sidebar_state="expanded")
st.title("🏢 Torn Company Listings (Solo API + Bulk ID Check)")

API_KEY = st.secrets.get("TORN_API_KEY")
SUPABASE_URL = st.secrets.get("SUPABASE_URL")
SUPABASE_KEY = st.secrets.get("SUPABASE_KEY")
if not all([API_KEY, SUPABASE_URL, SUPABASE_KEY]):
    st.error("❌ Missing required secrets")
    st.stop()

@st.cache_resource
def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)
supabase: Client = get_supabase_client()

# Rate limiting globals
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
    """Check rate limiting before making API call"""
    global _tokens, _last_call_time, _api_disabled_until, _consecutive_errors
    
    with _rate_lock:
        if time.time() < _api_disabled_until:
            remaining = int(_api_disabled_until - time.time())
            return False, f"API disabled for {remaining}s"
        
        _refill_tokens()
        
        if _tokens < 1:
            return False, "No tokens available"
        
        time_since_last = time.time() - _last_call_time
        if time_since_last < CALL_INTERVAL:
            time.sleep(CALL_INTERVAL - time_since_last)
        
        _tokens -= 1
        _last_call_time = time.time()
        return True, "OK"

def current_torn_date():
    return datetime.now(timezone.utc).date()

def abbreviate_number(num):
    if num >= 1_000_000:
        return f"{num / 1_000_000:.1f}M"
    elif num >= 1_000:
        return f"{num / 1_000:.1f}K"
    return str(int(num))

def format_number(num, use_abbreviation):
    if use_abbreviation:
        return abbreviate_number(num)
    return f"{int(num):,}"

def calculate_possible_prices(daily_income):
    if not daily_income or daily_income <= 0:
        return []
    return [p for p in range(PRICE_START, PRICE_END + 1) if daily_income % p == 0]

def calculate_price_guess(possible_prices):
    """Calculate price guess on-the-fly from possible prices"""
    if possible_prices and len(possible_prices) > 0:
        return possible_prices[len(possible_prices) // 2]
    return None

def generate_unique_key(base: str, suffix: str, company_id: int) -> str:
    """Generate unique widget key using hash instead of string replacement"""
    # Use hash of suffix to avoid collision between different snapshots
    suffix_hash = hashlib.md5(suffix.encode()).hexdigest()[:8]
    return f"{base}_{suffix_hash}_{company_id}"

def is_cache_stale(fetched_at_str):
    if not fetched_at_str:
        return True
    try:
        fetched_time = datetime.fromisoformat(fetched_at_str.replace('Z', '+00:00'))
        age = datetime.now(timezone.utc) - fetched_time
        return age > timedelta(hours=CACHE_HOURS)
    except:
        return True

def load_all_cached_companies():
    try:
        result = supabase.table("company_cache").select("*").execute()
        companies = []
        for data in result.data:
            if isinstance(data.get("possible_prices"), str):
                import json
                data["possible_prices"] = json.loads(data["possible_prices"])
            data["price_guess"] = calculate_price_guess(data.get("possible_prices", []))
            companies.append(data)
        return companies
    except Exception as e:
        st.error(f"Error loading from cache: {e}")
        return []

def clear_all_cache():
    try:
        supabase.table("company_cache").delete().neq("company_id", 0).execute()
        return True
    except Exception as e:
        st.error(f"Error clearing cache: {e}")
        return False

# ==================== BULK API FOR ID CHECKING ONLY ====================
BULK_API_URL = f"https://api.torn.com/company/28?key={API_KEY}&comment=TornAPI&selections=companies"

def fetch_company_ids_bulk():
    """
    Fetch active company IDs from bulk API. 
    SINGLE API CALL - used for pre-validation only.
    Returns: dict with 'ids' (list) and 'count' (int)
    """
    global _consecutive_errors, _api_disabled_until
    
    can_call, message = wait_for_rate_limit()
    if not can_call:
        return None, message
    
    try:
        response = requests.get(BULK_API_URL, timeout=30)
        
        if response.status_code == 429:
            _api_disabled_until = time.time() + COOLDOWN_SECONDS
            _consecutive_errors += 1
            return None, "Rate limit exceeded (429)"
            
        if response.status_code != 200:
            return None, f"HTTP {response.status_code}"
        
        data = response.json()
        if "error" in data:
            error_code = data.get("error", {}).get("code", 0)
            return None, f"API Error {error_code}"
        
        companies_data = data.get("company", {})
        if not companies_data:
            return None, "No company data"
        
        # Get IDs with daily_income > 0 (active companies)
        active_ids = []
        for company_id_str, info in companies_data.items():
            if info.get("daily_income", 0) > 0:
                active_ids.append(int(company_id_str))
        
        _consecutive_errors = 0
        
        return {
            "ids": sorted(active_ids),
            "count": len(active_ids),
            "raw_data": companies_data  # Store for quick lookup if needed
        }, "OK"
        
    except requests.RequestException as e:
        _consecutive_errors += 1
        return None, f"Network error: {str(e)}"
    except Exception as e:
        _consecutive_errors += 1
        return None, f"Error: {str(e)}"

def compare_with_snapshot(current_ids: List[int]):
    """Compare current API IDs with latest snapshot. Returns changes dict."""
    try:
        result = supabase.rpc("get_distinct_snapshot_dates").execute()
        if not result.data:
            return None, "No snapshots found"
        
        latest_date = result.data[0]["snapshot_date"]
        result = supabase.rpc("get_snapshot_by_date", {"target_date": latest_date}).execute()
        snapshot_ids = {row["company_id"] for row in result.data}
        current_set = set(current_ids)
        
        added = current_set - snapshot_ids
        removed = snapshot_ids - current_set
        
        return {
            "date": latest_date,
            "snapshot_count": len(snapshot_ids),
            "current_count": len(current_set),
            "added": sorted(added),
            "removed": sorted(removed),
            "unchanged": len(current_set & snapshot_ids)
        }, "OK"
        
    except Exception as e:
        return None, f"Comparison error: {e}"

# ==================== EMPLOYEE DATA PARSING ====================
def parse_employee_data(company_data: dict) -> List[Dict]:
    """
    Extract employee data from solo API response.
    Returns list of employee records or empty list if no employees.
    """
    employees = company_data.get("employees", {})
    if not employees:
        return []
    
    employee_list = []
    for emp_id, emp_data in employees.items():
        if not isinstance(emp_data, dict):
            continue
            
        # Handle last_action
        last_action = emp_data.get("last_action", {})
        if isinstance(last_action, dict):
            last_action_relative = last_action.get("relative", "")
            last_action_status = last_action.get("status", "")
            last_action_timestamp = last_action.get("timestamp", 0)
        else:
            last_action_relative = ""
            last_action_status = ""
            last_action_timestamp = 0
        
        # Handle status
        status = emp_data.get("status", {})
        if isinstance(status, dict):
            status_description = status.get("description", "")
            status_state = status.get("state", "")
            status_color = status.get("color", "")
            status_until = status.get("until", 0)
        else:
            status_description = ""
            status_state = ""
            status_color = ""
            status_until = 0
        
        employee_list.append({
            "employee_id": int(emp_id),
            "name": emp_data.get("name", ""),
            "position": emp_data.get("position", ""),
            "days_in_company": emp_data.get("days_in_company", 0),
            "last_action_relative": last_action_relative,
            "last_action_status": last_action_status,
            "last_action_timestamp": last_action_timestamp,
            "status_description": status_description,
            "status_state": status_state,
            "status_color": status_color,
            "status_until": status_until
        })
    
    return employee_list

def get_position_summary(employees: List[Dict]) -> Dict[str, int]:
    """Get count of employees by position."""
    if not employees:
        return {}
    positions = [emp["position"] for emp in employees if emp.get("position")]
    return dict(Counter(positions))

def format_position_summary(positions: Dict[str, int]) -> str:
    """Format position summary for display."""
    if not positions:
        return "No employees"
    # Sort by count descending, then by position name
    sorted_pos = sorted(positions.items(), key=lambda x: (-x[1], x[0]))
    return " | ".join([f"{pos}: {count}" for pos, count in sorted_pos])

# ==================== INDIVIDUAL API FOR DATA FETCHING ====================
INDIVIDUAL_API_URL = "https://api.torn.com/company/{company_id}?key={API_KEY}&selections=profile"

def fetch_company_individual(company_id):
    """
    Fetch individual company data via API call with rate limiting.
    This is the SOLO API approach - one call per company.
    NOW ALSO extracts employee data.
    """
    global _consecutive_errors, _api_disabled_until
    
    can_call, message = wait_for_rate_limit()
    if not can_call:
        return None, message, None
    
    url = INDIVIDUAL_API_URL.format(company_id=company_id, API_KEY=API_KEY)
    
    try:
        response = requests.get(url, timeout=30)
        
        if response.status_code == 429:
            _consecutive_errors += 1
            if _consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                _api_disabled_until = time.time() + COOLDOWN_SECONDS
                return None, f"Rate limit exceeded. API disabled for {COOLDOWN_SECONDS}s", None
            return None, "Rate limit (429)", None
            
        if response.status_code != 200:
            _consecutive_errors += 1
            return None, f"HTTP {response.status_code}", None
        
        data = response.json()
        
        if "error" in data:
            error_code = data.get("error", {}).get("code", 0)
            if error_code == 6:  # Company not found
                return None, "Company not found", None
            _consecutive_errors += 1
            return None, f"API Error {error_code}", None
        
        company_data = data.get("company", {})
        if not company_data:
            return None, "No data", None
        
        _consecutive_errors = 0
        
        # Extract employee data from same API response
        employee_list = parse_employee_data(company_data)
        position_summary = get_position_summary(employee_list)
        
        daily_income = company_data.get("daily_income", 0)
        possible_prices = calculate_possible_prices(daily_income)
        price_guess = calculate_price_guess(possible_prices)
        
        company_record = {
            "company_id": company_data.get("ID", company_id),
            "name": company_data.get("name", "Unknown"),
            "rating": company_data.get("rating", 0),
            "weekly_income": company_data.get("weekly_income", 0),
            "daily_income": daily_income,
            "employees": f"{company_data.get('employees_hired', 0)} / {company_data.get('employees_capacity', 0)}",
            "days_old": company_data.get("days_old", 0),
            "daily_customers": company_data.get("daily_customers", 0),
            "weekly_customers": company_data.get("weekly_customers", 0),
            "possible_prices": possible_prices,
            "price_guess": price_guess,
            "fetched_at": datetime.now(timezone.utc).isoformat()
        }
        
        # Save to cache
        try:
            supabase.table("company_cache").upsert(company_record).execute()
        except Exception:
            pass
        
        return company_record, "OK", employee_list
        
    except requests.RequestException as e:
        _consecutive_errors += 1
        return None, f"Network error: {str(e)}", None
    except Exception as e:
        _consecutive_errors += 1
        return None, f"Error: {str(e)}", None

# ==================== SNAPSHOT FUNCTIONS ====================
def get_existing_snapshots(snapshot_date):
    try:
        result = supabase.rpc("get_snapshot_by_date", {"target_date": snapshot_date}).execute()
        return {row["company_id"] for row in result.data}
    except Exception:
        return set()

# ==================== EMPLOYEE FETCH (DATABASE - RLS SAFE) ====================
def fetch_employee_snapshot_from_db(company_id: int, snapshot_date):
    """
    Fetch employee snapshot from database using RPC (RLS safe).
    """
    try:
        result = supabase.rpc(
            "get_employee_snapshot_by_company_and_date",
            {
                "target_company_id": company_id,
                "target_date": snapshot_date
            }
        ).execute()

        if result.data:
            return result.data
        return []
    except Exception:
        return []


def check_existing_snapshots_count(snapshot_date):
    try:
        result = supabase.rpc("get_snapshot_by_date", {"target_date": snapshot_date}).execute()
        return len(result.data)
    except Exception:
        return 0

def save_snapshots(companies, snapshot_date):
    """
    Save snapshots to database. Returns (saved_count, skipped_count, error_message).
    Matches Test 11 behavior: returns error string instead of displaying it.
    """
    global ALLOW_OVERRIDE_SAVE
    
    # Check for duplicate IDs in input data first
    seen_ids = set()
    unique_companies = []
    for c in companies:
        cid = c["company_id"]
        if cid not in seen_ids:
            seen_ids.add(cid)
            unique_companies.append(c)
    
    if ALLOW_OVERRIDE_SAVE:
        try:
            supabase.table("company_snapshots").delete().eq("snapshot_date", snapshot_date.isoformat()).execute()
            existing = set()
        except Exception as e:
            return 0, 0, f"Error clearing existing snapshots: {e}"
    else:
        existing = get_existing_snapshots(snapshot_date)
    
    to_save = []
    for c in unique_companies:
        cid = c["company_id"]
        if cid not in existing:
            possible_prices = c.get("possible_prices", [])
            price_guess = calculate_price_guess(possible_prices)
            estimated_sales = None
            if price_guess and c.get("daily_income"):
                estimated_sales = c['daily_income'] / price_guess
            
            # Ensure employees is never None
            employees = c.get("employees")
            if employees is None:
                employees = "0 / 0"
            
            to_save.append({
                "snapshot_date": snapshot_date.isoformat(),
                "company_id": cid,
                "name": c["name"],
                "rating": c["rating"],
                "weekly_income": c["weekly_income"],
                "employees": employees,
                "daily_income": c["daily_income"],
                "days_old": c["days_old"],
                "daily_customers": c.get("daily_customers", 0),
                "weekly_customers": c.get("weekly_customers", 0),
                "possible_prices": possible_prices,
                "price_guess": price_guess,
                "estimated_sales": estimated_sales,
                "metadata": {"saved_at": datetime.now(timezone.utc).isoformat(), "source": "solo_api"}
            })
    
    if not to_save:
        return 0, len(existing), None  # No error, just nothing to save
    
    try:
        batch_size = 50
        for i in range(0, len(to_save), batch_size):
            batch = to_save[i:i + batch_size]
            supabase.table("company_snapshots").insert(batch).execute()
        return len(to_save), len(existing), None  # Success, no error
    except Exception as e:
        error_str = str(e)
        if "duplicate key" in error_str.lower():
            return 0, 0, f"Duplicate key error: Some companies already exist for this date"
        return 0, 0, f"Save error: {error_str}"

def save_employee_snapshots(company_id: int, employees: List[Dict], snapshot_date):
    """
    Save employee snapshots to database.
    Called when saving company snapshots.
    """
    if not employees:
        return 0, None
    
    to_save = []
    for emp in employees:
        to_save.append({
            "snapshot_date": snapshot_date.isoformat(),
            "company_id": company_id,
            "employee_id": emp["employee_id"],
            "name": emp["name"],
            "position": emp["position"],
            "days_in_company": emp["days_in_company"],
            "last_action": emp["last_action_relative"],  # "6 hours ago"
            "created_at": datetime.now(timezone.utc).isoformat()
        })
    
    try:
        # Delete existing employees for this company/date to avoid duplicates
        supabase.table("employee_snapshots").delete().eq("company_id", company_id).eq("snapshot_date", snapshot_date.isoformat()).execute()
        
        batch_size = 50
        for i in range(0, len(to_save), batch_size):
            batch = to_save[i:i + batch_size]
            supabase.table("employee_snapshots").insert(batch).execute()
        return len(to_save), None
    except Exception as e:
        return 0, str(e)

# ==================== SESSION INITIALIZATION ====================
def init_session():
    defaults = {
        "companies_data": [],
        "fetch_index": 0,
        "deleted_ids": [],
        "filter_age_min": None,
        "filter_age_max": None,
        "filter_rating_min": None,
        "filter_rating_max": None,
        "filter_selected_ids": [],
        "view_mode": "Current",
        "snapshot_date": current_torn_date(),
        "to_fetch_target": 10,
        "fetch_triggered": False,
        "COMPANY_IDS": [],  # Now populated from bulk check
        "bulk_id_data": None,  # Store bulk check results
        "id_check_performed": False,
        "show_customers_in_history": False,
        "show_employees_for": None  # Which company to show employees for
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v
init_session()

# ==================== SIDEBAR ====================
st.sidebar.header("🔍 Filters & Actions")

# ==================== ID CHECK SECTION ====================
st.sidebar.markdown("### 🆔 Company ID Validation")
st.sidebar.caption("Check active companies via bulk API (instant)")

if st.sidebar.button("🔍 Check Active IDs", type="primary", use_container_width=True):
    with st.spinner("Fetching active company IDs..."):
        result, message = fetch_company_ids_bulk()
    
    if result is None:
        st.sidebar.error(f"❌ Failed: {message}")
    else:
        st.session_state.bulk_id_data = result
        st.session_state.COMPANY_IDS = result["ids"]
        st.session_state.id_check_performed = True
        
        # Compare with snapshot
        comparison, comp_msg = compare_with_snapshot(result["ids"])
        
        st.sidebar.markdown("---")
        st.sidebar.success(f"✅ Found {result['count']} active companies")
        
        if comparison:
            st.sidebar.markdown(f"**📊 vs Snapshot ({comparison['date']})**")
            
            col1, col2 = st.sidebar.columns(2)
            with col1:
                st.metric("Snapshot", comparison['snapshot_count'])
            with col2:
                st.metric("Current", comparison['current_count'])
            
            if comparison['added']:
                st.sidebar.success(f"🆕 {len(comparison['added'])} new companies")
                with st.sidebar.expander(f"View new IDs"):
                    st.sidebar.write(comparison['added'][:20])  # Show first 20
                    if len(comparison['added']) > 20:
                        st.sidebar.caption(f"...and {len(comparison['added']) - 20} more")
            
            if comparison['removed']:
                st.sidebar.error(f"🗑️ {len(comparison['removed'])} closed companies")
                with st.sidebar.expander(f"View closed IDs"):
                    st.sidebar.write(comparison['removed'][:20])
                    if len(comparison['removed']) > 20:
                        st.sidebar.caption(f"...and {len(comparison['removed']) - 20} more")
            
            if not comparison['added'] and not comparison['removed']:
                st.sidebar.info("✅ No changes since last snapshot")
        else:
            st.sidebar.warning(f"⚠️ Could not compare: {comp_msg}")

# Show current ID status
if st.session_state.id_check_performed and st.session_state.COMPANY_IDS:
    st.sidebar.caption(f"📋 {len(st.session_state.COMPANY_IDS)} IDs ready for fetch")
else:
    st.sidebar.caption("⏳ Run 'Check Active IDs' first")

st.sidebar.markdown("---")

# ==================== SNAPSHOT DATE ====================
st.sidebar.markdown("### 📅 Snapshot Date")
snapshot_date = st.sidebar.date_input(
    "Select date for snapshots", 
    value=st.session_state.snapshot_date,
    help="Choose date to save as. Useful for timezone differences."
)
st.session_state.snapshot_date = snapshot_date

existing_count = check_existing_snapshots_count(snapshot_date)
if existing_count > 0:
    if ALLOW_OVERRIDE_SAVE:
        st.sidebar.warning(f"⚠️ {existing_count} snapshots exist for {snapshot_date} - will be REPLACED")
    else:
        st.sidebar.info(f"ℹ️ {existing_count} snapshots already exist for {snapshot_date}")

# FIX: Match Test 11 sidebar save behavior exactly
if st.sidebar.button("💾 SAVE SNAPSHOT", type="primary", use_container_width=True, 
                     help=f"Save current data to snapshot date {snapshot_date}"):
    if not st.session_state.companies_data:
        st.sidebar.error("No data to save!")
    else:
        with st.spinner("Saving snapshots..."):
            saved_companies = 0
            saved_employees = 0
            error_msg = None
            
            # Save company snapshots
            saved, skipped, error_msg = save_snapshots(st.session_state.companies_data, snapshot_date)
            saved_companies = saved
            
            # Save employee snapshots if we have employee data
            if not error_msg and st.session_state.employee_data:
                for company_id, employees in st.session_state.employee_data.items():
                    emp_saved, emp_error = save_employee_snapshots(company_id, employees, snapshot_date)
                    if emp_error:
                        error_msg = f"Employee save error: {emp_error}"
                        break
                    saved_employees += emp_saved
        
        if error_msg:
            st.sidebar.error(f"❌ {error_msg}")
        elif ALLOW_OVERRIDE_SAVE:
            st.sidebar.success(f"✅ Saved {saved_companies} companies, {saved_employees} employees to {snapshot_date} (replaced existing)")
        else:
            if saved_companies:
                st.sidebar.success(f"✅ Saved {saved_companies} companies, {saved_employees} employees to {snapshot_date}")
            if skipped:
                st.sidebar.info(f"ℹ️ Skipped {skipped} already exist")
            if saved_companies == 0 and skipped == 0:
                st.sidebar.warning("No companies to save")

st.sidebar.markdown("---")

# ==================== FILTERS ====================
data = st.session_state.companies_data
if data:
    ages = [c["days_old"] for c in data]
    ratings = [c["rating"] for c in data]
    min_age, max_age = 0, max(10000, int(max(ages)))
    min_rating, max_rating = 0, max(10, int(max(ratings)))
    age_default = (int(min(ages)), int(max(ages)))
    rating_default = (int(min(ratings)), int(max(ratings)))
else:
    min_age, max_age = 0, 10000
    min_rating, max_rating = 0, 10
    age_default = (0, 10000)
    rating_default = (0, 10)

age_slider_value = (st.session_state.filter_age_min, st.session_state.filter_age_max) if st.session_state.filter_age_min is not None else age_default
rating_slider_value = (st.session_state.filter_rating_min, st.session_state.filter_rating_max) if st.session_state.filter_rating_min is not None else rating_default

age_min, age_max = st.sidebar.slider("Age (days)", min_value=min_age, max_value=max_age, value=age_slider_value)
rating_min, rating_max = st.sidebar.slider("Rating", min_value=min_rating, max_value=max_rating, value=rating_slider_value)
st.session_state.filter_age_min, st.session_state.filter_age_max = age_min, age_max
st.session_state.filter_rating_min, st.session_state.filter_rating_max = rating_min, rating_max

st.sidebar.markdown("---")
st.sidebar.subheader("⚡ Actions")

if st.sidebar.button("📦 Load All Cached", use_container_width=True):
    with st.spinner("Loading from cache..."):
        cached = load_all_cached_companies()
        st.session_state.companies_data = cached
    st.sidebar.success(f"✅ Loaded {len(cached)} companies")
    st.rerun()

if st.sidebar.button("🗑️ Clear Cache", use_container_width=True, type="secondary"):
    with st.spinner("Clearing cache..."):
        success = clear_all_cache()
    if success:
        st.sidebar.success("✅ Cache cleared!")
    st.rerun()

if st.sidebar.button("🔄 Reset Filters", use_container_width=True):
    st.session_state.filter_age_min = st.session_state.filter_age_max = None
    st.session_state.filter_rating_min = st.session_state.filter_rating_max = None
    st.session_state.filter_selected_ids = []
    st.rerun()

if st.sidebar.button("🔄 Refetch All", use_container_width=True, type="secondary"):
    st.session_state.companies_data = []
    st.session_state.fetch_index = 0
    st.session_state.deleted_ids = []
    st.rerun()

# ==================== DEBUG SECTION ====================
st.sidebar.markdown("---")
st.sidebar.subheader("🔧 DEBUG TOOLS")

# Debug 1: API Rate Limit Status
with st.sidebar.expander("1. API Rate Limit Status"):
    st.write(f"Tokens: {_tokens:.1f}/{TOKEN_BUCKET_SIZE}")
    st.write(f"Last call: {time.time() - _last_call_time:.1f}s ago")
    st.write(f"API disabled for: {max(0, _api_disabled_until - time.time()):.0f}s")
    st.write(f"Consecutive errors: {_consecutive_errors}")

# Debug 2: Database RPC Health
with st.sidebar.expander("2. Database RPC Health"):
    rpc_functions = {
        "get_distinct_snapshot_dates": {},
        "get_snapshot_by_date": {"target_date": str(current_torn_date())},
        "get_snapshots_in_range": {"start_date": "2024-01-01", "end_date": "2024-01-01"}
    }
    for func, params in rpc_functions.items():
        try:
            if params:
                test = supabase.rpc(func, params).execute()
            else:
                test = supabase.rpc(func).execute()
            st.success(f"✅ {func}")
        except Exception as e:
            st.error(f"❌ {func}: {str(e)[:30]}")

# Debug 3: Session State Keys
with st.sidebar.expander("3. Session State Keys"):
    price_keys = [k for k in st.session_state.keys() if 'p-nc' in str(k)]
    st.write(f"Keys with 'p-nc': {len(price_keys)}")
    if price_keys:
        st.write(price_keys[:5])

# Debug 4: Data Consistency
with st.sidebar.expander("4. Data Consistency"):
    st.write(f"View mode: {st.session_state.view_mode}")
    st.write(f"Companies loaded: {len(st.session_state.companies_data)}")
    st.write(f"IDs available: {len(st.session_state.COMPANY_IDS)}")
    st.write(f"ID check performed: {st.session_state.id_check_performed}")
    st.write(f"Employee data stored: {len(st.session_state.employee_data)} companies")
    
    if st.session_state.companies_data:
        ids = [c['company_id'] for c in st.session_state.companies_data]
        duplicates = [cid for cid in set(ids) if ids.count(cid) > 1]
        st.write(f"Duplicate IDs: {duplicates if duplicates else 'None'}")

# Debug 5: Price Calculation
with st.sidebar.expander("5. Price Calculation Sample"):
    if st.session_state.companies_data:
        sample = st.session_state.companies_data[0]
        daily = sample.get('daily_income', 0)
        possible = sample.get('possible_prices', [])
        st.write(f"Company: {sample.get('name')}")
        st.write(f"Daily income: ${daily:,}")
        st.write(f"Price range: {PRICE_START}-{PRICE_END}")
        st.write(f"Possible prices: {possible[:10]}...")
        st.write(f"Calculated guess: {calculate_price_guess(possible)}")
    else:
        st.write("No data loaded")

# Debug 6: Key Sanitizer Test
with st.sidebar.expander("6. Key Sanitizer Test"):
    test_cases = [
        " (Snapshot: 2026-02-09)",
        " (Snapshot: 2026-02-05)", 
        "",
        " (Historical)",
        " (Current Data)"
    ]
    st.write("Hash-based keys (new):")
    for test in test_cases:
        key = generate_unique_key("p-nc", test, 103190)
        st.code(f"{test[:20]}... → {key}")

# Debug 7: Memory Usage
with st.sidebar.expander("7. Session Memory"):
    total_size = 0
    for key, value in st.session_state.items():
        size = sys.getsizeof(str(value))
        total_size += size
        if size > 5000:  # Show items > 5KB
            st.write(f"• {key}: {size/1024:.1f} KB")
    st.write(f"Total: {total_size/1024:.1f} KB")

# Debug 8: Bulk ID Data
with st.sidebar.expander("8. Last Bulk Check"):
    if st.session_state.bulk_id_data:
        st.write(f"IDs count: {st.session_state.bulk_id_data['count']}")
        st.write(f"First 5 IDs: {st.session_state.bulk_id_data['ids'][:5]}")
        st.write(f"Last 5 IDs: {st.session_state.bulk_id_data['ids'][-5:]}")
    else:
        st.write("No bulk check performed yet")

# ==================== MAIN VIEW HEADER ====================
st.markdown(f"🕒 **Torn (GMT+0):** {current_torn_date()} | **Selected:** {snapshot_date}")
total_available = len(st.session_state.COMPANY_IDS) if st.session_state.COMPANY_IDS else 0
st.markdown(f"📋 **Available Companies:** {total_available} (Type 28 - Oil)")

st.markdown("### 🖥️ View Mode")
view_mode_display = st.radio(
    "Select view",
    ["📊 Current Data", "📅 Historical Snapshot", "📈 History Charts", "🏢 Legacy"],
    horizontal=True,
    label_visibility="collapsed",
    index=["Current", "Historical", "History Charts", "Legacy"].index(st.session_state.view_mode)
)

view_map = {
    "📊 Current Data": "Current", 
    "📅 Historical Snapshot": "Historical", 
    "📈 History Charts": "History Charts",
    "🏢 Legacy": "Legacy"
}
if view_map[view_mode_display] != st.session_state.view_mode:
    st.session_state.view_mode = view_map[view_mode_display]
    st.rerun()

st.markdown("---")

# ==================== COMPANY SELECTOR ====================
def get_available_companies():
    """Get companies based on current view mode"""
    if st.session_state.view_mode in ["Current", "Legacy"]:
        return st.session_state.companies_data
    else:
        try:
            result = supabase.rpc("get_distinct_snapshot_dates").execute()
            if not result.data:
                return []
            latest_date = result.data[0]["snapshot_date"]
            result = supabase.rpc("get_snapshot_by_date", {"target_date": latest_date}).execute()
            company_map = {}
            for row in result.data:
                cid = row["company_id"]
                if cid not in company_map:
                    possible_prices = row.get("possible_prices", [])
                    row["price_guess"] = calculate_price_guess(possible_prices)
                    company_map[cid] = row
            return [
                {"company_id": cid, "name": data["name"], "days_old": data.get("days_old", 0), "rating": data.get("rating", 0)}
                for cid, data in company_map.items()
            ]
        except:
            return []

available_companies = get_available_companies()
selected_ids = []

if available_companies:
    select_options = {
        f"{c['name'][:25]}... (ID:{c['company_id']}, {c['days_old']}d, ⭐{c['rating']})": int(c["company_id"]) 
        for c in sorted(available_companies, key=lambda x: x['name'])
    }
    
    default_selection = st.session_state.filter_selected_ids
    default_labels = [label for label, cid in select_options.items() if cid in default_selection]
    
    max_select = 5 if st.session_state.view_mode == "History Charts" else len(select_options)
    
    selected_labels = st.multiselect(
        "🏢 Quick Select", 
        options=list(select_options.keys()), 
        default=default_labels,
        max_selections=max_select,
        key=f"company_select_{st.session_state.view_mode}"
    )
    
    selected_ids = [select_options[label] for label in selected_labels]
    st.session_state.filter_selected_ids = selected_ids
    
    if selected_ids:
        st.caption(f"📌 Filtered to {len(selected_ids)} selected companies")

# ==================== VIEW FUNCTIONS ====================
def display_companies(companies_data, compact_view, use_abbr, age_min, age_max, rating_min, rating_max, selected_ids, title_suffix=""):
    if not companies_data:
        st.info(f"No companies to display{title_suffix}")
        return
    
    filtered = []
    for c in companies_data:
        age_ok = age_min <= c["days_old"] <= age_max
        rating_ok = rating_min <= c["rating"] <= rating_max
        company_id_int = int(c["company_id"])
        selected_ok = len(selected_ids) == 0 or company_id_int in selected_ids
        if age_ok and rating_ok and selected_ok:
            filtered.append(c)
    
    filtered = sorted(filtered, key=lambda x: (x['rating'], x['weekly_income']), reverse=True)
    
    st.markdown(f"### Showing {len(filtered)} of {len(companies_data)} companies{title_suffix}")
    if len(selected_ids) > 0:
        st.caption(f"📌 Filtered to {len(selected_ids)} selected companies")
    
    for c in filtered:
        possible_prices = c.get("possible_prices", [])
        if not c.get("price_guess"):
            c["price_guess"] = calculate_price_guess(possible_prices)
        
        employees = c.get("employees")
        if employees is None:
            employees = "0 / 0"
        
        # Get employee data for this company if available
        company_id = c['company_id']
        employee_list = st.session_state.employee_data.get(company_id, [])
        position_summary = get_position_summary(employee_list)
        
        prices = possible_prices
        if compact_view:
            # Compact view with employee summary
            col1, col2 = st.columns([3, 1])
            with col1:
                st.markdown(f"**{c['name']}** (ID: {c['company_id']}, {c['days_old']} days, {employees})  \n⭐ {c['rating']} | 💰 ${format_number(c['daily_income'], use_abbr)}/day | 📈 ${format_number(c['weekly_income'], use_abbr)}/wk")
            
            with col2:
                # Employee button
                if employee_list:
                    if st.button(f"👥 {len(employee_list)}", key=f"emp_btn_{company_id}", help="View employees"):
                        if st.session_state.show_employees_for == company_id:
                            st.session_state.show_employees_for = None
                        else:
                            st.session_state.show_employees_for = company_id
                        st.rerun()
                else:
                    st.caption("No emp data")
            
            # Show position summary if available
            if position_summary:
                pos_text = format_position_summary(position_summary)
                st.caption(f"📊 {pos_text}")
            
            # Show employee details if expanded
            if st.session_state.show_employees_for == company_id and employee_list:
                with st.expander("👥 Employee Details", expanded=True):
                    emp_df = pd.DataFrame([
                        {
                            "Name": emp["name"],
                            "Position": emp["position"],
                            "Days": emp["days_in_company"],
                            "Last Active": emp["last_action_relative"] or "Unknown",
                            "Status": emp["status_state"] or "Okay"
                        }
                        for emp in sorted(employee_list, key=lambda x: x["position"])
                    ])
                    st.dataframe(emp_df, use_container_width=True, hide_index=True)
            
            if len(prices) > 1:
                key = generate_unique_key("p", title_suffix, c['company_id'])
                p = st.selectbox("Price", prices, key=key)
                sales = c['daily_income'] / p
                st.success(f"💵 ${p} → {format_number(sales, use_abbr)} sales/day")
            elif len(prices) == 1:
                p = prices[0]
                sales = c['daily_income'] / p
                st.success(f"💵 ${p} → {format_number(sales, use_abbr)} sales/day")
            else:
                st.caption("❌ No price match")
            st.divider()
        else:
            # NON-COMPACT VIEW with employee integration
            st.subheader(f"{c['name']} (ID: {c['company_id']})")
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Age", f"{c['days_old']} days")
            col2.metric("Employees", employees)
            col3.metric("Rating", f"⭐ {c['rating']}")
            col4.metric("Daily", f"${format_number(c['daily_income'], use_abbr)}")
            
            col5, col6, col7, col8 = st.columns(4)
            col5.metric("Weekly", f"${format_number(c['weekly_income'], use_abbr)}")
            col6.metric("Daily Cust", f"{c.get('daily_customers', 0) or 0:,}")
            col7.metric("Weekly Cust", f"{c.get('weekly_customers', 0) or 0:,}")
            
            # Employee summary in 8th column
            if position_summary:
                pos_text = format_position_summary(position_summary)
                col8.metric("Positions", f"{len(position_summary)} types")
                col8.caption(pos_text[:50] + "..." if len(pos_text) > 50 else pos_text)
            else:
                col8.metric("Emp Data", "None")
            
            # Employee expander
            if employee_list:
                with st.expander(f"👥 View {len(employee_list)} Employees"):
                    emp_cols = st.columns([2, 2, 1, 2, 2])
                    emp_cols[0].write("**Name**")
                    emp_cols[1].write("**Position**")
                    emp_cols[2].write("**Days**")
                    emp_cols[3].write("**Last Active**")
                    emp_cols[4].write("**Status**")
                    
                    for emp in sorted(employee_list, key=lambda x: (x["position"], x["name"])):
                        emp_cols[0].write(emp["name"])
                        emp_cols[1].write(emp["position"])
                        emp_cols[2].write(str(emp["days_in_company"]))
                        emp_cols[3].write(emp["last_action_relative"] or "-")
                        
                        # Status with color indicator
                        status = emp["status_state"] or "Okay"
                        color = emp["status_color"] or "green"
                        color_emoji = {"green": "🟢", "red": "🔴", "blue": "🔵", "yellow": "🟡"}.get(color, "⚪")
                        emp_cols[4].write(f"{color_emoji} {status}")
            else:
                st.caption("👥 No detailed employee data available")
            
            if prices:
                if len(prices) > 1:
                    with st.expander("💵 Price Analysis"):
                        key = generate_unique_key("p_nc", title_suffix, c['company_id'])
                        p = st.selectbox("Price", prices, key=key)
                        sales = c['daily_income'] / p
                        st.success(f"💵 ${p} → {format_number(sales, use_abbr)}/day")
                elif len(prices) == 1:
                    p = prices[0]
                    sales = c['daily_income'] / p
                    st.success(f"💵 ${p} → {format_number(sales, use_abbr)}/day")
                    st.caption(f"Price: ${p}")
                else:
                    st.caption("❌ No price match")
            else:
                st.caption("❌ No price match")
            
            if c.get("price_guess"):
                guess_sales = c['daily_income'] / c["price_guess"]
                st.caption(f"💡 Price Guess: ${c['price_guess']} → {format_number(guess_sales, use_abbr)} sales/day")
            st.divider()

def show_history_charts(supabase_client, use_abbr, selected_ids):
    st.markdown("### 📊 Company History Viewer")
    
    try:
        dates_result = supabase_client.rpc("get_distinct_snapshot_dates").execute()
        
        if not dates_result.data:
            st.info("No snapshots available. Save some data first!")
            return
        
        available_dates = [d["snapshot_date"] for d in dates_result.data]
        
        if not selected_ids:
            st.caption("Select at least one company to view history")
            return
        
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.selectbox("From date", available_dates, index=len(available_dates)-1, key="history_start_date")
        with col2:
            end_date = st.selectbox("To date", available_dates, index=0, key="history_end_date")
        
        if start_date > end_date:
            st.error("Start date must be before end date")
            return
        
        show_customers = st.radio(
            "Show Customers in Table",
            ["Hide", "Show"],
            horizontal=True,
            index=1 if st.session_state.show_customers_in_history else 0,
            key="history_customers_toggle"
        )
        st.session_state.show_customers_in_history = (show_customers == "Show")
        
        range_result = supabase_client.rpc("get_snapshots_in_range", {
            "start_date": start_date, 
            "end_date": end_date
        }).execute()
        
        if not range_result.data:
            st.warning("No data found for selected date range")
            return
        
        history_data = []
        for row in range_result.data:
            if selected_ids and row["company_id"] not in selected_ids:
                continue
            entry = {
                "Company": row["name"],
                "Date": row["snapshot_date"],
                "Daily Income": format_number(row["daily_income"], use_abbr),
                "Weekly Income": format_number(row["weekly_income"], use_abbr),
                "Rating": str(row["rating"]),
                "Employees": row.get("employees") or "0 / 0",
                "Age": str(row["days_old"])
            }
            if st.session_state.show_customers_in_history:
                entry["Daily Cust"] = str(row.get("daily_customers", 0) or 0)
                entry["Weekly Cust"] = str(row.get("weekly_customers", 0) or 0)
            history_data.append(entry)
        
        if not history_data:
            st.warning("No data found for selected companies and date range")
            return
        
        st.markdown("#### Historical Data")
        df = pd.DataFrame(history_data)
        styled_df = df.style.set_properties(**{'text-align': 'left'})
        st.dataframe(styled_df, use_container_width=True, hide_index=True)
        
        st.markdown("#### Chart Settings")
        chart_type = st.selectbox("Chart type", ["Line", "Bar", "Area"], key="history_chart_type")
        
        st.markdown("**Rating Trend**")
        chart_data = df[["Company", "Date", "Rating"]].copy()
        chart_data["Rating"] = pd.to_numeric(chart_data["Rating"])
        chart_data = chart_data.pivot(index="Date", columns="Company", values="Rating")
        
        if chart_type == "Line":
            st.line_chart(chart_data)
        elif chart_type == "Bar":
            st.bar_chart(chart_data)
        elif chart_type == "Area":
            st.area_chart(chart_data)
        
        st.markdown("**Daily Income Trend**")
        chart_income_data = []
        for row in range_result.data:
            if selected_ids and row["company_id"] not in selected_ids:
                continue
            chart_income_data.append({
                "Company": row["name"], 
                "Date": row["snapshot_date"], 
                "Daily Income": row["daily_income"]
            })
        
        income_df = pd.DataFrame(chart_income_data)
        if not income_df.empty:
            income_pivot = income_df.pivot(index="Date", columns="Company", values="Daily Income")
            
            if chart_type == "Line":
                st.line_chart(income_pivot)
            elif chart_type == "Bar":
                st.bar_chart(income_pivot)
            elif chart_type == "Area":
                st.area_chart(income_pivot)
        else:
            st.info("No income data available for chart")
        
    except Exception as e:
        st.error(f"History module error: {e}")
        import traceback
        st.code(traceback.format_exc())

def show_historical_snapshot(supabase_client, compact_view, use_abbr, age_min, age_max, rating_min, rating_max, selected_ids):
    try:
        dates_result = supabase_client.rpc("get_distinct_snapshot_dates").execute()
        
        if not dates_result.data:
            st.info("No snapshots saved yet")
            return
        
        available_dates = [d["snapshot_date"] for d in dates_result.data]
        
        st.markdown("#### Select Snapshot Date")
        hist_date = st.selectbox("Available snapshots", options=available_dates, format_func=lambda x: x)
        
        st.caption(f"Querying database for snapshot_date = {hist_date}")
        
        result = supabase_client.rpc("get_snapshot_by_date", {"target_date": hist_date}).execute()
        
        st.caption(f"Found {len(result.data)} records in database")
        
        if not result.data:
            st.warning("No data for selected date")
            return
        
        seen_ids = set()
        hist_companies = []
        for row in result.data:
            cid = row["company_id"]
            if cid not in seen_ids:
                seen_ids.add(cid)
                possible_prices = row.get("possible_prices", [])
                price_guess = calculate_price_guess(possible_prices)
                
                employees = row.get("employees")
                if employees is None:
                    employees = "0 / 0"
                
                hist_companies.append({
                    "company_id": cid,
                    "name": row["name"],
                    "rating": row["rating"],
                    "weekly_income": row["weekly_income"],
                    "daily_income": row["daily_income"],
                    "employees": employees,
                    "days_old": row.get("days_old") or 0,
                    "daily_customers": row.get("daily_customers") or 0,
                    "weekly_customers": row.get("weekly_customers") or 0,
                    "possible_prices": possible_prices,
                    "price_guess": price_guess
                })
        
        display_companies(hist_companies, compact_view, use_abbr, age_min, age_max, rating_min, rating_max, selected_ids, title_suffix=f" (Snapshot: {hist_date})")
        
    except Exception as e:
        st.error(f"History error: {e}")

# ==================== VIEW ROUTING ====================
if st.session_state.view_mode == "Current":
    st.markdown("### 🚀 Fetch Companies (Individual API Calls)")
    st.caption("⚠️ Each company requires a separate API call with rate limiting")
    
    if not st.session_state.id_check_performed:
        st.warning("⚠️ Run 'Check Active IDs' in sidebar first to get valid company list")
    
    col_input, col_button = st.columns([3, 1])
    with col_input:
        max_fetch = len(st.session_state.COMPANY_IDS) if st.session_state.COMPANY_IDS else 0
        if max_fetch == 0:
            max_fetch = 50
        slider_max = min(50, max_fetch) if max_fetch > 0 else 50
        
        to_fetch = st.slider("Companies to fetch", min_value=1, max_value=slider_max, value=min(10, slider_max), step=1)
        if max_fetch > 50:
            with st.expander(f"⚙️ Fetch more (up to {max_fetch})"):
                to_fetch_large = st.number_input("Fetch count:", min_value=1, max_value=max_fetch, value=slider_max, step=10)
                to_fetch = to_fetch_large if to_fetch_large > 50 else to_fetch
    with col_button:
        st.markdown("<div style='height: 28px'></div>", unsafe_allow_html=True)
        if st.button("🚀 FETCH", type="primary", use_container_width=True):
            if not st.session_state.COMPANY_IDS:
                st.error("No company IDs available. Run 'Check Active IDs' first!")
            else:
                st.session_state.to_fetch_target = to_fetch
                st.session_state.fetch_triggered = True
                st.rerun()
    
    view_cols = st.columns([1, 1])
    with view_cols[0]:
        compact_view = st.toggle("📱 Compact view", value=True)
    with view_cols[1]:
        use_abbr = st.toggle("💰 Abbreviate (K/M)", value=False)
    
    if st.session_state.fetch_triggered:
        progress = st.progress(0.0)
        status = st.empty()
        target = min(st.session_state.to_fetch_target, len(st.session_state.COMPANY_IDS))
        existing_ids = {c["company_id"] for c in st.session_state.companies_data}
        
        error_count = 0
        rate_limit_hits = 0
        
        for i, cid in enumerate(st.session_state.COMPANY_IDS[:target]):
            current = len(st.session_state.companies_data)
            if cid in existing_ids:
                status.markdown(f"⏭️ **{current + 1}/{target}** | `{cid}` (already loaded)")
                progress.progress(min(1.0, (current + 1) / target))
                continue
            
            status.markdown(f"🕒 **{i + 1}/{target}** | Fetching Company `{cid}`...")
            progress.progress(min(1.0, (i + 1) / target))
            
            result, message, employee_list = fetch_company_individual(cid)
            
            if result:
                st.session_state.companies_data.append(result)
                existing_ids.add(cid)
                # Store employee data in session state
                if employee_list:
                status.markdown(f"✅ **{i + 1}/{target}** | Loaded `{cid}` - {result['name']}")
            else:
                error_count += 1
                if "Rate limit" in message or "429" in message:
                    rate_limit_hits += 1
                status.markdown(f"⚠️ **{i + 1}/{target}** | `{cid}` failed: {message}")
                if "Company not found" in message:
                    st.session_state.deleted_ids.append(cid)
            
            time.sleep(0.1)
        
        progress.empty()
        status.empty()
        st.session_state.fetch_triggered = False
        
        if error_count > 0:
            st.warning(f"⚠️ {error_count} errors during fetch ({rate_limit_hits} rate limits)")
        if st.session_state.deleted_ids:
            st.info(f"ℹ️ {len(st.session_state.deleted_ids)} companies not found (deleted/invalid)")
        st.rerun()
    
    display_companies(st.session_state.companies_data, compact_view, use_abbr, age_min, age_max, rating_min, rating_max, selected_ids)
    st.caption(f"Loaded: {len(st.session_state.companies_data)} | Deleted: {len(st.session_state.deleted_ids)}")

elif st.session_state.view_mode == "Historical":
    hist_cols = st.columns([1, 1])
    with hist_cols[0]:
        compact_view = st.toggle("📱 Compact view", value=True, key="hist_compact")
    with hist_cols[1]:
        use_abbr = st.toggle("💰 Abbreviate (K/M)", value=False, key="hist_abbr")
    
    show_historical_snapshot(supabase, compact_view, use_abbr, age_min, age_max, rating_min, rating_max, selected_ids)

elif st.session_state.view_mode == "History Charts":
    use_abbr = st.toggle("💰 Abbreviate (K/M)", value=False, key="chart_abbr")
    show_history_charts(supabase, use_abbr, selected_ids)

# ==================== LEGACY VIEW ====================
elif st.session_state.view_mode == "Legacy":
    legacy_cols = st.columns([1, 1])
    with legacy_cols[0]:
        compact_view = st.toggle("📱 Compact view", value=True, key="legacy_compact")
    with legacy_cols[1]:
        use_abbr = st.toggle("💰 Abbreviate (K/M)", value=False, key="legacy_abbr")
    
    try:
        latest_result = supabase.rpc("get_distinct_snapshot_dates").execute()
        if not latest_result.data:
            st.info("No snapshots available. Save some data first!")
            st.stop()
        latest_date = latest_result.data[0]["snapshot_date"]
    except Exception as e:
        st.error(f"Error loading latest snapshot: {e}")
        st.stop()
    
    st.markdown(f"### 🏢 Legacy View (Snapshot: {latest_date})")
    
    try:
        result = supabase.rpc("get_snapshot_by_date", {"target_date": latest_date}).execute()
        legacy_companies = []
        seen_ids = set()
        for row in result.data:
            cid = row["company_id"]
            if cid not in seen_ids:
                seen_ids.add(cid)
                possible_prices = row.get("possible_prices", [])
                price_guess = calculate_price_guess(possible_prices)
                
                employees = row.get("employees")
                if employees is None:
                    employees = "0 / 0"
                
                legacy_companies.append({
                    "company_id": cid,
                    "name": row["name"],
                    "rating": row["rating"],
                    "weekly_income": row["weekly_income"],
                    "daily_income": row["daily_income"],
                    "employees": employees,
                    "days_old": row.get("days_old") or 0,
                    "daily_customers": row.get("daily_customers") or 0,
                    "weekly_customers": row.get("weekly_customers") or 0,
                    "possible_prices": possible_prices,
                    "price_guess": price_guess
                })
    except Exception as e:
        st.error(f"Error loading companies: {e}")
        st.stop()
    
    age_min_f = st.session_state.filter_age_min if st.session_state.filter_age_min is not None else 0
    age_max_f = st.session_state.filter_age_max if st.session_state.filter_age_max is not None else 99999
    rating_min_f = st.session_state.filter_rating_min if st.session_state.filter_rating_min is not None else 0
    rating_max_f = st.session_state.filter_rating_max if st.session_state.filter_rating_max is not None else 999
    
    filtered = []
    for c in legacy_companies:
        age_ok = age_min_f <= c["days_old"] <= age_max_f
        rating_ok = rating_min_f <= c["rating"] <= rating_max_f
        if age_ok and rating_ok:
            filtered.append(c)
    
    filtered.sort(key=lambda x: (x["rating"], x["weekly_income"]), reverse=True)
    
    st.caption(f"Showing {len(filtered)} of {len(legacy_companies)} companies | Filters: ⏳ {age_min_f}-{age_max_f}d | ⭐ {rating_min_f}-{rating_max_f}")
    
    for c in filtered:
        name = c["name"]
        rating = c["rating"]
        age = c["days_old"]
        daily_inc = format_number(c["daily_income"], use_abbr)
        weekly_inc = format_number(c["weekly_income"], use_abbr)
        daily_cust = c.get("daily_customers") or 0
        weekly_cust = c.get("weekly_customers") or 0
        
        possible = c.get("possible_prices", [])
        price_guess = c.get("price_guess")
        price_indicator = ""
        
        if price_guess and c["daily_income"] > 0:
            sales = int(c["daily_income"] / price_guess)
            sales_fmt = format_number(sales, use_abbr)
            
            if len(possible) == 1:
                price_indicator = f" | 💯 ${price_guess}→{sales_fmt}"
            else:
                price_indicator = f" | 🔮 ${price_guess}→{sales_fmt}"
        elif len(possible) == 0:
            price_indicator = " | ❌ No match"
        
        if compact_view:
            line = f"**🏢 {name}** | ⭐ {rating} | ⏳ {age}d | 💰 ${daily_inc} | 📈 ${weekly_inc} | 🚶 {daily_cust:,} | 👥 {weekly_cust:,}{price_indicator}"
            st.markdown(line)
        else:
            st.markdown(f"**🏢 {name}** | ⭐ {rating} | ⏳ {age} days | 💰 ${daily_inc} | 📈 ${weekly_inc}")
            st.markdown(f"🚶 {daily_cust:,} daily | 👥 {weekly_cust:,} weekly{price_indicator}")
