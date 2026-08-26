"""
NSESignal Pro — Cloud Backend v7
KEY FIX: Non-blocking job queue for all heavy routes.
Every expensive operation runs in a background thread.
HTTP requests return in <1 second always — no more 502.
"""

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import os, json, math, time, threading, secrets, gc
from functools import wraps
from datetime import datetime
import pytz, requests as req_lib

app = Flask(__name__, template_folder="templates")
CORS(app)
IST = pytz.timezone("Asia/Kolkata")

# ── AUTH ──────────────────────────────────────────────────────────────────
APP_USER = os.environ.get("APP_USERNAME", "admin")
APP_PASS = os.environ.get("APP_PASSWORD", "nse2024")

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not (
            secrets.compare_digest(auth.username.encode(), APP_USER.encode()) and
            secrets.compare_digest(auth.password.encode(), APP_PASS.encode())
        ):
            return ("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="NSESignal"'})
        return f(*args, **kwargs)
    return decorated

def sanitise(obj):
    if isinstance(obj, dict):  return {k: sanitise(v) for k,v in obj.items()}
    if isinstance(obj, list):  return [sanitise(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

def sf(v, d=0.0):
    try: return float(v) if v is not None else d
    except: return d

# ── JOB CACHE SYSTEM ─────────────────────────────────────────────────────
# Each job: {result, running, last_run, error}
_jobs = {}
_jobs_lock = threading.Lock()

# In-memory stock data cache — 4 min TTL, avoids re-fetching same stock
_stock_cache = {}
_stock_cache_lock = threading.Lock()

# ── WATCHLIST HEALTH TRACKING ────────────────────────────────────────────
# Tracks symbols that consistently fail to fetch (dead/renamed/invalid tickers)
# so silent misses like a wrong ticker surface immediately instead of persisting for months.
_fetch_fail_counts = {}
_fetch_fail_lock = threading.Lock()
def _record_fetch_result(sym, ok):
    with _fetch_fail_lock:
        if ok: _fetch_fail_counts.pop(sym, None)
        else: _fetch_fail_counts[sym] = _fetch_fail_counts.get(sym, 0) + 1
def get_broken_symbols(min_fails=3):
    with _fetch_fail_lock:
        return {s: c for s, c in _fetch_fail_counts.items() if c >= min_fails}

# ── RESULTS CALENDAR CACHE ───────────────────────────────────────────────
# Populated by Hot Movers' fetch_upcoming_results() every 30 min during market hours.
# Reused by score_stock() so Short-Term scoring knows a stock is reacting to earnings
# rather than treating an earnings-day gap the same as speculative technical exhaustion.
_results_calendar = {}
_results_calendar_lock = threading.Lock()
def set_results_calendar(upcoming_list):
    with _results_calendar_lock:
        _results_calendar.clear()
        for item in (upcoming_list or []):
            sym = item.get("symbol")
            if sym: _results_calendar[sym] = item
def get_results_info(sym):
    with _results_calendar_lock:
        return _results_calendar.get(sym)

# ── DELIVERY % CACHE ──────────────────────────────────────────────────────
# Populated by Hot Movers' fetch_delivery_data() every 30 min during market hours
# (same cadence/source as before — this just makes the already-fetched NSE bhavcopy
# delivery data available to score_stock() too, not only the Hot Movers display).
_delivery_cache = {}
_delivery_cache_lock = threading.Lock()
def set_delivery_cache(delivery_dict):
    with _delivery_cache_lock:
        _delivery_cache.clear()
        _delivery_cache.update(delivery_dict or {})
def get_delivery_pct(sym):
    with _delivery_cache_lock:
        return _delivery_cache.get(sym)

# ── F&O ELIGIBLE SYMBOLS + LONG/SHORT BUILDUP CACHE ──────────────────────
# F&O eligibility is reviewed quarterly by NSE (expanded multiple times through
# 2024-2026) — fetched fresh periodically rather than hardcoded, since a stale
# hardcoded list would silently drift exactly like WATCHLIST itself did before
# this session's ticker-verification fixes.
_fno_eligible = set()
_fno_eligible_lock = threading.Lock()
_fno_eligible_fetched_at = 0
_fno_cache = {}
_fno_cache_lock = threading.Lock()
def set_fno_cache(d):
    with _fno_cache_lock:
        _fno_cache.clear(); _fno_cache.update(d or {})
def get_fno_buildup(sym):
    with _fno_cache_lock:
        return _fno_cache.get(sym)

# ── FULL SCAN SNAPSHOT (all watchlist stocks, not just top-20) ──────────────
# _do_scan only persists the top-20 slice into the "scan" job result (that's all the
# Short-Term tab needs), but Sector Pulse needs every stock's score/RS to compute real
# sector-level breadth, not just whichever 20 stocks happened to rank highest overall.
# The per-stock _stock_cache has this too but its 4-min TTL is far shorter than Sector
# Pulse's 2-hour run cadence, so it'd mostly be empty by the time Sector Pulse reads it —
# this snapshot has no TTL, just "whatever the most recent full scan found."
_full_scan_snapshot = {}
_full_scan_snapshot_lock = threading.Lock()
def set_full_scan_snapshot(d):
    with _full_scan_snapshot_lock:
        _full_scan_snapshot.clear(); _full_scan_snapshot.update(d or {})
def get_full_scan_snapshot():
    with _full_scan_snapshot_lock:
        return dict(_full_scan_snapshot)

CACHE_TTL = 240  # seconds

def get_cached_stock(sym):
    with _stock_cache_lock:
        e = _stock_cache.get(sym)
        if e and (time.time() - e["ts"]) < CACHE_TTL:
            return e["d"]
    return None

def set_cached_stock(sym, data):
    with _stock_cache_lock:
        _stock_cache[sym] = {"d": data, "ts": time.time()}

def clear_stock_cache():
    with _stock_cache_lock:
        _stock_cache.clear()

def get_job(name):
    with _jobs_lock:
        return _jobs.get(name, {"result":None,"running":False,"last_run":None,"error":None})

def set_job_running(name):
    with _jobs_lock:
        if _jobs.get(name,{}).get("running"): return False
        _jobs[name] = _jobs.get(name,{"result":None,"last_run":None,"error":None})
        _jobs[name]["running"] = True
        return True

def set_job_done(name, result):
    with _jobs_lock:
        _jobs[name] = {"result":result,"running":False,"last_run":datetime.now(IST),"error":None}

def set_job_error(name, error):
    with _jobs_lock:
        if name in _jobs:
            _jobs[name]["running"] = False
            _jobs[name]["error"] = str(error)

def job_response(name, start_fn, max_wait=0):
    """
    Standard pattern for all cached routes:
    - If result cached: return it instantly
    - If running: return status
    - If not started: start background thread
    - If max_wait>0: wait up to that many seconds for first result
    """
    job = get_job(name)
    if job["result"]:
        return jsonify(job["result"])
    if job["running"]:
        return jsonify({"status":"running","message":"Scan in progress — retry in 10 seconds"}), 202
    # Start background job
    if set_job_running(name):
        threading.Thread(target=start_fn, daemon=True).start()
    # Wait briefly for first result if requested
    if max_wait > 0:
        for _ in range(max_wait):
            time.sleep(1)
            j = get_job(name)
            if j["result"]: return jsonify(j["result"])
            if not j["running"]: break
    return jsonify({"status":"scanning","message":"Scan started — retry in 15 seconds"}), 202

# ── LAZY IMPORTS ──────────────────────────────────────────────────────────
_yf = None; _ta = None

def get_yf():
    global _yf
    if _yf is None:
        import yfinance as yf; _yf = yf
    return _yf

def get_ta():
    global _ta
    if _ta is None:
        import ta as ta_lib; _ta = ta_lib
    return _ta

def get_ns(sym): return sym.replace("&","%26") + ".NS"

def yf_retry(fn, retries=2, base_delay=1.5):
    """Retries a yfinance call on rate-limit/crumb errors with backoff + jitter.
    Yahoo's unofficial API intermittently rate-limits or invalidates the auth crumb
    under load — most such failures are transient and succeed on a short retry."""
    import random
    last_err=None
    for attempt in range(retries+1):
        try:
            return fn()
        except Exception as e:
            last_err=e
            msg=str(e)
            if "Rate limit" in msg or "Too Many Requests" in msg or "Invalid Crumb" in msg or "401" in msg:
                if attempt < retries:
                    time.sleep(base_delay*(attempt+1)+random.uniform(0,1))
                    continue
            raise
    raise last_err

# ── WATCHLIST ─────────────────────────────────────────────────────────────
WATCHLIST = list(dict.fromkeys([
    # NIFTY 50
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL","INDUSTOWER",
    "KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN","BAJFINANCE","WIPRO","M&M","ASHOKLEY",
    "TECHM","ULTRACEMCO","ONGC","NTPC","POWERGRID","SUNPHARMA","TATAMOTORS","HCLTECH",
    "NESTLEIND","TATASTEEL","JSWSTEEL","HINDALCO","COALINDIA","BPCL","IOC","GAIL","JINDALSTEL",
    "DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP","BAJAJFINSV","EICHERMOT","HEROMOTOCO",
    "ADANIENT","ADANIPORTS","LTIM","INDUSINDBK","ITC","VEDL","GRASIM","TATACONSUM",
    "BRITANNIA","BAJAJ-AUTO","TRENT",
    # NIFTY NEXT 50
    "SIEMENS","ENRIN","ABB","POWERINDIA","CGPOWER","HAVELLS","PIDILITIND","BERGEPAINT","MUTHOOTFIN","CHOLAFIN",
    "SBILIFE","HDFCLIFE","ICICIGI","STARHEALTH","MARICO","COLPAL","DABUR","GODREJCP","SAIL",
    "NMDC","AMBUJACEM","SHREECEM","IRCTC","BEL","HAL","BHEL","IDFCFIRSTB",
    "BANDHANBNK","FEDERALBNK","TORNTPHARM","LUPIN","AUROPHARMA","ZYDUSLIFE","ALKEM",
    "BIOCON","ZOMATO","DMART","DIXON","VOLTAS","POLYCAB","BALKRISIND","APOLLOTYRE",
    "MRF","INDIGO","CONCOR","TATACOMM","ANGELONE","CDSL","MCX","CAMS","NAUKRI","BSE",
    "INFOEDGE","LICI","MAXHEALTH","FORTIS","IRFC","RVNL",
    # IT & TECH
    "KPITTECH","MPHASIS","LTTS","PERSISTENT","COFORGE","BIRLASOFT","MASTEK","RAMCOSYS",
    "TANLA","ROUTE","CYIENT","SONATASOFT","INTELLECT","ZENSAR","TATAELXSI",
    "FIRSTSOURCE","RATEGAIN","NAZARA","DATAMATICS","SAKSOFT","TATATECH",
    # BANKING & FINANCE
    "RBLBANK","ABCAPITAL","SUNDARMFIN","CANFINHOME","AAVAS","MANAPPURAM",
    "MOTILALOSW","IIFL","PNBHOUSING","LICHSGFIN","CREDITACC","SPANDANA",
    "SBFC","UGRO","M&MFIN","REPCO","APTUS","ARMANFIN","EQUITASBNK","UJJIVANSFB",
    "BANKBARODA","PNB","CANBK","UNIONBANK","YESBANK","HDFCAMC","SBICARD","MFSL","KFINTECH",
    # PHARMA & HEALTHCARE
    "GRANULES","GLENMARK","JBCHEPHARM","NATCOPHARM","IPCALAB","ALEMBICLTD",
    "NEULANDLAB","SUVEN","WOCKHARDT","AJANTPHARM","SHILPAMED","KRSNAA",
    "MEDPLUS","LALGPATH","METROPOLIS","THYROCARE","GLAXO","PFIZER","ABBOTINDIA",
    "SANOFI","SEQUENT","CAPLIN","SOLARA","MANKIND","LAURUSLABS","GLAND",
    # DEFENCE & AEROSPACE
    "BEML","GRSE","COCHINSHIP","MAZAGON","GARDENREACH","MIDHANI","MTAR",
    "DATAPATTNS","HBLPOWER","SOLARIND","ZEN","PARAS","BDL",
    # CHEMICALS & SPECIALTY
    "DEEPAKNTR","NAVINFLUOR","TATACHEM","CLEANSCIENCE","FINEORG","ALKYLAMINE",
    "VINATIORG","AARTI","DEEPAKFERT","GNFC","CHAMBLFERT","COROMANDEL","PIIND",
    "RALLIS","DHANUKA","SUMICHEM","HERANBA","GHCL","APCOTEXIND","AARTIIND",
    "NOCIL","ROSSARI","LXCHEM","SUDARSCHEM","INSECTICID",
    # AUTO & ANCILLARY
    "MOTHERSON","MSUMI","BOSCHLTD","TIINDIA","ENDURANCE","SANSERA","SUPRAJIT","ESCORTS",
    "FIEM","GABRIEL","JAMNA","LUMAX","MINDA","SUBROS","CEATLTD","TVSMOTOR","MUNJALAU","FMGOETZE",
    # INFRASTRUCTURE & CAPEX
    "GMRINFRA","CUMMINSIND","ASTRAL","RAILTEL","CAPACITE","KNR","HGINFRA",
    "PNCINFRA","ASHOKA","NCC","HCC","IRCON","NBCC","ENGINERSIN",
    "THERMAX","KALPATPOWR","KEC",
    # REAL ESTATE
    "DLF","LODHA","GODREJPROP","PRESTIGE","OBEROIRLTY","PHOENIXLTD","SOBHA",
    "KOLTEPATIL","SUNTECK","BRIGADE","MAHLIFE",
    # METALS & MINING
    "APLAPOLLO","RATNAMANI","GRAPHITE","HINDCOPPER","NALCO","MOIL","WELCORP","SARDAEN",
    # FMCG & CONSUMER
    "RADICO","JUBLFOOD","TTKPRESTIG","HAWKINCOOK","SYMPHONY","WONDERLA","VBL","UBL","EMAMILTD",
    "PAGEIND","RAYMOND","SAFARI","VMART","SENCO","KALYANKJIL","BATAINDIA","MCDOWELL-N","EMIL",
    "VMM","CROMPTON","VGUARD","JYOTHYLAB",
    # ENERGY & POWER
    "ADANIGREEN","ADANIPOWER","ADANIENSOL","TATAPOWER","TORNTPOWER","NHPC","SJVN","IREDA",
    "RECLTD","PFC","CESC","JPPOWER","RPOWER","INOXWIND","SUZLON",
    # OIL & GAS
    "HINDPETRO","MRPL","CASTROLIND","GUJGASLTD","MGL","IGL","ATGL",
    # LOGISTICS
    "DELHIVERY","ALLCARGO","TCIEXP","VRL","SNOWMAN","BLUEDART",
    # AGRI & FOOD
    "KRBL","AVANTIFEED","BALRAMCHIN","TRIVENI","RENUKA","PRAJIND","KHAITANLTD",
    "LTFOODS","AWL","PATANJALI","PARADEEP","FACT",
    # CEMENT
    "JKCEMENT","DALMIACEM","RAMCOCEM","HEIDELBERG","BIRLACORPN","ACC",
    # NEW-AGE
    "NYKAA","PAYTM","POLICYBZR","EASEMYTRIP","SWIGGY","HONASA",
    # HOTELS
    "INDHOTEL","EIHOTEL","LEMONTREE","CHALET","THOMASCOOK",
    # SEMIS & ELECTRONICS
    "KAYNES","SYRMA","AMBER","PGEL","NETWEB",
]))

# ── SECTOR MAP ─────────────────────────────────────────────────────────────
SECTOR_STOCKS = {
    "Technology & AI":         ["TCS","INFY","HCLTECH","WIPRO","TECHM","LTIM","PERSISTENT","COFORGE","KPITTECH","MPHASIS"],
    "Semiconductors":          ["DIXON","KAYNES","SYRMA","AMBER","TATAELXSI","BEL","HAL","BEML"],
    "Defence & Aerospace":     ["HAL","BEL","BHEL","BEML","GRSE","COCHINSHIP","MAZAGON","GARDENREACH"],
    "Banking & Finance":       ["HDFCBANK","ICICIBANK","SBIN","KOTAKBANK","AXISBANK","INDUSINDBK","BANDHANBNK","IDFCFIRSTB"],
    "Pharma & Healthcare":     ["SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","LUPIN","AUROPHARMA","APOLLOHOSP","MAXHEALTH"],
    "EV & Auto":               ["TATAMOTORS","MARUTI","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT","MOTHERSON","BOSCHLTD"],
    "Renewable Energy":        ["ADANIGREEN","TATAPOWER","ADANIPOWER","NHPC","SJVN","IREDA","NTPC"],
    "Infrastructure & Capex":  ["LT","RVNL","IRFC","IRCTC","RAILTEL","GMRINFRA","KNR","HGINFRA"],
    "FMCG & Consumer":         ["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO","DABUR","COLPAL"],
    "Agriculture":             ["COROMANDEL","PIIND","CHAMBLFERT","GNFC","RALLIS","DHANUKA","DEEPAKFERT"],
    "Cement":                  ["ULTRACEMCO","AMBUJACEM","SHREECEM","JKCEMENT","DALMIACEM","RAMCOCEM"],
    "Metals & Mining":         ["TATASTEEL","JSWSTEEL","HINDALCO","VEDL","SAIL","NMDC","COALINDIA"],
    "Real Estate":             ["DLF","LODHA","GODREJPROP","PRESTIGE","SOBHA"],
    "Oil & Gas":               ["RELIANCE","ONGC","BPCL","IOC","GAIL"],
    "Telecom":                 ["BHARTIARTL","TATACOMM","ROUTE","TANLA"],
}

SEASONALITY = {
    "Technology & AI":    {1:2,2:1,3:1,4:2,5:2,6:1,7:3,8:2,9:2,10:2,11:1,12:1},
    "Semiconductors":     {1:2,2:2,3:1,4:2,5:2,6:1,7:2,8:3,9:2,10:2,11:2,12:1},
    "Defence & Aerospace":{1:2,2:2,3:3,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "Banking & Finance":  {1:2,2:2,3:2,4:3,5:2,6:1,7:2,8:2,9:2,10:3,11:2,12:2},
    "Pharma & Healthcare":{1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "EV & Auto":          {1:1,2:2,3:2,4:1,5:1,6:1,7:2,8:3,9:3,10:3,11:2,12:2},
    "Renewable Energy":   {1:2,2:2,3:2,4:2,5:2,6:3,7:3,8:3,9:2,10:2,11:2,12:1},
    "Infrastructure & Capex":{1:2,2:2,3:3,4:2,5:2,6:1,7:2,8:2,9:3,10:3,11:2,12:2},
    "FMCG & Consumer":    {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:3,11:3,12:3},
    "Agriculture":        {1:2,2:2,3:3,4:3,5:3,6:3,7:3,8:2,9:2,10:2,11:1,12:1},
    "Cement":             {1:2,2:2,3:2,4:1,5:1,6:1,7:2,8:2,9:3,10:3,11:3,12:2},
    "Metals & Mining":    {1:2,2:2,3:2,4:2,5:2,6:1,7:2,8:2,9:2,10:2,11:2,12:2},
    "Real Estate":        {1:2,2:2,3:2,4:2,5:1,6:1,7:2,8:2,9:2,10:3,11:3,12:2},
    "Oil & Gas":          {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "Telecom":            {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
}

# ── INDICATORS ────────────────────────────────────────────────────────────
def detect_vcp(daily):
    """Simplified Volatility Contraction Pattern check (Minervini-style), approximated
    without full pivot-swing detection: splits the trailing ~30 sessions into three equal
    legs and checks that each successive leg's price range is tighter than the last
    (progressive contraction) with volume also drying up in the final leg — the two
    defining VCP characteristics — plus price sitting near the base's high (close to the
    breakout pivot). This is a pragmatic approximation, not full chart-pattern recognition
    (real VCP tools like MarketSmith use tick-level swing/pivot analysis) — it catches the
    same "coiling spring" shape but will occasionally miss patterns with irregular leg
    boundaries. Runs off the same 60-day daily data already fetched for every stock, so it
    adds zero new API calls or memory overhead to the scan.
    Returns (vcp_setup: bool, contractions: int 0-2)."""
    try:
        if daily is None or len(daily) < 33: return False, 0
        close = daily["Close"].squeeze().astype(float).dropna()
        vol = daily["Volume"].squeeze().astype(float).dropna()
        if len(close) < 33 or len(vol) < 33: return False, 0
        c = close.iloc[-30:]; v = vol.iloc[-30:]
        legs_c = [c.iloc[0:10], c.iloc[10:20], c.iloc[20:30]]
        legs_v = [v.iloc[0:10], v.iloc[10:20], v.iloc[20:30]]
        ranges = [(float(leg.max())-float(leg.min()))/max(float(leg.mean()),0.01)*100 for leg in legs_c]
        vol_avgs = [float(leg.mean()) for leg in legs_v]
        tightening = ranges[0] > ranges[1] > ranges[2]
        vol_dryup = vol_avgs[2] < vol_avgs[0]*0.85 if vol_avgs[0] > 0 else False
        near_pivot = float(c.iloc[-1]) >= float(c.max())*0.95
        contractions = int(ranges[0]>ranges[1]) + int(ranges[1]>ranges[2])
        setup = bool(tightening and vol_dryup and near_pivot)
        return setup, contractions
    except Exception:
        return False, 0

def compute_advanced_signals(daily, regime=None):
    """RS-vs-NIFTY + VCP — computed off multi-day daily closes (kept deliberately separate
    from compute_indicators()'s `df`, which is often today's 5-min intraday bars, not
    multi-day history — both of these signals need real day-to-day price action)."""
    out = {"ret_20d": None, "rs_score": None, "vcp_setup": False, "vcp_contractions": 0}
    try:
        if daily is None or len(daily) < 25: return out
        close = daily["Close"].squeeze().astype(float).dropna()
        if len(close) < 25: return out
        lb = min(20, len(close)-1)
        ret = (float(close.iloc[-1]) / float(close.iloc[-1-lb]) - 1) * 100
        out["ret_20d"] = round(ret, 2)
        nifty_ret = (regime or {}).get("nifty_ret_20d")
        if nifty_ret is not None:
            out["rs_score"] = round(ret - nifty_ret, 2)
        setup, contractions = detect_vcp(daily)
        out["vcp_setup"] = setup; out["vcp_contractions"] = contractions
    except Exception:
        pass
    return out

def compute_indicators(df):
    if df is None or len(df) < 26: return None
    try:
        ta = get_ta()
        import pandas as pd
        close  = df["Close"].squeeze().astype(float)
        high   = df["High"].squeeze().astype(float)
        low    = df["Low"].squeeze().astype(float)
        volume = df["Volume"].squeeze().astype(float)
        opn    = df["Open"].squeeze().astype(float)
        mask   = close.notna()&volume.notna()&(close>0)&(volume>0)
        close=close[mask];high=high[mask];low=low[mask];volume=volume[mask];opn=opn[mask]
        if len(close)<26: return None
        r={}
        r["cmp"]        = round(float(close.iloc[-1]),2)
        r["open"]       = round(float(opn.iloc[-1]),2)
        r["high"]       = round(float(high.iloc[-1]),2)
        r["low"]        = round(float(low.iloc[-1]),2)
        r["prev_close"] = round(float(close.iloc[-2]),2)
        r["change_pct"] = round((r["cmp"]-r["prev_close"])/r["prev_close"]*100,2)
        if r["cmp"]<=0: return None
        rsi=ta.momentum.RSIIndicator(close=close,window=14).rsi()
        r["rsi"]=round(sf(rsi.iloc[-1],50),1)
        try:
            sk=ta.momentum.StochRSIIndicator(close=close,window=14,smooth1=3,smooth2=3).stochrsi_k()
            sd=ta.momentum.StochRSIIndicator(close=close,window=14,smooth1=3,smooth2=3).stochrsi_d()
            r["stoch_k"]=round(sf(sk.iloc[-1])*100,1); r["stoch_d"]=round(sf(sd.iloc[-1])*100,1)
            r["stoch_bull"]=bool(r["stoch_k"]>r["stoch_d"] and r["stoch_k"]<80)
            r["stoch_bounce"]=bool(r["stoch_k"]>r["stoch_d"] and r["stoch_k"]<30)
        except: r["stoch_k"]=r["stoch_d"]=None;r["stoch_bull"]=r["stoch_bounce"]=False
        m=ta.trend.MACD(close=close,window_slow=26,window_fast=12,window_sign=9)
        r["macd_hist"]=round(sf(m.macd_diff().iloc[-1]),3)
        r["macd_prev"]=round(sf(m.macd_diff().iloc[-2]),3)
        r["macd_bullish"]=bool(r["macd_hist"]>0 and r["macd_hist"]>r["macd_prev"])
        r["ema20"]=round(sf(ta.trend.EMAIndicator(close=close,window=20).ema_indicator().iloc[-1]),2)
        r["ema50"]=round(sf(ta.trend.EMAIndicator(close=close,window=50).ema_indicator().iloc[-1]),2)
        r["above_ema20"]=bool(r["cmp"]>r["ema20"]); r["golden_cross"]=bool(r["ema20"]>r["ema50"])
        typical=(high+low+close)/3; vwap=(typical*volume).cumsum()/volume.cumsum()
        r["vwap"]=round(sf(vwap.iloc[-1]),2); r["above_vwap"]=bool(r["cmp"]>r["vwap"])
        avg_vol=float(volume.iloc[-20:].mean()) if len(volume)>=20 else float(volume.mean())
        r["volume"]=int(float(volume.iloc[-1])); r["avg_volume"]=int(avg_vol)
        r["rel_volume"]=round(float(volume.iloc[-1])/avg_vol,2) if avg_vol>0 else 1.0
        bb=ta.volatility.BollingerBands(close=close,window=20,window_dev=2)
        r["bb_pct"]=round(sf(bb.bollinger_pband().iloc[-1],0.5),3)
        try:
            hl2=(high+low)/2; atr_i=ta.volatility.AverageTrueRange(high=high,low=low,close=close,window=10).average_true_range()
            upper=(hl2+3.0*atr_i).ffill(); lower=(hl2-3.0*atr_i).ffill()
            st=pd.Series(float("nan"),index=close.index); st.iloc[10]=float(upper.iloc[10]); bullish=True
            for i in range(11,len(close)):
                prev=st.iloc[i-1]
                if pd.isna(prev): st.iloc[i]=float(upper.iloc[i]); continue
                if float(close.iloc[i])>prev: st.iloc[i]=float(lower.iloc[i]); bullish=True
                else: st.iloc[i]=float(upper.iloc[i]); bullish=False
            r["supertrend_bull"]=bullish; r["supertrend_val"]=round(sf(st.iloc[-1]),2)
        except: r["supertrend_bull"]=False; r["supertrend_val"]=0
        try:
            adx_i=ta.trend.ADXIndicator(high=high,low=low,close=close,window=14)
            r["adx"]=round(sf(adx_i.adx().iloc[-1]),1)
            r["adx_pos"]=round(sf(adx_i.adx_pos().iloc[-1]),1); r["adx_neg"]=round(sf(adx_i.adx_neg().iloc[-1]),1)
            r["adx_strong"]=bool(r["adx"]>=25); r["adx_bullish"]=bool(r["adx_pos"]>r["adx_neg"])
        except: r["adx"]=0; r["adx_strong"]=False; r["adx_bullish"]=False
        # ── Reversal / Exhaustion indicators ──
        try:
            mfi_i=ta.volume.MFIIndicator(high=high,low=low,close=close,volume=volume,window=14)
            r["mfi"]=round(sf(mfi_i.money_flow_index().iloc[-1],50),1)
        except: r["mfi"]=50
        try:
            psar_i=ta.trend.PSARIndicator(high=high,low=low,close=close,step=0.02,max_step=0.2)
            psar_series=psar_i.psar()
            r["psar"]=round(sf(psar_series.iloc[-1]),2)
            cur_bull=bool(close.iloc[-1]>psar_series.iloc[-1])
            prev_bull=bool(close.iloc[-2]>psar_series.iloc[-2])
            r["psar_bullish"]=cur_bull
            r["psar_flip_bull"]=bool(cur_bull and not prev_bull)
            r["psar_flip_bear"]=bool(prev_bull and not cur_bull)
        except: r["psar"]=None; r["psar_bullish"]=False; r["psar_flip_bull"]=False; r["psar_flip_bear"]=False
        try:
            lookback=14
            if len(close)>=lookback+2:
                wc=close.iloc[-(lookback+1):-1]; wr=rsi.iloc[-(lookback+1):-1]
                p_high_idx=wc.idxmax(); p_low_idx=wc.idxmin()
                p_high=float(wc.max()); p_high_rsi=float(rsi.loc[p_high_idx])
                p_low=float(wc.min()); p_low_rsi=float(rsi.loc[p_low_idx])
                cur_price=float(close.iloc[-1]); cur_rsi=float(rsi.iloc[-1])
                r["bearish_divergence"]=bool(cur_price>=p_high and cur_rsi<p_high_rsi-2)
                r["bullish_divergence"]=bool(cur_price<=p_low and cur_rsi>p_low_rsi+2)
            else: r["bearish_divergence"]=False; r["bullish_divergence"]=False
        except: r["bearish_divergence"]=False; r["bullish_divergence"]=False

        r["week52_high"]=round(float(close.max()),2); r["week52_low"]=round(float(close.min()),2)
        r["pct_from_52h"]=round((r["cmp"]-r["week52_high"])/r["week52_high"]*100,1)
        r["near_52w_high"]=bool(r["pct_from_52h"]>=-3.0)
        r["breakout_setup"]=bool(r["pct_from_52h"]>=-3.0 and r["rel_volume"]>=1.5)
        try:
            atr_v=sf(ta.volatility.AverageTrueRange(high=high,low=low,close=close,window=14).average_true_range().iloc[-1])
            r["atr"]=round(atr_v,2)
            bv=sum([r["supertrend_bull"],r["macd_bullish"],r["above_vwap"],r["golden_cross"],45<=r["rsi"]<=72])
            r["direction"]="BULLISH" if bv>=3 else "BEARISH" if bv<=1 else "NEUTRAL"
            mult=2.5 if r["adx"]>=30 else 1.8 if r["adx"]>=20 else 1.2
            move=max(round(atr_v*mult,2),round(r["cmp"]*0.005,2))
            lean=bv>=2
            if r["direction"]=="BULLISH": r["target_price"]=round(r["cmp"]+move,2);r["stop_loss"]=round(r["cmp"]-atr_v,2);r["target_pct"]=round(move/r["cmp"]*100,2)
            elif r["direction"]=="BEARISH": r["target_price"]=round(r["cmp"]-move,2);r["stop_loss"]=round(r["cmp"]+atr_v,2);r["target_pct"]=round(-move/r["cmp"]*100,2)
            else:
                half=round(move*0.5,2); r["target_price"]=round(r["cmp"]+(half if lean else -half),2)
                r["stop_loss"]=round(r["cmp"]-atr_v*0.8,2); r["target_pct"]=round((half if lean else -half)/r["cmp"]*100,2)
            risk=abs(r["cmp"]-r["stop_loss"]); reward=abs(r["target_price"]-r["cmp"])
            r["risk_reward"]=round(reward/risk,2) if risk>0 else 0
        except: r["atr"]=0;r["direction"]="NEUTRAL";r["target_price"]=r["cmp"];r["stop_loss"]=r["cmp"];r["target_pct"]=0;r["risk_reward"]=0
        return r
    except Exception as e:
        print(f"  [IND ERR] {e}"); return None

def score_stock(ind, regime=None, results_info=None):
    if not ind: return 0,[]
    score=0; signals=[]; rt=(regime or {}).get("trend","NEUTRAL")
    # "Reacting to results" window: -5 (reported up to 5 trading days ago) through +1 (results tomorrow).
    # A post-earnings re-rating often runs for several sessions, not just the single reaction day —
    # narrower windows miss exactly the "still climbing 3 days after results" case (e.g. Bosch).
    results_today = bool(results_info and -5 <= results_info.get("days_away",99) <= 1)
    if rt=="BULL":   score+=1.0; signals.append("NIFTY BULL regime — tailwind ✓")
    elif rt=="BEAR": score-=2.0; signals.append("NIFTY BEAR regime — headwind ✗")
    else:            signals.append("NIFTY NEUTRAL regime")
    rsi=ind.get("rsi",50)
    if 52<=rsi<=68:  score+=2.0; signals.append(f"RSI {rsi} — ideal momentum zone ✓")
    elif 45<=rsi<52: score+=1.0; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68<rsi<=72: score+=0.5; signals.append(f"RSI {rsi} — strong, near overbought")
    elif rsi>72:     score-=1.5; signals.append(f"RSI {rsi} — overbought ✗")
    else:            score-=1.0; signals.append(f"RSI {rsi} — weak ✗")
    sk=ind.get("stoch_k"); sd=ind.get("stoch_d")
    if sk is not None:
        if ind.get("stoch_bounce"):    score+=2.0; signals.append(f"StochRSI {sk}/{sd} — oversold bounce ✓")
        elif ind.get("stoch_bull") and sk<60: score+=1.5; signals.append(f"StochRSI {sk}/{sd} — bullish ✓")
        elif sk>80: score-=1.0; signals.append(f"StochRSI {sk} — overbought ✗")
        else: signals.append(f"StochRSI {sk}/{sd} — neutral")
    if ind.get("macd_bullish"):      score+=2.0; signals.append(f"MACD expanding positive ({ind.get('macd_hist')}) ✓")
    elif ind.get("macd_hist",0)>0:   score+=1.0; signals.append(f"MACD positive fading ({ind.get('macd_hist')})")
    else:                            score-=1.0; signals.append(f"MACD bearish ({ind.get('macd_hist',0)}) ✗")
    if ind.get("above_vwap"):        score+=1.5; signals.append(f"Above VWAP ₹{ind.get('vwap')} ✓")
    else:                            score-=0.5; signals.append("Below VWAP ✗")
    if ind.get("golden_cross"):      score+=1.5; signals.append("Golden cross EMA20>EMA50 ✓")
    else:                            score-=0.5; signals.append("Death cross ✗")
    if ind.get("above_ema20"):       score+=0.5; signals.append("Above EMA20 ✓")
    rv=ind.get("rel_volume",1.0)
    if rv>=2.5: score+=2.0; signals.append(f"Volume {rv}x — institutional ✓")
    elif rv>=1.5: score+=1.0; signals.append(f"Volume {rv}x — above avg ✓")
    elif rv<0.7: score-=0.5; signals.append(f"Volume {rv}x — weak ✗")
    else: signals.append(f"Volume {rv}x — normal")
    if ind.get("supertrend_bull"):   score+=2.0; signals.append("Supertrend BULLISH ✓")
    else:                            score-=1.5; signals.append("Supertrend BEARISH ✗")
    adx=ind.get("adx",0)
    if adx>=30 and ind.get("adx_bullish"):   score+=2.0; signals.append(f"ADX {adx} — very strong ✓")
    elif adx>=25 and ind.get("adx_bullish"): score+=1.5; signals.append(f"ADX {adx} — strong ✓")
    elif adx>=20:                            score+=0.5; signals.append(f"ADX {adx} — moderate")
    else:                                    score-=0.5; signals.append(f"ADX {adx} — weak ✗")
    if ind.get("breakout_setup"):    score+=2.0; signals.append("52W BREAKOUT with volume ✓")
    elif ind.get("near_52w_high"):   score+=1.0; signals.append("Near 52W high — momentum ✓")
    bb=ind.get("bb_pct",0.5)
    if 0.2<=bb<=0.7: score+=0.5; signals.append(f"Bollinger {round(bb*100)}% — healthy ✓")
    elif bb>0.9:
        if results_today: signals.append("Bollinger upper band — results-day move, not penalized")
        else: score-=0.5; signals.append("Bollinger upper band ✗")
    # ── Reversal / Exhaustion signals ──
    if ind.get("bullish_divergence"):   score+=2.5; signals.append("Bullish RSI divergence — reversal building ✓")
    elif ind.get("bearish_divergence"): score-=2.5; signals.append("Bearish RSI divergence — exhaustion warning ✗")
    if ind.get("psar_flip_bull"):     score+=1.5; signals.append("Parabolic SAR flipped BULLISH ✓")
    elif ind.get("psar_flip_bear"):  score-=1.5; signals.append("Parabolic SAR flipped BEARISH ✗")
    elif ind.get("psar_bullish"):    signals.append("Parabolic SAR — trend intact bullish")
    else:                            signals.append("Parabolic SAR — trend intact bearish")
    mfi=ind.get("mfi",50)
    if mfi<=20:   score+=1.5; signals.append(f"MFI {mfi} — oversold, volume-backed bounce ✓")
    elif mfi>=80:
        if results_today: signals.append(f"MFI {mfi} — overbought, but results-day reaction, not penalized")
        else: score-=1.5; signals.append(f"MFI {mfi} — overbought, exhaustion risk ✗")
    # ── Results calendar boost ──
    if results_info:
        if results_today:
            beats=results_info.get("beats",0) or 0; misses=results_info.get("misses",0) or 0
            beat_str=results_info.get("beat_summary","—")
            da=results_info.get("days_away",0)
            when = "today" if da==0 else "tomorrow" if da==1 else f"{abs(da)}d ago"
            if beats>misses:
                score+=2.5; signals.append(f"📊 Results {when} — strong beat history ({beat_str}) ✓")
            elif beats>0 or misses>0:
                score+=1.0; signals.append(f"📊 Results {when} — mixed history ({beat_str})")
            else:
                score+=1.0; signals.append(f"📊 Results {when} — no beat/miss history available")
        else:
            days=results_info.get("days_away")
            if days is not None and days<=7:
                score+=0.5; signals.append(f"📅 Results in {days}d — pre-earnings watch")
    # ── Relative Strength vs NIFTY (new) — 20-trading-day excess return over the index.
    # This is the single most independently-validated "hidden" factor in momentum research
    # (Weinstein's Stage Analysis, IBD's RS Rating, Minervini's live track record): a stock
    # already beating the index tends to keep leading. Filters out the "textbook bullish
    # indicators, but the whole stock is a market laggard" false positive.
    rs=ind.get("rs_score")
    if rs is not None:
        if rs>=8:     score+=2.0; signals.append(f"RS vs NIFTY +{rs}% (20d) — strong leader ✓")
        elif rs>=3:   score+=1.0; signals.append(f"RS vs NIFTY +{rs}% (20d) — outperforming ✓")
        elif rs<=-8:  score-=1.5; signals.append(f"RS vs NIFTY {rs}% (20d) — badly lagging ✗")
        elif rs<=-3:  score-=0.5; signals.append(f"RS vs NIFTY {rs}% (20d) — underperforming ✗")
        else:         signals.append(f"RS vs NIFTY {rs}% (20d) — in-line with index")
    # ── VCP — Volatility Contraction Pattern (new). See detect_vcp() for the exact
    # (simplified) criteria — progressively tighter price ranges + volume dry-up + near pivot.
    if ind.get("vcp_setup"):
        score+=2.0; signals.append(f"VCP setup — {ind.get('vcp_contractions')} tightening contractions, volume drying up ✓")
    # ── Delivery % accumulation/distribution (new) — real ownership change (NSE bhavcopy),
    # not just intraday churn. Read together with price direction: high delivery + price up
    # is conviction buying; high delivery + price down can be distribution, not accumulation —
    # so it's never scored as a standalone positive regardless of price.
    dp=ind.get("delivery_pct")
    if dp is not None:
        chg=ind.get("change_pct",0) or 0
        if dp>=65 and chg>0:   score+=1.5; signals.append(f"Delivery {dp}% + price up — real accumulation ✓")
        elif dp>=65 and chg<0: score-=1.0; signals.append(f"Delivery {dp}% + price down — possible distribution ✗")
        elif dp>=65:            signals.append(f"Delivery {dp}% — high conviction, flat price")
        elif dp<25:              signals.append(f"Delivery {dp}% — mostly intraday churn")
    # ── F&O Long/Short Buildup (new, F&O-eligible stocks only) — price + open-interest
    # combined signal from the futures market. Long Buildup (fresh longs) is the most
    # durable bullish signal here; Short Covering looks similar on price alone but tends
    # to fade faster since it's short-exit-driven, not fresh conviction — scored lower.
    fno=ind.get("fno_buildup")
    if fno and fno.get("buildup"):
        bt=fno["buildup"]
        if bt=="LONG_BUILDUP":     score+=2.0; signals.append(f"F&O: LONG BUILDUP — price +{fno.get('price_chg')}%, OI +{fno.get('oi_chg_pct')}% ✓")
        elif bt=="SHORT_COVERING": score+=0.5; signals.append(f"F&O: SHORT COVERING — price +{fno.get('price_chg')}%, OI {fno.get('oi_chg_pct')}% (weaker, may fade)")
        elif bt=="SHORT_BUILDUP":  score-=2.0; signals.append(f"F&O: SHORT BUILDUP — price {fno.get('price_chg')}%, OI +{fno.get('oi_chg_pct')}% ✗")
        elif bt=="LONG_UNWINDING": score-=0.5; signals.append(f"F&O: LONG UNWINDING — price {fno.get('price_chg')}%, OI {fno.get('oi_chg_pct')}%")
    d=ind.get("direction","NEUTRAL")
    if d=="BULLISH":  signals.append(f"TARGET ₹{ind.get('target_price')} ({ind.get('target_pct')}%) | SL ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')} ✓")
    elif d=="BEARISH":signals.append(f"TARGET ₹{ind.get('target_price')} ({ind.get('target_pct')}%) | SL ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')} ✗")
    else:             signals.append(f"TARGET ₹{ind.get('target_price')} | SL ₹{ind.get('stop_loss')}")
    return max(0,min(23,round(score,1))), signals

def fetch_nifty_regime():
    try:
        yf=get_yf(); ta=get_ta()
        df=yf.Ticker("^NSEI").history(period="60d",interval="1d",auto_adjust=True)
        if df is None or len(df)<30: return {"trend":"NEUTRAL","strength":1,"change_pct":0}
        close=df["Close"].squeeze().astype(float)
        ema20=float(ta.trend.EMAIndicator(close=close,window=20).ema_indicator().iloc[-1])
        ema50=float(ta.trend.EMAIndicator(close=close,window=50).ema_indicator().iloc[-1])
        cmp=float(close.iloc[-1]); prev=float(close.iloc[-2]); chg=round((cmp-prev)/prev*100,2)
        strength=sum([cmp>ema20,ema20>ema50,chg>0])
        trend="BULL" if strength>=2 else "BEAR" if strength<=0 else "NEUTRAL"
        # 20-trading-day return for the index — benchmark leg of the Relative Strength signal.
        # 20d (not the more commonly cited 55d) because that's what fits inside the existing
        # 60-calendar-day daily fetch already used everywhere else in this file, without adding
        # a second, longer-period API call per scan cycle.
        nifty_ret_20d=None
        try:
            lb=min(20,len(close)-1)
            nifty_ret_20d=round((cmp/float(close.iloc[-1-lb])-1)*100,2)
        except: pass
        return {"trend":trend,"strength":strength,"ema20":round(ema20,2),"ema50":round(ema50,2),"cmp":round(cmp,2),"change_pct":chg,"nifty_ret_20d":nifty_ret_20d}
    except: return {"trend":"NEUTRAL","strength":1,"change_pct":0}

def batch_download(symbols, period="60d", interval="1d"):
    """Download OHLCV for multiple symbols in one HTTP call using yf.download().
    Returns dict of {symbol: dataframe}. Much faster than individual calls."""
    try:
        yf = get_yf()
        ns_syms = [get_ns(s) for s in symbols]
        # yf.download handles batching internally — single HTTP session
        data = yf.download(
            tickers=" ".join(ns_syms),
            period=period, interval=interval,
            group_by="ticker", auto_adjust=True,
            progress=False, threads=True, timeout=30
        )
        result = {}
        for sym, ns in zip(symbols, ns_syms):
            try:
                if len(symbols) == 1:
                    df = data  # single ticker returns flat df
                else:
                    df = data[ns] if ns in data.columns.get_level_values(0) else None
                if df is not None and len(df) >= 5:
                    result[sym] = df.dropna(how="all")
            except Exception as _e:
                print(f"[BATCH] {sym} slice err: {_e}")
        return result
    except Exception as e:
        print(f"[BATCH DL ERR] {e}")
        return {}

def fetch_one(sym, regime, prefetched_daily=None):
    try:
        cached=get_cached_stock(sym)
        if cached:
            sc,sg2=score_stock(cached.get("_ind",{}),regime,get_results_info(sym))
            r=dict(cached); r["score"]=sc; r["signals"]=sg2; return sanitise(r)
        yf=get_yf()
        for _att in range(3):
            try:
                ticker=yf.Ticker(get_ns(sym))
                intra=ticker.history(period="1d",interval="5m",auto_adjust=True)
                # Use pre-fetched daily if available, else fetch individually
                daily = prefetched_daily if prefetched_daily is not None and len(prefetched_daily)>=5                         else ticker.history(period="60d",interval="1d",auto_adjust=True)
                break
            except Exception as _re:
                if "429" in str(_re) or "rate" in str(_re).lower() or "crumb" in str(_re).lower() or "401" in str(_re): time.sleep(2**_att)
                else: raise
        df=intra if (intra is not None and len(intra)>=15) else daily
        ind=compute_indicators(df)
        if not ind: _record_fetch_result(sym, False); return None
        _record_fetch_result(sym, True)
        # RS-vs-NIFTY + VCP (multi-day daily closes) + delivery % + F&O buildup (both from
        # cache) — merged into ind before scoring so score_stock() can read them like any
        # other signal.
        ind.update(compute_advanced_signals(daily, regime))
        ind["delivery_pct"] = get_delivery_pct(sym)
        ind["fno_buildup"] = get_fno_buildup(sym)
        score,signals=score_stock(ind,regime,get_results_info(sym))
        # Add 14-day price history for sparkline (already have daily data)
        price_hist = []
        try:
            if daily is not None and len(daily) >= 5:
                price_hist = [round(float(p),2) for p in daily["Close"].tail(14).tolist() if p and p==p]
        except Exception: pass
        res=sanitise({"symbol":sym,"score":score,"signals":signals,"price_history":price_hist,**ind})
        # Flatten fno_buildup to a plain string (badges/frontend just need the classification,
        # not the full price/OI dict — kept nested in _ind for score_stock's re-scoring path
        # off cached data, see fetch_one's cache-hit branch above).
        _fno=res.get("fno_buildup")
        res["fno_buildup"]=_fno.get("buildup") if isinstance(_fno,dict) else None
        res["fno_oi_chg_pct"]=_fno.get("oi_chg_pct") if isinstance(_fno,dict) else None
        res["_ind"]=ind; set_cached_stock(sym,res); return res
    except: _record_fetch_result(sym, False); return None

# ── SHORT-TERM SCAN ────────────────────────────────────────────────────────
def _do_scan():
    """Scans all stocks with 20 parallel workers.
    Saves partial top10 results progressively so frontend never times out."""
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as _CFTimeoutError
        now = datetime.now(IST)
        print(f"\n[SCAN] {now.strftime('%I:%M %p IST')} — {len(WATCHLIST)} stocks")
        regime = fetch_nifty_regime()
        with _jobs_lock:
            _jobs["regime"] = {"result": regime}

        results = []; errors = 0; completed = 0

        def save_partial(res, done, err, partial=True):
            sorted_res = sorted(res, key=lambda x: x["score"], reverse=True)
            # Raw movers by |%change| — independent of setup-quality score, so a stock that's
            # already extended (and thus penalized in scoring) still shows up here if it's genuinely moving.
            movers = sorted(res, key=lambda x: abs(x.get("change_pct") or 0), reverse=True)[:12]
            movers_slim = [{"symbol":m.get("symbol"),"cmp":m.get("cmp"),"change_pct":m.get("change_pct"),
                             "rel_volume":m.get("rel_volume"),"score":m.get("score")} for m in movers]
            # Bull / Bear split — top 20 gainers and top 20 losers by raw %change, zero score
            # filtering (same rationale as top_movers above, just split by direction and widened
            # from a shared top-12 so a strong-breadth day doesn't crowd out smaller-cap movers
            # on either side — e.g. a PSU/fertiliser rally pushing a mid-cap faller off a combined list).
            bull_pool = sorted([r for r in res if (r.get("change_pct") or 0) > 0],
                                key=lambda x: x.get("change_pct") or 0, reverse=True)[:20]
            bear_pool = sorted([r for r in res if (r.get("change_pct") or 0) < 0],
                                key=lambda x: x.get("change_pct") or 0)[:20]
            def _slim(m): return {"symbol":m.get("symbol"),"cmp":m.get("cmp"),"change_pct":m.get("change_pct"),
                                    "rel_volume":m.get("rel_volume"),"score":m.get("score")}
            bull_movers = [_slim(m) for m in bull_pool]
            bear_movers = [_slim(m) for m in bear_pool]
            with _jobs_lock:
                if "scan" not in _jobs: _jobs["scan"] = {}
                _jobs["scan"]["result"] = sanitise({
                    "status": "success",
                    "scan_time": now.strftime("%I:%M %p IST"),
                    "date": now.strftime("%d %b %Y"),
                    "scanned": done, "total": len(WATCHLIST),
                    "errors": err, "top10": sorted_res[:20], "top_movers": movers_slim,
                    "bull_movers": bull_movers, "bear_movers": bear_movers,
                    "cached": True, "partial": partial,
                    "market_regime": regime,
                    "nifty_change": regime.get("change_pct", 0),
                    "nifty_trend": regime.get("trend", "NEUTRAL"),
                })

        # Pre-batch daily data in groups of 50 — one HTTP call per batch
        print(f"[SCAN] Pre-fetching daily data in batches...")
        daily_cache = {}
        for batch_start in range(0, len(WATCHLIST), 50):
            batch = WATCHLIST[batch_start:batch_start+50]
            batch_data = batch_download(batch, period="60d", interval="1d")
            daily_cache.update(batch_data)
            print(f"[SCAN] Batch daily fetched: {len(daily_cache)}/{len(WATCHLIST)}")
            gc.collect()
            time.sleep(1.5)  # spread requests over time — gentler on Yahoo's rate limiter

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = {ex.submit(fetch_one, sym, regime, daily_cache.get(sym)): sym for sym in WATCHLIST}
            try:
                for f in as_completed(futures, timeout=280):
                    try:
                        r = f.result(timeout=8)
                        if r: results.append(r)
                        else: errors += 1
                    except: errors += 1
                    completed += 1
                    # Save partial result every 50 stocks — frontend can read these
                    if completed % 50 == 0:
                        save_partial(results, completed, errors, partial=True)
                        print(f"[SCAN] {completed}/{len(WATCHLIST)} done, {len(results)} valid")
            except _CFTimeoutError:
                # Ran out of time at the current concurrency — finalize with whatever completed
                # instead of leaving the job stuck at the last 50-stock checkpoint forever.
                print(f"[SCAN] as_completed timeout — finalizing with {completed}/{len(WATCHLIST)} done")

        del daily_cache; gc.collect()
        # Final save — use `completed` (not len(WATCHLIST)) so a timeout-fallback finalize
        # honestly reports how many were actually scanned, never claims more than really happened
        save_partial(results, completed, errors, partial=False)
        # Snapshot ALL scanned stocks (not just top-20) for Sector Pulse's breadth calc —
        # see _full_scan_snapshot comment above.
        set_full_scan_snapshot({r["symbol"]: {"score":r.get("score"), "ret_20d":r.get("ret_20d"),
                                                "rs_score":r.get("rs_score"), "change_pct":r.get("change_pct")}
                                 for r in results if r.get("symbol")})
        with _jobs_lock:
            _jobs["scan"]["running"] = False
            _jobs["scan"]["last_run"] = now
        print(f"[SCAN] Complete — {len(results)} valid, {errors} errors")

    except Exception as e:
        print(f"[SCAN ERR] {e}")
        set_job_error("scan", e)


def _is_market_open():
    n=datetime.now(IST)
    if n.weekday()>=5: return False
    mo=n.replace(hour=9,minute=15,second=0,microsecond=0)
    mc=n.replace(hour=15,minute=30,second=0,microsecond=0)
    return mo<=n<=mc

def _scheduler():
    time.sleep(8); _hot_last=0; _sec_last=0; _scan_last=0; _fno_last=0
    while True:
        try:
            now_ts=time.time()
            market_open=_is_market_open()
            # Scan every 5 min during market hours. Outside market hours, only every 30 min —
            # data doesn't change after close, so there's no reason to hammer Yahoo Finance
            # every 5 min for 18 hours a day when nothing's moving.
            scan_interval = 300 if market_open else 1800
            if now_ts-_scan_last>scan_interval or _scan_last==0:
                if set_job_running("scan"):
                    _do_scan(); _scan_last=now_ts
            if market_open and now_ts-_hot_last>1800:
                if set_job_running("hot_movers"):
                    threading.Thread(target=_do_hot_movers,daemon=True).start()
                    _hot_last=now_ts; print("[SCHED] Auto hot movers")
            if market_open and now_ts-_fno_last>1800:
                threading.Thread(target=_do_fno_buildup,daemon=True).start()
                _fno_last=now_ts; print("[SCHED] Auto F&O buildup")
            if now_ts-_sec_last>7200:
                if set_job_running("sector_pulse"):
                    threading.Thread(target=_do_sector_pulse,daemon=True).start()
                    _sec_last=now_ts; print("[SCHED] Auto sector pulse")
        except Exception as e: print(f"[SCHED ERR] {e}")
        time.sleep(5*60)

def _keep_alive():
    url=os.environ.get("RENDER_EXTERNAL_URL","")
    if not url: return
    while True:
        time.sleep(14*60)
        try: req_lib.get(f"{url}/health",timeout=10)
        except Exception as _ex: print(f'[WARN] silent fail: {_ex}')

# ── FUNDAMENTALS ──────────────────────────────────────────────────────────
def fetch_fundamentals(sym):
    try:
        yf=get_yf(); tk=yf.Ticker(get_ns(sym))
        info=yf_retry(lambda: tk.info)
        if not info or len(info)<10: return None
        cmp=info.get("currentPrice") or info.get("regularMarketPrice")
        if not cmp: return None
        def sg(k,d=None): return info.get(k,d)
        def pct(k): v=sg(k); return round(v*100,1) if v is not None else None
        def rnd(k,n=2): v=sg(k); return round(v,n) if v is not None else None
        r={"symbol":sym,"cmp":round(float(cmp),2),"company":sg("longName",sym),"sector":sg("sector","—"),
           "market_cap":sg("marketCap"),
           "pe":rnd("trailingPE"),"forward_pe":rnd("forwardPE"),"pb":rnd("priceToBook"),
           "peg_direct":rnd("pegRatio"),"ev_ebitda":rnd("enterpriseToEbitda"),
           "roe":pct("returnOnEquity"),"roa":pct("returnOnAssets"),
           "debt_equity":round(sg("debtToEquity")/100,2) if sg("debtToEquity") else None,
           "current_ratio":rnd("currentRatio"),
           "gross_margin":pct("grossMargins"),"op_margin":pct("operatingMargins"),
           "profit_margin":pct("profitMargins"),
           "earnings_growth":pct("earningsGrowth"),"revenue_growth":pct("revenueGrowth"),
           "eps_trailing":rnd("trailingEps"),"eps_forward":rnd("forwardEps"),
           "fcf_positive":bool(sg("freeCashflow") and sg("freeCashflow")>0),
           "fcf":sg("freeCashflow"),"dividend_yield":pct("dividendYield"),
           "week52_high":sg("fiftyTwoWeekHigh"),"week52_low":sg("fiftyTwoWeekLow"),
           "analyst_target":rnd("targetMeanPrice"),"analyst_high":rnd("targetHighPrice"),
           "analyst_low":rnd("targetLowPrice"),"num_analysts":sg("numberOfAnalystOpinions",0),
           "analyst_recommendation":sg("recommendationKey","—"),
           "insider_holding":pct("heldPercentInsiders"),
           "institution_holding":pct("heldPercentInstitutions"),
           "beta":rnd("beta"),"short_ratio":rnd("shortRatio")}
        r["analyst_upside"]=round((r["analyst_target"]-r["cmp"])/r["cmp"]*100,1) if r["analyst_target"] else None
        r["pct_from_52h"]=round((r["cmp"]-r["week52_high"])/r["week52_high"]*100,1) if r["week52_high"] else None
        r["pct_from_52l"]=round((r["cmp"]-r["week52_low"])/r["week52_low"]*100,1) if r["week52_low"] else None
        pe=r["pe"]; eg=r["earnings_growth"]
        r["peg"]=r["peg_direct"] or (round(pe/eg,2) if pe and eg and eg>0 else None)
        r["promoter_holding"]=r["insider_holding"]
        # ── Reuse the Short-Term scan's cached technical data if it's fresh — avoids a duplicate
        # yfinance call for every stock, cutting request volume (and rate-limit risk) roughly in half ──
        cached=get_cached_stock(sym)
        if cached and cached.get("rsi") is not None:
            r["bearish_divergence"]=bool(cached.get("bearish_divergence"))
            r["bullish_divergence"]=bool(cached.get("bullish_divergence"))
            r["rsi"]=cached.get("rsi")
        else:
            try:
                daily=tk.history(period="3mo",interval="1d",auto_adjust=True)
                tech=compute_indicators(daily) if daily is not None and len(daily)>=20 else None
                r["bearish_divergence"]=bool(tech.get("bearish_divergence")) if tech else False
                r["bullish_divergence"]=bool(tech.get("bullish_divergence")) if tech else False
                r["rsi"]=tech.get("rsi") if tech else None
            except Exception:
                r["bearish_divergence"]=False; r["bullish_divergence"]=False; r["rsi"]=None
        return sanitise(r)
    except Exception as e: print(f"[FUND ERR] {sym}: {e}"); return None

def score_fundamentals(f):
    if not f: return 0,[]
    score=0; signals=[]
    pe=f.get("pe")
    if pe:
        if 0<pe<=12:   score+=3.0; signals.append(f"P/E {pe} — deep value ✓")
        elif pe<=20:   score+=2.0; signals.append(f"P/E {pe} — attractively valued ✓")
        elif pe<=30:   score+=1.0; signals.append(f"P/E {pe} — fairly valued")
        elif pe>50:    score-=1.5; signals.append(f"P/E {pe} — very expensive ✗")
        else:          signals.append(f"P/E {pe} — moderate")
    peg=f.get("peg")
    if peg:
        if 0<peg<=0.8: score+=3.0; signals.append(f"PEG {peg} — exceptional value vs growth ✓")
        elif peg<=1.2: score+=2.0; signals.append(f"PEG {peg} — cheap vs growth ✓")
        elif peg<=1.8: score+=1.0; signals.append(f"PEG {peg} — fair ✓")
        elif peg>3.0:  score-=1.0; signals.append(f"PEG {peg} — expensive ✗")
    fpe=f.get("forward_pe")
    if fpe and pe and fpe<pe: score+=0.5; signals.append(f"Forward P/E {fpe} < trailing — earnings growing ✓")
    ev=f.get("ev_ebitda")
    if ev:
        if ev<10: score+=1.0; signals.append(f"EV/EBITDA {ev} — cheap enterprise ✓")
        elif ev>30: score-=0.5; signals.append(f"EV/EBITDA {ev} — expensive ✗")
    roe=f.get("roe")
    if roe:
        if roe>=25:  score+=3.0; signals.append(f"ROE {roe}% — exceptional ✓")
        elif roe>=18:score+=2.0; signals.append(f"ROE {roe}% — excellent ✓")
        elif roe>=12:score+=1.0; signals.append(f"ROE {roe}% — good ✓")
        elif roe<8:  score-=1.0; signals.append(f"ROE {roe}% — weak ✗")
        else:        signals.append(f"ROE {roe}% — moderate")
    roa=f.get("roa")
    if roa:
        if roa>=15: score+=1.0; signals.append(f"ROA {roa}% — highly efficient ✓")
        elif roa>=8:score+=0.5; signals.append(f"ROA {roa}% — efficient ✓")
    de=f.get("debt_equity")
    if de is not None:
        if de<=0.2:  score+=2.0; signals.append(f"D/E {de} — near debt-free ✓")
        elif de<=0.5:score+=1.5; signals.append(f"D/E {de} — very healthy ✓")
        elif de<=1.0:score+=0.5; signals.append(f"D/E {de} — manageable")
        elif de>2.0: score-=2.0; signals.append(f"D/E {de} — high leverage ✗")
        else:        score-=0.5; signals.append(f"D/E {de} — elevated ✗")
    cr=f.get("current_ratio")
    if cr:
        if cr>=2.0:  score+=1.0; signals.append(f"Current ratio {cr} — strong liquidity ✓")
        elif cr>=1.2:score+=0.5; signals.append(f"Current ratio {cr} — adequate ✓")
        elif cr<1.0: score-=1.0; signals.append(f"Current ratio {cr} — liquidity risk ✗")
    gm=f.get("gross_margin")
    if gm:
        if gm>=40:  score+=1.0; signals.append(f"Gross margin {gm}% — pricing power ✓")
        elif gm>=25:score+=0.5; signals.append(f"Gross margin {gm}% — healthy ✓")
        elif gm<10: score-=0.5; signals.append(f"Gross margin {gm}% — thin ✗")
    eg=f.get("earnings_growth")
    if eg is not None:
        if eg>=30:   score+=3.0; signals.append(f"EPS growth {eg}% — exceptional ✓")
        elif eg>=20: score+=2.0; signals.append(f"EPS growth {eg}% — strong ✓")
        elif eg>=10: score+=1.0; signals.append(f"EPS growth {eg}% — healthy ✓")
        elif eg<0:   score-=2.0; signals.append(f"EPS growth {eg}% — declining ✗")
        else:        signals.append(f"EPS growth {eg}% — modest")
    rg=f.get("revenue_growth")
    if rg is not None:
        if rg>=25:   score+=2.0; signals.append(f"Revenue growth {rg}% — rapid ✓")
        elif rg>=15: score+=1.5; signals.append(f"Revenue growth {rg}% — strong ✓")
        elif rg>=8:  score+=0.5; signals.append(f"Revenue growth {rg}% — healthy ✓")
        elif rg<0:   score-=1.5; signals.append(f"Revenue growth {rg}% — shrinking ✗")
    et=f.get("eps_trailing"); ef=f.get("eps_forward")
    if et and ef and et>0:
        acc=round((ef-et)/abs(et)*100,1)
        if acc>=20:  score+=1.0; signals.append(f"EPS accelerating +{acc}% forward ✓")
        elif acc<-15:score-=0.5; signals.append(f"EPS decelerating {acc}% ✗")
    if f.get("fcf_positive"): score+=1.5; signals.append("Free cash flow positive ✓")
    else:                      score-=1.0; signals.append("Free cash flow negative ✗")
    dy=f.get("dividend_yield")
    if dy and dy>0 and dy>=3: score+=0.5; signals.append(f"Dividend yield {dy}% ✓")
    p52h=f.get("pct_from_52h")
    if p52h is not None:
        if p52h>=-5:    score+=1.0; signals.append(f"Near 52W high ({p52h}%) — momentum ✓")
        elif p52h<=-30: score+=1.5; signals.append(f"{abs(p52h)}% below 52W high — value entry ✓")
        else:           signals.append(f"{abs(p52h)}% below 52W high")
    upside=f.get("analyst_upside"); na=f.get("num_analysts",0); rec=f.get("analyst_recommendation","")
    if upside and na>=3:
        if upside>=30:   score+=2.0; signals.append(f"Analyst target +{upside}% ({na} analysts) ✓")
        elif upside>=15: score+=1.5; signals.append(f"Analyst target +{upside}% ({na} analysts) ✓")
        elif upside>=5:  score+=0.5; signals.append(f"Analyst target +{upside}% ✓")
        elif upside<-10: score-=1.0; signals.append(f"Analyst downside {upside}% ✗")
    if rec in ("strongBuy","buy"):     score+=0.5; signals.append("Analyst consensus: BUY ✓")
    elif rec in ("sell","strongSell"): score-=0.5; signals.append("Analyst consensus: SELL ✗")
    ins=f.get("insider_holding")
    if ins:
        if ins>=60:  score+=1.0; signals.append(f"Promoter holding {ins}% — high confidence ✓")
        elif ins>=45:score+=0.5; signals.append(f"Promoter holding {ins}% — healthy ✓")
        elif ins<25: score-=0.5; signals.append(f"Promoter holding {ins}% — low ✗")
    inst=f.get("institution_holding")
    if inst and inst>=15: score+=0.5; signals.append(f"Institutional holding {inst}% — smart money ✓")
    beta=f.get("beta")
    if beta:
        if 0.5<=beta<=1.2: score+=0.5; signals.append(f"Beta {beta} — stable ✓")
        elif beta>2.0:     score-=0.5; signals.append(f"Beta {beta} — high volatility ✗")
    if f.get("bearish_divergence"):
        score-=1.5; signals.append("Bearish RSI divergence — technical exhaustion despite fundamentals ✗")
    elif f.get("bullish_divergence"):
        score+=0.5; signals.append("Bullish RSI divergence — technical tailwind aligning ✓")
    return max(0,min(23,round(score,1))),signals

# ── LT SCAN ───────────────────────────────────────────────────────────────
def _do_lt_scan():
    """Scans the full watchlist for fundamentals in batches of 25.
    Saves partial top15 after each batch — frontend shows results progressively."""
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        now = datetime.now(IST)
        lt_list = WATCHLIST  # Full watchlist — every stock gets a fundamentals pass, not just the top 200
        print(f"[LT] Starting — {len(lt_list)} stocks")

        all_results = []
        batch_size  = 25

        for batch_num, batch_start in enumerate(range(0, len(lt_list), batch_size)):
            batch = lt_list[batch_start:batch_start + batch_size]
            print(f"[LT] Batch {batch_num+1}: {len(batch)} stocks")

            with ThreadPoolExecutor(max_workers=8) as ex:
                futures = {ex.submit(fetch_fundamentals, sym): sym for sym in batch}
                for f in as_completed(futures, timeout=150):
                    try:
                        r = f.result(timeout=20)
                        if r and (r.get("pe") or r.get("roe")):
                            score, sigs = score_fundamentals(r)
                            grade = ("A+" if score>=11 else "A" if score>=8.5
                                     else "B" if score>=6 else "C" if score>=3.5 else "D")
                            r["score"] = score; r["signals"] = sigs; r["grade"] = grade
                            all_results.append(sanitise(r))
                    except Exception as _ex: print(f'[WARN] {_ex}')
            gc.collect()
            time.sleep(2)  # spread requests over time — gentler on Yahoo's rate limiter

            all_results.sort(key=lambda x: x.get("score", 0), reverse=True)
            scanned = batch_start + len(batch)
            more    = scanned < len(lt_list)

            # Save partial result after every batch
            with _jobs_lock:
                if "lt_scan" not in _jobs: _jobs["lt_scan"] = {}
                _jobs["lt_scan"]["result"] = {
                    "status": "success",
                    "scan_time": now.strftime("%I:%M %p IST"),
                    "date": now.strftime("%d %b %Y"),
                    "scanned": scanned, "total": len(lt_list),
                    "top15": all_results[:20], "partial": more,
                }
                _jobs["lt_scan"]["running"] = more
            print(f"[LT] Batch {batch_num+1} done — {len(all_results)} valid so far")

        with _jobs_lock:
            _jobs["lt_scan"]["result"]["partial"] = False
            _jobs["lt_scan"]["running"] = False
            _jobs["lt_scan"]["last_run"] = now
        print(f"[LT] Complete — {len(all_results)} valid from {len(lt_list)} stocks")
        gc.collect()

    except Exception as e:
        print(f"[LT ERR] {e}")
        set_job_error("lt_scan", e)


def _do_sector_pulse():
    try:
        now=datetime.now(IST); month_num=now.month; month_name=now.strftime("%B"); year=now.year
        global_theme="AI and technology driving global markets. Semiconductor and defence sectors in focus."
        india_theme="India capex cycle strong. PLI manufacturing push. Defence indigenisation accelerating."
        sector_ai={}
        api_key=os.environ.get("ANTHROPIC_API_KEY","")
        if api_key:
            try:
                import anthropic
                client=anthropic.Anthropic(api_key=api_key)
                sectors_list=list(SECTOR_STOCKS.keys())
                # Fetch live news headlines
                live_headlines = []
                for _nurl in ["https://economictimes.indiatimes.com/markets/rss.cms",
                               "https://www.moneycontrol.com/rss/latestnews.xml"]:
                    try:
                        _nr = requests.get(_nurl, timeout=6, headers={"User-Agent":"Mozilla/5.0"})
                        if _nr.ok:
                            _titles = re.findall(r'<title><![CDATA[(.+?)]]></title>', _nr.text)
                            _titles += re.findall(r"<title>(.+?)</title>", _nr.text)
                            _clean = [t.strip() for t in _titles
                                      if 15 < len(t.strip()) < 200
                                      and "CDATA" not in t and "RSS" not in t][:10]
                            live_headlines.extend(_clean)
                            if len(live_headlines) >= 12: break
                    except Exception as _ne: print(f"[NEWS] {_ne}")
                news_ctx = ("\nLIVE HEADLINES TODAY:\n" + "\n".join(f"- {h}" for h in live_headlines[:12])) if live_headlines else ""
                print(f"[SECTOR] {len(live_headlines)} live headlines fetched")

                prompt=f"""Senior equity analyst. Today is {month_name} {year}.
Score each NSE sector for near-term boom potential. Consider global macro, India policies, seasonal patterns.{news_ctx}
Sectors: {", ".join(sectors_list)}
Return ONLY JSON: {{"global_theme":"2 sentences","india_theme":"2 sentences","sectors":[{{"name":"sector","news_score":8,"trend":"BOOMING","catalyst":"main catalyst","risk":"main risk","why":"2 sentences"}}]}}
news_score 1-10. trend: BOOMING/RISING/NEUTRAL/FALLING/AVOID. All {len(sectors_list)} sectors."""
                msg=client.messages.create(model="claude-sonnet-4-6",max_tokens=2500,messages=[{"role":"user","content":prompt}])
                text=msg.content[0].text; s,e=text.find("{"),text.rfind("}")
                if s!=-1 and e>s:
                    parsed=json.loads(text[s:e+1])
                    global_theme=parsed.get("global_theme",global_theme)
                    india_theme=parsed.get("india_theme",india_theme)
                    for item in parsed.get("sectors",[]): sector_ai[item["name"]]=item
            except Exception as ex: print(f"[SECTOR AI] {ex}")
        scan_result=get_job("scan").get("result",{})
        _all=scan_result.get("top10",[])
        with _jobs_lock:
            _fr=_jobs.get("scan",{}).get("result",{})
        if _fr: _all=_fr.get("top10",_all)
        sym_scores={r["symbol"]:r["score"] for r in _all}
        # Sector Rotation Breadth (new) — real, price-based sector-level Relative Strength
        # vs NIFTY, independent of the AI/seasonal narrative score above. Uses the FULL scan
        # snapshot (all ~364 watchlist stocks), not just the top-20 `_all` slice above, since
        # most of any given sector's stocks won't make a global top-20 on a given day even
        # when the whole sector is quietly rotating in. This is the "check sector RS before
        # individual stock RS" idea — ranks sectors by real momentum 1-2 days ahead of it
        # showing up obviously in individual stock picks.
        snapshot=get_full_scan_snapshot()
        regime=get_job("scan").get("result",{}).get("market_regime") or fetch_nifty_regime()
        nifty_ret_20d=regime.get("nifty_ret_20d")
        results=[]
        for sector,stocks_list in SECTOR_STOCKS.items():
            sea_score=SEASONALITY.get(sector,{}).get(month_num,2)
            ai=sector_ai.get(sector,{})
            news_score=ai.get("news_score",5)
            tech_scores=[sym_scores[s] for s in stocks_list if s in sym_scores]
            tech_avg=round(sum(tech_scores)/len(tech_scores),1) if tech_scores else 0
            tech_syms=[s for s in stocks_list if s in sym_scores][:3]
            news_pts=round(news_score/10*4,1); sea_pts=round((sea_score/3)*3,1)
            tech_pts=round(min(tech_avg/23*3,3),1) if tech_avg else 1.0
            combined=round(news_pts+sea_pts+tech_pts,1)
            boom="🔥 VERY HIGH" if combined>=8 else "⚡ HIGH" if combined>=6.5 else "📈 MODERATE" if combined>=5 else "➡️ NEUTRAL" if combined>=3.5 else "📉 WEAK"
            boom_color="#00e5a0" if combined>=8 else "#3d9bff" if combined>=6.5 else "#f59e0b" if combined>=5 else "#6888a8" if combined>=3.5 else "#ff4d6d"
            # Sector breadth: average 20d return + % of sector's stocks that are individually
            # outperforming NIFTY — the participation-breadth part of a real breadth metric,
            # not just a single averaged number that one outlier stock could dominate.
            sec_rets=[snapshot[s]["ret_20d"] for s in stocks_list if s in snapshot and snapshot[s].get("ret_20d") is not None]
            sector_ret_20d=round(sum(sec_rets)/len(sec_rets),2) if sec_rets else None
            sector_rs=round(sector_ret_20d-nifty_ret_20d,2) if (sector_ret_20d is not None and nifty_ret_20d is not None) else None
            sec_rs_list=[snapshot[s]["rs_score"] for s in stocks_list if s in snapshot and snapshot[s].get("rs_score") is not None]
            breadth_pct=round(100*sum(1 for v in sec_rs_list if v>0)/len(sec_rs_list),0) if sec_rs_list else None
            results.append({"sector":sector,"combined_score":combined,"boom_label":boom,"boom_color":boom_color,
                "trend":ai.get("trend","NEUTRAL"),"news_score":news_score,"news_pts":news_pts,
                "seasonal_score":sea_score,"seasonal_pts":sea_pts,"tech_pts":tech_pts,
                "catalyst":ai.get("catalyst",""),"risk":ai.get("risk",""),"why":ai.get("why",""),
                "top_stocks":stocks_list[:5],"tech_stocks":tech_syms,
                "sector_rs":sector_rs,"sector_ret_20d":sector_ret_20d,"breadth_pct":breadth_pct,
                "breadth_sample":len(sec_rets)})
        results.sort(key=lambda x:x["combined_score"],reverse=True)
        # Rotation leaders — same sectors, re-ranked purely by real price-based RS (sector_rs),
        # separate from the combined AI/seasonal/tech score above. Only sectors with enough
        # breadth data to be meaningful (at least 3 stocks with fresh RS data) are ranked;
        # a sector with 1-2 data points isn't a reliable breadth read.
        rotation=[r for r in results if r["sector_rs"] is not None and r["breadth_sample"]>=3]
        rotation.sort(key=lambda x:x["sector_rs"],reverse=True)
        rotation_leaders=[{"sector":r["sector"],"sector_rs":r["sector_rs"],"sector_ret_20d":r["sector_ret_20d"],
                            "breadth_pct":r["breadth_pct"]} for r in rotation]
        set_job_done("sector_pulse",{"status":"success","scan_time":now.strftime("%I:%M %p IST"),
            "date":now.strftime("%d %b %Y"),"month":month_name,
            "global_theme":global_theme,"india_theme":india_theme,"sectors":results,
            "rotation_leaders":rotation_leaders})
        print(f"[SECTOR] Done")
    except Exception as e:
        print(f"[SECTOR ERR] {e}"); set_job_error("sector_pulse",e)

# ── HOT MOVERS ────────────────────────────────────────────────────────────
NSE_HEADERS={
    "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":"application/json, text/plain, */*","Accept-Language":"en-US,en;q=0.9",
    "Referer":"https://www.nseindia.com/","Connection":"keep-alive",
}


def fetch_upcoming_results(session):
    """
    Fetch upcoming board meetings / results dates from NSE.
    Returns stocks with results in next 7 days.
    These are high-priority pre-results momentum candidates.
    """
    upcoming = []
    try:
        import datetime as dt
        today = dt.datetime.now(IST).date()
        # NSE corporate actions / board meetings endpoint
        urls = [
            "https://www.nseindia.com/api/corporates-corporateActions?index=equities&section=boardMeetings",
            "https://www.nseindia.com/api/event-calendar",
        ]
        for url in urls:
            try:
                r = session.get(url, headers=NSE_HEADERS, timeout=8)
                if not r.ok: continue
                data = r.json()
                items = data.get("data") or data if isinstance(data, list) else []
                for item in items[:100]:
                    sym    = (item.get("symbol") or item.get("Symbol","")).strip().upper()
                    purpose= item.get("purpose") or item.get("description","")
                    date_s = item.get("bm_date") or item.get("date","")
                    if not sym or not date_s: continue
                    # Only results-related meetings
                    purpose_lower = str(purpose).lower()
                    if not any(k in purpose_lower for k in ["result","quarterly","financial","q1","q2","q3","q4","annual"]): continue
                    # Parse date
                    try:
                        for fmt in ["%d-%b-%Y","%Y-%m-%d","%d-%m-%Y","%d/%m/%Y"]:
                            try:
                                meeting_date = dt.datetime.strptime(date_s.strip(), fmt).date()
                                break
                            except: continue
                        days_away = (meeting_date - today).days
                        if -7 <= days_away <= 7:  # results in the last week, or upcoming week
                            # Fetch earnings beat/miss history
                            _beat_miss=[]; _beats=0; _misses=0; _beat_str="—"
                            try:
                                _yft=get_yf().Ticker(get_ns(sym))
                                _eh=_yft.earnings_history
                                if _eh is not None and not _eh.empty:
                                    for _,_row in _eh.tail(4).iterrows():
                                        _est=_row.get("epsEstimate"); _act=_row.get("epsActual")
                                        if _est is not None and _act is not None and float(_est)!=0:
                                            _s=round((float(_act)-float(_est))/abs(float(_est))*100,1)
                                            _beat_miss.append({"surprise_pct":_s,"beat":_s>0,
                                                "est":round(float(_est),2),"actual":round(float(_act),2)})
                                    _beats=sum(1 for x in _beat_miss if x.get("beat"))
                                    _misses=len(_beat_miss)-_beats
                                    _beat_str=f"{_beats}B/{_misses}M last {len(_beat_miss)}Q"
                            except Exception: pass
                            urgency = ("TODAY" if days_away==0 else f"Reported {abs(days_away)}d ago" if days_away<0
                                       else f"In {days_away} day{'s' if days_away>1 else ''}")
                            upcoming.append({
                                      "symbol":     sym,
                                      "date":       date_s,
                                      "days_away":  days_away,
                                      "purpose":    purpose,
                                      "urgency":    urgency,
                                      "urgency_color": "#ff4d6d" if -1<=days_away<=1 else "#f59e0b" if days_away<=3 else "#3d9bff",
                                      "beat_summary": _beat_str,
                                      "beats": _beats, "misses": _misses,
                                  })
                    except: continue
                if upcoming: break
            except Exception as e:
                print(f"[RESULTS CAL] {url}: {e}")
                continue
    except Exception as e:
        print(f"[RESULTS CAL ERR] {e}")

    # Deduplicate and sort by urgency
    seen = set()
    unique = []
    for u in upcoming:
        if u["symbol"] not in seen:
            seen.add(u["symbol"])
            unique.append(u)
    unique.sort(key=lambda x: x["days_away"])
    return unique[:20]

def fetch_delivery_data(session, now):
    """NSE bhavcopy delivery % for today, keyed by symbol. Extracted out of _do_hot_movers
    so both Hot Movers' display AND score_stock()'s accumulation/distribution signal can
    reuse the same fetch instead of hitting NSE twice."""
    delivery_pct = {}
    try:
        today_str = now.strftime("%d-%m-%Y")
        delv_url = f"https://www.nseindia.com/api/deliveryposition?date={today_str}&type=&mode=downloaded"
        dr = session.get(delv_url, headers=NSE_HEADERS, timeout=10)
        if dr.ok:
            ddata = dr.json().get("data", [])
            for item in ddata:
                sym2 = str(item.get("symbol","")).strip().upper()
                delv = item.get("deliveryToTradedQuantity") or item.get("pctDlvToTradedQty")
                if sym2 and delv:
                    try: delivery_pct[sym2] = round(float(delv), 1)
                    except Exception as _ex: print(f"[WARN] {_ex}")
            print(f"[DELV] Got delivery % for {len(delivery_pct)} stocks")
    except Exception as e: print(f"[DELV ERR] {e}")
    return delivery_pct

def fetch_fno_eligible_symbols():
    """NSE's F&O market-lot list — plain static CSV, no session/cookie needed unlike
    most other NSE endpoints in this file. Tries the current archive host first, falls
    back to the legacy host NSE sometimes still serves it from."""
    urls = [
        "https://nsearchives.nseindia.com/content/fo/fo_mktlots.csv",
        "https://www1.nseindia.com/content/fo/fo_mktlots.csv",
    ]
    for url in urls:
        try:
            r = req_lib.get(url, headers=NSE_HEADERS, timeout=10)
            if r.ok and "SYMBOL" in r.text.upper():
                syms = set()
                for line in r.text.splitlines()[1:]:
                    parts = [p.strip() for p in line.split(",")]
                    if len(parts) >= 2 and parts[1]:
                        sym = parts[1].upper()
                        if sym and sym not in ("SYMBOL","NIFTY","BANKNIFTY","FINNIFTY","MIDCPNIFTY","NIFTYNXT50"):
                            syms.add(sym)
                if len(syms) > 50:
                    print(f"[FNO LIST] {len(syms)} F&O-eligible symbols from {url}")
                    return syms
        except Exception as e:
            print(f"[FNO LIST ERR] {url}: {e}")
    return set()

def get_fno_eligible():
    """Refreshes every 6h — F&O eligibility doesn't change intraday, no need to hit this
    every scan cycle."""
    global _fno_eligible, _fno_eligible_fetched_at
    with _fno_eligible_lock:
        if time.time()-_fno_eligible_fetched_at > 6*3600 or not _fno_eligible:
            fresh = fetch_fno_eligible_symbols()
            if fresh:
                _fno_eligible = fresh; _fno_eligible_fetched_at = time.time()
        return set(_fno_eligible)

def _do_fno_buildup():
    """Long/Short buildup classification for the F&O-eligible subset of WATCHLIST —
    combines futures price change with open-interest change:
      price up   + OI up   -> LONG BUILDUP    (fresh longs — most durable, bullish)
      price up   + OI down -> SHORT COVERING  (shorts exiting — tends to fade faster)
      price down + OI up   -> SHORT BUILDUP   (fresh shorts — durable, bearish)
      price down + OI down -> LONG UNWINDING  (longs exiting)
    Runs every 30 min (same cadence as Hot Movers/Delivery), not every 5-min scan —
    hitting NSE's per-symbol quote-derivative endpoint for all ~190-220 F&O names on
    every scan cycle would be slow and a real rate-limit risk on top of the delivery/
    FII/results-calendar NSE calls already made each cycle.
    NOTE: NSE's quote-derivative JSON field names for OI-change aren't 100% confirmed
    against a live payload (no way to test against NSE's anti-bot layer from this
    environment) — field names below are based on the same underlying API's documented
    shape via the well-established nsetools library. First successful fetch logs the
    actual top-level metadata keys seen, same defensive-logging pattern already used for
    FII/DII data in this file, so field names can be corrected in one line if needed.
    """
    try:
        import requests as rq
        eligible = get_fno_eligible()
        targets = [s for s in WATCHLIST if s in eligible] if eligible else []
        if not targets:
            print("[FNO] No F&O-eligible symbols matched (list fetch may have failed) — skipping")
            return
        session = rq.Session()
        for _attempt in range(3):
            try:
                r0 = session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=8)
                if r0.ok: break
            except Exception as _ex: print(f"[WARN] {_ex}")
            time.sleep(1)

        _logged_keys = {"done": False}
        def fetch_one_fno(sym):
            try:
                r = session.get(f"https://www.nseindia.com/api/quote-derivative?symbol={sym}",
                                 headers=NSE_HEADERS, timeout=10)
                if not r.ok: return sym, None
                data = r.json()
                stocks = data.get("stocks", [])
                fut = next((s for s in stocks if "FUT" in str(s.get("metadata",{}).get("instrumentType","")).upper()), None)
                if not fut: return sym, None
                meta = fut.get("metadata", {})
                if not _logged_keys["done"]:
                    print(f"[FNO] Sample metadata keys: {list(meta.keys())[:15]}")
                    _logged_keys["done"] = True
                chg_pct = meta.get("pChange")
                oi_chg_pct = meta.get("pchangeinOpenInterest") or meta.get("changeInOpenInterestPercentage")
                if chg_pct is None or oi_chg_pct is None: return sym, None
                chg_pct = float(chg_pct); oi_chg_pct = float(oi_chg_pct)
                if chg_pct>0 and oi_chg_pct>0:   bt="LONG_BUILDUP"
                elif chg_pct>0 and oi_chg_pct<0: bt="SHORT_COVERING"
                elif chg_pct<0 and oi_chg_pct>0: bt="SHORT_BUILDUP"
                elif chg_pct<0 and oi_chg_pct<0: bt="LONG_UNWINDING"
                else: bt=None
                return sym, {"buildup":bt,"price_chg":round(chg_pct,2),"oi_chg_pct":round(oi_chg_pct,2)}
            except Exception:
                return sym, None

        from concurrent.futures import ThreadPoolExecutor, as_completed
        result = {}
        with ThreadPoolExecutor(max_workers=8) as ex:
            futs = {ex.submit(fetch_one_fno, s): s for s in targets}
            for f in as_completed(futs):
                sym, val = f.result()
                if val: result[sym] = val
        set_fno_cache(result)
        print(f"[FNO] Buildup classified for {len(result)}/{len(targets)} F&O-eligible stocks")
    except Exception as e:
        print(f"[FNO ERR] {e}")

def _do_hot_movers():
    try:
        import requests as rq
        now=datetime.now(IST); print(f"[HOT] Starting")
        session=rq.Session()
        # Prime NSE session with retry
        for _attempt in range(3):
            try:
                r0 = session.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=8)
                if r0.ok: break
            except Exception as _ex: print(f'[WARN] {_ex}')
            time.sleep(1)

        # NSE gainers/losers/active — fetch in parallel
        gainers=[]; losers=[]; active=[]; circuits=[]
        nse_endpoints = {
            "gainers": "https://www.nseindia.com/api/live-analysis-variations?index=gainers",
            "losers":  "https://www.nseindia.com/api/live-analysis-variations?index=losers",
            "active":  "https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume",
            "circuits":"https://www.nseindia.com/api/live-analysis-data-for-alerts?index=uppercircuit",
        }
        def fetch_nse_endpoint(key_url):
            key, url = key_url
            try:
                r = session.get(url, headers=NSE_HEADERS, timeout=10)
                if r.ok:
                    items = r.json().get("data", [])
                    return key, [{"symbol":i.get("symbol","").strip().upper(),
                        "ltp":i.get("lastPrice") or i.get("ltp") or 0,
                        "change_pct":i.get("pChange") or i.get("percentChange") or 0,
                        "volume":i.get("totalTradedVolume") or i.get("quantityTraded") or 0,
                        "prev_close":i.get("previousPrice") or i.get("previousClose") or 0,
                    } for i in items[:20] if i.get("symbol")]
            except Exception as _ex: print(f'[WARN] {_ex}')
            return key, []
        from concurrent.futures import ThreadPoolExecutor as _TPE, as_completed as _ac
        with _TPE(max_workers=4) as _ex:
            _futs = {_ex.submit(fetch_nse_endpoint, kv): kv for kv in nse_endpoints.items()}
            for _f in _ac(_futs, timeout=15):
                try:
                    key, cleaned = _f.result(timeout=12)
                    if key=="gainers": gainers=cleaned
                    elif key=="losers": losers=cleaned
                    elif key=="active": active=cleaned
                    elif key=="circuits": circuits=cleaned
                except Exception as _ex: print(f'[WARN] silent fail: {_ex}')

        # Bulk deals
        bulk=[]; block=[]
        for key,url in [
            ("bulk","https://www.nseindia.com/api/bulk-deals"),
            ("block","https://www.nseindia.com/api/block-deals"),
        ]:
            try:
                r=session.get(url,headers=NSE_HEADERS,timeout=8)
                if r.ok:
                    items=r.json().get("data",[])
                    cleaned=[{"symbol":i.get("symbol","").strip().upper(),
                        "client":i.get("clientName","—"),"buy_sell":i.get("buySell","—"),
                        "quantity":i.get("quantityTraded",0),"price":i.get("tradePrice",0),
                    } for i in items[:20] if i.get("symbol")]
                    if key=="bulk": bulk=cleaned
                    else: block=cleaned
            except Exception as _ex: print(f'[WARN] silent fail: {_ex}')

        # Combine deals
        deal_map={}
        for d in bulk+block:
            sym=d["symbol"]
            if sym not in deal_map: deal_map[sym]={"symbol":sym,"deals":[],"buy_count":0,"sell_count":0}
            deal_map[sym]["deals"].append(d)
            if "BUY" in str(d.get("buy_sell","")).upper(): deal_map[sym]["buy_count"]+=1
            else: deal_map[sym]["sell_count"]+=1
        sorted_deals=sorted(deal_map.values(),key=lambda x:x["buy_count"],reverse=True)

        # Volume shockers + 52W breakouts from Yahoo Finance
        from concurrent.futures import ThreadPoolExecutor, as_completed
        yf=get_yf(); ta=get_ta()
        shockers=[]

        def check_stock(sym):
            try:
                daily=yf.Ticker(get_ns(sym)).history(period="60d",interval="1d",auto_adjust=True)
                if daily is None or len(daily)<26: return None
                close=daily["Close"].squeeze().astype(float); volume=daily["Volume"].squeeze().astype(float)
                mask=close.notna()&(close>0)&volume.notna(); close=close[mask]; volume=volume[mask]
                if len(close)<20: return None
                cmp=float(close.iloc[-1]); prev=float(close.iloc[-2])
                chg=round((cmp-prev)/prev*100,2)
                avg_vol=float(volume.iloc[-20:].mean())
                today_vol=float(volume.iloc[-1])
                rel=round(today_vol/avg_vol,1) if avg_vol>0 else 1.0
                if rel<1.8: return None
                rsi_val=None
                try: rsi_val=round(float(ta.momentum.RSIIndicator(close=close,window=14).rsi().iloc[-1]),1)
                except Exception as _ex: print(f'[WARN] {_ex}')
                w52h=float(close.max()); w52l=float(close.min())
                pct52=round((cmp-w52h)/w52h*100,1)
                is_bo=pct52>=-5.0 and rel>=1.5 and (rsi_val is None or rsi_val<=75)
                near_bo=-15.0<=pct52<-5.0 and rel>=2.0
                st="52W BREAKOUT" if is_bo else "NEAR BREAKOUT" if near_bo else "VOLUME SPIKE"
                sc="#00e5a0" if is_bo else "#3d9bff" if near_bo else "#f59e0b"
                return sanitise({"symbol":sym,"ltp":round(cmp,2),"change_pct":chg,"rel_volume":rel,
                    "volume":int(today_vol),"avg_volume":int(avg_vol),"rsi":rsi_val,
                    "pct_from_52h":pct52,"week52_high":round(w52h,2),"is_breakout":is_bo,
                    "near_breakout":near_bo,"setup_type":st,"setup_color":sc})
            except: return None

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures={ex.submit(check_stock,sym):sym for sym in WATCHLIST[:160]}
            for f in as_completed(futures,timeout=120):
                try:
                    r=f.result(timeout=8)
                    if r: shockers.append(r)
                except Exception as _ex: print(f'[WARN] {_ex}')
        gc.collect()
        shockers.sort(key=lambda x:(2 if x.get("is_breakout") else 1 if x.get("near_breakout") else 0, x.get("rel_volume",0)),reverse=True)

        # Fetch upcoming results calendar
        try:
            upcoming_results = fetch_upcoming_results(session)
            set_results_calendar(upcoming_results)  # cache for Short-Term scoring to reuse
        except:
            upcoming_results = []

        # Delivery % from NSE bhavcopy
        delivery_pct = fetch_delivery_data(session, now)
        set_delivery_cache(delivery_pct)  # cache for Short-Term/My Stock scoring to reuse

        # FII/DII data from NSE
        fii_dii={"fii_net":None,"dii_net":None,"fii_buy":None,"fii_sell":None,"dii_buy":None,"dii_sell":None}
        try:
            fr=session.get("https://www.nseindia.com/api/fiidiiTradeReact",headers=NSE_HEADERS,timeout=10)
            if fr.ok:
                fd=fr.json()
                if isinstance(fd,list) and fd:
                    lat=fd[0]
                    print(f"[FII] API keys: {list(lat.keys())[:10]}")
                    # Handle multiple possible key formats from NSE API
                    def _fii_get(d, *keys):
                        for k in keys:
                            v = d.get(k)
                            if v is not None: return v
                        return None
                    fii_dii={
                        "fii_buy":  _fii_get(lat,"fIIBuy","buyValue","fiiBuyValue","FII_BUY"),
                        "fii_sell": _fii_get(lat,"fIISell","sellValue","fiiSellValue","FII_SELL"),
                        "fii_net":  _fii_get(lat,"fIINet","netValue","fiiNetValue","FII_NET"),
                        "dii_buy":  _fii_get(lat,"dIIBuy","diiBuyValue","DII_BUY"),
                        "dii_sell": _fii_get(lat,"dIISell","diiSellValue","DII_SELL"),
                        "dii_net":  _fii_get(lat,"dIINet","diiNetValue","DII_NET"),
                        "date":     _fii_get(lat,"date","tradeDate","Date") or now.strftime("%d-%b-%Y"),
                    }
                    print(f"[FII] net={fii_dii['fii_net']} DII={fii_dii['dii_net']}")
        except Exception as e: print(f"[FII ERR] {e}")

        set_job_done("hot_movers",sanitise({
            "status":"success","fii_dii":fii_dii,"scan_time":now.strftime("%I:%M %p IST"),"date":now.strftime("%d %b %Y, %A"),
            "gainers":gainers[:10],"losers":losers[:5],"most_active":active[:8],
            "upper_circuits":circuits[:15],"volume_shockers":shockers[:18],
            "bulk_block_deals":sorted_deals[:12],"raw_bulk":bulk[:12],"raw_block":block[:8],
            "upcoming_results":upcoming_results,
        }))
        print(f"[HOT] Done — {len(shockers)} volume shockers, {len(circuits)} circuits")
    except Exception as e:
        print(f"[HOT ERR] {e}"); set_job_error("hot_movers",e)

# ── ROUTES ────────────────────────────────────────────────────────────────
_started=False

@app.before_request
def start_bg():
    global _started
    if not _started:
        _started=True
        threading.Thread(target=_scheduler,  daemon=True).start()
        threading.Thread(target=_keep_alive, daemon=True).start()
        print("[STARTUP] Background threads started")

@app.route("/")
@app.route("/app")
@require_auth
def frontend(): return render_template("index.html")

@app.route("/health")
def health():
    now=datetime.now(IST)
    jobs_status={name: {"running":get_job(name).get("running"),"has_result":bool(get_job(name).get("result"))} for name in ["scan","lt_scan","sector_pulse","hot_movers"]}
    broken=get_broken_symbols()
    return jsonify({"status":"running","service":"NSESignal Pro v7","time":now.strftime("%I:%M:%S %p IST"),
                    "watchlist":len(WATCHLIST),"jobs":jobs_status,"message":"Backend live — non-blocking job queue",
                    "broken_symbols":broken,"broken_symbols_count":len(broken)})

@app.route("/scan")
@require_auth
def scan():
    """Non-blocking. Returns instantly — either cached result or 202.
    Frontend polls every 5s. Scan runs in background thread."""
    job = get_job("scan")
    result = job.get("result")
    if result:
        # Return the real saved state whenever one exists — even if top10 is still empty
        # (e.g. an early batch hasn't produced a scored stock yet). An empty top10 used to
        # be treated as "nothing has happened", freezing the displayed scanned count at 0.
        if result.get("top10") or not result.get("partial", True):
            return jsonify(result)
        return jsonify(result), 202
    # Start scan if not running
    if not job.get("running", False):
        if set_job_running("scan"):
            threading.Thread(target=_do_scan, daemon=True).start()
    # Return 202 immediately — frontend poll() handles waiting
    return jsonify({
        "status": "scanning",
        "message": "Scan started — results appear as stocks are processed",
        "top10": [],
        "scanned": 0,
        "total": len(WATCHLIST),
    }), 202


@app.route("/refresh")
@require_auth
def refresh():
    with _jobs_lock:
        if "scan" in _jobs: _jobs["scan"]["result"]=None
    if set_job_running("scan"): threading.Thread(target=_do_scan,daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/indices")
@require_auth
def indices():
    try:
        yf=get_yf(); out={}
        for name,sym in [("nifty","^NSEI"),("sensex","^BSESN"),("banknifty","^NSEBANK")]:
            try:
                # fast_info gives last_price vs previous_close directly — this is the correct
                # "today's change" comparison. The old code diffed the last two 5-min candles
                # against each other instead of against yesterday's close, which gave a tiny/
                # wrong change value (and a stale one entirely once the market was closed).
                fi = yf_retry(lambda: yf.Ticker(sym).fast_info)
                c = float(fi["last_price"]); p = float(fi["previous_close"])
                if c and p:
                    out[name]={"price":round(c,2),"change":round(c-p,2),"pct":round((c-p)/p*100,2)}
                else:
                    out[name]={}
            except Exception:
                # Fallback: previous behaviour, but compare against prior day's close, not
                # the prior 5-min candle
                try:
                    df=yf.Ticker(sym).history(period="5d",interval="1d",auto_adjust=True)
                    if df is not None and len(df)>1:
                        c=float(df["Close"].iloc[-1]); p=float(df["Close"].iloc[-2])
                        out[name]={"price":round(c,2),"change":round(c-p,2),"pct":round((c-p)/p*100,2)}
                    else:
                        out[name]={}
                except Exception:
                    out[name]={}
        return jsonify(out)
    except: return jsonify({"nifty":{},"sensex":{},"banknifty":{}})

@app.route("/analyse/<symbol>")
@require_auth
def analyse(symbol):
    try:
        sym=symbol.upper().strip(); yf=get_yf()
        _ticker=yf.Ticker(get_ns(sym))
        intra=_ticker.history(period="1d",interval="5m",auto_adjust=True)
        daily=_ticker.history(period="60d",interval="1d",auto_adjust=True)
        df=intra if (intra is not None and len(intra)>=15) else daily
        ind=compute_indicators(df)
        if not ind: return jsonify({"status":"error","message":f"No data for {sym}"}),404
        price_hist=[]
        try:
            if daily is not None and len(daily)>=5:
                price_hist=[round(float(p),2) for p in daily["Close"].tail(14).tolist() if p and p==p]
        except Exception: pass
        regime=get_job("scan").get("result",{}).get("market_regime") or fetch_nifty_regime()
        ind.update(compute_advanced_signals(daily, regime))
        ind["delivery_pct"] = get_delivery_pct(sym)
        ind["fno_buildup"] = get_fno_buildup(sym)
        t_score,t_sigs=score_stock(ind,regime,get_results_info(sym))
        fund=fetch_fundamentals(sym)
        f_score,f_sigs=score_fundamentals(fund) if fund else (0,["Fundamental data unavailable"])
        combined=round(t_score*0.6+f_score*0.4,1); rt=regime.get("trend","NEUTRAL")
        adj=2 if rt=="BEAR" else 0
        if combined>=9.5+adj: verdict="BUY";vc="#00e5a0";vs="Strong technical + fundamental setup. Entry with stop-loss."
        elif combined>=5.5+adj: verdict="HOLD";vc="#f59e0b";vs="Mixed signals. Hold if in position. Wait for confirmation."
        else: verdict="SELL / AVOID";vc="#ff4d6d";vs="Weak setup. Avoid new entry. Protect capital."
        all_sigs=t_sigs+f_sigs
        return jsonify(sanitise({
            "status":"success","symbol":sym,"company":fund.get("company",sym) if fund else sym,
            "sector":fund.get("sector","—") if fund else "—",
            "verdict":verdict,"verdict_color":vc,"verdict_icon":"▲" if verdict=="BUY" else "▼" if "SELL" in verdict else "●",
            "verdict_summary":vs,"combined_score":combined,"tech_score":t_score,"fund_score":f_score,
            "regime":rt,"regime_strength":regime.get("strength",1),
            "cmp":ind.get("cmp"),"change_pct":ind.get("change_pct"),
            "target_price":ind.get("target_price"),"stop_loss":ind.get("stop_loss"),
            "target_pct":ind.get("target_pct"),"risk_reward":ind.get("risk_reward"),
            "direction":ind.get("direction"),"rsi":ind.get("rsi"),
            "stoch_rsi_k":ind.get("stoch_k"),"stoch_rsi_d":ind.get("stoch_d"),
            "stoch_k":ind.get("stoch_k"),"stoch_d":ind.get("stoch_d"),"stoch_bull":ind.get("stoch_bull"),
            "macd_hist":ind.get("macd_hist"),"macd_bullish":ind.get("macd_bullish"),
            "supertrend_bull":ind.get("supertrend_bull"),"adx":ind.get("adx"),
            "adx_bullish":ind.get("adx_bullish"),"above_vwap":ind.get("above_vwap"),
            "vwap":ind.get("vwap"),"golden_cross":ind.get("golden_cross"),
            "ema20":ind.get("ema20"),"ema50":ind.get("ema50"),"above_ema20":ind.get("above_ema20"),
            "rel_volume":ind.get("rel_volume"),"breakout_setup":ind.get("breakout_setup"),
            "pct_from_52h":ind.get("pct_from_52h"),"week52_high":ind.get("week52_high"),
            "week52_low":ind.get("week52_low"),"atr":ind.get("atr"),
            "mfi":ind.get("mfi"),"psar":ind.get("psar"),"psar_bullish":ind.get("psar_bullish"),
            "psar_flip_bull":ind.get("psar_flip_bull"),"psar_flip_bear":ind.get("psar_flip_bear"),
            "bullish_divergence":ind.get("bullish_divergence"),"bearish_divergence":ind.get("bearish_divergence"),
            "rs_score":ind.get("rs_score"),"ret_20d":ind.get("ret_20d"),
            "vcp_setup":ind.get("vcp_setup"),"vcp_contractions":ind.get("vcp_contractions"),
            "delivery_pct":ind.get("delivery_pct"),
            "fno_buildup":(ind.get("fno_buildup") or {}).get("buildup"),
            "fno_oi_chg_pct":(ind.get("fno_buildup") or {}).get("oi_chg_pct"),
            "price_history":price_hist,
            "pe":fund.get("pe") if fund else None,"peg":fund.get("peg") if fund else None,
            "roe":fund.get("roe") if fund else None,"debt_equity":fund.get("debt_equity") if fund else None,
            "earnings_growth":fund.get("earnings_growth") if fund else None,
            "revenue_growth":fund.get("revenue_growth") if fund else None,
            "fcf_positive":fund.get("fcf_positive") if fund else None,
            "analyst_target":fund.get("analyst_target") if fund else None,
            "analyst_upside":fund.get("analyst_upside") if fund else None,
            "num_analysts":fund.get("num_analysts") if fund else None,
            "promoter_holding":fund.get("insider_holding") if fund else None,
            "market_cap":fund.get("market_cap") if fund else None,
            "tech_signals":t_sigs,"fund_signals":f_sigs,
            "bullish_reasons":[s for s in all_sigs if "✓" in s][:4],
            "bearish_reasons":[s for s in all_sigs if "✗" in s][:4],
        }))
    except Exception as e:
        print(f"[ANALYSE ERR] {e}"); return jsonify({"status":"error","message":str(e)}),500

@app.route("/lt-scan")
@require_auth
def lt_scan():
    """Non-blocking. Returns instantly — either cached result or 202."""
    job = get_job("lt_scan")
    result = job.get("result")
    if result:
        # Return the real saved state whenever one exists — even if top15 is still empty
        # (e.g. an early batch had no stock with pe/roe data yet). Previously an empty
        # top15 was treated as "nothing has happened", freezing the displayed scanned
        # count at 0 until some batch finally qualified, then jumping straight to the
        # accumulated total all at once.
        if result.get("top15") or not result.get("partial", True):
            return jsonify(result)      # has entries, or the scan is genuinely finished
        return jsonify(result), 202     # still scanning — but report the real progress
    if not job.get("running", False):
        if set_job_running("lt_scan"):
            threading.Thread(target=_do_lt_scan, daemon=True).start()
    return jsonify({
        "status": "scanning",
        "message": "Fundamental scan started — first results in ~90 seconds",
        "top15": [],
        "scanned": 0,
        "total": len(WATCHLIST),
    }), 202


@app.route("/lt-refresh")
@require_auth
def lt_refresh():
    job = get_job("lt_scan")
    if job.get("running"):
        # A scan is already progressing in the background — don't blank the display,
        # that's what was causing the "resets back to a low number" behaviour.
        r = job.get("result") or {}
        return jsonify({"status":"already_running","scanned":r.get("scanned",0),"total":r.get("total",len(WATCHLIST))})
    with _jobs_lock:
        if "lt_scan" in _jobs: _jobs["lt_scan"]["result"]=None
    if set_job_running("lt_scan"): threading.Thread(target=_do_lt_scan,daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/sector-pulse")
@require_auth
def sector_pulse():
    """Non-blocking. Returns instantly — either cached result or 202."""
    job = get_job("sector_pulse")
    result = job.get("result")
    if result and result.get("status") == "success":
        return jsonify(result)
    if not job.get("running", False):
        if set_job_running("sector_pulse"):
            threading.Thread(target=_do_sector_pulse, daemon=True).start()
    return jsonify({"status":"scanning","message":"Sector analysis running — results in ~20-30s"}), 202

@app.route("/hot-movers")
@require_auth
def hot_movers():
    """Non-blocking. Returns instantly — either cached result or 202."""
    job = get_job("hot_movers")
    result = job.get("result")
    if result and result.get("status") == "success":
        return jsonify(result)
    if not job.get("running", False):
        if set_job_running("hot_movers"):
            threading.Thread(target=_do_hot_movers, daemon=True).start()
    return jsonify({"status":"scanning","message":"Hot movers scan running — results in ~30-60s"}), 202

@app.route("/hot-refresh")
@require_auth
def hot_refresh():
    with _jobs_lock:
        if "hot_movers" in _jobs: _jobs["hot_movers"]["result"]=None
    if set_job_running("hot_movers"): threading.Thread(target=_do_hot_movers,daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/sector-refresh")
@require_auth
def sector_refresh():
    with _jobs_lock:
        if "sector_pulse" in _jobs: _jobs["sector_pulse"]["result"]=None
    if set_job_running("sector_pulse"): threading.Thread(target=_do_sector_pulse,daemon=True).start()
    return jsonify({"status":"started"})

@app.route("/news", methods=["POST"])
def news(): return jsonify({})

if __name__=="__main__":
    threading.Thread(target=_scheduler,  daemon=True).start()
    threading.Thread(target=_keep_alive, daemon=True).start()
    port=int(os.environ.get("PORT",5000))
    print(f"NSESignal Pro v7 — {len(WATCHLIST)} stocks — port {port}")
    app.run(host="0.0.0.0",port=port)
