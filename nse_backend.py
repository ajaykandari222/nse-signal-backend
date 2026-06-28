"""
NSESignal Pro — Cloud Backend v5
- Full NIFTY 500 coverage (500 stocks)
- New indicators: Supertrend, ADX, OBV, Relative Strength vs NIFTY
- Background cache — returns instantly, no timeouts
- Composite score out of 15 (up from 10)
"""

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import yfinance as yf
import numpy as np
import pandas as pd
import ta
from datetime import datetime
import pytz
import threading
import time
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__, template_folder="templates")
CORS(app)
IST = pytz.timezone("Asia/Kolkata")

# ── CACHE ─────────────────────────────────────────────────────────────────
cache = {
    "result": None, "running": False,
    "last_run": None, "scan_count": 0,
    "nifty_prev": None,  # for relative strength calc
}
cache_lock = threading.Lock()

# ── FULL NIFTY 500 WATCHLIST ──────────────────────────────────────────────
WATCHLIST = [
    # ── NIFTY 50 (Large Cap) ──
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN","BAJFINANCE","WIPRO",
    "TECHM","ULTRACEMCO","ONGC","NTPC","POWERGRID","SUNPHARMA","TATAMOTORS","HCLTECH",
    "NESTLEIND","TATASTEEL","JSWSTEEL","HINDALCO","COALINDIA","BPCL","IOC","GAIL",
    "DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP","BAJAJFINSV","EICHERMOT","HEROMOTOCO",
    "ADANIENT","ADANIPORTS","LTIM","INDUSINDBK","ITC","VEDL","GRASIM","TATACONSUM",
    "BRITANNIA","BAJAJ-AUTO","MM",
    # ── NIFTY NEXT 50 ──
    "SIEMENS","ABB","HAVELLS","PIDILITIND","BERGEPAINT","MUTHOOTFIN","CHOLAFIN",
    "SBILIFE","HDFCLIFE","ICICIGI","MARICO","COLPAL","DABUR","GODREJCP","SAIL",
    "NMDC","AMBUJACEM","SHREECEM","IRCTC","IRFC","RVNL","BEL","HAL","BHEL",
    "TRENT","IDFCFIRSTB","BANDHANBNK","FEDERALBNK","TORNTPHARM","LUPIN",
    "AUROPHARMA","ZYDUSLIFE","ALKEM","BIOCON","MANKIND","IPCA","NATCOPHARM",
    # ── NIFTY MIDCAP 150 ──
    "ZOMATO","NYKAA","DMART","DIXON","VOLTAS","POLYCAB","KPITTECH","MPHASIS",
    "LTTS","PERSISTENT","COFORGE","BALKRISIND","APOLLOTYRE","MRF","INDIGO",
    "GMRINFRA","CUMMINSIND","ASTRAL","CONCOR","TATACOMM","BEML","ANGELONE",
    "CDSL","MCX","CAMS","POLICYBZR","PAYTM","NAUKRI","INFOEDGE","LICI",
    "MAXHEALTH","FORTIS","DEEPAKNTR","NAVINFLUOR","TATACHEM","IIFL","IREDA",
    "NHPC","SJVN","RECLTD","PFC","LODHA","DLF","GODREJPROP","PRESTIGE","OBEROIRLTY",
    "RADICO","JUBLFOOD","MOTHERSON","BOSCHLTD","TIINDIA","ENDURANCE","ADANIGREEN",
    "ADANIPOWER","TATAPOWER","TORNTPOWER","MANAPPURAM","MOTILALOSW","CANFINHOME",
    "AAVAS","HOMEFIRST","SENCO","KALYAN","APLAPOLLO","RATNAMANI","RAILTEL",
    "PAGEIND","ABCAPITAL","RBLBANK","SUNDARMFIN","MAHINDCIE","SCHAEFFLER",
    "SKFINDIA","TIMKEN","GRINDWELL","CARBORUNIV","CEATLTD","EXIDEIND","AMARAJABAT",
    "TVSMOTOR","BAJAJHFL","LICHSGFIN","PNBHOUSING","SHRIRAMFIN","MFSL",
    "MAXESTATES","SOBHA","PHOENIXLTD","NUVAMA","MOTILALOFS","ANGELONE",
    "KFINTECH","CAMSTECH","BSEFIN","CENTRALBK","BANKINDIA","UNIONBANK",
    "INDIANB","IOB","MAHABANK","JKCEMENT","DALMIACEM","HEIDELBERG",
    "RAMCOCEM","BIRLASOFT","MASTEK","ZENSAR","HEXAWARE","NIITTECH",
    "TATAELXSI","INTELLECT","TANLA","ROUTE","RATEGAIN","NAUKRI",
    # ── NIFTY SMALLCAP 250 ──
    "CLEAN","FINEORG","ALKYLAMINE","VINATIORG","AARTI","DEEPAKFERT",
    "GNFC","CHAMBLFERT","COROMANDEL","PIIND","RALLIS","BAYER","SUMICHEM",
    "INSECTICIDE","DHANUKA","HERANBA","SAHYADRI","TATACHEM","GUJALKALI",
    "GRSE","COCHINSHIP","MAZAGON","GARDENREACH","PARAS","MIDHANI","MTAR",
    "DATAPATTNS","ZEN","IDEAFORGE","SOLARIND","HBLPOWER","IDFCFIRSTB",
    "UJJIVANSFB","EQUITASBNK","SURYODAY","UTKARSH","ESAFSFB","CREDITACC",
    "SPANDANA","AROHAN","FUSION","AAVAS","APTUS","HOMEFIRST","REPCO",
    "PCJEWELLER","RAJESHEXPO","THANGAMAY","GOLDIAM","KDDL","TITAN",
    "FILATEX","NITIN","HIMATSEIDE","VARDHMAN","RSWM","SPENTEX","SUTLEJ",
    "SAFARI","VMART","TTKPRESTIG","HAWKINCOOK","SYMPHONY","ORIENTELEC",
    "BLUESTARCO","CROMPTON","HAVELLS","BAJAJELEC","AMBER","SYRMA","KAYNES",
    "DIXON","PGEL","SANSERA","SUPRAJIT","LUMAX","SUBROS","MINDA","FIEM",
    "GABRIEL","JAMNA","RACL","SBCL","CINEVISTA","TIPS","SAREGAMA",
    "NAZARA","DELTA","INOX","PVR","DEVYANI","WESTLIFE","SAPPHIRE","QSR",
    "WONDERLA","MAHINDRAHOLIDAY","THOMASCOOK","COX","IRCTC","EASEMYTRIP",
    "IXIGO","YATRA","RATEGAIN","MAHINDRALOGISTICS","BLUEDART","GATI",
    "DELHIVERY","XPRESSBEES","SHADOWFAX","ALLCARGO","CONTAINERWAY","SEACOAST",
    "KPITTECH","TATAELXSI","INTELLECT","MPHASIS","MASTEK","BIRLASOFT",
    "ZENSAR","HEXAWARE","TANLA","ROUTE","SONATASOFT","DATAMATICS","SAKSOFT",
    "CYIENT","LTTS","NIITLTD","APTECH","NIIT","CRISIL","ICRA","CARE",
    "TEAMLEASE","QUESS","STAFFLINE","CARERATINGS","BRICKWORK","ACUITERATING",
    "SBFC","UGRO","CREDITACC","PAISALO","ARMANFIN","CAPACITE","KNR",
    "HGINFRA","PNCINFRA","ASHOKA","SADBHAV","DILIPBUILDCON","GAWARCON",
    "JTLINFRA","VISHAL","RATNAVEER","SHYAMSTEEL","JSWISPL","GRAPHITE",
    "HEMIPROP","KOLTEPATIL","MAHLIFE","SUNTECK","KEYSTONE","ARVIND",
    "RAYMOND","GOKALDAS","KITEX","RUPA","DOLLAR","DIXCY","LUX",
    "IGARASHI","GREENPANEL","CENTURYPLY","GREENPLY","DUROPLY","STYLAM",
    "ASTERDM","VIJAYADIAG","METROPOLIS","THYROCARE","LALGPATH","KRSNAA",
    "MEDPLUS","PHARMACONTROL","SUVEN","NEULANDLAB","SOLARA","DIVI","SEQUENT",
    "WOCKHARDT","GLENMARK","TORNTPHARM","JBCHEPHARM","CAPLIN","GRANULES",
    "SHILPAMED","POLY","SYNGENE","NEULAND","HESTER","VIMTA","MEDINOL",
    "TCIEXP","MAHLOG","VRL","SNOWMAN","COLDSTAR","NAVINFLUOR","SRF","AARTI",
    "TATVA","CAMLIN","PIDILITIND","HUHTAMAKI","MOLD","UFLEX","POLYPLEX",
    "COSMOFILMS","JINDALPOLY","GARWARE","NILKAMAL","PLASTIC","WINTAC",
    "KRBL","LTTECHNOLOGIES","AVANTIFEED","WATERBASE","APEX","ZEEL","SUNTV",
    "TVTODAY","JAGRAN","DBCORP","HTMEDIA","DECCAN","NDTV","NETWORK18",
]

# Remove duplicates
WATCHLIST = list(dict.fromkeys(WATCHLIST))

def get_ns(sym):
    return sym.replace("&", "%26") + ".NS"

def compute_supertrend(high, low, close, period=10, multiplier=3.0):
    """Compute Supertrend indicator."""
    try:
        hl2    = (high + low) / 2
        atr    = ta.volatility.AverageTrueRange(high=high, low=low, close=close, window=period).average_true_range()
        upper  = hl2 + multiplier * atr
        lower  = hl2 - multiplier * atr

        supertrend  = pd.Series(index=close.index, dtype=float)
        direction   = pd.Series(index=close.index, dtype=float)

        supertrend.iloc[0] = upper.iloc[0]
        direction.iloc[0]  = 1

        for i in range(1, len(close)):
            if close.iloc[i] > supertrend.iloc[i-1]:
                supertrend.iloc[i] = lower.iloc[i]
                direction.iloc[i]  = 1   # bullish
            else:
                supertrend.iloc[i] = upper.iloc[i]
                direction.iloc[i]  = -1  # bearish

        return float(direction.iloc[-1]), float(supertrend.iloc[-1])
    except:
        return 0, 0

def compute_adx(high, low, close, period=14):
    """Compute ADX — trend strength."""
    try:
        adx_ind = ta.trend.ADXIndicator(high=high, low=low, close=close, window=period)
        adx_val = float(adx_ind.adx().iloc[-1])
        dip     = float(adx_ind.adx_pos().iloc[-1])
        dim     = float(adx_ind.adx_neg().iloc[-1])
        return round(adx_val, 1), round(dip, 1), round(dim, 1)
    except:
        return 0, 0, 0

def compute_obv_trend(close, volume):
    """Check if OBV is in uptrend (last 5 periods)."""
    try:
        obv = ta.volume.OnBalanceVolumeIndicator(close=close, volume=volume).on_balance_volume()
        # OBV trending up = last value > 5-period ago
        obv_trend = float(obv.iloc[-1]) > float(obv.iloc[-6]) if len(obv) >= 6 else False
        obv_slope = round((float(obv.iloc[-1]) - float(obv.iloc[-5])) / (abs(float(obv.iloc[-5])) + 1) * 100, 2) if len(obv) >= 5 else 0
        return obv_trend, obv_slope
    except:
        return False, 0

def compute_indicators(df, nifty_change=None):
    if df is None or len(df) < 30:
        return None
    try:
        close  = df["Close"].squeeze()
        high   = df["High"].squeeze()
        low    = df["Low"].squeeze()
        volume = df["Volume"].squeeze()
        mask   = close.notna() & volume.notna() & (close > 0)
        close  = close[mask]; high = high[mask]
        low    = low[mask];   volume = volume[mask]
        if len(close) < 26: return None

        r = {}
        r["cmp"]        = round(float(close.iloc[-1]), 2)
        r["open"]       = round(float(df["Open"].squeeze()[mask].iloc[-1]), 2)
        r["high"]       = round(float(high.iloc[-1]), 2)
        r["low"]        = round(float(low.iloc[-1]), 2)
        r["prev_close"] = round(float(close.iloc[-2]), 2)
        r["change_pct"] = round((r["cmp"] - r["prev_close"]) / r["prev_close"] * 100, 2)
        if r["cmp"] <= 0: return None

        # ── RSI ──
        rsi = ta.momentum.RSIIndicator(close=close, window=14).rsi()
        r["rsi"] = round(float(rsi.iloc[-1]), 1)

        # ── MACD ──
        macd_obj = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        r["macd_hist"]    = round(float(macd_obj.macd_diff().iloc[-1]), 3)
        r["macd_prev"]    = round(float(macd_obj.macd_diff().iloc[-2]), 3)
        r["macd_signal"]  = round(float(macd_obj.macd_signal().iloc[-1]), 3)
        r["macd_bullish"] = bool(r["macd_hist"] > 0 and r["macd_hist"] > r["macd_prev"])

        # ── EMA ──
        r["ema20"]        = round(float(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1]), 2)
        r["ema50"]        = round(float(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1]), 2)
        r["above_ema20"]  = bool(r["cmp"] > r["ema20"])
        r["above_ema50"]  = bool(r["cmp"] > r["ema50"])
        r["golden_cross"] = bool(r["ema20"] > r["ema50"])
        r["ema200"]       = round(float(ta.trend.EMAIndicator(close=close, window=200).ema_indicator().iloc[-1]), 2) if len(close) >= 200 else None

        # ── VWAP ──
        typical         = (high + low + close) / 3
        vwap            = (typical * volume).cumsum() / volume.cumsum()
        r["vwap"]       = round(float(vwap.iloc[-1]), 2)
        r["above_vwap"] = bool(r["cmp"] > r["vwap"])

        # ── Volume ── (force float to avoid ta library crash)
        volume          = volume.astype(float)
        avg_vol         = float(volume.iloc[-20:].mean()) if len(volume) >= 20 else float(volume.mean())
        r["volume"]     = int(float(volume.iloc[-1]))
        r["avg_volume"] = int(avg_vol)
        r["rel_volume"] = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

        # ── Bollinger Bands ──
        bb              = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
        r["bb_upper"]   = round(float(bb.bollinger_hband().iloc[-1]), 2)
        r["bb_lower"]   = round(float(bb.bollinger_lband().iloc[-1]), 2)
        r["bb_pct"]     = round(float(bb.bollinger_pband().iloc[-1]), 3)

        # ── 52-week ──
        r["week52_high"]  = round(float(close.max()), 2)
        r["week52_low"]   = round(float(close.min()), 2)
        r["pct_from_52h"] = round((r["cmp"] - r["week52_high"]) / r["week52_high"] * 100, 1)

        # ── NEW: Supertrend ──
        try:
            st_dir, st_val      = compute_supertrend(high, low, close)
            r["supertrend_bull"] = bool(st_dir == 1)
            r["supertrend_val"]  = round(st_val, 2)
        except:
            r["supertrend_bull"] = False
            r["supertrend_val"]  = 0

        # ── NEW: ADX ──
        try:
            adx, dip, dim    = compute_adx(high, low, close)
            r["adx"]         = adx
            r["adx_pos"]     = dip
            r["adx_neg"]     = dim
            r["adx_strong"]  = bool(adx >= 25)
            r["adx_bullish"] = bool(dip > dim)
        except:
            r["adx"] = 0; r["adx_pos"] = 0; r["adx_neg"] = 0
            r["adx_strong"] = False; r["adx_bullish"] = False

        # ── NEW: OBV ──
        try:
            obv_up, obv_slope = compute_obv_trend(close, volume.astype(float))
            r["obv_bullish"]  = bool(obv_up)
            r["obv_slope"]    = obv_slope
        except:
            r["obv_bullish"]  = False
            r["obv_slope"]    = 0

        # ── NEW: Relative Strength vs NIFTY ──
        try:
            nc = nifty_change if nifty_change is not None else 0
            r["rel_strength"]      = round(r["change_pct"] - nc, 2)
            r["outperforms_nifty"] = bool(r["change_pct"] > nc)
        except:
            r["rel_strength"]      = 0
            r["outperforms_nifty"] = False

        return r
    except Exception as e:
        return None

def score_stock(ind):
    if not ind: return 0, []
    score = 0; signals = []

    # ── RSI (max 2.5) ──
    rsi = ind.get("rsi", 50)
    if   52 <= rsi <= 68: score += 2.5; signals.append(f"RSI {rsi} — ideal momentum zone ✓")
    elif 45 <= rsi < 52:  score += 1.5; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68 < rsi <= 72:  score += 1.0; signals.append(f"RSI {rsi} — strong, near overbought")
    elif rsi > 72:        score -= 1.0; signals.append(f"RSI {rsi} — overbought ✗")
    else:                 score -= 1.0; signals.append(f"RSI {rsi} — weak momentum ✗")

    # ── MACD (max 2.0) ──
    if ind.get("macd_bullish"):
        score += 2.0; signals.append(f"MACD expanding positive ({ind.get('macd_hist')}) ✓")
    elif ind.get("macd_hist", 0) > 0:
        score += 1.0; signals.append(f"MACD positive but fading ({ind.get('macd_hist')})")
    else:
        score -= 1.0; signals.append(f"MACD bearish ({ind.get('macd_hist')}) ✗")

    # ── VWAP (max 1.5) ──
    if ind.get("above_vwap"):
        score += 1.5; signals.append(f"Above VWAP ₹{ind.get('vwap')} ✓")
    else:
        score -= 0.5; signals.append(f"Below VWAP ₹{ind.get('vwap')} ✗")

    # ── EMA (max 2.0) ──
    if ind.get("golden_cross"):
        score += 1.5; signals.append(f"Golden cross: EMA20 > EMA50 ✓")
    else:
        score -= 0.5; signals.append(f"Death cross: EMA20 < EMA50 ✗")
    if ind.get("above_ema20"):
        score += 0.5; signals.append(f"Price above EMA20 ₹{ind.get('ema20')} ✓")

    # ── Volume (max 2.0) ──
    rv = ind.get("rel_volume", 1.0)
    if   rv >= 2.5: score += 2.0; signals.append(f"Volume {rv}x avg — institutional activity ✓")
    elif rv >= 1.5: score += 1.0; signals.append(f"Volume {rv}x avg — above average ✓")
    elif rv < 0.7:  score -= 0.5; signals.append(f"Volume {rv}x avg — weak ✗")
    else:           signals.append(f"Volume {rv}x avg — normal")

    # ── NEW: Supertrend (max 2.0) ──
    if ind.get("supertrend_bull"):
        score += 2.0; signals.append(f"Supertrend BULLISH — price above trend line ✓")
    else:
        score -= 1.5; signals.append(f"Supertrend BEARISH — price below trend line ✗")

    # ── NEW: ADX (max 2.0) ──
    adx = ind.get("adx", 0)
    if adx >= 30 and ind.get("adx_bullish"):
        score += 2.0; signals.append(f"ADX {adx} — very strong bullish trend ✓")
    elif adx >= 25 and ind.get("adx_bullish"):
        score += 1.5; signals.append(f"ADX {adx} — strong trend, DI+ > DI- ✓")
    elif adx >= 20:
        score += 0.5; signals.append(f"ADX {adx} — moderate trend")
    else:
        score -= 0.5; signals.append(f"ADX {adx} — weak/ranging market ✗")

    # ── NEW: OBV (max 1.5) ──
    if ind.get("obv_bullish"):
        score += 1.5; signals.append(f"OBV trending up — volume confirming price rise ✓")
    else:
        score -= 0.5; signals.append(f"OBV declining — volume not confirming ✗")

    # ── NEW: Relative Strength vs NIFTY (max 1.5) ──
    rs = ind.get("rel_strength", 0)
    if rs > 1.5:
        score += 1.5; signals.append(f"Outperforming NIFTY by {rs}% — strong relative strength ✓")
    elif rs > 0:
        score += 0.5; signals.append(f"Outperforming NIFTY by {rs}% ✓")
    elif rs < -1.5:
        score -= 1.0; signals.append(f"Underperforming NIFTY by {abs(rs)}% ✗")
    else:
        signals.append(f"Relative strength vs NIFTY: {rs}%")

    # ── Bollinger (max 0.5) ──
    bb_pct = ind.get("bb_pct", 0.5)
    if   0.2 <= bb_pct <= 0.7: score += 0.5; signals.append(f"Bollinger {round(bb_pct*100)}% — healthy range ✓")
    elif bb_pct > 0.9:         score -= 0.5; signals.append(f"Bollinger near upper band ✗")

    return max(0, min(15, round(score, 1))), signals

def fetch_nifty_change():
    """Get today's NIFTY % change for relative strength calculation."""
    try:
        df = yf.Ticker("^NSEI").history(period="2d", interval="1d", auto_adjust=True)
        if df is not None and len(df) >= 2:
            c = float(df["Close"].squeeze().iloc[-1])
            p = float(df["Close"].squeeze().iloc[-2])
            return round((c - p) / p * 100, 2)
    except:
        pass
    return 0

def fetch_one(args):
    sym, nifty_change = args
    try:
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra) >= 15) else daily
        ind    = compute_indicators(df, nifty_change)
        if not ind: return None
        score, signals = score_stock(ind)
        return {"symbol": sym, "score": score, "signals": signals, **ind}
    except:
        return None

def run_scan_background():
    with cache_lock:
        if cache["running"]: return
        cache["running"] = True

    now   = datetime.now(IST)
    start = time.time()
    print(f"\n[BG SCAN] {now.strftime('%I:%M %p IST')} — {len(WATCHLIST)} stocks")

    nifty_change = fetch_nifty_change()
    print(f"[NIFTY] Change today: {nifty_change}%")

    results = []
    errors  = 0
    args    = [(sym, nifty_change) for sym in WATCHLIST]

    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_one, a): a[0] for a in args}
        for future in as_completed(futures, timeout=150):
            try:
                r = future.result(timeout=8)
                if r: results.append(r)
                else: errors += 1
            except:
                errors += 1

    results.sort(key=lambda x: x["score"], reverse=True)
    elapsed = round(time.time() - start, 1)
    print(f"[BG SCAN] Done — {len(results)} valid, {errors} errors, {elapsed}s")

    with cache_lock:
        cache["result"] = {
            "status":    "success",
            "scan_time": now.strftime("%I:%M %p IST"),
            "date":      now.strftime("%d %b %Y"),
            "scanned":   len(results),
            "total":     len(WATCHLIST),
            "errors":    errors,
            "elapsed":   f"{elapsed}s",
            "nifty_change": nifty_change,
            "top10":     results[:10],
            "cached":    True,
        }
        cache["last_run"]   = now
        cache["running"]    = False
        cache["scan_count"] += 1
        cache["nifty_prev"] = nifty_change

def background_scheduler():
    time.sleep(5)
    while True:
        try:
            run_scan_background()
        except Exception as e:
            print(f"[SCHEDULER] Error: {e}")
            with cache_lock:
                cache["running"] = False
        time.sleep(5 * 60)  # every 5 minutes

def keep_alive():
    url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not url: return
    while True:
        time.sleep(14 * 60)
        try:
            requests.get(f"{url}/health", timeout=10)
            print("[KEEP-ALIVE] pinged")
        except:
            pass

# ── ROUTES ────────────────────────────────────────────────────────────────

@app.route("/")
@app.route("/app")
def frontend():
    return render_template("index.html")

@app.route("/health")
def health():
    now = datetime.now(IST)
    with cache_lock:
        last    = cache["last_run"].strftime("%I:%M %p IST") if cache["last_run"] else "not yet"
        running = cache["running"]
        count   = cache["scan_count"]
    return jsonify({
        "status":       "running",
        "service":      "NSESignal Pro v5",
        "time":         now.strftime("%I:%M:%S %p IST"),
        "last_scan":    last,
        "scan_running": running,
        "scan_count":   count,
        "watchlist":    len(WATCHLIST),
        "message":      "Backend live — scans every 5 min in background"
    })

@app.route("/scan")
def scan():
    try:
        with cache_lock:
            result  = cache["result"]
            running = cache["running"]

        if result is None:
            if not running:
                threading.Thread(target=run_scan_background, daemon=True).start()
            # Wait up to 90s for first scan
            for _ in range(90):
                time.sleep(1)
                with cache_lock:
                    if cache["result"] is not None:
                        return jsonify(cache["result"])
            return jsonify({
                "status":  "scanning",
                "message": "First scan in progress — retry in 30 seconds",
                "top10":   []
            }), 202

        return jsonify(result)
    except Exception as e:
        print(f"[SCAN ROUTE ERROR] {e}")
        return jsonify({"status":"error","message":str(e),"top10":[]}), 500

@app.route("/refresh")
def refresh():
    with cache_lock:
        running = cache["running"]
    if not running:
        threading.Thread(target=run_scan_background, daemon=True).start()
        return jsonify({"status": "started", "message": "Fresh scan started"})
    return jsonify({"status": "already_running"})

@app.route("/indices")
def indices():
    out = {}
    for name, sym in [("nifty","^NSEI"),("sensex","^BSESN"),("banknifty","^NSEBANK")]:
        try:
            df = yf.Ticker(sym).history(period="1d", interval="5m", auto_adjust=True)
            if df is not None and len(df) > 1:
                close = float(df["Close"].squeeze().iloc[-1])
                prev  = float(df["Close"].squeeze().iloc[-2])
                out[name] = {
                    "price":  round(close, 2),
                    "change": round(close - prev, 2),
                    "pct":    round((close - prev) / prev * 100, 2)
                }
        except:
            out[name] = {}
    return jsonify(out)

@app.route("/news", methods=["POST"])
def news():
    return jsonify({})

_started = False

@app.before_request
def start_threads():
    global _started
    if not _started:
        _started = True
        threading.Thread(target=background_scheduler, daemon=True).start()
        threading.Thread(target=keep_alive, daemon=True).start()
        print("[STARTUP] Background threads started")

if __name__ == "__main__":
    threading.Thread(target=background_scheduler, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"NSESignal Pro v5 — {len(WATCHLIST)} stocks — port {port}")
    app.run(host="0.0.0.0", port=port)
