"""
NSESignal Pro — Cloud Backend v5 STABLE
- Reverted to proven v4 indicator set (RSI, MACD, EMA, VWAP, Volume, BB)
- Added Supertrend and ADX only — both well tested
- OBV and Relative Strength removed (were causing crashes)
- Background cache — returns instantly, no timeouts
- Full NIFTY 500 + small cap watchlist
"""

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import numpy as np
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
}
cache_lock = threading.Lock()

# ── FULL WATCHLIST ────────────────────────────────────────────────────────
WATCHLIST = list(dict.fromkeys([
    # NIFTY 50
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN",
    "BHARTIARTL","KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN",
    "BAJFINANCE","WIPRO","TECHM","ULTRACEMCO","ONGC","NTPC","POWERGRID",
    "SUNPHARMA","TATAMOTORS","HCLTECH","NESTLEIND","TATASTEEL","JSWSTEEL",
    "HINDALCO","COALINDIA","BPCL","IOC","GAIL","DRREDDY","CIPLA","DIVISLAB",
    "APOLLOHOSP","BAJAJFINSV","EICHERMOT","HEROMOTOCO","ADANIENT","ADANIPORTS",
    "LTIM","INDUSINDBK","ITC","VEDL","GRASIM","TATACONSUM","BRITANNIA","BAJAJ-AUTO",
    # NIFTY NEXT 50
    "SIEMENS","ABB","HAVELLS","PIDILITIND","BERGEPAINT","MUTHOOTFIN","CHOLAFIN",
    "SBILIFE","HDFCLIFE","ICICIGI","MARICO","COLPAL","DABUR","GODREJCP",
    "SAIL","NMDC","AMBUJACEM","SHREECEM","IRCTC","IRFC","RVNL","BEL","HAL",
    "BHEL","TRENT","IDFCFIRSTB","BANDHANBNK","FEDERALBNK","TORNTPHARM","LUPIN",
    "AUROPHARMA","ZYDUSLIFE","ALKEM","BIOCON","MANKIND",
    # NIFTY MIDCAP 150
    "ZOMATO","NYKAA","DMART","DIXON","VOLTAS","POLYCAB","KPITTECH","MPHASIS",
    "LTTS","PERSISTENT","COFORGE","BALKRISIND","APOLLOTYRE","MRF","INDIGO",
    "GMRINFRA","CUMMINSIND","ASTRAL","CONCOR","TATACOMM","BEML","ANGELONE",
    "CDSL","MCX","CAMS","POLICYBZR","PAYTM","NAUKRI","INFOEDGE","LICI",
    "MAXHEALTH","FORTIS","DEEPAKNTR","NAVINFLUOR","TATACHEM","IIFL","IREDA",
    "NHPC","SJVN","RECLTD","PFC","LODHA","DLF","GODREJPROP","PRESTIGE",
    "RADICO","JUBLFOOD","MOTHERSON","BOSCHLTD","TIINDIA","ENDURANCE",
    "ADANIGREEN","ADANIPOWER","TATAPOWER","TORNTPOWER","MANAPPURAM","MOTILALOSW",
    "CANFINHOME","AAVAS","HOMEFIRST","SENCO","KALYAN","APLAPOLLO","RATNAMANI",
    "RAILTEL","PAGEIND","ABCAPITAL","RBLBANK","SUNDARMFIN","SCHAEFFLER",
    "SKFINDIA","TIMKEN","GRINDWELL","CEATLTD","EXIDEIND","TVSMOTOR",
    "BAJAJHFL","LICHSGFIN","SHRIRAMFIN","MAXESTATES","SOBHA","PHOENIXLTD",
    "NUVAMA","CENTRALBK","BANKINDIA","UNIONBANK","INDIANB",
    "JKCEMENT","DALMIACEM","RAMCOCEM","BIRLASOFT","MASTEK","ZENSAR",
    "INTELLECT","TANLA","ROUTE","SONATASOFT","CYIENT",
    # NIFTY SMALLCAP 250
    "CLEANSCIENCE","FINEORG","ALKYLAMINE","VINATIORG","AARTI","DEEPAKFERT",
    "GNFC","CHAMBLFERT","COROMANDEL","PIIND","RALLIS","SUMICHEM",
    "INSECTICIDE","DHANUKA","GRSE","COCHINSHIP","MAZAGON","GARDENREACH",
    "PARAS","MIDHANI","MTAR","DATAPATTNS","HBLPOWER",
    "UJJIVANSFB","EQUITASBNK","SURYODAY","UTKARSH","CREDITACC",
    "SPANDANA","SAFARI","VMART","TTKPRESTIG","HAWKINCOOK","SYMPHONY",
    "CROMPTON","AMBER","SYRMA","KAYNES","SANSERA","SUPRAJIT","LUMAX",
    "GABRIEL","KRBL","AVANTIFEED","WATERBASE","ZEEL","SUNTV",
    "TVTODAY","NAZARA","WONDERLA","DELHIVERY","ALLCARGO",
    "CRISIL","ICRA","TEAMLEASE","QUESS","SBFC","UGRO","PAISALO",
    "CAPACITE","KNR","HGINFRA","PNCINFRA","ASHOKA","DILIPBUILDCON",
    "GRAPHITE","HEMIPROP","KOLTEPATIL","ARVIND","RAYMOND","GOKALDAS",
    "IGARASHI","GREENPANEL","CENTURYPLY","ASTERDM","METROPOLIS",
    "THYROCARE","LALGPATH","KRSNAA","MEDPLUS","SUVEN","NEULANDLAB",
    "SOLARA","GRANULES","WOCKHARDT","GLENMARK","JBCHEPHARM","CAPLIN",
    "TCIEXP","SNOWMAN","SRF","UFLEX","POLYPLEX","COSMOFILMS","GARWARE",
    "NILKAMAL","NETWORK18","SAREGAMA","TIPS",
]))

def get_ns(sym):
    return sym.replace("&", "%26") + ".NS"

def safe_float(val, default=0.0):
    try:
        return float(val)
    except:
        return default

def compute_supertrend(high, low, close, period=10, multiplier=3.0):
    try:
        if len(close) < period + 5:
            return 0, 0
        hl2 = (high + low) / 2
        atr = ta.volatility.AverageTrueRange(
            high=high, low=low, close=close, window=period
        ).average_true_range()

        upper = (hl2 + multiplier * atr).fillna(method="ffill")
        lower = (hl2 - multiplier * atr).fillna(method="ffill")

        st    = pd.Series(np.nan, index=close.index)
        direc = pd.Series(1,      index=close.index)

        st.iloc[period] = upper.iloc[period]

        for i in range(period + 1, len(close)):
            prev_st = st.iloc[i-1]
            if pd.isna(prev_st):
                st.iloc[i] = upper.iloc[i]
                continue
            if close.iloc[i] > prev_st:
                st.iloc[i]    = lower.iloc[i]
                direc.iloc[i] = 1
            else:
                st.iloc[i]    = upper.iloc[i]
                direc.iloc[i] = -1

        return int(direc.iloc[-1]), safe_float(st.iloc[-1])
    except Exception as e:
        print(f"  [ST err] {e}")
        return 0, 0

def compute_adx(high, low, close, period=14):
    try:
        if len(close) < period * 2:
            return 0, 0, 0
        ind = ta.trend.ADXIndicator(high=high, low=low, close=close, window=period)
        return (
            round(safe_float(ind.adx().iloc[-1]), 1),
            round(safe_float(ind.adx_pos().iloc[-1]), 1),
            round(safe_float(ind.adx_neg().iloc[-1]), 1),
        )
    except Exception as e:
        print(f"  [ADX err] {e}")
        return 0, 0, 0

def compute_indicators(df):
    if df is None or len(df) < 30:
        return None
    try:
        close  = df["Close"].squeeze().astype(float)
        high   = df["High"].squeeze().astype(float)
        low    = df["Low"].squeeze().astype(float)
        volume = df["Volume"].squeeze().astype(float)
        opn    = df["Open"].squeeze().astype(float)

        mask   = close.notna() & volume.notna() & (close > 0) & (volume > 0)
        close  = close[mask]; high   = high[mask]
        low    = low[mask];   volume = volume[mask]; opn = opn[mask]

        if len(close) < 26:
            return None

        r = {}
        r["cmp"]        = round(float(close.iloc[-1]), 2)
        r["open"]       = round(float(opn.iloc[-1]), 2)
        r["high"]       = round(float(high.iloc[-1]), 2)
        r["low"]        = round(float(low.iloc[-1]), 2)
        r["prev_close"] = round(float(close.iloc[-2]), 2)
        r["change_pct"] = round((r["cmp"] - r["prev_close"]) / r["prev_close"] * 100, 2)
        if r["cmp"] <= 0:
            return None

        # RSI
        rsi = ta.momentum.RSIIndicator(close=close, window=14).rsi()
        r["rsi"] = round(safe_float(rsi.iloc[-1], 50), 1)

        # MACD
        macd_obj         = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        r["macd_hist"]   = round(safe_float(macd_obj.macd_diff().iloc[-1]), 3)
        r["macd_prev"]   = round(safe_float(macd_obj.macd_diff().iloc[-2]), 3)
        r["macd_signal"] = round(safe_float(macd_obj.macd_signal().iloc[-1]), 3)
        r["macd_bullish"]= bool(r["macd_hist"] > 0 and r["macd_hist"] > r["macd_prev"])

        # EMA
        r["ema20"]       = round(safe_float(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1]), 2)
        r["ema50"]       = round(safe_float(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1]), 2)
        r["above_ema20"] = bool(r["cmp"] > r["ema20"])
        r["above_ema50"] = bool(r["cmp"] > r["ema50"])
        r["golden_cross"]= bool(r["ema20"] > r["ema50"])
        r["ema200"]      = round(safe_float(ta.trend.EMAIndicator(close=close, window=200).ema_indicator().iloc[-1]), 2) if len(close) >= 200 else None

        # VWAP
        typical          = (high + low + close) / 3
        vwap             = (typical * volume).cumsum() / volume.cumsum()
        r["vwap"]        = round(safe_float(vwap.iloc[-1]), 2)
        r["above_vwap"]  = bool(r["cmp"] > r["vwap"])

        # Volume
        avg_vol          = float(volume.iloc[-20:].mean()) if len(volume) >= 20 else float(volume.mean())
        r["volume"]      = int(float(volume.iloc[-1]))
        r["avg_volume"]  = int(avg_vol)
        r["rel_volume"]  = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

        # Bollinger
        bb               = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
        r["bb_upper"]    = round(safe_float(bb.bollinger_hband().iloc[-1]), 2)
        r["bb_lower"]    = round(safe_float(bb.bollinger_lband().iloc[-1]), 2)
        r["bb_pct"]      = round(safe_float(bb.bollinger_pband().iloc[-1], 0.5), 3)

        # 52-week
        r["week52_high"] = round(float(close.max()), 2)
        r["week52_low"]  = round(float(close.min()), 2)
        r["pct_from_52h"]= round((r["cmp"] - r["week52_high"]) / r["week52_high"] * 100, 1)

        # Supertrend
        st_dir, st_val       = compute_supertrend(high, low, close)
        r["supertrend_bull"] = bool(st_dir == 1)
        r["supertrend_val"]  = round(st_val, 2)

        # ADX
        adx, dip, dim    = compute_adx(high, low, close)
        r["adx"]         = adx
        r["adx_pos"]     = dip
        r["adx_neg"]     = dim
        r["adx_strong"]  = bool(adx >= 25)
        r["adx_bullish"] = bool(dip > dim)

        return r
    except Exception as e:
        print(f"  [IND err] {e}")
        return None

def score_stock(ind):
    if not ind:
        return 0, []
    score = 0
    signals = []

    rsi = ind.get("rsi", 50)
    if   52 <= rsi <= 68: score += 2.5; signals.append(f"RSI {rsi} — ideal momentum zone ✓")
    elif 45 <= rsi <  52: score += 1.5; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68 <  rsi <= 72: score += 1.0; signals.append(f"RSI {rsi} — strong, near overbought")
    elif rsi > 72:        score -= 1.0; signals.append(f"RSI {rsi} — overbought ✗")
    else:                 score -= 1.0; signals.append(f"RSI {rsi} — weak momentum ✗")

    if ind.get("macd_bullish"):
        score += 2.0; signals.append(f"MACD expanding positive ({ind['macd_hist']}) ✓")
    elif ind.get("macd_hist", 0) > 0:
        score += 1.0; signals.append(f"MACD positive but fading ({ind['macd_hist']})")
    else:
        score -= 1.0; signals.append(f"MACD bearish ({ind.get('macd_hist', 0)}) ✗")

    if ind.get("above_vwap"):
        score += 1.5; signals.append(f"Above VWAP ₹{ind['vwap']} ✓")
    else:
        score -= 0.5; signals.append(f"Below VWAP ₹{ind.get('vwap', 0)} ✗")

    if ind.get("golden_cross"):
        score += 1.5; signals.append(f"Golden cross: EMA20 > EMA50 ✓")
    else:
        score -= 0.5; signals.append(f"Death cross: EMA20 < EMA50 ✗")
    if ind.get("above_ema20"):
        score += 0.5; signals.append(f"Price above EMA20 ₹{ind['ema20']} ✓")

    rv = ind.get("rel_volume", 1.0)
    if   rv >= 2.5: score += 2.0; signals.append(f"Volume {rv}x avg — institutional activity ✓")
    elif rv >= 1.5: score += 1.0; signals.append(f"Volume {rv}x avg — above average ✓")
    elif rv <  0.7: score -= 0.5; signals.append(f"Volume {rv}x avg — weak ✗")
    else:           signals.append(f"Volume {rv}x avg — normal")

    if ind.get("supertrend_bull"):
        score += 2.0; signals.append(f"Supertrend BULLISH ✓")
    else:
        score -= 1.5; signals.append(f"Supertrend BEARISH ✗")

    adx = ind.get("adx", 0)
    if   adx >= 30 and ind.get("adx_bullish"): score += 2.0; signals.append(f"ADX {adx} — very strong bullish trend ✓")
    elif adx >= 25 and ind.get("adx_bullish"): score += 1.5; signals.append(f"ADX {adx} — strong trend, DI+>DI- ✓")
    elif adx >= 20:                             score += 0.5; signals.append(f"ADX {adx} — moderate trend")
    else:                                       score -= 0.5; signals.append(f"ADX {adx} — weak/ranging ✗")

    bb_pct = ind.get("bb_pct", 0.5)
    if   0.2 <= bb_pct <= 0.7: score += 0.5; signals.append(f"Bollinger {round(bb_pct*100)}% — healthy ✓")
    elif bb_pct > 0.9:         score -= 0.5; signals.append(f"Bollinger near upper band ✗")

    return max(0, min(12, round(score, 1))), signals

def fetch_one(sym):
    try:
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra) >= 15) else daily
        ind    = compute_indicators(df)
        if not ind:
            return None
        score, signals = score_stock(ind)
        return {"symbol": sym, "score": score, "signals": signals, **ind}
    except:
        return None

def run_scan_background():
    with cache_lock:
        if cache["running"]:
            return
        cache["running"] = True

    now   = datetime.now(IST)
    start = time.time()
    print(f"\n[BG SCAN] {now.strftime('%I:%M %p IST')} — {len(WATCHLIST)} stocks")

    results = []
    errors  = 0

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(fetch_one, sym): sym for sym in WATCHLIST}
        for future in as_completed(futures, timeout=150):
            try:
                r = future.result(timeout=8)
                if r:
                    results.append(r)
                else:
                    errors += 1
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
            "top10":     results[:10],
            "cached":    True,
        }
        cache["last_run"]   = now
        cache["running"]    = False
        cache["scan_count"] += 1

def background_scheduler():
    time.sleep(5)
    while True:
        try:
            run_scan_background()
        except Exception as e:
            print(f"[SCHEDULER] {e}")
            with cache_lock:
                cache["running"] = False
        time.sleep(5 * 60)

def keep_alive():
    url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not url:
        return
    while True:
        time.sleep(14 * 60)
        try:
            requests.get(f"{url}/health", timeout=10)
            print("[KEEP-ALIVE] pinged")
        except:
            pass

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
        print(f"[SCAN ERR] {e}")
        return jsonify({"status": "error", "message": str(e), "top10": []}), 500

@app.route("/refresh")
def refresh():
    with cache_lock:
        running = cache["running"]
    if not running:
        threading.Thread(target=run_scan_background, daemon=True).start()
        return jsonify({"status": "started"})
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
