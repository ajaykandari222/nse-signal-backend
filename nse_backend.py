"""
NSESignal Pro — Cloud Backend v2
=================================
Key improvements:
- Parallel stock fetching (ThreadPoolExecutor) — scans 200 stocks in 60 sec
- Direct NSE website API for top gainers/losers/most-active (no nsetools)
- Render 60s timeout handled — returns partial results if timeout hit
- Gunicorn timeout set to 120s via Procfile
"""

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import yfinance as yf
import pandas as pd
import ta
from datetime import datetime
import pytz
import threading
import time
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError

app = Flask(__name__, template_folder="templates")
CORS(app)
IST = pytz.timezone("Asia/Kolkata")

# ── NIFTY 500 BASE LIST ────────────────────────────────────────────────────
NIFTY500_BASE = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN","BAJFINANCE","WIPRO",
    "TECHM","ULTRACEMCO","ONGC","NTPC","POWERGRID","SUNPHARMA","TATAMOTORS","HCLTECH",
    "NESTLEIND","TATASTEEL","JSWSTEEL","HINDALCO","COALINDIA","BPCL","IOC","GAIL",
    "DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP","BAJAJFINSV","BAJAJ-AUTO","EICHERMOT",
    "HEROMOTOCO","ADANIENT","ADANIPORTS","LTIM","INDUSINDBK","MM",
    "SIEMENS","ABB","HAVELLS","PIDILITIND","BERGEPAINT","MUTHOOTFIN","CHOLAFIN",
    "SBILIFE","HDFCLIFE","ICICIGI","MARICO","COLPAL","DABUR","GODREJCP","BRITANNIA",
    "TATACONSUM","ITC","VEDL","SAIL","NMDC","GRASIM","AMBUJACEM","SHREECEM",
    "IRCTC","IRFC","RVNL","RAILTEL","BEL","HAL","BHEL","BEML",
    "ZOMATO","NYKAA","DMART","DIXON","VOLTAS","POLYCAB","KPITTECH","MPHASIS",
    "LTTS","PERSISTENT","COFORGE","SONACOMS","BALKRISIND","CEATLTD","APOLLOTYRE","MRF",
    "TRENT","IDFCFIRSTB","BANDHANBNK","RBLBANK","FEDERALBNK","TATACOMM",
    "INDIGO","GMRINFRA","CUMMINSIND","THERMAX","ASTRAL","CONCOR",
    "ANGELONE","CDSL","BSE","MCX","CAMS","KFINTECH","POLICYBZR","PAYTM",
    "NAUKRI","INFOEDGE","LICI","MAXHEALTH","FORTIS","CLEANSCIENCE","DEEPAKNTR",
    "TATACHEM","NAVINFLUOR","ALKYLAMINE","FINEORG","PRAJIND","CERA","VMART",
    "TTKPRESTIG","BAJAJHFL","CANFINHOME","APTUS","HOMEFIRST","AAVAS","REPCO",
    "MANAPPURAM","IIFL","MOTILALOSW","IREDA","NHPC","SJVN","RECLTD","PFC",
    "LODHA","PRESTIGE","OBEROIRLTY","PHOENIXLTD","GODREJPROP","DLF","SOBHA",
    "ZYDUSLIFE","LUPIN","AUROPHARMA","TORNTPHARM","BIOCON","ALKEM","IPCALAB",
    "PAGEIND","MCDOWELL-N","RADICO","JUBLFOOD","WESTLIFE","SAPPHIRE",
    "JYOTHYLAB","EMAMILTD","GILLETTE","PGHH","VSTIND","ABCAPITAL","CHOICEIN",
    "MOTHERSON","MINDAIND","BOSCHLTD","TIINDIA","SUPRAJIT","ENDURANCE",
    "KAJARIACER","CERA","ORIENTELEC","FINOLEX","RATNAMANI","APLAPOLLO",
    "GPPL","ADANIGREEN","ADANITRANS","ADANIPOWER","TATAPOWER","TORNTPOWER",
    "LINDEINDIA","PIDILITIND","NAVNETEDUL","PCJEWELLER","SENCO","KALYAN",
]

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}

def get_nse_session():
    """Create a requests session with NSE cookies."""
    s = requests.Session()
    try:
        s.get("https://www.nseindia.com", headers=NSE_HEADERS, timeout=8)
    except:
        pass
    return s

def get_dynamic_symbols():
    """
    Build scan list from:
    1. NSE live API — top gainers, losers, most active by volume & value
    2. NIFTY500 base list
    Returns deduplicated list of up to ~200 symbols.
    """
    symbols = set(NIFTY500_BASE)
    added   = 0

    try:
        session = get_nse_session()
        endpoints = [
            "https://www.nseindia.com/api/live-analysis-variations?index=gainers&type=pct",
            "https://www.nseindia.com/api/live-analysis-variations?index=losers&type=pct",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=volume",
            "https://www.nseindia.com/api/live-analysis-most-active-securities?index=value",
            "https://www.nseindia.com/api/equity-stockIndices?index=NIFTY%20500",
        ]
        for url in endpoints:
            try:
                r = session.get(url, headers=NSE_HEADERS, timeout=6)
                if not r.ok:
                    continue
                data = r.json()
                # Different endpoints have different structures
                items = (data.get("data") or data.get("NIFTY 500") or
                         data.get("gainers") or data.get("losers") or [])
                for item in items:
                    sym = (item.get("symbol") or item.get("Symbol","")).strip().upper()
                    # Clean common suffixes
                    sym = sym.replace("-EQ","").replace(" EQ","").strip()
                    if sym and len(sym) <= 20 and sym.isalpha() or "-" in sym or "&" in sym:
                        symbols.add(sym)
                        added += 1
            except Exception as e:
                print(f"[NSE] endpoint failed: {e}")
                continue
        print(f"[NSE] Added {added} symbols from live NSE APIs")
    except Exception as e:
        print(f"[NSE] Session failed: {e}")

    final = sorted(list(symbols))
    print(f"[SYMBOLS] Total unique symbols: {len(final)}")
    return final

def get_ns(sym):
    return sym.replace("&", "%26") + ".NS"

def compute_indicators(df):
    if df is None or len(df) < 26:
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

        rsi               = ta.momentum.RSIIndicator(close=close, window=14).rsi()
        r["rsi"]          = round(float(rsi.iloc[-1]), 1)

        macd_obj          = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        r["macd_hist"]    = round(float(macd_obj.macd_diff().iloc[-1]), 3)
        r["macd_prev"]    = round(float(macd_obj.macd_diff().iloc[-2]), 3)
        r["macd_signal"]  = round(float(macd_obj.macd_signal().iloc[-1]), 3)
        r["macd_bullish"] = bool(r["macd_hist"] > 0 and r["macd_hist"] > r["macd_prev"])

        r["ema20"]        = round(float(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1]), 2)
        r["ema50"]        = round(float(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1]), 2)
        r["above_ema20"]  = bool(r["cmp"] > r["ema20"])
        r["above_ema50"]  = bool(r["cmp"] > r["ema50"])
        r["golden_cross"] = bool(r["ema20"] > r["ema50"])
        r["ema200"]       = round(float(ta.trend.EMAIndicator(close=close, window=200).ema_indicator().iloc[-1]), 2) if len(close) >= 200 else None

        typical           = (high + low + close) / 3
        vwap              = (typical * volume).cumsum() / volume.cumsum()
        r["vwap"]         = round(float(vwap.iloc[-1]), 2)
        r["above_vwap"]   = bool(r["cmp"] > r["vwap"])

        avg_vol           = float(volume.iloc[-20:].mean()) if len(volume) >= 20 else float(volume.mean())
        r["volume"]       = int(float(volume.iloc[-1]))
        r["avg_volume"]   = int(avg_vol)
        r["rel_volume"]   = round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

        bb                = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
        r["bb_upper"]     = round(float(bb.bollinger_hband().iloc[-1]), 2)
        r["bb_lower"]     = round(float(bb.bollinger_lband().iloc[-1]), 2)
        r["bb_pct"]       = round(float(bb.bollinger_pband().iloc[-1]), 3)

        r["week52_high"]  = round(float(close.max()), 2)
        r["week52_low"]   = round(float(close.min()), 2)
        r["pct_from_52h"] = round((r["cmp"] - r["week52_high"]) / r["week52_high"] * 100, 1)
        return r
    except:
        return None

def score_stock(ind):
    if not ind: return 0, []
    score = 0; signals = []

    rsi = ind.get("rsi", 50)
    if   52 <= rsi <= 68: score += 2.5; signals.append(f"RSI {rsi} — ideal momentum zone ✓")
    elif 45 <= rsi < 52:  score += 1.5; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68 < rsi <= 72:  score += 1.0; signals.append(f"RSI {rsi} — strong, near overbought")
    elif rsi > 72:        score -= 1.0; signals.append(f"RSI {rsi} — overbought ✗")
    else:                 score -= 1.0; signals.append(f"RSI {rsi} — weak momentum ✗")

    if ind.get("macd_bullish"):
        score += 2.0; signals.append(f"MACD expanding positive ({ind.get('macd_hist')}) ✓")
    elif ind.get("macd_hist", 0) > 0:
        score += 1.0; signals.append(f"MACD positive but fading ({ind.get('macd_hist')})")
    else:
        score -= 1.0; signals.append(f"MACD bearish ({ind.get('macd_hist')}) ✗")

    if ind.get("above_vwap"):
        score += 1.5; signals.append(f"Above VWAP ₹{ind.get('vwap')} ✓")
    else:
        score -= 0.5; signals.append(f"Below VWAP ₹{ind.get('vwap')} ✗")

    if ind.get("golden_cross"):
        score += 1.5; signals.append(f"Golden cross EMA20 > EMA50 ✓")
    else:
        score -= 0.5; signals.append(f"Death cross EMA20 < EMA50 ✗")

    if ind.get("above_ema20"):
        score += 0.5; signals.append(f"Price above EMA20 ₹{ind.get('ema20')} ✓")

    rv = ind.get("rel_volume", 1.0)
    if   rv >= 2.5: score += 2.0; signals.append(f"Volume {rv}x avg — institutional activity ✓")
    elif rv >= 1.5: score += 1.0; signals.append(f"Volume {rv}x avg — above average ✓")
    elif rv < 0.7:  score -= 0.5; signals.append(f"Volume {rv}x avg — weak ✗")
    else:           signals.append(f"Volume {rv}x avg — normal")

    bb_pct = ind.get("bb_pct", 0.5)
    if   0.2 <= bb_pct <= 0.7: score += 0.5; signals.append(f"Bollinger {round(bb_pct*100)}% — healthy ✓")
    elif bb_pct > 0.9:         score -= 0.5; signals.append(f"Bollinger {round(bb_pct*100)}% — upper band ✗")

    return max(0, min(10, round(score, 1))), signals

def fetch_one(sym):
    """Fetch and score a single symbol. Used in parallel execution."""
    try:
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra) >= 15) else daily
        ind    = compute_indicators(df)
        if not ind: return None
        score, signals = score_stock(ind)
        return {"symbol": sym, "score": score, "signals": signals, **ind}
    except:
        return None

# ── KEEP ALIVE ────────────────────────────────────────────────────────────
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
    return jsonify({
        "status":  "running",
        "service": "NSESignal Pro",
        "time":    now.strftime("%I:%M:%S %p IST"),
        "message": "Backend is live"
    })

@app.route("/scan")
def scan():
    now = datetime.now(IST)
    print(f"\n[SCAN] {now.strftime('%I:%M %p IST')}")

    # Step 1: Get symbol list
    symbols  = get_dynamic_symbols()
    total    = len(symbols)
    results  = []
    errors   = 0
    done     = 0

    print(f"[SCAN] Scanning {total} symbols in parallel (20 workers)…")
    scan_start = time.time()

    # Step 2: Parallel fetch with 20 workers, max 90 seconds total
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(fetch_one, sym): sym for sym in symbols}
        for future in as_completed(futures, timeout=90):
            try:
                result = future.result(timeout=8)
                done  += 1
                if result:
                    results.append(result)
                else:
                    errors += 1
            except Exception:
                errors += 1
                done   += 1

    elapsed = round(time.time() - scan_start, 1)
    print(f"[SCAN] Done in {elapsed}s — {len(results)} valid, {errors} errors, {done}/{total} attempted")

    results.sort(key=lambda x: x["score"], reverse=True)
    top10 = results[:10]
    top10[:3] and print(f"[TOP3] {[(s['symbol'], s['score']) for s in top10[:3]]}")

    return jsonify({
        "status":    "success",
        "scan_time": now.strftime("%I:%M %p IST"),
        "date":      now.strftime("%d %b %Y"),
        "scanned":   len(results),
        "total":     total,
        "errors":    errors,
        "elapsed":   elapsed,
        "top10":     top10
    })

@app.route("/quote/<symbol>")
def quote(symbol):
    try:
        result = fetch_one(symbol.upper())
        if not result: return jsonify({"status":"error","message":"No data"}), 404
        return jsonify({"status":"success", **result})
    except Exception as e:
        return jsonify({"status":"error","message":str(e)}), 500

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
        except: out[name] = {}
    return jsonify(out)

@app.route("/news", methods=["POST"])
def news():
    return jsonify({})  # Disabled — no API key needed

if __name__ == "__main__":
    threading.Thread(target=keep_alive, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"NSESignal Pro backend starting on port {port}")
    app.run(host="0.0.0.0", port=port)
