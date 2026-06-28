"""
NSESignal Pro — Cloud Backend for Render.com
=============================================
Deploy this on Render.com free tier.
No local setup needed — runs 24/7 in the cloud.
"""

from flask import Flask, jsonify
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

app = Flask(__name__)
CORS(app)  # Allow all origins — needed for Claude artifact

IST = pytz.timezone("Asia/Kolkata")

# ── NSE WATCHLIST ─────────────────────────────────────────────────────────
WATCHLIST = [
    # Large Cap
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN",
    "BHARTIARTL","KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN",
    "BAJFINANCE","WIPRO","TECHM","ULTRACEMCO","ONGC","NTPC",
    # Mid Cap momentum
    "ZOMATO","NYKAA","DMART","ADANIPORTS","ADANIENT","TATAMOTORS",
    "TATASTEEL","JSWSTEEL","HINDALCO","BEL","HAL","BHEL","DIXON","VOLTAS",
    "MUTHOOTFIN","CHOLAFIN","BAJAJFINSV","HCLTECH","SUNPHARMA",
    # Small/Mid momentum
    "IRFC","IRCTC","CDSL","MCX","CAMS","ANGELONE","POLICYBZR",
    "NAUKRI","INDIGO","TATACOMM","PERSISTENT","COFORGE","LICI","COALINDIA"
]

def get_ns(sym):
    return sym + ".NS"

def compute_indicators(df):
    if df is None or len(df) < 26:
        return None
    try:
        close  = df["Close"].squeeze()
        high   = df["High"].squeeze()
        low    = df["Low"].squeeze()
        volume = df["Volume"].squeeze()

        r = {}
        r["cmp"]        = round(float(close.iloc[-1]), 2)
        r["open"]       = round(float(df["Open"].squeeze().iloc[-1]), 2)
        r["high"]       = round(float(high.iloc[-1]), 2)
        r["low"]        = round(float(low.iloc[-1]), 2)
        r["prev_close"] = round(float(close.iloc[-2]), 2) if len(close) > 1 else r["cmp"]
        r["change_pct"] = round((r["cmp"] - r["prev_close"]) / r["prev_close"] * 100, 2)

        # RSI
        rsi = ta.momentum.RSIIndicator(close=close, window=14).rsi()
        r["rsi"] = round(float(rsi.iloc[-1]), 1)

        # MACD
        macd_obj = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        r["macd_hist"]    = round(float(macd_obj.macd_diff().iloc[-1]), 3)
        r["macd_prev"]    = round(float(macd_obj.macd_diff().iloc[-2]), 3)
        r["macd_signal"]  = round(float(macd_obj.macd_signal().iloc[-1]), 3)
        r["macd_bullish"] = bool(r["macd_hist"] > 0 and r["macd_hist"] > r["macd_prev"])

        # EMA
        r["ema20"]       = round(float(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1]), 2)
        r["ema50"]       = round(float(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1]), 2)
        r["above_ema20"] = bool(r["cmp"] > r["ema20"])
        r["above_ema50"] = bool(r["cmp"] > r["ema50"])
        r["golden_cross"]= bool(r["ema20"] > r["ema50"])

        # EMA200 if enough data
        if len(close) >= 200:
            r["ema200"] = round(float(ta.trend.EMAIndicator(close=close, window=200).ema_indicator().iloc[-1]), 2)
        else:
            r["ema200"] = None

        # VWAP
        typical = (high + low + close) / 3
        vwap    = (typical * volume).cumsum() / volume.cumsum()
        r["vwap"]       = round(float(vwap.iloc[-1]), 2)
        r["above_vwap"] = bool(r["cmp"] > r["vwap"])

        # Volume
        avg_vol      = float(volume.iloc[-20:].mean()) if len(volume) >= 20 else float(volume.mean())
        r["volume"]    = int(float(volume.iloc[-1]))
        r["avg_volume"]= int(avg_vol)
        r["rel_volume"]= round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

        # Bollinger Bands
        bb = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
        r["bb_upper"] = round(float(bb.bollinger_hband().iloc[-1]), 2)
        r["bb_lower"] = round(float(bb.bollinger_lband().iloc[-1]), 2)
        r["bb_mid"]   = round(float(bb.bollinger_mavg().iloc[-1]), 2)
        r["bb_pct"]   = round(float(bb.bollinger_pband().iloc[-1]), 3)

        # 52-week
        r["week52_high"]    = round(float(close.max()), 2)
        r["week52_low"]     = round(float(close.min()), 2)
        r["pct_from_52h"]   = round((r["cmp"] - r["week52_high"]) / r["week52_high"] * 100, 1)

        return r
    except Exception as e:
        print(f"    indicator error: {e}")
        return None

def score_stock(ind):
    if not ind:
        return 0, []
    score   = 0
    signals = []

    # RSI
    rsi = ind.get("rsi", 50)
    if 52 <= rsi <= 68:
        score += 2.5; signals.append(f"RSI {rsi} — ideal momentum zone (52–68) ✓")
    elif 45 <= rsi < 52:
        score += 1.5; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68 < rsi <= 72:
        score += 1.0; signals.append(f"RSI {rsi} — strong but approaching overbought")
    elif rsi > 72:
        score -= 1.0; signals.append(f"RSI {rsi} — overbought, pullback risk ✗")
    else:
        score -= 1.0; signals.append(f"RSI {rsi} — weak momentum ✗")

    # MACD
    if ind.get("macd_bullish"):
        score += 2.0; signals.append(f"MACD histogram positive & expanding ({ind.get('macd_hist')}) ✓")
    elif ind.get("macd_hist", 0) > 0:
        score += 1.0; signals.append(f"MACD positive but fading ({ind.get('macd_hist')})")
    else:
        score -= 1.0; signals.append(f"MACD bearish ({ind.get('macd_hist')}) ✗")

    # VWAP
    if ind.get("above_vwap"):
        score += 1.5; signals.append(f"Price ₹{ind['cmp']} above VWAP ₹{ind.get('vwap')} ✓")
    else:
        score -= 0.5; signals.append(f"Price below VWAP ₹{ind.get('vwap')} ✗")

    # EMA structure
    if ind.get("golden_cross"):
        score += 1.5; signals.append(f"Golden cross: EMA20 ₹{ind.get('ema20')} > EMA50 ₹{ind.get('ema50')} ✓")
    else:
        score -= 0.5; signals.append(f"Death cross: EMA20 < EMA50 ✗")

    if ind.get("above_ema20"):
        score += 0.5; signals.append(f"Price above EMA20 ₹{ind.get('ema20')} ✓")

    # Volume
    rv = ind.get("rel_volume", 1.0)
    if rv >= 2.5:
        score += 2.0; signals.append(f"Volume {rv}x average — strong institutional activity ✓")
    elif rv >= 1.5:
        score += 1.0; signals.append(f"Volume {rv}x average — above average ✓")
    elif rv < 0.7:
        score -= 0.5; signals.append(f"Volume {rv}x average — weak conviction ✗")
    else:
        signals.append(f"Volume {rv}x average — normal")

    # Bollinger position
    bb_pct = ind.get("bb_pct", 0.5)
    if 0.2 <= bb_pct <= 0.7:
        score += 0.5; signals.append(f"Bollinger position {round(bb_pct*100)}% — healthy range ✓")
    elif bb_pct > 0.9:
        score -= 0.5; signals.append(f"Bollinger position {round(bb_pct*100)}% — near upper band ✗")

    score = max(0, min(10, round(score, 1)))
    return score, signals

# ── KEEP-ALIVE (prevents Render free tier from sleeping) ─────────────────
def keep_alive():
    """Ping self every 14 minutes so Render doesn't sleep the instance."""
    url = os.environ.get("RENDER_EXTERNAL_URL", "")
    if not url:
        return
    while True:
        time.sleep(14 * 60)
        try:
            requests.get(f"{url}/health", timeout=10)
            print("[KEEP-ALIVE] pinged self")
        except:
            pass

# ── ROUTES ────────────────────────────────────────────────────────────────

@app.route("/")
@app.route("/health")
def health():
    now = datetime.now(IST)
    return jsonify({
        "status":  "running",
        "service": "NSESignal Pro Backend",
        "time":    now.strftime("%I:%M:%S %p IST"),
        "stocks":  len(WATCHLIST),
        "message": "Backend is live — call /scan to run the full technical scan"
    })

@app.route("/scan")
def scan():
    print(f"\n[SCAN] Starting — {len(WATCHLIST)} stocks")
    now     = datetime.now(IST)
    results = []
    errors  = []

    for sym in WATCHLIST:
        try:
            ticker = yf.Ticker(get_ns(sym))
            # Try 5-min intraday first (market hours), fall back to daily
            intra = ticker.history(period="1d",  interval="5m",  auto_adjust=True)
            daily = ticker.history(period="60d", interval="1d",  auto_adjust=True)
            df    = intra if (intra is not None and len(intra) >= 15) else daily
            ind   = compute_indicators(df)
            if not ind:
                errors.append(sym); continue
            score, signals = score_stock(ind)
            results.append({ "symbol": sym, "score": score, "signals": signals, **ind })
            print(f"  ✓ {sym}: ₹{ind['cmp']} RSI:{ind.get('rsi')} Score:{score}")
        except Exception as e:
            errors.append(sym)
            print(f"  ✗ {sym}: {e}")

    results.sort(key=lambda x: x["score"], reverse=True)

    return jsonify({
        "status":    "success",
        "scan_time": now.strftime("%I:%M %p IST"),
        "date":      now.strftime("%d %b %Y"),
        "scanned":   len(results),
        "errors":    len(errors),
        "top10":     results[:10]
    })

@app.route("/quote/<symbol>")
def quote(symbol):
    try:
        sym    = symbol.upper()
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra) >= 15) else daily
        ind    = compute_indicators(df)
        if not ind:
            return jsonify({"status":"error","message":"No data"}), 404
        score, signals = score_stock(ind)
        return jsonify({ "status":"success", "symbol":sym, "score":score, "signals":signals, **ind })
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
        except Exception as e:
            out[name] = {"error": str(e)}
    return jsonify(out)

if __name__ == "__main__":
    # Start keep-alive thread for Render free tier
    t = threading.Thread(target=keep_alive, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 5000))
    print(f"NSESignal Pro backend starting on port {port}")
    app.run(host="0.0.0.0", port=port)
