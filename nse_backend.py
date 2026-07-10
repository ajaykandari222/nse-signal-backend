"""
NSESignal Pro — Stable Backend v6
Rebuilt lean for Render free tier (512MB RAM).
Key fixes:
- Lazy imports (yfinance/ta loaded only when needed, not at startup)
- Single background thread (no competing scanners)
- All routes return JSON even on crash
- Gunicorn: 1 worker, 4 threads, 120s timeout
"""

from flask import Flask, jsonify, render_template, request
from flask_cors import CORS
import os, json, math, time, threading, hashlib, secrets
from functools import wraps
from datetime import datetime
import pytz
import requests

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

# ── SANITISE NaN ──────────────────────────────────────────────────────────
def sanitise(obj):
    if isinstance(obj, dict):  return {k: sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):  return [sanitise(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)): return None
    return obj

def safe_f(v, default=0.0):
    try: return float(v) if v is not None else default
    except: return default

# ── WATCHLIST ─────────────────────────────────────────────────────────────
WATCHLIST = [
    "RELIANCE","TCS","HDFCBANK","INFY","ICICIBANK","HINDUNILVR","SBIN","BHARTIARTL",
    "KOTAKBANK","LT","AXISBANK","ASIANPAINT","MARUTI","TITAN","BAJFINANCE","WIPRO",
    "TECHM","ULTRACEMCO","ONGC","NTPC","POWERGRID","SUNPHARMA","TATAMOTORS","HCLTECH",
    "NESTLEIND","TATASTEEL","JSWSTEEL","HINDALCO","COALINDIA","BPCL","IOC","GAIL",
    "DRREDDY","CIPLA","DIVISLAB","APOLLOHOSP","BAJAJFINSV","EICHERMOT","HEROMOTOCO",
    "ADANIENT","ADANIPORTS","LTIM","INDUSINDBK","ITC","VEDL","GRASIM","TATACONSUM",
    "BRITANNIA","BAJAJ-AUTO","SIEMENS","ABB","HAVELLS","PIDILITIND","BERGEPAINT",
    "MUTHOOTFIN","CHOLAFIN","SBILIFE","HDFCLIFE","ICICIGI","MARICO","COLPAL","DABUR",
    "GODREJCP","SAIL","NMDC","AMBUJACEM","SHREECEM","IRCTC","IRFC","RVNL","BEL","HAL",
    "BHEL","TRENT","IDFCFIRSTB","BANDHANBNK","FEDERALBNK","TORNTPHARM","LUPIN",
    "AUROPHARMA","ZYDUSLIFE","ALKEM","BIOCON","ZOMATO","NYKAA","DMART","DIXON",
    "VOLTAS","POLYCAB","KPITTECH","MPHASIS","LTTS","PERSISTENT","COFORGE","BALKRISIND",
    "APOLLOTYRE","MRF","INDIGO","GMRINFRA","CUMMINSIND","ASTRAL","CONCOR","TATACOMM",
    "BEML","ANGELONE","CDSL","MCX","CAMS","POLICYBZR","PAYTM","NAUKRI","INFOEDGE",
    "LICI","MAXHEALTH","FORTIS","DEEPAKNTR","NAVINFLUOR","TATACHEM","IIFL","IREDA",
    "NHPC","SJVN","RECLTD","PFC","LODHA","DLF","GODREJPROP","RADICO","JUBLFOOD",
    "MOTHERSON","BOSCHLTD","TIINDIA","ENDURANCE","ADANIGREEN","ADANIPOWER","TATAPOWER",
    "TORNTPOWER","MANAPPURAM","MOTILALOSW","CANFINHOME","AAVAS","SENCO","KALYAN",
    "APLAPOLLO","RATNAMANI","RAILTEL","PAGEIND","ABCAPITAL","RBLBANK","SUNDARMFIN",
    "KAYNES","SYRMA","AMBER","TATAELXSI","GRSE","COCHINSHIP","MAZAGON","GARDENREACH",
    "COROMANDEL","PIIND","CHAMBLFERT","GNFC","RALLIS","DHANUKA","KRBL","AVANTIFEED",
    "JKCEMENT","DALMIACEM","RAMCOCEM","BIRLASOFT","MASTEK","ZENSAR","INTELLECT",
    "TANLA","ROUTE","SONATASOFT","CYIENT","CLEANSCIENCE","FINEORG","ALKYLAMINE",
    "VINATIORG","AARTI","DEEPAKFERT","CAPACITE","KNR","HGINFRA","PNCINFRA","ASHOKA",
    "METROPOLIS","THYROCARE","LALGPATH","GRANULES","WOCKHARDT","GLENMARK","JBCHEPHARM",
]
WATCHLIST = list(dict.fromkeys(WATCHLIST))  # deduplicate

# ── SECTOR MAP ────────────────────────────────────────────────────────────
SECTOR_STOCKS = {
    "Technology & AI":         ["TCS","INFY","HCLTECH","WIPRO","TECHM","LTIM","PERSISTENT","COFORGE","KPITTECH","MPHASIS"],
    "Semiconductors":          ["DIXON","KAYNES","SYRMA","AMBER","TATAELXSI","BEL","HAL","BEML"],
    "Defence & Aerospace":     ["HAL","BEL","BHEL","BEML","GRSE","COCHINSHIP","MAZAGON","GARDENREACH"],
    "Banking & Finance":       ["HDFCBANK","ICICIBANK","SBIN","KOTAKBANK","AXISBANK","INDUSINDBK","BANDHANBNK","IDFCFIRSTB"],
    "Pharma & Healthcare":     ["SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","LUPIN","AUROPHARMA","APOLLOHOSP","MAXHEALTH"],
    "EV & Auto":               ["TATAMOTORS","MARUTI","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT","TVSMOTOR","MOTHERSON","BOSCHLTD"],
    "Renewable Energy":        ["ADANIGREEN","TATAPOWER","ADANIPOWER","TORNTPOWER","NHPC","SJVN","IREDA","NTPC"],
    "Infrastructure & Capex":  ["LT","RVNL","IRFC","IRCTC","RAILTEL","GMRINFRA","KNR","HGINFRA"],
    "FMCG & Consumer":         ["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO","DABUR","COLPAL","GODREJCP"],
    "Agriculture & Fertiliser":["COROMANDEL","PIIND","CHAMBLFERT","GNFC","RALLIS","DHANUKA","DEEPAKFERT","KRBL"],
    "Cement & Construction":   ["ULTRACEMCO","AMBUJACEM","SHREECEM","JKCEMENT","DALMIACEM","RAMCOCEM","LT","SIEMENS"],
    "Metals & Mining":         ["TATASTEEL","JSWSTEEL","HINDALCO","VEDL","SAIL","NMDC","COALINDIA","APLAPOLLO"],
    "Real Estate":             ["DLF","LODHA","GODREJPROP","OBEROIRLTY","PHOENIXLTD","PRESTIGE","SOBHA"],
    "Oil & Gas":               ["RELIANCE","ONGC","BPCL","IOC","GAIL","HINDPETRO"],
    "Telecom":                 ["BHARTIARTL","TATACOMM","ROUTE","TANLA"],
}

SEASONALITY = {
    "Technology & AI":         {1:2,2:1,3:1,4:2,5:2,6:1,7:3,8:2,9:2,10:2,11:1,12:1},
    "Semiconductors":          {1:2,2:2,3:1,4:2,5:2,6:1,7:2,8:3,9:2,10:2,11:2,12:1},
    "Defence & Aerospace":     {1:2,2:2,3:3,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "Banking & Finance":       {1:2,2:2,3:2,4:3,5:2,6:1,7:2,8:2,9:2,10:3,11:2,12:2},
    "Pharma & Healthcare":     {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "EV & Auto":               {1:1,2:2,3:2,4:1,5:1,6:1,7:2,8:3,9:3,10:3,11:2,12:2},
    "Renewable Energy":        {1:2,2:2,3:2,4:2,5:2,6:3,7:3,8:3,9:2,10:2,11:2,12:1},
    "Infrastructure & Capex":  {1:2,2:2,3:3,4:2,5:2,6:1,7:2,8:2,9:3,10:3,11:2,12:2},
    "FMCG & Consumer":         {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:3,11:3,12:3},
    "Agriculture & Fertiliser":{1:2,2:2,3:3,4:3,5:3,6:3,7:3,8:2,9:2,10:2,11:1,12:1},
    "Cement & Construction":   {1:2,2:2,3:2,4:1,5:1,6:1,7:2,8:2,9:3,10:3,11:3,12:2},
    "Metals & Mining":         {1:2,2:2,3:2,4:2,5:2,6:1,7:2,8:2,9:2,10:2,11:2,12:2},
    "Real Estate":             {1:2,2:2,3:2,4:2,5:1,6:1,7:2,8:2,9:2,10:3,11:3,12:2},
    "Oil & Gas":               {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
    "Telecom":                 {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
}

SEASONAL_REASON = {
    "Technology & AI":         {7:"Q1 earnings — IT companies report strong results",10:"Q2 results + global tech spending"},
    "Semiconductors":          {8:"Back-to-school + festive pre-stocking drives electronics demand"},
    "Defence & Aerospace":     {3:"Budget session — defence allocations and contracts finalised"},
    "Banking & Finance":       {4:"Q4 results + credit growth peak",10:"Q2 results + festive credit surge"},
    "EV & Auto":               {9:"Navratri/Dussehra — peak auto sales",8:"Pre-festive inventory build"},
    "Renewable Energy":        {6:"Summer peak power demand",7:"Strong solar irradiance + policy push"},
    "Infrastructure & Capex":  {9:"Post-monsoon construction season",10:"Govt capex acceleration Q3"},
    "FMCG & Consumer":         {10:"Diwali — peak FMCG season",11:"Post-festive restocking",12:"Year-end consumption"},
    "Agriculture & Fertiliser":{5:"Kharif sowing begins",6:"Monsoon onset — peak fertiliser demand",4:"Rabi harvest + Kharif prep"},
    "Cement & Construction":   {9:"Post-monsoon construction boom",10:"Peak cement demand quarter"},
    "Real Estate":             {10:"Diwali launches — peak buying season",11:"Year-end property purchases"},
}

# ── CACHE ─────────────────────────────────────────────────────────────────
cache = {"result": None, "running": False, "last_run": None, "scan_count": 0, "regime": None}
cache_lock = threading.Lock()

# ── LAZY IMPORTS ──────────────────────────────────────────────────────────
_yf = None
_ta = None

def get_yf():
    global _yf
    if _yf is None:
        import yfinance as yf
        _yf = yf
    return _yf

def get_ta():
    global _ta
    if _ta is None:
        import ta as ta_lib
        _ta = ta_lib
    return _ta

def get_ns(sym):
    return sym.replace("&", "%26") + ".NS"

# ── INDICATORS ────────────────────────────────────────────────────────────
def compute_indicators(df):
    if df is None or len(df) < 26: return None
    try:
        ta = get_ta()
        import pandas as pd
        import numpy as np

        close  = df["Close"].squeeze().astype(float)
        high   = df["High"].squeeze().astype(float)
        low    = df["Low"].squeeze().astype(float)
        volume = df["Volume"].squeeze().astype(float)
        opn    = df["Open"].squeeze().astype(float)
        mask   = close.notna() & volume.notna() & (close > 0) & (volume > 0)
        close  = close[mask]; high = high[mask]; low = low[mask]
        volume = volume[mask]; opn = opn[mask]
        if len(close) < 26: return None

        r = {}
        r["cmp"]         = round(float(close.iloc[-1]), 2)
        r["open"]        = round(float(opn.iloc[-1]), 2)
        r["high"]        = round(float(high.iloc[-1]), 2)
        r["low"]         = round(float(low.iloc[-1]), 2)
        r["prev_close"]  = round(float(close.iloc[-2]), 2)
        r["change_pct"]  = round((r["cmp"] - r["prev_close"]) / r["prev_close"] * 100, 2)
        if r["cmp"] <= 0: return None

        # RSI
        rsi = ta.momentum.RSIIndicator(close=close, window=14).rsi()
        r["rsi"] = round(safe_f(rsi.iloc[-1], 50), 1)

        # StochRSI
        try:
            sk = ta.momentum.StochRSIIndicator(close=close, window=14, smooth1=3, smooth2=3).stochrsi_k()
            sd = ta.momentum.StochRSIIndicator(close=close, window=14, smooth1=3, smooth2=3).stochrsi_d()
            r["stoch_k"] = round(safe_f(sk.iloc[-1]) * 100, 1)
            r["stoch_d"] = round(safe_f(sd.iloc[-1]) * 100, 1)
            r["stoch_bull"]   = bool(r["stoch_k"] > r["stoch_d"] and r["stoch_k"] < 80)
            r["stoch_bounce"] = bool(r["stoch_k"] > r["stoch_d"] and r["stoch_k"] < 30)
        except:
            r["stoch_k"] = r["stoch_d"] = None
            r["stoch_bull"] = r["stoch_bounce"] = False

        # MACD
        m = ta.trend.MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
        r["macd_hist"]    = round(safe_f(m.macd_diff().iloc[-1]), 3)
        r["macd_prev"]    = round(safe_f(m.macd_diff().iloc[-2]), 3)
        r["macd_bullish"] = bool(r["macd_hist"] > 0 and r["macd_hist"] > r["macd_prev"])

        # EMA
        r["ema20"]        = round(safe_f(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1]), 2)
        r["ema50"]        = round(safe_f(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1]), 2)
        r["above_ema20"]  = bool(r["cmp"] > r["ema20"])
        r["golden_cross"] = bool(r["ema20"] > r["ema50"])

        # VWAP
        typical       = (high + low + close) / 3
        vwap          = (typical * volume).cumsum() / volume.cumsum()
        r["vwap"]     = round(safe_f(vwap.iloc[-1]), 2)
        r["above_vwap"]= bool(r["cmp"] > r["vwap"])

        # Volume
        avg_vol        = float(volume.iloc[-20:].mean()) if len(volume)>=20 else float(volume.mean())
        r["volume"]    = int(float(volume.iloc[-1]))
        r["avg_volume"]= int(avg_vol)
        r["rel_volume"]= round(float(volume.iloc[-1]) / avg_vol, 2) if avg_vol > 0 else 1.0

        # Bollinger
        bb            = ta.volatility.BollingerBands(close=close, window=20, window_dev=2)
        r["bb_pct"]   = round(safe_f(bb.bollinger_pband().iloc[-1], 0.5), 3)

        # Supertrend
        try:
            hl2   = (high + low) / 2
            atr   = ta.volatility.AverageTrueRange(high=high, low=low, close=close, window=10).average_true_range()
            upper = (hl2 + 3.0 * atr).fillna(method="ffill")
            lower = (hl2 - 3.0 * atr).fillna(method="ffill")
            import pandas as pd
            st    = pd.Series(float("nan"), index=close.index)
            st.iloc[10] = float(upper.iloc[10])
            bullish = True
            for i in range(11, len(close)):
                prev = st.iloc[i-1]
                if pd.isna(prev): st.iloc[i] = float(upper.iloc[i]); continue
                if float(close.iloc[i]) > prev:
                    st.iloc[i] = float(lower.iloc[i]); bullish = True
                else:
                    st.iloc[i] = float(upper.iloc[i]); bullish = False
            r["supertrend_bull"] = bullish
            r["supertrend_val"]  = round(safe_f(st.iloc[-1]), 2)
        except:
            r["supertrend_bull"] = False; r["supertrend_val"] = 0

        # ADX
        try:
            adx_ind    = ta.trend.ADXIndicator(high=high, low=low, close=close, window=14)
            r["adx"]   = round(safe_f(adx_ind.adx().iloc[-1]), 1)
            r["adx_pos"]= round(safe_f(adx_ind.adx_pos().iloc[-1]), 1)
            r["adx_neg"]= round(safe_f(adx_ind.adx_neg().iloc[-1]), 1)
            r["adx_strong"]  = bool(r["adx"] >= 25)
            r["adx_bullish"] = bool(r["adx_pos"] > r["adx_neg"])
        except:
            r["adx"] = 0; r["adx_strong"] = False; r["adx_bullish"] = False

        # 52W
        r["week52_high"] = round(float(close.max()), 2)
        r["week52_low"]  = round(float(close.min()), 2)
        r["pct_from_52h"]= round((r["cmp"] - r["week52_high"]) / r["week52_high"] * 100, 1)
        r["near_52w_high"]  = bool(r["pct_from_52h"] >= -3.0)
        r["breakout_setup"] = bool(r["pct_from_52h"] >= -3.0 and r["rel_volume"] >= 1.5)

        # ATR target
        try:
            atr_v = safe_f(ta.volatility.AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range().iloc[-1])
            r["atr"] = round(atr_v, 2)
            bull_votes = sum([r["supertrend_bull"], r["macd_bullish"], r["above_vwap"], r["golden_cross"], 45<=r["rsi"]<=72])
            r["direction"] = "BULLISH" if bull_votes >= 3 else "BEARISH" if bull_votes <= 1 else "NEUTRAL"
            mult = 2.5 if r["adx"] >= 30 else 1.8 if r["adx"] >= 20 else 1.2
            move = max(round(atr_v * mult, 2), round(r["cmp"] * 0.005, 2))
            lean = bull_votes >= 2
            if r["direction"] == "BULLISH":
                r["target_price"] = round(r["cmp"] + move, 2)
                r["stop_loss"]    = round(r["cmp"] - atr_v, 2)
                r["target_pct"]   = round(move / r["cmp"] * 100, 2)
            elif r["direction"] == "BEARISH":
                r["target_price"] = round(r["cmp"] - move, 2)
                r["stop_loss"]    = round(r["cmp"] + atr_v, 2)
                r["target_pct"]   = round(-move / r["cmp"] * 100, 2)
            else:
                half = round(move * 0.5, 2)
                r["target_price"] = round(r["cmp"] + (half if lean else -half), 2)
                r["stop_loss"]    = round(r["cmp"] - atr_v * 0.8, 2)
                r["target_pct"]   = round((half if lean else -half) / r["cmp"] * 100, 2)
            risk   = abs(r["cmp"] - r["stop_loss"])
            reward = abs(r["target_price"] - r["cmp"])
            r["risk_reward"] = round(reward / risk, 2) if risk > 0 else 0
        except:
            r["atr"] = 0; r["direction"] = "NEUTRAL"
            r["target_price"] = r["cmp"]; r["stop_loss"] = r["cmp"]
            r["target_pct"] = 0; r["risk_reward"] = 0

        return r
    except Exception as e:
        print(f"  [IND ERR] {e}")
        return None

def score_stock(ind, regime=None):
    if not ind: return 0, []
    score = 0; signals = []
    rt = (regime or {}).get("trend", "NEUTRAL")
    if rt == "BULL":   score += 1.0; signals.append(f"NIFTY BULL regime — tailwind ✓")
    elif rt == "BEAR": score -= 2.0; signals.append(f"NIFTY BEAR regime — headwind ✗")
    else:              signals.append("NIFTY NEUTRAL regime")

    rsi = ind.get("rsi", 50)
    if   52<=rsi<=68: score+=2.0; signals.append(f"RSI {rsi} — ideal momentum zone ✓")
    elif 45<=rsi<52:  score+=1.0; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68<rsi<=72:  score+=0.5; signals.append(f"RSI {rsi} — strong, near overbought")
    elif rsi>72:      score-=1.5; signals.append(f"RSI {rsi} — overbought ✗")
    else:             score-=1.0; signals.append(f"RSI {rsi} — weak momentum ✗")

    sk = ind.get("stoch_k"); sd = ind.get("stoch_d")
    if sk is not None:
        if ind.get("stoch_bounce"): score+=2.0; signals.append(f"StochRSI {sk}/{sd} — oversold bounce ✓")
        elif ind.get("stoch_bull") and sk<60: score+=1.5; signals.append(f"StochRSI {sk}/{sd} — K>D momentum ✓")
        elif sk>80: score-=1.0; signals.append(f"StochRSI {sk} — overbought ✗")
        else: signals.append(f"StochRSI {sk}/{sd} — neutral")

    if ind.get("macd_bullish"): score+=2.0; signals.append(f"MACD expanding positive ({ind.get('macd_hist')}) ✓")
    elif ind.get("macd_hist",0)>0: score+=1.0; signals.append(f"MACD positive fading ({ind.get('macd_hist')})")
    else: score-=1.0; signals.append(f"MACD bearish ({ind.get('macd_hist',0)}) ✗")

    if ind.get("above_vwap"): score+=1.5; signals.append(f"Above VWAP ₹{ind.get('vwap')} ✓")
    else: score-=0.5; signals.append(f"Below VWAP ✗")

    if ind.get("golden_cross"): score+=1.5; signals.append(f"Golden cross EMA20>EMA50 ✓")
    else: score-=0.5; signals.append(f"Death cross EMA20<EMA50 ✗")
    if ind.get("above_ema20"): score+=0.5; signals.append(f"Price above EMA20 ✓")

    rv = ind.get("rel_volume",1.0)
    if rv>=2.5: score+=2.0; signals.append(f"Volume {rv}x avg — institutional ✓")
    elif rv>=1.5: score+=1.0; signals.append(f"Volume {rv}x avg — above avg ✓")
    elif rv<0.7: score-=0.5; signals.append(f"Volume {rv}x — weak ✗")
    else: signals.append(f"Volume {rv}x avg — normal")

    if ind.get("supertrend_bull"): score+=2.0; signals.append("Supertrend BULLISH ✓")
    else: score-=1.5; signals.append("Supertrend BEARISH ✗")

    adx = ind.get("adx",0)
    if adx>=30 and ind.get("adx_bullish"): score+=2.0; signals.append(f"ADX {adx} — very strong bullish ✓")
    elif adx>=25 and ind.get("adx_bullish"): score+=1.5; signals.append(f"ADX {adx} — strong DI+>DI- ✓")
    elif adx>=20: score+=0.5; signals.append(f"ADX {adx} — moderate")
    else: score-=0.5; signals.append(f"ADX {adx} — weak/ranging ✗")

    if ind.get("breakout_setup"): score+=2.0; signals.append("52W BREAKOUT SETUP with volume ✓")
    elif ind.get("near_52w_high"): score+=1.0; signals.append(f"Near 52W high — momentum ✓")

    bb = ind.get("bb_pct",0.5)
    if 0.2<=bb<=0.7: score+=0.5; signals.append(f"Bollinger {round(bb*100)}% — healthy ✓")
    elif bb>0.9: score-=0.5; signals.append("Bollinger near upper band ✗")

    d = ind.get("direction","NEUTRAL")
    if d=="BULLISH": signals.append(f"TARGET: ₹{ind.get('target_price')} ({ind.get('target_pct')}%) | SL: ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')} ✓")
    elif d=="BEARISH": signals.append(f"TARGET: ₹{ind.get('target_price')} ({ind.get('target_pct')}%) | SL: ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')} ✗")
    else: signals.append(f"TARGET: ₹{ind.get('target_price')} | SL: ₹{ind.get('stop_loss')}")

    return max(0, min(17, round(score, 1))), signals

def fetch_nifty_regime():
    try:
        yf = get_yf(); ta = get_ta()
        df = yf.Ticker("^NSEI").history(period="60d", interval="1d", auto_adjust=True)
        if df is None or len(df) < 30: return {"trend":"NEUTRAL","strength":1,"change_pct":0}
        close = df["Close"].squeeze().astype(float)
        ema20 = float(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1])
        ema50 = float(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1])
        cmp   = float(close.iloc[-1]); prev = float(close.iloc[-2])
        chg   = round((cmp-prev)/prev*100, 2)
        strength = sum([cmp>ema20, ema20>ema50, chg>0])
        trend = "BULL" if strength>=2 else "BEAR" if strength<=0 else "NEUTRAL"
        return {"trend":trend,"strength":strength,"ema20":round(ema20,2),"ema50":round(ema50,2),"cmp":round(cmp,2),"change_pct":chg}
    except: return {"trend":"NEUTRAL","strength":1,"change_pct":0}

def fetch_one(sym, regime):
    try:
        yf = get_yf()
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra)>=15) else daily
        ind    = compute_indicators(df)
        if not ind: return None
        score, signals = score_stock(ind, regime)
        return sanitise({"symbol":sym,"score":score,"signals":signals,**ind})
    except: return None

def run_scan():
    with cache_lock:
        if cache["running"]: return
        cache["running"] = True
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        now = datetime.now(IST)
        print(f"\n[SCAN] {now.strftime('%I:%M %p IST')} — {len(WATCHLIST)} stocks")
        regime = fetch_nifty_regime()
        print(f"[REGIME] {regime.get('trend')} ({regime.get('change_pct')}%)")
        results = []; errors = 0
        with ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(fetch_one, sym, regime): sym for sym in WATCHLIST}
            for f in as_completed(futures, timeout=120):
                try:
                    r = f.result(timeout=8)
                    if r: results.append(r)
                    else: errors += 1
                except: errors += 1
        results.sort(key=lambda x: x["score"], reverse=True)
        elapsed = "done"
        print(f"[SCAN] {len(results)} valid, {errors} errors")
        with cache_lock:
            cache["result"] = sanitise({
                "status":"success","scan_time":now.strftime("%I:%M %p IST"),
                "date":now.strftime("%d %b %Y"),"scanned":len(results),
                "total":len(WATCHLIST),"errors":errors,"top10":results[:10],
                "cached":True,"market_regime":regime,
                "nifty_change":regime.get("change_pct",0),"nifty_trend":regime.get("trend","NEUTRAL"),
            })
            cache["last_run"] = now; cache["running"] = False
            cache["scan_count"] += 1; cache["regime"] = regime
    except Exception as e:
        print(f"[SCAN ERR] {e}")
        with cache_lock: cache["running"] = False

def scheduler():
    time.sleep(8)
    while True:
        try: run_scan()
        except Exception as e:
            print(f"[SCHED ERR] {e}")
            with cache_lock: cache["running"] = False
        time.sleep(5*60)

def keep_alive():
    url = os.environ.get("RENDER_EXTERNAL_URL","")
    if not url: return
    while True:
        time.sleep(14*60)
        try: requests.get(f"{url}/health", timeout=10)
        except: pass

_started = False

@app.before_request
def start_bg():
    global _started
    if not _started:
        _started = True
        threading.Thread(target=scheduler,   daemon=True).start()
        threading.Thread(target=keep_alive,  daemon=True).start()
        print("[STARTUP] Background threads started")

# ── FUNDAMENTALS (on-demand only, not background) ─────────────────────────
def fetch_fundamentals(sym):
    try:
        yf = get_yf()
        info = yf.Ticker(get_ns(sym)).info
        if not info or len(info)<10: return None
        cmp = info.get("currentPrice") or info.get("regularMarketPrice")
        if not cmp: return None
        def sg(k, d=None): return info.get(k, d)
        r = {
            "symbol":sym,"cmp":round(float(cmp),2),
            "company":sg("longName",sym),"sector":sg("sector","—"),"industry":sg("industry","—"),
            "market_cap":sg("marketCap"),
            "pe":round(sg("trailingPE"),2) if sg("trailingPE") else None,
            "forward_pe":round(sg("forwardPE"),2) if sg("forwardPE") else None,
            "pb":round(sg("priceToBook"),2) if sg("priceToBook") else None,
            "roe":round(sg("returnOnEquity")*100,1) if sg("returnOnEquity") else None,
            "roa":round(sg("returnOnAssets")*100,1) if sg("returnOnAssets") else None,
            "profit_margin":round(sg("profitMargins")*100,1) if sg("profitMargins") else None,
            "op_margin":round(sg("operatingMargins")*100,1) if sg("operatingMargins") else None,
            "debt_equity":round(sg("debtToEquity")/100,2) if sg("debtToEquity") else None,
            "current_ratio":round(sg("currentRatio"),2) if sg("currentRatio") else None,
            "revenue_growth":round(sg("revenueGrowth")*100,1) if sg("revenueGrowth") else None,
            "earnings_growth":round(sg("earningsGrowth")*100,1) if sg("earningsGrowth") else None,
            "fcf_positive":bool(sg("freeCashflow") and sg("freeCashflow")>0),
            "free_cashflow":sg("freeCashflow"),
            "week52_high":sg("fiftyTwoWeekHigh"),
            "week52_low":sg("fiftyTwoWeekLow"),
            "analyst_target":round(sg("targetMeanPrice"),2) if sg("targetMeanPrice") else None,
            "num_analysts":sg("numberOfAnalystOpinions",0),
            "recommendation":sg("recommendationKey","—"),
            "insider_holding":round(sg("heldPercentInsiders")*100,1) if sg("heldPercentInsiders") else None,
        }
        r["analyst_upside"] = round((r["analyst_target"]-r["cmp"])/r["cmp"]*100,1) if r["analyst_target"] else None
        r["pct_from_52h"] = round((r["cmp"]-r["week52_high"])/r["week52_high"]*100,1) if r["week52_high"] else None
        pe = r["pe"]; eg = r["earnings_growth"]
        r["peg"] = round(pe/(eg),2) if pe and eg and eg>0 else None
        return sanitise(r)
    except: return None

def score_fundamentals(f):
    if not f: return 0, []
    score=0; signals=[]
    pe=f.get("pe")
    if pe:
        if 0<pe<=15:  score+=3.0; signals.append(f"P/E {pe} — attractively valued ✓")
        elif pe<=25:  score+=1.5; signals.append(f"P/E {pe} — fairly valued")
        elif pe>40:   score-=1.0; signals.append(f"P/E {pe} — expensive ✗")
        else:         signals.append(f"P/E {pe} — moderate")
    peg=f.get("peg")
    if peg:
        if 0<peg<=1.0:  score+=2.0; signals.append(f"PEG {peg} — cheap vs growth ✓")
        elif peg<=1.5:  score+=1.0; signals.append(f"PEG {peg} — fair for growth ✓")
        elif peg>2.5:   score-=1.0; signals.append(f"PEG {peg} — expensive for growth ✗")
    roe=f.get("roe")
    if roe:
        if roe>=20:   score+=2.5; signals.append(f"ROE {roe}% — excellent capital efficiency ✓")
        elif roe>=15: score+=1.5; signals.append(f"ROE {roe}% — good efficiency ✓")
        elif roe<8:   score-=1.0; signals.append(f"ROE {roe}% — weak ✗")
        else:         signals.append(f"ROE {roe}% — moderate")
    de=f.get("debt_equity")
    if de is not None:
        if de<=0.3:   score+=2.0; signals.append(f"Debt/Equity {de} — very healthy ✓")
        elif de<=0.6: score+=1.0; signals.append(f"Debt/Equity {de} — manageable ✓")
        elif de>1.5:  score-=1.5; signals.append(f"Debt/Equity {de} — high leverage ✗")
        else:         signals.append(f"Debt/Equity {de} — moderate")
    eg=f.get("earnings_growth")
    if eg is not None:
        if eg>=25:   score+=2.5; signals.append(f"Earnings growth {eg}% — exceptional ✓")
        elif eg>=15: score+=1.5; signals.append(f"Earnings growth {eg}% — strong ✓")
        elif eg<0:   score-=1.5; signals.append(f"Earnings growth {eg}% — declining ✗")
        else:        signals.append(f"Earnings growth {eg}% — modest")
    rg=f.get("revenue_growth")
    if rg is not None:
        if rg>=20:  score+=1.5; signals.append(f"Revenue growth {rg}% — rapidly expanding ✓")
        elif rg>=10:score+=0.8; signals.append(f"Revenue growth {rg}% — healthy ✓")
        elif rg<0:  score-=1.0; signals.append(f"Revenue growth {rg}% — shrinking ✗")
    if f.get("fcf_positive"): score+=1.0; signals.append("Free cash flow positive ✓")
    else:                      score-=0.5; signals.append("Free cash flow negative ✗")
    p52=f.get("pct_from_52h")
    if p52 is not None:
        if -40<=p52<=-15: score+=1.5; signals.append(f"{abs(p52)}% below 52W high — value entry ✓")
        elif p52>=-5:      signals.append(f"Near 52W high ({p52}%) — momentum zone")
    upside=f.get("analyst_upside")
    if upside and f.get("num_analysts",0)>=3:
        if upside>=25:   score+=1.5; signals.append(f"Analyst upside +{upside}% ({f['num_analysts']} analysts) ✓")
        elif upside>=10: score+=0.8; signals.append(f"Analyst upside +{upside}% ✓")
        elif upside<-10: score-=0.5; signals.append(f"Analyst downside {upside}% ✗")
    return max(0, min(17, round(score,1))), signals

# ── ROUTES ────────────────────────────────────────────────────────────────
@app.route("/")

@app.route("/app")
@require_auth
def frontend(): return render_template("index.html")

@app.route("/health")
def health():
    now = datetime.now(IST)
    with cache_lock:
        last    = cache["last_run"].strftime("%I:%M %p IST") if cache["last_run"] else "not yet"
        running = cache["running"]
        count   = cache["scan_count"]
    return jsonify({"status":"running","service":"NSESignal Pro v6","time":now.strftime("%I:%M:%S %p IST"),
                    "last_scan":last,"scan_running":running,"scan_count":count,"watchlist":len(WATCHLIST),
                    "message":"Backend live"})

@app.route("/scan")
@require_auth
def scan():
    try:
        with cache_lock:
            result = cache["result"]; running = cache["running"]
        if result is None:
            if not running:
                threading.Thread(target=run_scan, daemon=True).start()
            for _ in range(90):
                time.sleep(1)
                with cache_lock:
                    if cache["result"]: return jsonify(cache["result"])
            return jsonify({"status":"scanning","message":"First scan in progress — retry in 30s","top10":[]}), 202
        return jsonify(result)
    except Exception as e:
        return jsonify({"status":"error","message":str(e),"top10":[]}), 500

@app.route("/refresh")
@require_auth
def refresh():
    with cache_lock: running = cache["running"]
    if not running: threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({"status":"started" if not running else "already_running"})

@app.route("/indices")
@require_auth
def indices():
    try:
        yf = get_yf(); out = {}
        for name, sym in [("nifty","^NSEI"),("sensex","^BSESN"),("banknifty","^NSEBANK")]:
            try:
                df = yf.Ticker(sym).history(period="1d", interval="5m", auto_adjust=True)
                if df is not None and len(df)>1:
                    c = float(df["Close"].squeeze().iloc[-1])
                    p = float(df["Close"].squeeze().iloc[-2])
                    out[name] = {"price":round(c,2),"change":round(c-p,2),"pct":round((c-p)/p*100,2)}
            except: out[name]={}
        return jsonify(out)
    except Exception as e:
        return jsonify({"nifty":{},"sensex":{},"banknifty":{}}), 200

@app.route("/analyse/<symbol>")
@require_auth
def analyse(symbol):
    try:
        sym = symbol.upper().strip()
        yf  = get_yf()
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra)>=15) else daily
        ind    = compute_indicators(df)
        if not ind: return jsonify({"status":"error","message":f"No data for {sym}. Check NSE symbol."}), 404
        with cache_lock: regime = cache.get("regime") or {}
        if not regime: regime = fetch_nifty_regime()
        t_score, t_sigs = score_stock(ind, regime)
        fund = fetch_fundamentals(sym)
        f_score, f_sigs = score_fundamentals(fund) if fund else (0,["Fundamental data unavailable"])
        combined = round(t_score*0.6 + f_score*0.4, 1)
        rt = regime.get("trend","NEUTRAL")
        threshold_adj = 2 if rt=="BEAR" else 0
        if combined >= 9.5+threshold_adj:
            verdict="BUY"; vc="#00e5a0"; vi="▲"
            vs="Strong setup — multiple indicators aligned. Entry with defined stop-loss."
        elif combined >= 5.5+threshold_adj:
            verdict="HOLD"; vc="#f59e0b"; vi="●"
            vs="Mixed signals. Hold if already in position. Wait for confirmation for new entry."
        else:
            verdict="SELL / AVOID"; vc="#ff4d6d"; vi="▼"
            vs="Weak setup. Avoid new entry. Exit if holding and protect capital."
        all_sigs = t_sigs + f_sigs
        bull_r = [s for s in all_sigs if "✓" in s][:4]
        bear_r = [s for s in all_sigs if "✗" in s][:4]
        return jsonify(sanitise({
            "status":"success","symbol":sym,
            "company":fund.get("company",sym) if fund else sym,
            "sector":fund.get("sector","—") if fund else "—",
            "verdict":verdict,"verdict_color":vc,"verdict_icon":vi,"verdict_summary":vs,
            "combined_score":combined,"tech_score":t_score,"fund_score":f_score,
            "regime":rt,"regime_strength":regime.get("strength",1),
            "cmp":ind.get("cmp"),"change_pct":ind.get("change_pct"),
            "target_price":ind.get("target_price"),"stop_loss":ind.get("stop_loss"),
            "target_pct":ind.get("target_pct"),"risk_reward":ind.get("risk_reward"),
            "direction":ind.get("direction"),"rsi":ind.get("rsi"),
            "stoch_rsi_k":ind.get("stoch_k"),"stoch_rsi_d":ind.get("stoch_d"),
            "macd_hist":ind.get("macd_hist"),"macd_bullish":ind.get("macd_bullish"),
            "supertrend_bull":ind.get("supertrend_bull"),"adx":ind.get("adx"),
            "adx_bullish":ind.get("adx_bullish"),"above_vwap":ind.get("above_vwap"),
            "vwap":ind.get("vwap"),"golden_cross":ind.get("golden_cross"),
            "ema20":ind.get("ema20"),"ema50":ind.get("ema50"),
            "rel_volume":ind.get("rel_volume"),"breakout_setup":ind.get("breakout_setup"),
            "pct_from_52h":ind.get("pct_from_52h"),"week52_high":ind.get("week52_high"),
            "week52_low":ind.get("week52_low"),"atr":ind.get("atr"),
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
            "bullish_reasons":bull_r,"bearish_reasons":bear_r,
        }))
    except Exception as e:
        print(f"[ANALYSE ERR] {e}")
        return jsonify({"status":"error","message":str(e)}), 500

@app.route("/lt-scan")
@require_auth
def lt_scan():
    """Long-term scan — analyses top stocks on demand (no background cache)."""
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        now = datetime.now(IST)
        print(f"[LT SCAN] Starting on-demand fundamental scan")
        results = []
        # Only scan top 80 most liquid — faster and more reliable fundamental data
        lt_list = WATCHLIST[:80]
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {ex.submit(fetch_fundamentals, sym): sym for sym in lt_list}
            for f in as_completed(futures, timeout=120):
                try:
                    r = f.result(timeout=10)
                    if r and (r.get("pe") or r.get("roe")):
                        score, sigs = score_fundamentals(r)
                        grade = "A+" if score>=11 else "A" if score>=8.5 else "B" if score>=6 else "C" if score>=3.5 else "D"
                        r["score"]=score; r["signals"]=sigs; r["grade"]=grade
                        results.append(sanitise(r))
                except: pass
        results.sort(key=lambda x: x.get("score",0), reverse=True)
        return jsonify({"status":"success","scan_time":now.strftime("%I:%M %p IST"),
                        "date":now.strftime("%d %b %Y"),"scanned":len(results),
                        "total":len(lt_list),"top15":results[:15]})
    except Exception as e:
        print(f"[LT SCAN ERR] {e}")
        return jsonify({"status":"error","message":str(e),"top15":[]}), 500

@app.route("/sector-pulse")
@require_auth
def sector_pulse():
    try:
        now = datetime.now(IST)
        month_num  = now.month
        month_name = now.strftime("%B")
        year = now.year

        # Get news sentiment from Claude if API key set
        global_theme = f"AI and technology driving global markets. Semiconductor demand at multi-year highs. Fed policy uncertainty creating volatility."
        india_theme  = f"India's capex cycle strong. Defence indigenisation accelerating. PLI manufacturing push boosting electronics and chemicals."
        sector_ai = {}

        api_key = os.environ.get("ANTHROPIC_API_KEY","")
        if api_key:
            try:
                import anthropic
                client = anthropic.Anthropic(api_key=api_key)
                sectors_list = list(SECTOR_STOCKS.keys())
                prompt = f"""Senior equity analyst covering Indian markets. Today is {month_name} {year}.

Score each NSE sector for near-term boom potential considering global macro, India policies, seasonal patterns.

Sectors: {", ".join(sectors_list)}

Return ONLY JSON:
{{"global_theme":"2 sentences","india_theme":"2 sentences","sectors":[{{"name":"sector","news_score":8,"trend":"BOOMING","catalyst":"main catalyst","risk":"main risk","why":"2 sentences"}}]}}

news_score 1-10. trend: BOOMING/RISING/NEUTRAL/FALLING/AVOID. All {len(sectors_list)} sectors."""

                msg = client.messages.create(model="claude-sonnet-4-6", max_tokens=2500,
                    messages=[{"role":"user","content":prompt}])
                text = msg.content[0].text
                s,e = text.find("{"), text.rfind("}")
                if s!=-1 and e>s:
                    parsed = json.loads(text[s:e+1])
                    global_theme = parsed.get("global_theme", global_theme)
                    india_theme  = parsed.get("india_theme",  india_theme)
                    for item in parsed.get("sectors",[]):
                        sector_ai[item["name"]] = item
            except Exception as ex:
                print(f"[SECTOR AI] {ex}")

        # Get cached tech scores
        with cache_lock: cached = cache.get("result") or {}
        sym_scores = {r["symbol"]:r["score"] for r in cached.get("top10",[])}

        results = []
        for sector, stocks_list in SECTOR_STOCKS.items():
            sea_score = SEASONALITY.get(sector,{}).get(month_num, 2)
            sea_reason = SEASONAL_REASON.get(sector,{}).get(month_num,"")
            ai = sector_ai.get(sector,{})
            news_score = ai.get("news_score",5)
            tech_scores = [sym_scores[s] for s in stocks_list if s in sym_scores]
            tech_avg = round(sum(tech_scores)/len(tech_scores),1) if tech_scores else 0
            tech_syms = [s for s in stocks_list if s in sym_scores][:3]
            news_pts = round(news_score/10*4, 1)
            sea_pts  = round((sea_score/3)*3, 1)
            tech_pts = round(min(tech_avg/17*3,3),1) if tech_avg else 1.0
            combined = round(news_pts + sea_pts + tech_pts, 1)
            boom = "🔥 VERY HIGH" if combined>=8 else "⚡ HIGH" if combined>=6.5 else "📈 MODERATE" if combined>=5 else "➡️ NEUTRAL" if combined>=3.5 else "📉 WEAK"
            boom_color = "#00e5a0" if combined>=8 else "#3d9bff" if combined>=6.5 else "#f59e0b" if combined>=5 else "#6888a8" if combined>=3.5 else "#ff4d6d"
            results.append({
                "sector":sector,"combined_score":combined,"boom_label":boom,"boom_color":boom_color,
                "trend":ai.get("trend","NEUTRAL"),"news_score":news_score,"news_pts":news_pts,
                "seasonal_score":sea_score,"seasonal_pts":sea_pts,"tech_pts":tech_pts,
                "catalyst":ai.get("catalyst",""),"risk":ai.get("risk",""),"why":ai.get("why",""),
                "seasonal_reason":sea_reason,"top_stocks":stocks_list[:5],"tech_stocks":tech_syms,
            })

        results.sort(key=lambda x: x["combined_score"], reverse=True)
        return jsonify({"status":"success","scan_time":now.strftime("%I:%M %p IST"),
                        "date":now.strftime("%d %b %Y"),"month":month_name,
                        "global_theme":global_theme,"india_theme":india_theme,"sectors":results})
    except Exception as e:
        print(f"[SECTOR ERR] {e}")
        return jsonify({"status":"error","message":str(e)}), 500

@app.route("/news", methods=["POST"])
def news(): return jsonify({})

if __name__ == "__main__":
    threading.Thread(target=scheduler,  daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    port = int(os.environ.get("PORT",5000))
    print(f"NSESignal Pro v6 — {len(WATCHLIST)} stocks — port {port}")
    app.run(host="0.0.0.0", port=port)
