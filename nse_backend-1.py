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
import math
import hashlib
import secrets
try:
    import anthropic as _anthropic_lib
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__, template_folder="templates")
CORS(app)

# ── PASSWORD PROTECTION ───────────────────────────────────────────────────
# Set your password via Render environment variable: APP_PASSWORD
# If not set, defaults to "nse2024" — change this in Render dashboard
APP_PASSWORD = os.environ.get("APP_PASSWORD", "nse2024")
APP_USERNAME = os.environ.get("APP_USERNAME", "admin")

def check_auth(username, password):
    """Check if username/password is correct using constant-time comparison."""
    correct_user = secrets.compare_digest(username.encode(), APP_USERNAME.encode())
    correct_pass = secrets.compare_digest(password.encode(), APP_PASSWORD.encode())
    return correct_user and correct_pass

def require_auth(f):
    """Decorator that requires HTTP Basic Auth."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return (
                "Unauthorized — please provide correct credentials.",
                401,
                {"WWW-Authenticate": 'Basic realm="NSESignal Pro"'}
            )
        return f(*args, **kwargs)
    return decorated
IST = pytz.timezone("Asia/Kolkata")

# ── NaN SANITISER ────────────────────────────────────────────────────────
def sanitise(obj):
    """Recursively replace NaN/Inf with None so jsonify never crashes."""
    if isinstance(obj, dict):
        return {k: sanitise(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitise(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

# ── CACHE ─────────────────────────────────────────────────────────────────
cache = {
    "result": None, "running": False,
    "last_run": None, "scan_count": 0,
}
cache_lock = threading.Lock()

# Long-term value/growth screener cache (separate from short-term technical cache)
lt_cache = {
    "result": None, "running": False,
    "last_run": None, "scan_count": 0,
}
lt_cache_lock = threading.Lock()

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

def compute_stoch_rsi(close, rsi_period=14, stoch_period=14, smooth_k=3, smooth_d=3):
    """Stochastic RSI — more sensitive than plain RSI for early momentum detection."""
    try:
        if len(close) < rsi_period + stoch_period + smooth_k + smooth_d:
            return None, None
        ind = ta.momentum.StochRSIIndicator(
            close=close,
            window=rsi_period,
            smooth1=smooth_k,
            smooth2=smooth_d
        )
        k = float(ind.stochrsi_k().iloc[-1]) * 100  # scale to 0-100
        d = float(ind.stochrsi_d().iloc[-1]) * 100
        return round(k, 1), round(d, 1)
    except:
        return None, None

def fetch_nifty_regime():
    """
    Fetch NIFTY 50 data and determine market regime.
    Returns dict with: trend (BULL/BEAR/NEUTRAL), ema20, ema50, change_pct, strength (0-3).
    Strength 3 = strong bull, 0 = strong bear.
    """
    try:
        ticker = yf.Ticker("^NSEI")
        df = ticker.history(period="60d", interval="1d", auto_adjust=True)
        if df is None or len(df) < 30:
            return {"trend": "NEUTRAL", "strength": 1, "ema20": None, "ema50": None, "change_pct": 0}

        close = df["Close"].squeeze().astype(float)
        ema20 = float(ta.trend.EMAIndicator(close=close, window=20).ema_indicator().iloc[-1])
        ema50 = float(ta.trend.EMAIndicator(close=close, window=50).ema_indicator().iloc[-1])
        cmp   = float(close.iloc[-1])
        prev  = float(close.iloc[-2])
        chg   = round((cmp - prev) / prev * 100, 2)

        # Regime scoring: 0-3
        # 3 = strong bull (price > EMA20 > EMA50, positive change)
        # 0 = strong bear (price < EMA20 < EMA50, negative change)
        strength = 0
        if cmp > ema20:  strength += 1
        if ema20 > ema50: strength += 1
        if chg > 0:       strength += 1

        if strength >= 2:
            trend = "BULL"
        elif strength <= 0:
            trend = "BEAR"
        else:
            trend = "NEUTRAL"

        return {
            "trend":      trend,
            "strength":   strength,
            "ema20":      round(ema20, 2),
            "ema50":      round(ema50, 2),
            "cmp":        round(cmp, 2),
            "change_pct": chg,
        }
    except Exception as e:
        print(f"[NIFTY REGIME] {e}")
        return {"trend": "NEUTRAL", "strength": 1, "ema20": None, "ema50": None, "change_pct": 0}

def fetch_promoter_holding(sym):
    """
    Fetch promoter holding % from NSE shareholding pattern API.
    Returns (current_pct, prev_quarter_pct) or (None, None) if unavailable.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nseindia.com/",
        }
        session = requests.Session()
        # Get cookie first
        session.get("https://www.nseindia.com", headers=headers, timeout=5)
        r = session.get(
            f"https://www.nseindia.com/api/corporate-shareholding-pattern?symbol={sym}&series=EQ&from=&to=&csvFlag=0",
            headers=headers,
            timeout=6
        )
        if not r.ok:
            return None, None
        data = r.json()
        # NSE returns latest quarter first
        quarters = data.get("data", [])
        if not quarters:
            return None, None

        def get_promoter(q):
            total = q.get("shareHolding", [])
            for item in total:
                if "Promoter" in item.get("category", ""):
                    return item.get("perSharesHeld", 0)
            return None

        current  = get_promoter(quarters[0]) if len(quarters) >= 1 else None
        previous = get_promoter(quarters[1]) if len(quarters) >= 2 else None
        return current, previous
    except:
        return None, None

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

        # ── StochRSI (more sensitive momentum signal) ──
        try:
            sk, sd = compute_stoch_rsi(close)
            r["stoch_rsi_k"]      = sk
            r["stoch_rsi_d"]      = sd
            # Bullish: K crossed above D and both < 80 (not overbought)
            r["stoch_rsi_bull"]   = bool(sk and sd and sk > sd and sk < 80)
            # Oversold bounce: coming up from oversold zone
            r["stoch_rsi_bounce"] = bool(sk and sd and sk > sd and sk < 30)
        except:
            r["stoch_rsi_k"] = r["stoch_rsi_d"] = None
            r["stoch_rsi_bull"] = r["stoch_rsi_bounce"] = False

        # ── 52-Week Breakout Detection ──
        try:
            w52h = float(close.max())
            w52l = float(close.min())
            pct_from_high = (r["cmp"] - w52h) / w52h * 100
            pct_from_low  = (r["cmp"] - w52l) / w52l * 100
            r["week52_high"]       = round(w52h, 2)
            r["week52_low"]        = round(w52l, 2)
            r["pct_from_52h"]      = round(pct_from_high, 1)
            r["pct_from_52l"]      = round(pct_from_low, 1)
            # Near 52W high breakout: within 3% of high with volume spike
            r["near_52w_high"]     = bool(pct_from_high >= -3.0)
            r["breakout_setup"]    = bool(pct_from_high >= -3.0 and r["rel_volume"] >= 1.5)
            # Deep value: 15-40% below high but fundamentally ok (avoids falling knives)
            r["value_zone"]        = bool(-40 <= pct_from_high <= -15)
        except:
            r["near_52w_high"] = r["breakout_setup"] = r["value_zone"] = False
            r["pct_from_52h"] = r["pct_from_52l"] = 0

        # ── TARGET PRICE & DIRECTION (ATR-based, 2-3 day projection) ──
        try:
            atr_ind = ta.volatility.AverageTrueRange(high=high, low=low, close=close, window=14)
            atr_val = safe_float(atr_ind.average_true_range().iloc[-1])
            r["atr"] = round(atr_val, 2)

            # Direction: bullish if supertrend + macd + above vwap agree
            bull_votes = sum([
                r["supertrend_bull"],
                r["macd_bullish"],
                r["above_vwap"],
                r["golden_cross"],
                45 <= r["rsi"] <= 72,
            ])
            # Lean bullish/bearish even on split votes — NEUTRAL only on a true 2-2-1 tie
            if bull_votes >= 3:
                r["direction"] = "BULLISH"
            elif bull_votes <= 2:
                r["direction"] = "BEARISH" if bull_votes <= 1 else "NEUTRAL"
            else:
                r["direction"] = "NEUTRAL"

            # Target = CMP +/- (ATR x multiplier based on trend strength)
            # Stronger trend (higher ADX) = larger expected move
            if adx >= 30:
                multiplier = 2.5   # strong trend — bigger expected move
            elif adx >= 20:
                multiplier = 1.8   # moderate trend
            else:
                multiplier = 1.2   # weak/ranging — smaller expected move

            move_amount = round(atr_val * multiplier, 2)
            # Ensure a minimum visible move even on low-volatility stocks (at least 0.5% of price)
            min_move = round(r["cmp"] * 0.005, 2)
            if move_amount < min_move:
                move_amount = min_move

            if r["direction"] == "BULLISH":
                r["target_price"]   = round(r["cmp"] + move_amount, 2)
                r["stop_loss"]      = round(r["cmp"] - (atr_val * 1.0), 2)
                r["target_pct"]     = round((move_amount / r["cmp"]) * 100, 2)
            elif r["direction"] == "BEARISH":
                r["target_price"]   = round(r["cmp"] - move_amount, 2)
                r["stop_loss"]      = round(r["cmp"] + (atr_val * 1.0), 2)
                r["target_pct"]     = round(-(move_amount / r["cmp"]) * 100, 2)
            else:
                # NEUTRAL — still show a small directional lean based on which side has more votes
                lean_up = bull_votes >= 2
                half_move = round(move_amount * 0.5, 2)
                if lean_up:
                    r["target_price"] = round(r["cmp"] + half_move, 2)
                    r["target_pct"]   = round((half_move / r["cmp"]) * 100, 2)
                else:
                    r["target_price"] = round(r["cmp"] - half_move, 2)
                    r["target_pct"]   = round(-(half_move / r["cmp"]) * 100, 2)
                r["stop_loss"] = round(r["cmp"] - (atr_val * 0.8), 2)

            # Risk:Reward ratio
            risk   = abs(r["cmp"] - r["stop_loss"])
            reward = abs(r["target_price"] - r["cmp"])
            r["risk_reward"] = round(reward / risk, 2) if risk > 0 else 0

        except Exception as e:
            r["atr"] = 0
            r["direction"] = "NEUTRAL"
            r["target_price"] = r["cmp"]
            r["stop_loss"] = r["cmp"]
            r["target_pct"] = 0.0
            r["risk_reward"] = 0

        return r
    except Exception as e:
        print(f"  [IND err] {e}")
        return None

def score_stock(ind, market_regime=None):
    """
    Score a stock 0-15 across all technical indicators.
    market_regime: dict from fetch_nifty_regime() — adjusts scoring for market context.
    """
    if not ind:
        return 0, []
    score = 0
    signals = []

    # ── MARKET REGIME CONTEXT (bonus/penalty based on NIFTY trend) ──
    regime = market_regime or {}
    regime_trend    = regime.get("trend", "NEUTRAL")
    regime_strength = regime.get("strength", 1)

    if regime_trend == "BULL":
        score += 1.0
        signals.append(f"NIFTY in BULL regime (EMA20 > EMA50, strength {regime_strength}/3) — tailwind ✓")
    elif regime_trend == "BEAR":
        score -= 2.0
        signals.append(f"NIFTY in BEAR regime — headwind for all BULL setups ✗")
    else:
        signals.append(f"NIFTY NEUTRAL — mixed market conditions")

    # ── RSI (max 2.0) ──
    rsi = ind.get("rsi", 50)
    if   52 <= rsi <= 68: score += 2.0; signals.append(f"RSI {rsi} — ideal momentum zone ✓")
    elif 45 <= rsi <  52: score += 1.0; signals.append(f"RSI {rsi} — building momentum ✓")
    elif 68 <  rsi <= 72: score += 0.5; signals.append(f"RSI {rsi} — strong, approaching overbought")
    elif rsi > 72:        score -= 1.5; signals.append(f"RSI {rsi} — overbought, pullback risk ✗")
    else:                 score -= 1.0; signals.append(f"RSI {rsi} — weak momentum ✗")

    # ── StochRSI (max 2.0) — more sensitive than RSI alone ──
    sk = ind.get("stoch_rsi_k")
    sd = ind.get("stoch_rsi_d")
    if sk is not None and sd is not None:
        if ind.get("stoch_rsi_bounce"):
            score += 2.0; signals.append(f"StochRSI {sk}/{sd} — oversold bounce signal ✓")
        elif ind.get("stoch_rsi_bull") and sk < 60:
            score += 1.5; signals.append(f"StochRSI {sk}/{sd} — K above D, momentum building ✓")
        elif ind.get("stoch_rsi_bull"):
            score += 0.8; signals.append(f"StochRSI {sk}/{sd} — bullish cross")
        elif sk > 80:
            score -= 1.0; signals.append(f"StochRSI {sk}/{sd} — overbought zone ✗")
        else:
            signals.append(f"StochRSI {sk}/{sd} — neutral")
    
    # ── MACD (max 2.0) ──
    if ind.get("macd_bullish"):
        score += 2.0; signals.append(f"MACD expanding positive ({ind.get('macd_hist')}) ✓")
    elif ind.get("macd_hist", 0) > 0:
        score += 1.0; signals.append(f"MACD positive but fading ({ind.get('macd_hist')})")
    else:
        score -= 1.0; signals.append(f"MACD bearish ({ind.get('macd_hist', 0)}) ✗")

    # ── VWAP (max 1.5) ──
    if ind.get("above_vwap"):
        score += 1.5; signals.append(f"Above VWAP ₹{ind.get('vwap')} ✓")
    else:
        score -= 0.5; signals.append(f"Below VWAP ₹{ind.get('vwap')} ✗")

    # ── EMA Structure (max 2.0) ──
    if ind.get("golden_cross"):
        score += 1.5; signals.append(f"Golden cross: EMA20 ₹{ind.get('ema20')} > EMA50 ₹{ind.get('ema50')} ✓")
    else:
        score -= 0.5; signals.append(f"Death cross: EMA20 < EMA50 ✗")
    if ind.get("above_ema20"):
        score += 0.5; signals.append(f"Price above EMA20 ✓")

    # ── Volume (max 2.0) ──
    rv = ind.get("rel_volume", 1.0)
    if   rv >= 2.5: score += 2.0; signals.append(f"Volume {rv}x avg — strong institutional activity ✓")
    elif rv >= 1.5: score += 1.0; signals.append(f"Volume {rv}x avg — above average ✓")
    elif rv <  0.7: score -= 0.5; signals.append(f"Volume {rv}x avg — weak conviction ✗")
    else:           signals.append(f"Volume {rv}x avg — normal")

    # ── Supertrend (max 2.0) ──
    if ind.get("supertrend_bull"):
        score += 2.0; signals.append(f"Supertrend BULLISH — price above trend line ✓")
    else:
        score -= 1.5; signals.append(f"Supertrend BEARISH — price below trend line ✗")

    # ── ADX — Trend Strength (max 2.0) ──
    adx = ind.get("adx", 0)
    if   adx >= 30 and ind.get("adx_bullish"): score += 2.0; signals.append(f"ADX {adx} — very strong bullish trend ✓")
    elif adx >= 25 and ind.get("adx_bullish"): score += 1.5; signals.append(f"ADX {adx} — strong trend DI+>DI- ✓")
    elif adx >= 20:                             score += 0.5; signals.append(f"ADX {adx} — moderate trend")
    else:                                       score -= 0.5; signals.append(f"ADX {adx} — weak/ranging market ✗")

    # ── 52-Week Breakout (max 2.0) ──
    if ind.get("breakout_setup"):
        score += 2.0; signals.append(f"52W BREAKOUT SETUP — within 3% of 52W high with volume spike ✓")
    elif ind.get("near_52w_high"):
        score += 1.0; signals.append(f"Near 52W high ({ind.get('pct_from_52h')}%) — momentum continuation ✓")
    elif ind.get("value_zone"):
        score += 0.5; signals.append(f"{abs(ind.get('pct_from_52h', 0))}% below 52W high — potential value entry")

    # ── Bollinger (max 0.5) ──
    bb_pct = ind.get("bb_pct", 0.5)
    if   0.2 <= bb_pct <= 0.7: score += 0.5; signals.append(f"Bollinger {round(bb_pct*100)}% — healthy range ✓")
    elif bb_pct > 0.9:         score -= 0.5; signals.append(f"Bollinger near upper band ✗")

    # ── Target price summary ──
    if ind.get("direction") == "BULLISH":
        signals.append(f"TARGET: ₹{ind.get('target_price')} ({ind.get('target_pct')}%) | SL: ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')} ✓")
    elif ind.get("direction") == "BEARISH":
        signals.append(f"TARGET: ₹{ind.get('target_price')} ({ind.get('target_pct')}%) | SL: ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')} ✗")
    else:
        signals.append(f"TARGET: ₹{ind.get('target_price')} | SL: ₹{ind.get('stop_loss')} | R:R 1:{ind.get('risk_reward')}")

    return max(0, min(17, round(score, 1))), signals

# ════════════════════════════════════════════════════════════════════════
# LONG-TERM VALUE + GROWTH SCREENER
# Separate from short-term technical scanner above.
# Uses fundamental data: P/E, ROE, Debt/Equity, Growth rates, FCF, etc.
# ════════════════════════════════════════════════════════════════════════

def safe_get(d, key, default=None):
    """Safely get a value from yfinance info dict, handling None/missing."""
    try:
        v = d.get(key, default)
        if v is None:
            return default
        return v
    except:
        return default

def compute_fundamentals(sym):
    """
    Fetch fundamental data for a stock and score it on value + quality + growth.
    Returns None if insufficient data (illiquid/newly listed stocks).
    """
    try:
        ticker = yf.Ticker(get_ns(sym))
        info   = ticker.info

        if not info or len(info) < 10:
            return None

        cmp_price = safe_get(info, "currentPrice") or safe_get(info, "regularMarketPrice")
        if not cmp_price or cmp_price <= 0:
            return None

        r = {"symbol": sym, "cmp": round(float(cmp_price), 2)}
        r["company"]    = safe_get(info, "longName", sym)
        r["sector"]     = safe_get(info, "sector", "—")
        r["industry"]   = safe_get(info, "industry", "—")
        r["market_cap"] = safe_get(info, "marketCap", 0)

        # ── VALUATION METRICS ──
        pe        = safe_get(info, "trailingPE")
        forward_pe= safe_get(info, "forwardPE")
        pb        = safe_get(info, "priceToBook")
        r["pe"]         = round(pe, 2) if pe else None
        r["forward_pe"] = round(forward_pe, 2) if forward_pe else None
        r["pb"]         = round(pb, 2) if pb else None

        # ── PROFITABILITY / QUALITY METRICS ──
        roe   = safe_get(info, "returnOnEquity")
        roa   = safe_get(info, "returnOnAssets")
        margin= safe_get(info, "profitMargins")
        op_margin = safe_get(info, "operatingMargins")
        r["roe"]           = round(roe * 100, 1) if roe else None
        r["roa"]           = round(roa * 100, 1) if roa else None
        r["profit_margin"] = round(margin * 100, 1) if margin else None
        r["op_margin"]     = round(op_margin * 100, 1) if op_margin else None

        # ── DEBT / BALANCE SHEET HEALTH ──
        de_ratio    = safe_get(info, "debtToEquity")
        current_r   = safe_get(info, "currentRatio")
        r["debt_equity"]  = round(de_ratio / 100, 2) if de_ratio else None  # yfinance gives as percentage
        r["current_ratio"]= round(current_r, 2) if current_r else None

        # ── GROWTH METRICS ──
        rev_growth  = safe_get(info, "revenueGrowth")
        earn_growth = safe_get(info, "earningsGrowth")
        r["revenue_growth"]  = round(rev_growth * 100, 1) if rev_growth else None
        r["earnings_growth"] = round(earn_growth * 100, 1) if earn_growth else None

        # ── CASH FLOW ──
        fcf = safe_get(info, "freeCashflow")
        op_cf = safe_get(info, "operatingCashflow")
        r["free_cashflow"]     = fcf
        r["operating_cashflow"]= op_cf
        r["fcf_positive"]      = bool(fcf and fcf > 0)

        # ── PEG RATIO (computed manually — yfinance's pegRatio field is broken) ──
        if pe and earn_growth and earn_growth > 0:
            r["peg"] = round(pe / (earn_growth * 100), 2)
        else:
            r["peg"] = None

        # ── PRICE POSITION ──
        high52 = safe_get(info, "fiftyTwoWeekHigh")
        low52  = safe_get(info, "fiftyTwoWeekLow")
        if high52 and low52 and high52 > 0:
            r["week52_high"] = round(high52, 2)
            r["week52_low"]  = round(low52, 2)
            r["pct_from_52h"]= round((r["cmp"] - high52) / high52 * 100, 1)
            r["pct_from_52l"]= round((r["cmp"] - low52) / low52 * 100, 1)
        else:
            r["week52_high"] = r["week52_low"] = None
            r["pct_from_52h"] = r["pct_from_52l"] = 0

        # ── ANALYST TARGETS ──
        target_mean = safe_get(info, "targetMeanPrice")
        target_high = safe_get(info, "targetHighPrice")
        rec_key     = safe_get(info, "recommendationKey", "—")
        n_analysts  = safe_get(info, "numberOfAnalystOpinions", 0)
        r["analyst_target"]    = round(target_mean, 2) if target_mean else None
        r["analyst_target_high"]= round(target_high, 2) if target_high else None
        r["analyst_upside"]    = round((target_mean - r["cmp"]) / r["cmp"] * 100, 1) if target_mean else None
        r["recommendation"]    = rec_key
        r["num_analysts"]      = n_analysts

        # ── PROMOTER / INSIDER ──
        held_pct = safe_get(info, "heldPercentInsiders")
        r["insider_holding"] = round(held_pct * 100, 1) if held_pct else None

        # Try NSE API for actual promoter holding (more accurate for Indian stocks)
        try:
            promoter_curr, promoter_prev = fetch_promoter_holding(sym)
            r["promoter_holding"]    = promoter_curr
            r["promoter_prev"]       = promoter_prev
            r["promoter_increasing"] = bool(promoter_curr and promoter_prev and promoter_curr > promoter_prev)
        except:
            r["promoter_holding"] = r["promoter_prev"] = None
            r["promoter_increasing"] = False

        return sanitise(r)
    except Exception as e:
        return None

def score_fundamentals(f):
    """
    Score a stock 0-15 on value + quality + growth dimensions.
    Higher score = more attractive for long-term holding.
    """
    if not f:
        return 0, []
    score = 0
    signals = []

    # ── Valuation (max 3) ──
    pe = f.get("pe")
    if pe is not None:
        if 0 < pe <= 15:
            score += 3.0; signals.append(f"P/E {pe} — attractively valued ✓")
        elif 15 < pe <= 25:
            score += 1.5; signals.append(f"P/E {pe} — fairly valued")
        elif pe > 40:
            score -= 1.0; signals.append(f"P/E {pe} — expensive ✗")
        else:
            signals.append(f"P/E {pe} — moderate valuation")
    else:
        signals.append("P/E — not available")

    # ── PEG ratio (max 2) — cheap relative to growth ──
    peg = f.get("peg")
    if peg is not None:
        if 0 < peg <= 1.0:
            score += 2.0; signals.append(f"PEG {peg} — cheap relative to growth ✓")
        elif 1.0 < peg <= 1.5:
            score += 1.0; signals.append(f"PEG {peg} — reasonably priced for growth ✓")
        elif peg > 2.5:
            score -= 1.0; signals.append(f"PEG {peg} — expensive for its growth ✗")

    # ── ROE — quality of management (max 2.5) ──
    roe = f.get("roe")
    if roe is not None:
        if roe >= 20:
            score += 2.5; signals.append(f"ROE {roe}% — excellent capital efficiency ✓")
        elif roe >= 15:
            score += 1.5; signals.append(f"ROE {roe}% — good capital efficiency ✓")
        elif roe < 8:
            score -= 1.0; signals.append(f"ROE {roe}% — weak capital efficiency ✗")
        else:
            signals.append(f"ROE {roe}% — moderate")

    # ── Debt/Equity — balance sheet safety (max 2) ──
    de = f.get("debt_equity")
    if de is not None:
        if de <= 0.3:
            score += 2.0; signals.append(f"Debt/Equity {de} — very healthy balance sheet ✓")
        elif de <= 0.6:
            score += 1.0; signals.append(f"Debt/Equity {de} — manageable debt ✓")
        elif de > 1.5:
            score -= 1.5; signals.append(f"Debt/Equity {de} — high leverage risk ✗")
        else:
            signals.append(f"Debt/Equity {de} — moderate")

    # ── Earnings growth (max 2.5) ──
    eg = f.get("earnings_growth")
    if eg is not None:
        if eg >= 25:
            score += 2.5; signals.append(f"Earnings growth {eg}% — exceptional ✓")
        elif eg >= 15:
            score += 1.5; signals.append(f"Earnings growth {eg}% — strong ✓")
        elif eg < 0:
            score -= 1.5; signals.append(f"Earnings growth {eg}% — declining ✗")
        else:
            signals.append(f"Earnings growth {eg}% — modest")

    # ── Revenue growth (max 1.5) ──
    rg = f.get("revenue_growth")
    if rg is not None:
        if rg >= 20:
            score += 1.5; signals.append(f"Revenue growth {rg}% — rapidly expanding ✓")
        elif rg >= 10:
            score += 0.8; signals.append(f"Revenue growth {rg}% — healthy growth ✓")
        elif rg < 0:
            score -= 1.0; signals.append(f"Revenue growth {rg}% — shrinking ✗")

    # ── Free cash flow (max 1) ──
    if f.get("fcf_positive"):
        score += 1.0; signals.append("Free cash flow positive — real cash generation ✓")
    else:
        score -= 0.5; signals.append("Free cash flow negative or unavailable ✗")

    # ── Price position — value opportunity (max 1.5) ──
    pct52h = f.get("pct_from_52h", 0)
    if pct52h is not None:
        if -40 <= pct52h <= -15:
            score += 1.5; signals.append(f"{abs(pct52h)}% below 52W high — potential value entry ✓")
        elif pct52h < -50:
            score += 0.5; signals.append(f"{abs(pct52h)}% below 52W high — deeply discounted (check why)")
        elif pct52h > -5:
            signals.append(f"Near 52W high ({pct52h}%) — momentum but less margin of safety")

    # ── Analyst sentiment (max 1.5) ──
    upside = f.get("analyst_upside")
    if upside is not None and f.get("num_analysts", 0) >= 3:
        if upside >= 25:
            score += 1.5; signals.append(f"Analyst target implies +{upside}% upside ({f.get('num_analysts')} analysts) ✓")
        elif upside >= 10:
            score += 0.8; signals.append(f"Analyst target implies +{upside}% upside ✓")
        elif upside < -10:
            score -= 0.5; signals.append(f"Analyst target implies {upside}% — overvalued per analysts ✗")

    # ── Promoter Holding — India-specific key signal (max 2.0) ──
    promoter = f.get("promoter_holding")
    if promoter is not None:
        if promoter >= 60 and f.get("promoter_increasing"):
            score += 2.0; signals.append(f"Promoter holding {promoter}% and INCREASING — very strong conviction ✓")
        elif promoter >= 60:
            score += 1.2; signals.append(f"Promoter holding {promoter}% — high skin in the game ✓")
        elif promoter >= 45 and f.get("promoter_increasing"):
            score += 1.0; signals.append(f"Promoter holding {promoter}% and increasing ✓")
        elif promoter >= 45:
            score += 0.5; signals.append(f"Promoter holding {promoter}% — reasonable")
        elif promoter < 25:
            score -= 0.5; signals.append(f"Promoter holding {promoter}% — low promoter stake ✗")
        else:
            signals.append(f"Promoter holding {promoter}%")
        prev = f.get("promoter_prev")
        if prev and promoter and promoter < prev:
            score -= 1.0; signals.append(f"Promoter stake declining ({prev}% → {promoter}%) — red flag ✗")
    else:
        signals.append("Promoter holding — data unavailable (check NSE/BSE)")

    return max(0, min(17, round(score, 1))), signals

def fetch_one_fundamental(sym):
    try:
        f = compute_fundamentals(sym)
        if not f:
            return None
        score, signals = score_fundamentals(f)
        f["score"] = score
        f["signals"] = signals
        # Grade
        f["grade"] = "A+" if score>=11 else "A" if score>=8.5 else "B" if score>=6 else "C" if score>=3.5 else "D"
        return sanitise(f)
    except:
        return None

def run_lt_scan_background():
    with lt_cache_lock:
        if lt_cache["running"]:
            return
        lt_cache["running"] = True

    now   = datetime.now(IST)
    start = time.time()
    print(f"\n[LT SCAN] {now.strftime('%I:%M %p IST')} — {len(WATCHLIST)} stocks (fundamental)")

    results = []
    errors  = 0

    # Fundamentals are slower to fetch (more data per call) — fewer parallel workers
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_one_fundamental, sym): sym for sym in WATCHLIST}
        for future in as_completed(futures, timeout=240):
            try:
                r = future.result(timeout=10)
                if r:
                    results.append(r)
                else:
                    errors += 1
            except:
                errors += 1

    # Filter: only keep stocks with at least PE or ROE data (avoid empty shells)
    results = [r for r in results if r.get("pe") is not None or r.get("roe") is not None]
    results.sort(key=lambda x: x["score"], reverse=True)

    elapsed = round(time.time() - start, 1)
    print(f"[LT SCAN] Done — {len(results)} valid, {errors} errors, {elapsed}s")

    with lt_cache_lock:
        lt_cache["result"] = {
            "status":    "success",
            "scan_time": now.strftime("%I:%M %p IST"),
            "date":      now.strftime("%d %b %Y"),
            "scanned":   len(results),
            "total":     len(WATCHLIST),
            "errors":    errors,
            "elapsed":   f"{elapsed}s",
            "top15":     results[:15],
            "cached":    True,
        }
        lt_cache["last_run"]   = now
        lt_cache["running"]    = False
        lt_cache["scan_count"] += 1

def lt_background_scheduler():
    """Long-term fundamentals change slowly — scan every 6 hours, not 5 minutes.
    Staggered well after short-term scanner and checks it's not actively running
    to avoid resource contention on Render's limited free-tier CPU."""
    time.sleep(90)  # let short-term scanner finish its first run completely
    while True:
        try:
            # Wait until short-term scan is NOT running before starting fundamental scan
            waited = 0
            while waited < 60:
                with cache_lock:
                    if not cache["running"]:
                        break
                time.sleep(2)
                waited += 2
            run_lt_scan_background()
        except Exception as e:
            print(f"[LT SCHEDULER] {e}")
            with lt_cache_lock:
                lt_cache["running"] = False
        time.sleep(6 * 60 * 60)  # every 6 hours

def fetch_one(args):
    """Fetch and score one symbol. args = (sym, market_regime) or just sym."""
    if isinstance(args, tuple):
        sym, market_regime = args
    else:
        sym, market_regime = args, None
    try:
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m", auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d", auto_adjust=True)
        df     = intra if (intra is not None and len(intra) >= 15) else daily
        ind    = compute_indicators(df)
        if not ind:
            return None
        score, signals = score_stock(ind, market_regime)
        return sanitise({"symbol": sym, "score": score, "signals": signals, **ind})
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

    # Step 1: Fetch NIFTY market regime — context for all stock scores
    print("[REGIME] Fetching NIFTY 50 trend…")
    market_regime = fetch_nifty_regime()
    print(f"[REGIME] NIFTY {market_regime.get('trend')} | strength:{market_regime.get('strength')}/3 | change:{market_regime.get('change_pct')}%")

    results = []
    errors  = 0
    scan_args = [(sym, market_regime) for sym in WATCHLIST]

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = {executor.submit(fetch_one, arg): arg[0] for arg in scan_args}
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
        # Sanitise NaN/Inf before storing — JSON cannot encode NaN
        results = [sanitise(r) for r in results]
        cache["result"] = {
            "status":         "success",
            "scan_time":      now.strftime("%I:%M %p IST"),
            "date":           now.strftime("%d %b %Y"),
            "scanned":        len(results),
            "total":          len(WATCHLIST),
            "errors":         errors,
            "elapsed":        f"{elapsed}s",
            "top10":          results[:10],
            "cached":         True,
            "market_regime":  market_regime,
            "nifty_change":   market_regime.get("change_pct", 0),
            "nifty_trend":    market_regime.get("trend", "NEUTRAL"),
        }
        cache["market_regime"] = market_regime
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
@require_auth
def frontend():
    return render_template("index.html")



# ═══════════════════════════════════════════════════════════════════════
# SECTOR PULSE ENGINE
# Combines: Seasonality + Policy/News Sentiment + Technical Momentum
# ═══════════════════════════════════════════════════════════════════════

# ── Sector → NSE stock mapping ─────────────────────────────────────────
SECTOR_STOCKS = {
    "Technology & AI":        ["TCS","INFY","HCLTECH","WIPRO","TECHM","LTIM","PERSISTENT","COFORGE","KPITTECH","MPHASIS"],
    "Semiconductors & Electronics": ["DIXON","KAYNES","SYRMA","AMBER","PGEL","TATAELXSI","BHEL","BEL","HAL","BEML"],
    "Defence & Aerospace":    ["HAL","BEL","BHEL","BEML","GRSE","COCHINSHIP","MAZAGON","GARDENREACH","MIDHANI","MTAR"],
    "Banking & Finance":      ["HDFCBANK","ICICIBANK","SBIN","KOTAKBANK","AXISBANK","INDUSINDBK","BANDHANBNK","IDFCFIRSTB","FEDERALBNK","RBLBANK"],
    "Pharma & Healthcare":    ["SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","LUPIN","AUROPHARMA","ALKEM","TORNTPHARM","APOLLOHOSP","MAXHEALTH"],
    "EV & Auto":              ["TATAMOTORS","MARUTI","BAJAJ-AUTO","HEROMOTOCO","EICHERMOT","TVSMOTOR","MOTHERSON","BOSCHLTD","BALKRISIND","TIINDIA"],
    "Renewable Energy":       ["ADANIGREEN","TATAPOWER","ADANIPOWER","TORNTPOWER","NHPC","SJVN","IREDA","CUMMINSIND","POWERGRID","NTPC"],
    "Infrastructure & Capex": ["LT","RVNL","IRFC","IRCTC","RAILTEL","GMRINFRA","KNR","HGINFRA","PNCINFRA","ASHOKA"],
    "FMCG & Consumer":        ["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO","DABUR","COLPAL","GODREJCP","EMAMILTD","TATACONSUM"],
    "Agriculture & Fertiliser":["COROMANDEL","PIIND","CHAMBLFERT","GNFC","RALLIS","DHANUKA","SUMICHEM","DEEPAKFERT","KRBL","AVANTIFEED"],
    "Cement & Construction":  ["ULTRACEMCO","AMBUJACEM","SHREECEM","JKCEMENT","DALMIACEM","RAMCOCEM","LT","SIEMENS","ABB","THERMAX"],
    "Metals & Mining":        ["TATASTEEL","JSWSTEEL","HINDALCO","VEDL","SAIL","NMDC","COALINDIA","APLAPOLLO","RATNAMANI","GRAPHITE"],
    "Real Estate":            ["DLF","LODHA","PRESTIGE","OBEROIRLTY","PHOENIXLTD","GODREJPROP","SOBHA","KOLTEPATIL","HEMIPROP","MAXESTATES"],
    "Oil & Gas":              ["RELIANCE","ONGC","BPCL","IOC","GAIL","DEEPAKFERT","MRPL","CASTROLIND","GULF","HINDPETRO"],
    "Telecom & Media":        ["BHARTIARTL","INDUS","TATACOMM","NETWORK18","ZEEL","SUNTV","NAZARA","ROUTE","TANLA","ONMOBILE"],
}

# ── Seasonality scores by month (1=Jan .. 12=Dec) ──────────────────────
# Based on historical NSE sector performance patterns
SEASONALITY = {
    "Technology & AI":         {1:2,2:1,3:1,4:2,5:2,6:1,7:3,8:2,9:2,10:2,11:1,12:1},
    "Semiconductors & Electronics":{1:2,2:2,3:1,4:2,5:2,6:1,7:2,8:3,9:2,10:2,11:2,12:1},
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
    "Telecom & Media":         {1:2,2:2,3:2,4:2,5:2,6:2,7:2,8:2,9:2,10:2,11:2,12:2},
}

# ── Seasonality explanations ────────────────────────────────────────────
SEASONALITY_REASONS = {
    "Technology & AI":         {7:"Q1 earnings season — IT companies report strong results",10:"Q2 results season + global tech spending"},
    "Semiconductors & Electronics":{8:"Back-to-school + festive pre-stocking drives electronics demand"},
    "Defence & Aerospace":     {3:"Budget session — defence allocations finalised and contracts awarded"},
    "Banking & Finance":       {4:"Q4 results + credit growth peak before monsoon",10:"Q2 results + festive credit offtake surge"},
    "EV & Auto":               {9:"Navratri/Dussehra — peak auto sales season in India",8:"Pre-festive inventory build"},
    "Renewable Energy":        {6:"Summer peak power demand + monsoon wind energy harvest",7:"Strong solar irradiance + policy announcements"},
    "Infrastructure & Capex":  {9:"Post-monsoon construction season begins",10:"Govt capex acceleration in Q3"},
    "FMCG & Consumer":         {10:"Diwali — peak FMCG consumption season",11:"Post-festive restocking"},
    "Agriculture & Fertiliser":{5:"Kharif sowing season begins",6:"Monsoon onset — maximum fertiliser demand",4:"Rabi harvest + Kharif prep"},
    "Cement & Construction":   {9:"Post-monsoon construction boom begins",10:"Peak cement demand quarter"},
    "Real Estate":             {10:"Diwali launches — peak real estate buying season",11:"Year-end property purchases"},
}

def get_current_month():
    return datetime.now(IST).month

def get_seasonality_score(sector):
    month = get_current_month()
    score_map = SEASONALITY.get(sector, {})
    return score_map.get(month, 2)

def get_seasonality_reason(sector):
    month = get_current_month()
    reasons = SEASONALITY_REASONS.get(sector, {})
    return reasons.get(month, "")

def get_sector_tech_score(sector):
    """Get average technical score for top stocks in the sector."""
    stocks_list = SECTOR_STOCKS.get(sector, [])[:6]
    with cache_lock:
        cached = cache.get("result")

    scores = []
    if cached and cached.get("top10"):
        # Check if any sector stocks are in the recent scan results
        all_results = cached.get("top10", [])
        sym_scores = {r["symbol"]: r["score"] for r in all_results}
        for sym in stocks_list:
            if sym in sym_scores:
                scores.append(sym_scores[sym])

    if not scores:
        return 0, []

    avg = round(sum(scores) / len(scores), 1)
    top_syms = [s for s in stocks_list if s in sym_scores][:3]
    return avg, top_syms

def build_sector_pulse_prompt(month_name, year):
    sectors = list(SECTOR_STOCKS.keys())
    return f"""You are a senior equity research analyst covering Indian markets. Today is {month_name} {year}.

Analyse the current global and Indian market sentiment for each of these NSE sectors and score them.

Consider:
- Global macroeconomic trends (US Fed policy, China economy, global commodity prices)
- India-specific themes (government policies, PLI schemes, infrastructure push, digital India)
- Current geopolitical events impacting sectors
- Recent earnings trends and analyst upgrades/downgrades
- Structural themes (AI adoption, EV transition, renewable energy push, defence indigenisation)
- Near-term catalysts expected in next 1-3 months

Sectors to score: {", ".join(sectors)}

Return ONLY valid JSON — no markdown, no backticks:
{{"scan_date":"{month_name} {year}","global_theme":"2-sentence summary of dominant global market theme","india_theme":"2-sentence summary of dominant India market theme","sectors":[{{"name":"exact sector name from list","news_score":8,"conviction":"HIGH","trend":"BOOMING","catalyst":"Main catalyst driving this sector right now","risk":"Main risk to this thesis","why":"2-3 sentences of reasoning"}}]}}

news_score: 1-10 (10 = strongest conviction)
conviction: HIGH / MEDIUM / LOW
trend: BOOMING / RISING / NEUTRAL / FALLING / AVOID

Return all {len(sectors)} sectors. Order by news_score descending."""

@app.route("/analyse/<symbol>")
@require_auth
def analyse(symbol):
    """
    Full analysis for a single stock — technical + fundamental + verdict.
    Returns BUY / HOLD / SELL with detailed reasoning.
    """
    try:
        sym = symbol.upper().strip()

        # ── Fetch technical indicators ──
        ticker = yf.Ticker(get_ns(sym))
        intra  = ticker.history(period="1d",  interval="5m",  auto_adjust=True)
        daily  = ticker.history(period="60d", interval="1d",  auto_adjust=True)
        df     = intra if (intra is not None and len(intra) >= 15) else daily
        ind    = compute_indicators(df)
        if not ind:
            return jsonify({"status": "error", "message": f"No data found for {sym}. Check the NSE symbol."}), 404

        # ── Fetch market regime ──
        with cache_lock:
            market_regime = cache.get("market_regime") or fetch_nifty_regime()

        tech_score, tech_signals = score_stock(ind, market_regime)

        # ── Fetch fundamentals ──
        fund = compute_fundamentals(sym)
        fund_score, fund_signals = score_fundamentals(fund) if fund else (0, ["Fundamental data unavailable"])

        # ── Fetch promoter holding ──
        if fund:
            try:
                pc, pp = fetch_promoter_holding(sym)
                fund["promoter_holding"]    = pc
                fund["promoter_prev"]       = pp
                fund["promoter_increasing"] = bool(pc and pp and pc > pp)
                fund_score, fund_signals    = score_fundamentals(fund)
            except:
                pass

        # ── Combined verdict ──
        # Weight: 60% technical (short-term), 40% fundamental (quality)
        combined = round(tech_score * 0.6 + fund_score * 0.4, 1)
        max_combined = round(17 * 0.6 + 17 * 0.4, 1)  # 17

        # Regime penalty — if NIFTY bearish, raise bar for BUY
        regime_trend = market_regime.get("trend", "NEUTRAL") if market_regime else "NEUTRAL"
        regime_penalty = 2 if regime_trend == "BEAR" else 0

        buy_threshold  = 9.5 + regime_penalty
        hold_threshold = 5.5 + regime_penalty

        if combined >= buy_threshold:
            verdict = "BUY"
            verdict_color = "#00e5a0"
            verdict_icon  = "▲"
            verdict_summary = "Strong technical setup with solid fundamentals. Multiple indicators aligned. Entry recommended with defined stop-loss."
        elif combined >= hold_threshold:
            verdict = "HOLD"
            verdict_color = "#f59e0b"
            verdict_icon  = "●"
            verdict_summary = "Mixed signals — some indicators positive, some weak. Hold if already in position. New entry only on confirmation."
        else:
            verdict = "SELL / AVOID"
            verdict_color = "#ff4d6d"
            verdict_icon  = "▼"
            verdict_summary = "Weak technical or fundamental setup. Exit or avoid new entry. Wait for better conditions."

        # ── Key reasons (top 3 bullish + top 3 bearish signals) ──
        bullish_reasons = [s for s in tech_signals + fund_signals if "✓" in s][:4]
        bearish_reasons = [s for s in tech_signals + fund_signals if "✗" in s][:4]

        return jsonify(sanitise({
            "status":          "success",
            "symbol":          sym,
            "company":         fund.get("company", sym) if fund else sym,
            "sector":          fund.get("sector", "—")  if fund else "—",
            "verdict":         verdict,
            "verdict_color":   verdict_color,
            "verdict_icon":    verdict_icon,
            "verdict_summary": verdict_summary,
            "combined_score":  combined,
            "max_score":       max_combined,
            "tech_score":      tech_score,
            "fund_score":      fund_score,
            "regime":          regime_trend,
            "regime_strength": market_regime.get("strength", 1) if market_regime else 1,

            # Technical data
            "cmp":             ind.get("cmp"),
            "change_pct":      ind.get("change_pct"),
            "target_price":    ind.get("target_price"),
            "stop_loss":       ind.get("stop_loss"),
            "target_pct":      ind.get("target_pct"),
            "risk_reward":     ind.get("risk_reward"),
            "direction":       ind.get("direction"),
            "rsi":             ind.get("rsi"),
            "stoch_rsi_k":     ind.get("stoch_rsi_k"),
            "stoch_rsi_d":     ind.get("stoch_rsi_d"),
            "macd_hist":       ind.get("macd_hist"),
            "macd_bullish":    ind.get("macd_bullish"),
            "supertrend_bull": ind.get("supertrend_bull"),
            "adx":             ind.get("adx"),
            "adx_bullish":     ind.get("adx_bullish"),
            "above_vwap":      ind.get("above_vwap"),
            "vwap":            ind.get("vwap"),
            "golden_cross":    ind.get("golden_cross"),
            "ema20":           ind.get("ema20"),
            "ema50":           ind.get("ema50"),
            "rel_volume":      ind.get("rel_volume"),
            "breakout_setup":  ind.get("breakout_setup"),
            "pct_from_52h":    ind.get("pct_from_52h"),
            "week52_high":     ind.get("week52_high"),
            "week52_low":      ind.get("week52_low"),
            "atr":             ind.get("atr"),

            # Fundamental data
            "pe":              fund.get("pe")              if fund else None,
            "peg":             fund.get("peg")             if fund else None,
            "pb":              fund.get("pb")              if fund else None,
            "roe":             fund.get("roe")             if fund else None,
            "debt_equity":     fund.get("debt_equity")     if fund else None,
            "earnings_growth": fund.get("earnings_growth") if fund else None,
            "revenue_growth":  fund.get("revenue_growth")  if fund else None,
            "fcf_positive":    fund.get("fcf_positive")    if fund else None,
            "profit_margin":   fund.get("profit_margin")   if fund else None,
            "analyst_target":  fund.get("analyst_target")  if fund else None,
            "analyst_upside":  fund.get("analyst_upside")  if fund else None,
            "num_analysts":    fund.get("num_analysts")    if fund else None,
            "promoter_holding":fund.get("promoter_holding")if fund else None,
            "promoter_increasing": fund.get("promoter_increasing") if fund else None,
            "market_cap":      fund.get("market_cap")      if fund else None,

            # Signals
            "tech_signals":    tech_signals,
            "fund_signals":    fund_signals,
            "bullish_reasons": bullish_reasons,
            "bearish_reasons": bearish_reasons,
        }))

    except Exception as e:
        print(f"[ANALYSE ERR] {sym}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/sector-pulse")
@require_auth
def sector_pulse():
    """
    Returns sector conviction scores combining:
    1. AI news sentiment (Claude via Anthropic API call from backend)
    2. Seasonality (calendar-based historical patterns)
    3. Technical momentum (from cached short-term scan)
    """
    try:
        now        = datetime.now(IST)
        month_name = now.strftime("%B")
        year       = now.year
        month_num  = now.month

        # ── Step 1: Get news sentiment from Claude ──
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        sector_data = {}

        if api_key and ANTHROPIC_AVAILABLE:
            try:
                client = _anthropic_lib.Anthropic(api_key=api_key)
                prompt = build_sector_pulse_prompt(month_name, year)
                msg = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=3000,
                    system="You are a senior equity research analyst. Return only valid JSON as instructed.",
                    messages=[{"role":"user","content":prompt}]
                )
                text = msg.content[0].text
                s, e = text.find("{"), text.rfind("}")
                if s != -1 and e > s:
                    parsed = json.loads(text[s:e+1])
                    global_theme = parsed.get("global_theme","")
                    india_theme  = parsed.get("india_theme","")
                    for item in parsed.get("sectors",[]):
                        sector_data[item["name"]] = item
            except Exception as ex:
                print(f"[SECTOR AI] {ex}")
                global_theme = "AI and technology driving global equity markets with semiconductor demand at multi-year highs."
                india_theme  = "India's capex cycle and domestic consumption remain strong amid global uncertainty."
        else:
            global_theme = "AI and technology driving global equity markets. Semiconductor demand at multi-year highs."
            india_theme  = "India infrastructure push, defence indigenisation, and PLI schemes driving domestic manufacturing."

        # ── Step 2: Build combined sector scores ──
        results = []
        for sector, stocks_list in SECTOR_STOCKS.items():
            seasonal_score = get_seasonality_score(sector)
            seasonal_reason = get_seasonality_reason(sector)
            tech_avg, tech_syms = get_sector_tech_score(sector)

            # News score from Claude (default 5 if not available)
            ai_data = sector_data.get(sector, {})
            news_score  = ai_data.get("news_score", 5)
            conviction  = ai_data.get("conviction", "MEDIUM")
            trend       = ai_data.get("trend", "NEUTRAL")
            catalyst    = ai_data.get("catalyst", "")
            risk        = ai_data.get("risk", "")
            why         = ai_data.get("why", "")

            # Combined conviction score (out of 10)
            # News: 0-4 pts | Seasonality: 0-3 pts | Technical: 0-3 pts
            news_pts     = round(news_score / 10 * 4, 1)
            seasonal_pts = round((seasonal_score / 3) * 3, 1)
            tech_pts     = round(min(tech_avg / 17 * 3, 3), 1) if tech_avg else 1.0

            combined = round(news_pts + seasonal_pts + tech_pts, 1)

            # Boom conviction label
            if combined >= 8.0:
                boom = "🔥 VERY HIGH"
                boom_color = "#00e5a0"
            elif combined >= 6.5:
                boom = "⚡ HIGH"
                boom_color = "#3d9bff"
            elif combined >= 5.0:
                boom = "📈 MODERATE"
                boom_color = "#f59e0b"
            elif combined >= 3.5:
                boom = "➡️ NEUTRAL"
                boom_color = "#6888a8"
            else:
                boom = "📉 WEAK"
                boom_color = "#ff4d6d"

            results.append(sanitise({
                "sector":           sector,
                "combined_score":   combined,
                "boom_label":       boom,
                "boom_color":       boom_color,
                "conviction":       conviction,
                "trend":            trend,
                "news_score":       news_score,
                "news_pts":         news_pts,
                "seasonal_score":   seasonal_score,
                "seasonal_pts":     seasonal_pts,
                "tech_pts":         tech_pts,
                "catalyst":         catalyst,
                "risk":             risk,
                "why":              why,
                "seasonal_reason":  seasonal_reason,
                "top_stocks":       stocks_list[:5],
                "tech_stocks":      tech_syms,
            }))

        results.sort(key=lambda x: x["combined_score"], reverse=True)

        return jsonify({
            "status":       "success",
            "scan_time":    now.strftime("%I:%M %p IST"),
            "date":         now.strftime("%d %b %Y"),
            "month":        month_name,
            "global_theme": global_theme,
            "india_theme":  india_theme,
            "sectors":      results,
        })

    except Exception as e:
        print(f"[SECTOR PULSE ERR] {e}")
        return jsonify({"status":"error","message":str(e)}), 500

@app.route("/health")
def health():
    now = datetime.now(IST)
    with cache_lock:
        last    = cache["last_run"].strftime("%I:%M %p IST") if cache["last_run"] else "not yet"
        running = cache["running"]
        count   = cache["scan_count"]
    with lt_cache_lock:
        lt_last    = lt_cache["last_run"].strftime("%I:%M %p IST") if lt_cache["last_run"] else "not yet"
        lt_running = lt_cache["running"]
        lt_count   = lt_cache["scan_count"]
    return jsonify({
        "status":       "running",
        "service":      "NSESignal Pro v6",
        "time":         now.strftime("%I:%M:%S %p IST"),
        "last_scan":    last,
        "scan_running": running,
        "scan_count":   count,
        "lt_last_scan": lt_last,
        "lt_running":   lt_running,
        "lt_count":     lt_count,
        "watchlist":    len(WATCHLIST),
        "message":      "Backend live — technical scans every 5 min, fundamental scans every 6 hrs"
    })

@app.route("/scan")
@require_auth
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
@require_auth
def refresh():
    with cache_lock:
        running = cache["running"]
    if not running:
        threading.Thread(target=run_scan_background, daemon=True).start()
        return jsonify({"status": "started"})
    return jsonify({"status": "already_running"})

@app.route("/lt-scan")
@require_auth
def lt_scan():
    """Long-term value/growth screener — returns cached fundamental analysis."""
    try:
        with lt_cache_lock:
            result  = lt_cache["result"]
            running = lt_cache["running"]

        if result is None:
            if not running:
                threading.Thread(target=run_lt_scan_background, daemon=True).start()
            # Fundamentals scan takes longer — wait up to 150s
            for _ in range(150):
                time.sleep(1)
                with lt_cache_lock:
                    if lt_cache["result"] is not None:
                        return jsonify(lt_cache["result"])
            return jsonify({
                "status":  "scanning",
                "message": "First long-term scan in progress — retry in 60 seconds",
                "top15":   []
            }), 202

        return jsonify(result)
    except Exception as e:
        print(f"[LT SCAN ERR] {e}")
        return jsonify({"status": "error", "message": str(e), "top15": []}), 500

@app.route("/lt-refresh")
@require_auth
def lt_refresh():
    with lt_cache_lock:
        running = lt_cache["running"]
    if not running:
        threading.Thread(target=run_lt_scan_background, daemon=True).start()
        return jsonify({"status": "started"})
    return jsonify({"status": "already_running"})

@app.route("/indices")
@require_auth
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
        threading.Thread(target=lt_background_scheduler, daemon=True).start()
        threading.Thread(target=keep_alive, daemon=True).start()
        print("[STARTUP] Background threads started (short-term + long-term + keep-alive)")

if __name__ == "__main__":
    threading.Thread(target=background_scheduler, daemon=True).start()
    threading.Thread(target=lt_background_scheduler, daemon=True).start()
    threading.Thread(target=keep_alive, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    print(f"NSESignal Pro v5 — {len(WATCHLIST)} stocks — port {port}")
    app.run(host="0.0.0.0", port=port)
