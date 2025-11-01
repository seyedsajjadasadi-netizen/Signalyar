# ================== بخش ۱: تنظیمات، ایمپورت‌ها، تلگرام، لاگ‌گیری پایه ==================
import os
import time
import random
import requests
import pandas as pd
import ta
import glob
import json
import warnings
import csv

from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple

warnings.filterwarnings("ignore", message="The NumPy module was reloaded")

# تنظیمات تلگرام
TOKEN = "8247452747:AAFCwJukYuFd3cRctYk1Q3yFW_r_LJl4Elk"                 # توکن تلگرام
CHAT_ID_USER = "8384524553"          # آی‌دی چت خصوصی (دایرکت خودت)
CHAT_ID_CHANNEL = "-1002024805980"       # آی‌دی کانال خصوصی (برای سیگنال‌ها)
BOT_API = f"https://api.telegram.org/bot{TOKEN}"

# افزونه کنترل گزارش خودکار
ENABLE_AUTO_REPORTS = False  # اگر True شود، زمان‌بندی گزارش‌های انتهای run_once فعال می‌شود

# ================== آماده‌سازی مسیرها و فایل‌های پایه ==================
LOG_DIR = "logs"
LOG_BASE_NAME = "signals"
LOG_EXT = ".csv"

def ensure_log_dir():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def get_current_log_path():
    ensure_log_dir()
    return os.path.join(LOG_DIR, f"{LOG_BASE_NAME}{LOG_EXT}")

def init_log(path):
    ensure_log_dir()
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("timestamp,symbol,signal,entry,stop,target1,target2,target3,rrr,tf,analysis,data_source,reason,result\n")

# گزارش‌ها
REPORTS_DIR = "logs"
REPORTS_BASE_NAME = "reports"
REPORTS_EXT = ".csv"

def ensure_reports_dir():
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)

def get_reports_path():
    ensure_reports_dir()
    return os.path.join(REPORTS_DIR, f"{REPORTS_BASE_NAME}{REPORTS_EXT}")

def init_reports_log(path):
    ensure_reports_dir()
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("timestamp,type,message\n")

# اجرای اولیه
init_log(get_current_log_path())
init_reports_log(get_reports_path())

# فهرست کوین‌ها
SYMBOLS = [
    "bitcoin","ethereum","binancecoin","solana","ripple","dogecoin","cardano",
    "avalanche-2","polkadot","chainlink","litecoin","bitcoin-cash","tron",
    "cosmos","aave","uniswap","near","internet-computer","ethereum-classic",
    "the-open-network","sui","aptos","ondo-finance","arbitrum","optimism","shiba-inu",
    "pepe","band-protocol","stellar","filecoin","the-sandbox","worldcoin","mantra",
    "pancakeswap-token","algorand","pax-gold"
]

# نمادهای نمایشی
symbol_display = {
    "bitcoin": "BTC","ethereum": "ETH","binancecoin": "BNB","solana": "SOL",
    "ripple": "XRP","dogecoin": "DOGE","cardano": "ADA","avalanche-2": "AVAX",
    "polkadot": "DOT","chainlink": "LINK","litecoin": "LTC","bitcoin-cash": "BCH",
    "tron": "TRX","cosmos": "ATOM","aave": "AAVE","uniswap": "UNI",
    "near": "NEAR","internet-computer": "ICP","ethereum-classic": "ETC",
    "the-open-network": "TON","sui": "SUI","aptos": "APT","ondo-finance": "ONDO",
    "arbitrum": "ARB","optimism": "OP","shiba-inu": "SHIB","pepe": "PEPE",
    "band-protocol": "BAND","stellar": "XLM","filecoin": "FIL","the-sandbox": "SAND",
    "worldcoin": "WLD","mantra": "OM","pancakeswap-token": "CAKE","algorand": "ALGO",
    "pax-gold": "PAXG"
}

# تایم‌فریم‌ها
TIMEFRAMES = ["30min","1h","2h","4h"]

# مسیر لاگ واحد و استاندارد
def get_current_log_path():
    return os.path.join("logs", "signals.csv")

# پایدارسازی آخرین سیگنال‌ها
LAST_SIGNALS_PATH = "last_signals.json"
last_signals: Dict[Tuple[str, str], Dict[str, str]] = {}

def load_last_signals():
    """بارگذاری آخرین سیگنال‌ها از فایل JSON (اگر وجود داشته باشد)"""
    global last_signals
    try:
        if os.path.exists(LAST_SIGNALS_PATH):
            with open(LAST_SIGNALS_PATH, "r", encoding="utf-8") as f:
                last_signals = json.load(f)
        else:
            last_signals = {}
    except Exception as e:
        print("❌ خطا در load_last_signals:", e)
        last_signals = {}

# پیام‌های انگیزشی
motivation_messages = [
    "⏳ صبر یعنی واکنش در بهترین فرصت، نه در اولین فرصت 💡",
    "💪 تریدر موفق کسیه که منتظر بهترین موقعیت می‌مونه.",
    "🧘‍♂️ آرامش = تصمیم درست. بازار همیشه فرصت می‌ده.",
    "🚀 موفقیت در ترید = صبر ✚ نظم ✚ مدیریت سرمایه"
]

# سوییچ تایم‌زون
USE_IRAN_TZ = True
IRAN_TZ = timezone(timedelta(hours=3, minutes=30))

def now_dt():
    return datetime.now(IRAN_TZ if USE_IRAN_TZ else timezone.utc)

# ================== توابع ارسال ==================

# سیگنال‌ها → هم کانال + هم چت خصوصی
def send_signal(text: str):
    try:
        if not TOKEN:
            return
        url = f"{BOT_API}/sendMessage"
        if CHAT_ID_CHANNEL:
            requests.post(url, data={"chat_id": CHAT_ID_CHANNEL, "text": text}, timeout=12)
        if CHAT_ID_USER:
            requests.post(url, data={"chat_id": CHAT_ID_USER, "text": text}, timeout=12)
        return
    except Exception as e:
        print("❌ خطا در ارسال سیگنال:", e)
        return

# گزارش‌ها و منو → فقط چت خصوصی
def send_report(text: str):
    try:
        if not TOKEN or not CHAT_ID_USER:
            return
        url = f"{BOT_API}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID_USER, "text": text}, timeout=12)
        return
    except Exception as e:
        print("❌ خطا در ارسال گزارش:", e)
        return

    # ================== بخش ۱/۲: ساخت گزارش ساده روزانه ==================
import pandas as pd
from datetime import datetime, timedelta, timezone

# ---------- خواندن لاگ سیگنال‌ها ----------
def _read_logs_df():
    try:
        df = pd.read_csv(get_current_log_path(), encoding="utf-8")
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return df
    except Exception as e:
        print("❌ خطا در خواندن لاگ:", e)
        return pd.DataFrame()

# ---------- outcome دقیق‌تر ----------
def outcome(row):
    r = str(row.get("result", "")).strip().lower()
    reason_raw = str(row.get("reason", "")).strip()
    reason = reason_raw.lower()

    stop_keys = ["stopped", "stop", "استاپ", "باطل", "لغو"]
    hit_keys = ["hit_target", "target1", "target", "تارگت", "تارگت اول"]

    if r in ["stopped", "stop"] or any(k in reason for k in stop_keys):
        return "stopped"
    if r in ["hit_target"] or any(k in reason for k in hit_keys):
        return "hit_target"
    return "active"

# ---------- ساخت پیام خلاصه ----------
def _aggregate_performance(df):
    total = len(df)
    stopped = sum(outcome(r) == "stopped" for _, r in df.iterrows())
    hit = sum(outcome(r) == "hit_target" for _, r in df.iterrows())
    active = total - stopped - hit
    return {"total": total, "stopped": stopped, "hit": hit, "active": active}

def _build_summary_message(title, stats):
    return (f"{title}\n"
            f"- کل سیگنال‌ها: {stats['total']}\n"
            f"- تارگت خورده: {stats['hit']}\n"
            f"- استاپ خورده: {stats['stopped']}\n"
            f"- فعال: {stats['active']}")

# ---------- گزارش روزانه ----------
def send_daily_summary(return_only: bool = True):
    df = _read_logs_df()
    if df.empty:
        msg = "📊 گزارش روزانه:\nداده‌ای موجود نیست."
    else:
        cutoff = pd.Timestamp.now(tz=IRAN_TZ if USE_IRAN_TZ else timezone.utc) - pd.Timedelta(days=1)
        df_d = df[df["timestamp"] >= cutoff]
        stats = _aggregate_performance(df_d)
        msg = _build_summary_message("📊 گزارش روزانه", stats)

    if return_only:
        return msg
    else:
        send_report(msg)
        return msg

        # ================== بخش ۱/۳: گزارش‌های ساده روزانه، هفتگی، ماهانه ==================
import pandas as pd
from datetime import datetime, timedelta, timezone

# ---------- خواندن لاگ سیگنال‌ها ----------
def _read_logs_df():
    try:
        df = pd.read_csv(get_current_log_path(), encoding="utf-8")
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        return df
    except Exception as e:
        print("❌ خطا در خواندن لاگ:", e)
        return pd.DataFrame()

# ---------- outcome دقیق‌تر ----------
def outcome(row):
    r = str(row.get("result", "")).strip().lower()
    reason_raw = str(row.get("reason", "")).strip()
    reason = reason_raw.lower()

    stop_keys = ["stopped", "stop", "استاپ", "باطل", "لغو"]
    hit_keys = ["hit_target", "target1", "target", "تارگت", "تارگت اول"]

    if r in ["stopped", "stop"] or any(k in reason for k in stop_keys):
        return "stopped"
    if r in ["hit_target"] or any(k in reason for k in hit_keys):
        return "hit_target"
    return "active"

# ---------- ساخت پیام خلاصه ----------
def _aggregate_performance(df):
    total = len(df)
    stopped = sum(outcome(r) == "stopped" for _, r in df.iterrows())
    hit = sum(outcome(r) == "hit_target" for _, r in df.iterrows())
    active = total - stopped - hit
    return {"total": total, "stopped": stopped, "hit": hit, "active": active}

def _build_summary_message(title, stats):
    return (f"{title}\n"
            f"- کل سیگنال‌ها: {stats['total']}\n"
            f"- تارگت خورده: {stats['hit']}\n"
            f"- استاپ خورده: {stats['stopped']}\n"
            f"- فعال: {stats['active']}")

# ---------- گزارش روزانه ----------
def send_daily_summary(return_only: bool = True):
    df = _read_logs_df()
    if df.empty:
        msg = "📊 گزارش روزانه:\nداده‌ای موجود نیست."
    else:
        cutoff = pd.Timestamp.now(tz=IRAN_TZ if USE_IRAN_TZ else timezone.utc) - pd.Timedelta(days=1)
        df_d = df[df["timestamp"] >= cutoff]
        stats = _aggregate_performance(df_d)
        msg = _build_summary_message("📊 گزارش روزانه", stats)

    if return_only:
        return msg
    else:
        send_report(msg)
        return msg

# ---------- گزارش هفتگی ----------
def send_weekly_summary(return_only: bool = True):
    df = _read_logs_df()
    if df.empty:
        msg = "📆 گزارش هفتگی:\nداده‌ای موجود نیست."
    else:
        now = pd.Timestamp.now(tz=IRAN_TZ if USE_IRAN_TZ else timezone.utc)
        start = now - pd.Timedelta(days=7)
        df_w = df[df["timestamp"] >= start]
        stats = _aggregate_performance(df_w)
        msg = _build_summary_message("📆 گزارش هفتگی", stats)

    if return_only:
        return msg
    else:
        send_report(msg)
        return msg

# ---------- گزارش ماهانه ----------
def send_monthly_summary(return_only: bool = True):
    df = _read_logs_df()
    if df.empty:
        msg = "🗓️ گزارش ماهانه:\nداده‌ای موجود نیست."
    else:
        now = pd.Timestamp.now(tz=IRAN_TZ if USE_IRAN_TZ else timezone.utc)
        start = now - pd.Timedelta(days=30)
        df_m = df[df["timestamp"] >= start]
        stats = _aggregate_performance(df_m)
        msg = _build_summary_message("🗓️ گزارش ماهانه", stats)

    if return_only:
        return msg
    else:
        send_report(msg)
        return msg

# ================== بخش ۲: دریافت داده‌ها و نرمال‌سازی ==================
def fetch_data(coin_id, days=30):
    try:
        # استثنا برای MATIC (اولویت با Binance)
        if coin_id == "polygon":
            try:
                symbol = "MATICUSDT"
                url = "https://api.binance.com/api/v3/klines"
                r = requests.get(url, params={"symbol": symbol, "interval": "1h", "limit": 500}, timeout=20)
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data, columns=[
                        "ts","Open","High","Low","Close","Volume","c1","c2","c3","c4","c5","c6"
                    ])
                    ts_col = pd.to_datetime(pd.to_numeric(df["ts"]), unit="ms", utc=True)
                    df["ts"] = ts_col.dt.tz_convert(IRAN_TZ) if USE_IRAN_TZ else ts_col
                    for col in ["Open","High","Low","Close","Volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna()
                    if not df.empty:
                        return df[["ts","Open","High","Low","Close","Volume"]].set_index("ts"), "Binance (MATIC priority)"
            except Exception as e:
                print(f"⚠️ MATIC → خطا Binance: {e}")

        # کوین‌های بزرگ: CoinGecko OHLC
        big_coins = ["bitcoin","ethereum","binancecoin","solana","ripple","dogecoin","cardano"]
        if coin_id in big_coins:
            try:
                url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc"
                r = requests.get(url, params={"vs_currency": "usd", "days": days}, timeout=20)
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data, columns=["ts","Open","High","Low","Close"])
                    ts_col = pd.to_datetime(pd.to_numeric(df["ts"]), unit="ms", utc=True)
                    df["ts"] = ts_col.dt.tz_convert(IRAN_TZ) if USE_IRAN_TZ else ts_col
                    for col in ["Open","High","Low","Close"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna()
                    if not df.empty:
                        return df.set_index("ts"), "CoinGecko OHLC"
            except Exception as e:
                print(f"⚠️ {coin_id.upper()} → خطا OHLC: {e}")

        # CoinGecko MarketChart (Resample به 1h)
        try:
            url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
            r = requests.get(url, params={"vs_currency": "usd", "days": days}, timeout=20)
            data = r.json()
            if "prices" in data and data["prices"]:
                dfp = pd.DataFrame(data["prices"], columns=["ts","price"])
                ts_col = pd.to_datetime(pd.to_numeric(dfp["ts"]), unit="ms", utc=True)
                dfp["ts"] = ts_col.dt.tz_convert(IRAN_TZ) if USE_IRAN_TZ else ts_col
                dfp = dfp.set_index("ts").sort_index()
                o = dfp["price"].resample("1h").first()
                h = dfp["price"].resample("1h").max()
                l = dfp["price"].resample("1h").min()
                c = dfp["price"].resample("1h").last()
                df = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c}).dropna()
                if not df.empty:
                    return df, "CoinGecko MarketChart"
        except Exception as e:
            print(f"⚠️ {coin_id.upper()} → خطا MarketChart: {e}")

        # Binance fallback
        try:
            sym = symbol_display.get(coin_id, "").upper()
            if sym:
                symbol = f"{sym}USDT"
                url = "https://api.binance.com/api/v3/klines"
                r = requests.get(url, params={"symbol": symbol, "interval": "1h", "limit": 500}, timeout=20)
                data = r.json()
                if isinstance(data, list) and len(data) > 0:
                    df = pd.DataFrame(data, columns=[
                        "ts","Open","High","Low","Close","Volume","c1","c2","c3","c4","c5","c6"
                    ])
                    ts_col = pd.to_datetime(pd.to_numeric(df["ts"]), unit="ms", utc=True)
                    df["ts"] = ts_col.dt.tz_convert(IRAN_TZ) if USE_IRAN_TZ else ts_col
                    for col in ["Open","High","Low","Close","Volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna()
                    if not df.empty:
                        return df[["ts","Open","High","Low","Close","Volume"]].set_index("ts"), "Binance"
        except Exception as e:
            print(f"⚠️ {coin_id.upper()} → خطا Binance: {e}")

        # KuCoin fallback
        try:
            sym = symbol_display.get(coin_id, "").upper()
            if sym:
                symbol = f"{sym}-USDT"
                url = "https://api.kucoin.com/api/v1/market/candles"
                r = requests.get(url, params={"symbol": symbol, "type": "1hour"}, timeout=20)
                data = r.json()
                if data.get("data"):
                    df = pd.DataFrame(data["data"], columns=["ts","Open","Close","High","Low","Volume","Turnover"])
                    ts_col = pd.to_datetime(pd.to_numeric(df["ts"]), unit="s", utc=True)
                    df["ts"] = ts_col.dt.tz_convert(IRAN_TZ) if USE_IRAN_TZ else ts_col
                    for col in ["Open","High","Low","Close","Volume"]:
                        df[col] = pd.to_numeric(df[col], errors="coerce")
                    df = df.dropna()
                    if not df.empty:
                        return df[["ts","Open","Close","High","Low","Volume"]].set_index("ts"), "KuCoin"
        except Exception as e:
            print(f"⚠️ {coin_id.upper()} → خطا KuCoin: {e}")

        # SimplePrice fallback
        try:
            url = "https://api.coingecko.com/api/v3/simple/price"
            r = requests.get(url, params={"ids": coin_id, "vs_currencies": "usd"}, timeout=10)
            data = r.json()
            if coin_id in data and "usd" in data[coin_id]:
                price = float(data[coin_id]["usd"])
                now_ts = pd.Timestamp.now(tz=IRAN_TZ) if USE_IRAN_TZ else pd.Timestamp.utcnow().tz_localize("UTC")
                df = pd.DataFrame(
                    {"Open": [price], "High": [price], "Low": [price], "Close": [price]},
                    index=[now_ts]
                )
                return df, "SimplePrice"
        except Exception as e:
            print(f"⚠️ {coin_id.upper()} → خطا SimplePrice: {e}")

        print(f"❌ {coin_id.upper()} → هیچ داده‌ای پیدا نشد")
        return None, None

    except Exception as e:
        print(f"⛔ خطا کلی fetch_data برای {coin_id}: {e}")
        return None, None

        # ------------------ بخش ۳: سیگنال اندیکاتوری ------------------
def get_indicator_signal(df):
    """
    بررسی سیگنال اندیکاتوری:
    - EMA9 و EMA21 برای کراس کوتاه‌مدت
    - EMA50 برای روند کلی
    - MACD (25,7,80) برای مومنتوم
    - RSI برای قدرت نسبی
    """
    if df is None or df.empty or len(df) < 60:
        return None

    close = df["Close"]

    # EMA ها
    ema9  = ta.trend.EMAIndicator(close, 9).ema_indicator()
    ema21 = ta.trend.EMAIndicator(close, 21).ema_indicator()
    ema50 = ta.trend.EMAIndicator(close, 50).ema_indicator()

    # MACD با پارامترهای انتخابی (25,7,80)
    macd  = ta.trend.MACD(close, window_slow=25, window_fast=7, window_sign=80)

    # RSI
    rsi   = ta.momentum.RSIIndicator(close, 14).rsi()

    # --- شرط خرید ---
    if ema9.iloc[-1] > ema21.iloc[-1] \
       and close.iloc[-1] > ema50.iloc[-1] \
       and macd.macd().iloc[-1] > macd.macd_signal().iloc[-1] \
       and rsi.iloc[-1] > 55:
        return "buy"

    # --- شرط فروش ---
    if ema9.iloc[-1] < ema21.iloc[-1] \
       and close.iloc[-1] < ema50.iloc[-1] \
       and macd.macd().iloc[-1] < macd.macd_signal().iloc[-1] \
       and rsi.iloc[-1] < 45:
        return "sell"

    return None
# ================== بخش ۴: الگوهای کندلی، سیگنال کلاسیک و Wyckoff ==================
def candle_patterns(df):
    """
    تشخیص الگوهای کندلی ساده:
    - Bullish/Bearish Engulfing
    - Hammer / Hanging Man
    - Double Top / Double Bottom
    """
    if df is None or df.empty or len(df) < 12:
        return None

    o = df["Open"].iloc[-2:]
    h = df["High"].iloc[-2:]
    l = df["Low"].iloc[-2:]
    c = df["Close"].iloc[-2:]

    # Bullish Engulfing
    if c.iloc[-2] < o.iloc[-2] and c.iloc[-1] > o.iloc[-1] \
       and c.iloc[-1] > o.iloc[-2] and o.iloc[-1] < c.iloc[-2]:
        return "bullish_engulfing"

    # Bearish Engulfing
    if c.iloc[-2] > o.iloc[-2] and c.iloc[-1] < o.iloc[-1] \
       and c.iloc[-1] < o.iloc[-2] and o.iloc[-1] > c.iloc[-2]:
        return "bearish_engulfing"

    # Hammer / Hanging Man
    body  = abs(c.iloc[-1] - o.iloc[-1])
    lower = min(o.iloc[-1], c.iloc[-1]) - l.iloc[-1]
    upper = h.iloc[-1] - max(c.iloc[-1], o.iloc[-1])

    if body < lower * 0.3 and upper < body:
        return "hammer"
    if body < lower * 0.3 and upper > body:
        return "hanging_man"

    # Double Top / Double Bottom
    if abs(df["High"].tail(10).max() - df["High"].iloc[-1]) < 0.01 * df["High"].tail(10).mean():
        return "double_top"
    if abs(df["Low"].tail(10).min() - df["Low"].iloc[-1]) < 0.01 * df["Low"].tail(10).mean():
        return "double_bottom"

    return None

def get_classic_signal(df):
    """
    سیگنال کلاسیک بر اساس:
    - الگوهای کندلی
    - RSI
    - شکست سقف/کف اخیر
    """
    if df is None or df.empty or len(df) < 25:
        return None

    c = df["Close"].iloc[-1]
    rsi_val = ta.momentum.RSIIndicator(df["Close"], 14).rsi().iloc[-1]
    pattern = candle_patterns(df)

    if pattern in ["bullish_engulfing", "hammer", "double_bottom"] and rsi_val > 45:
        return "buy"
    if pattern in ["bearish_engulfing", "hanging_man", "double_top"] and rsi_val < 55:
        return "sell"

    if c > df["High"].iloc[-4:-1].max() and rsi_val > 50:
        return "buy"
    if c < df["Low"].iloc[-4:-1].min() and rsi_val < 50:
        return "sell"

    return None

def wyckoff_phase(df):
    """
    تشخیص فازهای وایکوف (ساده‌شده):
    - Accumulation
    - Distribution
    """
    if df is None or df.empty or len(df) < 30:
        return None

    closes = df["Close"].tail(20)
    highs  = df["High"].tail(20)
    lows   = df["Low"].tail(20)
    rng    = highs.max() - lows.min()

    if rng / closes.mean() < 0.1:
        # نرم‌تر از شرط یکنواخت بالا/پایین
        ma = closes.rolling(5).mean().iloc[-1]
        prev_ma = closes.rolling(5).mean().iloc[-5]
        if closes.iloc[-1] > ma and ma > prev_ma:
            return "accumulation"
        if closes.iloc[-1] < ma and ma < prev_ma:
            return "distribution"

    return None

# ================== بخش ۵: SL/TP، هم‌جهتی، فرمت پیام و ساخت کندل‌ها ==================
def calculate_sl_tp(entry, direction, df):
    """
    محاسبه حد ضرر و تارگت‌ها بر اساس ATR.
    شرط: RRR باید >= 2.5 باشد.
    """
    if df is None or df.empty or len(df) < 20:
        return None, None, None

    atr = ta.volatility.AverageTrueRange(
        df["High"], df["Low"], df["Close"], 14
    ).average_true_range().iloc[-1]

    if pd.isna(atr) or atr <= 0:
        return None, None, None

    stop = entry - 1.5 * atr if direction == "buy" else entry + 1.5 * atr
    targets = [
        entry + k * atr if direction == "buy" else entry - k * atr
        for k in (5, 10, 15)
    ]

    risk = abs(entry - stop)
    reward = abs(targets[0] - entry)
    rrr = reward / risk if risk > 0 else 0.0

    if rrr < 2.5:
        return None, None, None

    return stop, targets, rrr

def is_aligned(coin_id: str, tf: str, signal: str, df_base: pd.DataFrame) -> bool:
    """
    بررسی هم‌جهتی سیگنال با تایم‌فریم بالاتر.
    اگر داده کافی نباشد یا تایم‌فریم بالاتر وجود نداشته باشد، True برمی‌گرداند.
    """
    tf_order = {"30min": "1h", "1h": "2h", "2h": "4h", "4h": None}
    higher_tf = tf_order.get(tf)

    if higher_tf is None:
        return True

    df_higher = build_candles(df_base, higher_tf)
    if df_higher is None or df_higher.empty:
        return True

    sig_higher = get_indicator_signal(df_higher) or get_classic_signal(df_higher)
    return sig_higher == signal

def format_price(value: float) -> str:
    if value >= 1000:
        return f"{value:.2f}"
    elif value >= 1:
        return f"{value:.4f}"
    else:
        return f"{value:.8f}"

def format_signal(symbol_id, sig, entry, targets, stop, tf, analysis_type, rrr, reason=None):
    display = symbol_display.get(symbol_id, symbol_id.upper())
    targets = sorted(targets) if sig == "buy" else sorted(targets, reverse=True)

    txt  = f"📊 سیگنال {display}\n\n"
    txt += f"✅ {sig.upper()} | ورود: {format_price(entry)}\n"
    txt += "🎯 تارگت‌ها:\n"
    for i, t in enumerate(targets, 1):
        txt += f"{i}) {format_price(t)}\n"

    txt += f"🛑 حد ضرر: {format_price(stop)}\n"
    txt += f"⏱ تایم‌فریم: {tf.upper()}\n"
    txt += f"📌 نوع تحلیل: {analysis_type}\n"
    txt += f"📐 RRR: {rrr:.2f}"

    if reason:
        txt += f"\n🔎 دلیل: {reason}"
        if "fibo" in str(reason).lower():
            txt += "\n🧭 دایورژنس + فیبوناچی"

    return txt

def build_candles(df, tf):
    """
    ساخت کندل‌های جدید بر اساس تایم‌فریم انتخابی.
    """
    try:
        rule = {"30min": "30min", "1h": "1h", "2h": "2h", "4h": "4h"}.get(tf)
        if not rule:
            return None

        o = df["Open"].resample(rule).first()
        h = df["High"].resample(rule).max()
        l = df["Low"].resample(rule).min()
        c = df["Close"].resample(rule).last()
        vol = df["Volume"].resample(rule).sum() if "Volume" in df.columns else None

        df_new = pd.DataFrame({"Open": o, "High": h, "Low": l, "Close": c})
        if vol is not None:
            df_new["Volume"] = vol

        return df_new.dropna()
    except Exception:
        return None

# ================== بخش ۶: رجیستری، شناسه سیگنال، پس‌ازصدور و امتیازدهی ==================
SIX_HOURS = 6 * 3600  # چرخه سراسری ضداسپم برای هر کوین

@dataclass
class SignalMetaAudit:
    coin_id: str
    side: str
    timeframe: str
    entry: float
    stop: float
    targets: List[float]
    rrr: float
    analysis_type: str
    data_source: str

@dataclass
class PostStatus:
    status: str        # "✅ Target1", "❌ باطل‌شده", "⚠️ در حال بررسی", "✅ تأیید شد"
    reason: Optional[str] = None
    ts: float = field(default_factory=time.time)

class SignalRegistry:
    def __init__(self):
        self.last_issuance_ts_per_coin: Dict[str, float] = {}
        self.last_candle_fp: Dict[Tuple[str, str], str] = {}
        self.post_status: Dict[str, PostStatus] = {}
        self.active_signals: Dict[str, SignalMetaAudit] = {}

    def can_issue_6h(self, coin_id: str, now_ts: Optional[float] = None) -> Tuple[bool, str]:
        now_ts = now_ts or time.time()
        last = self.last_issuance_ts_per_coin.get(coin_id)
        if last is None or (now_ts - last) >= SIX_HOURS:
            return True, "OK"
        wait = int(SIX_HOURS - (now_ts - last))
        return False, f"⏳ هنوز {wait} ثانیه تا چرخه ۶ ساعته باقی‌ست"

    def mark_issued(self, coin_id: str, now_ts: Optional[float] = None):
        self.last_issuance_ts_per_coin[coin_id] = now_ts or time.time()

    def is_same_candle(self, coin_id: str, timeframe: str, candle_fp: str) -> bool:
        key = (coin_id, timeframe)
        last_fp = self.last_candle_fp.get(key)
        if last_fp == candle_fp:
            return True
        self.last_candle_fp[key] = candle_fp
        return False

    def set_post_status(self, signal_id: str, status: str, reason: Optional[str] = None):
        self.post_status[signal_id] = PostStatus(status=status, reason=reason)

def make_signal_id(coin_id: str, tf: str, entry: float, ts_iso: str) -> str:
    disp = symbol_display.get(coin_id, coin_id.upper())
    return f"{disp}-{tf}-{format_price(entry)}-{ts_iso}"

def rate_signal(has_confirmation: bool,
                near_key_level: bool,
                breakout_with_volume: bool,
                wyckoff_ok: bool) -> Tuple[int, List[str], str]:
    stars = 3
    notes: List[str] = []

    if has_confirmation:
        stars += 1
    else:
        notes.append("عدم کندل تأییدیه")

    if breakout_with_volume:
        stars += 1
    else:
        notes.append("حجم شکست کافی نیست")

    if near_key_level:
        stars -= 1
        notes.append("نزدیکی به سطح کلیدی → احتمال شکار نقدینگی")

    if not wyckoff_ok:
        stars -= 1
        notes.append("وایکوف ناسازگار")

    stars = max(1, min(5, stars))
    label = "❌ باطل‌شده" if stars <= 2 and not has_confirmation else \
            ("✅ معتبر" if stars >= 4 and has_confirmation else "⚠️ پرریسک")

    return stars, notes, label

def evaluate_post_status(current_price: float,
                         side: str,
                         stop: float,
                         t1: float,
                         confirmation_after_issue: bool) -> Tuple[str, Optional[str]]:
    if side.lower() == "buy":
        if current_price <= stop:
            return "❌ باطل‌شده", "استاپ فعال شد"
        if current_price >= t1:
            return "✅ Target1", "تارگت اول فعال شد"
    else:
        if current_price >= stop:
            return "❌ باطل‌شده", "استاپ فعال شد"
        if current_price <= t1:
            return "✅ Target1", "تارگت اول فعال شد"

    if confirmation_after_issue:
        return "✅ تأیید شد", "کندل تأییدیه بعد از صدور شکل گرفت"

    return "⚠️ در حال بررسی", None

def score_classic_signal(rrr_ok: bool,
                         volume_ok: bool,
                         candle_ok: bool,
                         aligned_ok: bool,
                         ema_ok: bool):
    """
    امتیازدهی سیگنال کلاسیک:
    - 3 شرط → ⚠️ پرریسک
    - 4 شرط → 🟡 متعادل
    - 5 شرط → 🟢 قوی
    - کمتر از 3 شرط → ارسال نشود
    """
    conditions = {
        "RRR ≥ 2.5": rrr_ok,
        "حجم شکست کافی": volume_ok,
        "کندل تأییدیه": candle_ok,
        "هم‌جهتی تایم‌فریم بزرگ‌تر": aligned_ok,
        "موقعیت EMA/MA": ema_ok
    }

    passed = [k for k, v in conditions.items() if v]
    failed = [k for k, v in conditions.items() if not v]
    score = len(passed)

    if score >= 5:
        return 5, "🟢 قوی", failed
    elif score == 4:
        return 4, "🟡 متعادل", failed
    elif score == 3:
        return 3, "⚠️ پرریسک", failed
    else:
        return None, None, None

# ================== بخش ۷: پایدارسازی last_signals ==================
load_last_signals()

# ================== بخش ۷/۲: مدیریت لاگ مداوم ==================
LOG_BASE_NAME = "signals"
LOG_EXT = ".csv"
LOG_DIR = "logs"
MAX_LINES_PER_FILE = 5000

def ensure_log_dir():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

def get_continuous_log_path():
    ensure_log_dir()
    return os.path.join(LOG_DIR, f"{LOG_BASE_NAME}{LOG_EXT}")

def get_next_part_path(current_path):
    ensure_log_dir()
    base = os.path.splitext(os.path.basename(current_path))[0]
    ext = os.path.splitext(current_path)[1]
    if "_part" in base:
        prefix, part = base.split("_part")
        try:
            n = int(part)
            return os.path.join(LOG_DIR, f"{prefix}_part{n+1}{ext}")
        except ValueError:
            return os.path.join(LOG_DIR, f"{LOG_BASE_NAME}_part2{ext}")
    else:
        return os.path.join(LOG_DIR, f"{LOG_BASE_NAME}_part2{ext}")

def init_continuous_log(path):
    ensure_log_dir()
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            # هدر هماهنگ با گزارش‌ها
            f.write("timestamp,symbol,signal,entry,stop,target1,target2,target3,rrr,tf,analysis,data_source,reason,result\n")

def append_log_line_continuous(line, path=None):
    if path is None:
        path = get_continuous_log_path()
    init_continuous_log(path)

    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception:
        return

    if len(lines) > MAX_LINES_PER_FILE:
        next_path = get_next_part_path(path)
        with open(next_path, "w", encoding="utf-8") as nf:
            nf.writelines(lines[:1])  # هدر
        print(f"ℹ️ لاگ از حد {MAX_LINES_PER_FILE} خط گذشت. ادامه در: {next_path}")

        # ================== بخش ۷/۳: پیام انگیزشی بعد از ۲ ساعت ==================
last_signal_time = None

def update_last_signal_time():
    global last_signal_time
    last_signal_time = now_dt()

def check_motivation_message():
    global last_signal_time
    if last_signal_time is None:
        last_signal_time = now_dt()
        return

    diff = now_dt() - last_signal_time
    if diff.total_seconds() >= 2 * 3600:  # ۲ ساعت
        msg = get_random_motivation()
        send_signal(f"🌟 {msg}")   # ← حالا هم کانال هم چت خصوصی
        last_signal_time = now_dt()

def get_random_motivation():
    motivations = [
        "هر روز یک قدم کوچک هم پیشرفت است.",
        "یاد بگیر، تلاش کن، ادامه بده.",
        "موفقیت سهم کسانی است که صبر می‌کنند.",
        "رویاهاتو باور کن، بعد دنبالش برو.",
        "هر سختی پلی است به سمت رشد.",
        "امروز بهترین زمان شروع است.",
        "هیچ تلاشی بی‌ثمر نمی‌ماند."
    ]
    return random.choice(motivations)

    # ================== بخش ۸/۱a: دایورژنس، فیبو، لاگ، رجیستری ==================
def check_divergence(df, rsi_period=14, lookback=30, pivot_lookback=5, threshold=0.5):
    if df is None or df.empty or len(df) < lookback:
        return None

    close = df["Close"].iloc[-lookback:]
    rsi = ta.momentum.RSIIndicator(df["Close"], rsi_period).rsi().iloc[-lookback:]

    def find_pivots(series, lb):
        pivots_high, pivots_low = [], []
        for i in range(lb, len(series) - lb):
            window = series[i - lb:i + lb + 1]
            if series[i] == window.max():
                pivots_high.append((i, series[i]))
            if series[i] == window.min():
                pivots_low.append((i, series[i]))
        return pivots_high, pivots_low

    price_highs, price_lows = find_pivots(close.values, pivot_lookback)
    rsi_highs, rsi_lows = find_pivots(rsi.values, pivot_lookback)

    if len(price_highs) < 2 or len(price_lows) < 2:
        return None

    ph1, ph2 = price_highs[-2], price_highs[-1]
    pl1, pl2 = price_lows[-2], price_lows[-1]
    rh1, rh2 = rsi_highs[-2], rsi_highs[-1] if len(rsi_highs) >= 2 else (None, None)
    rl1, rl2 = rsi_lows[-2], rsi_lows[-1] if len(rsi_lows) >= 2 else (None, None)

    if pl2[1] < pl1[1] and rl2 and rl1 and rl2[1] > rl1[1] + threshold:
        return "regular_bull"
    if ph2[1] > ph1[1] and rh2 and rh1 and rh2[1] < rh1[1] - threshold:
        return "regular_bear"
    if pl2[1] > pl1[1] and rl2 and rl1 and rl2[1] < rl1[1] - threshold:
        return "hidden_bull"
    if ph2[1] < ph1[1] and rh2 and rh1 and rh2[1] > rh1[1] + threshold:
        return "hidden_bear"

    return None


def is_fibo_zone(df, level=0.618, tolerance=0.01):
    if df is None or df.empty:
        return False, level
    price = df["Close"].iloc[-1]
    high, low = df["High"].max(), df["Low"].min()
    fibo_price = high - (high - low) * level
    return abs(price - fibo_price) <= (price * tolerance), level


def log_signal(coin_id, signal, entry, stop, targets, rrr, tf, analysis_type, data_source, reason=None, result=""):
    t1 = format_price(targets[0]) if targets and len(targets) > 0 else ""
    t2 = format_price(targets[1]) if targets and len(targets) > 1 else ""
    t3 = format_price(targets[2]) if targets and len(targets) > 2 else ""
    timestamp = now_dt().isoformat()
    symbol_disp = symbol_display.get(coin_id, coin_id.upper())
    rrr_str = f"{rrr:.2f}" if rrr is not None else ""

    entry_str = format_price(entry) if entry is not None else ""
    stop_str = format_price(stop) if stop is not None else ""
    reason_str = reason or ""

    line = ",".join([
        timestamp,
        symbol_disp,
        signal or "",
        entry_str,
        stop_str,
        t1,
        t2,
        t3,
        rrr_str,
        tf or "",
        analysis_type or "",
        data_source or "",
        reason_str,
        result or ""
    ])
    append_log_line_continuous(line)
    update_last_signal_time()


# ---------- رجیستری وضعیت سیگنال‌ها ----------
registry = SignalRegistry()


# ---------- فیلتر MA20/MA50 ----------
def ma_filter(df, side):
    ma20 = df["Close"].rolling(20).mean().iloc[-1]
    ma50 = df["Close"].rolling(50).mean().iloc[-1]
    price = df["Close"].iloc[-1]

    if side == "buy":
        return price > ma20 and ma20 > ma50
    elif side == "sell":
        return price < ma20 and ma20 < ma50
    return False

    # ================== بخش ۸/۱b: مدیریت ذخیره آخرین سیگنال‌ها (نسخه اصلاح‌شده) ==================
import json, os

LAST_SIGNALS_FILE = "last_signals.json"
last_signals = {}

def save_last_signals():
    try:
        # کلیدها رو به رشته تبدیل می‌کنیم
        serializable = {str(k): v for k, v in last_signals.items()}
        with open(LAST_SIGNALS_FILE, "w", encoding="utf-8") as f:
            json.dump(serializable, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print("❌ خطا در ذخیره last_signals:", e)

def load_last_signals():
    global last_signals
    if os.path.exists(LAST_SIGNALS_FILE):
        try:
            with open(LAST_SIGNALS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            # کلیدها رو دوباره به tuple برگردونیم (coin_id, tf)
            last_signals = {}
            for k, v in data.items():
                if "_" in k:
                    coin_id, tf = k.split("_", 1)
                    last_signals[(coin_id, tf)] = v
                else:
                    last_signals[k] = v
        except Exception as e:
            print("❌ خطا در بارگذاری last_signals:", e)
            last_signals = {}
    else:
        last_signals = {}

        # ================== بخش ۸/۲a: تابع analyze_coin ==================
def analyze_coin(coin_id, now):
    signals = {}
    df_base, data_source = fetch_data(coin_id, days=30)
    if df_base is None or df_base.empty:
        log_signal(coin_id, "none", None, None, None, None,
                   "", "", data_source, reason="No data")
        return {}

    for tf in TIMEFRAMES:
        df_tf = build_candles(df_base, tf)
        if df_tf is None or df_tf.empty or "Close" not in df_tf.columns or len(df_tf["Close"]) == 0:
            continue

        sig_ind = get_indicator_signal(df_tf)
        sig_cls = get_classic_signal(df_tf) if tf in ["2h", "4h"] else None
        score_cls = None
        if sig_cls:
            score_cls, _, _ = score_classic_signal(True, True, True, True, True)

        div = check_divergence(df_tf)
        if div:
            in_618, lvl_618 = is_fibo_zone(df_tf, 0.618)
            in_1618, lvl_1618 = is_fibo_zone(df_tf, 1.618)
            if in_618 or in_1618:
                fibo_label = f"{div}_fibo_{lvl_618 if in_618 else lvl_1618}"
                if div.endswith("bull"):
                    signals[tf] = ("buy", "دایورژنس+فیبو", df_tf, fibo_label, score_cls, data_source)
                elif div.endswith("bear"):
                    signals[tf] = ("sell", "دایورژنس+فیبو", df_tf, fibo_label, score_cls, data_source)

        if tf not in signals:
            if sig_ind and sig_cls and sig_ind == sig_cls:
                signals[tf] = (sig_ind, "قوی (اندیکاتوری + کلاسیک)", df_tf, None, score_cls, data_source)
            elif sig_cls and score_cls is not None and score_cls >= 4:
                signals[tf] = (sig_cls, "کلاسیک (متعادل یا ایده‌آل)", df_tf, None, score_cls, data_source)
            elif sig_ind:
                signals[tf] = (sig_ind, "اندیکاتوری", df_tf, None, score_cls, data_source)

    return signals

    # ================== بخش ۸/۲b: تابع run_once ==================
def run_once():
    global last_signal_time

    print("▶️ شروع اجرای ربات...")
    now = now_dt()
    total = len(SYMBOLS)
    with_data, no_data = 0, 0
    no_data_list, issued_pairs = [], []
    used_symbols = set()
    count_strong = 0
    count_classic = 0
    count_indicator = 0
    count_rejected = 0
    any_signal = False

    # --- پیگیری وضعیت سیگنال‌های فعال ---
    try:
        for sig_id, meta in list(registry.active_signals.items()):
            df_now, _src = fetch_data(meta.coin_id, days=3)
            if df_now is None or df_now.empty or "Close" not in df_now.columns or len(df_now["Close"]) == 0:
                continue
            current_price = float(df_now["Close"].iloc[-1])

            df_1h = build_candles(df_now, "1h")
            conf = False
            if df_1h is not None and not df_1h.empty:
                conf_sig = get_indicator_signal(df_1h) or get_classic_signal(df_1h)
                conf = (conf_sig == meta.side)

            status, reason = evaluate_post_status(current_price, meta.side, meta.stop, meta.targets[0], conf)
            registry.set_post_status(sig_id, status, reason)
            log_signal(meta.coin_id, meta.side, meta.entry, meta.stop, meta.targets, meta.rrr,
                       meta.timeframe, meta.analysis_type, meta.data_source,
                       reason=f"Post-status: {status}" + (f" ({reason})" if reason else ""))

            if status == "❌ باطل‌شده":
                last_key = f"{meta.coin_id}_{meta.timeframe}"
                if last_key in last_signals:
                    last_signals[last_key]["status"] = "stopped"
                    save_last_signals()
    except Exception as e:
        print("❌ خطا در بررسی سیگنال‌های فعال:", e)

    # --- تولید سیگنال‌های جدید ---
    for coin_id in SYMBOLS:
        try:
            signals = analyze_coin(coin_id, now)
        except Exception as e:
            print(f"❌ خطا در analyze_coin {coin_id}:", e)
            no_data += 1
            no_data_list.append(coin_id)
            continue

        if not signals:
            no_data += 1
            no_data_list.append(coin_id)
            continue
        else:
            with_data += 1

        # --- انتخاب و صدور سیگنال نهایی ---
        for tf in reversed(TIMEFRAMES):
            if tf not in signals:
                continue

            sig, analysis_type, df_tf, sig_reason, score_cls, data_source = signals[tf]

            if df_tf is None or df_tf.empty or "Close" not in df_tf.columns or len(df_tf["Close"]) == 0:
                count_rejected += 1
                continue

            try:
                last_ts = df_tf.index[-1]
                entry = float(df_tf["Close"].iloc[-1])
            except Exception:
                count_rejected += 1
                continue

            stop, targets, rrr = calculate_sl_tp(entry, sig, df_tf)
            if not stop or targets is None or len(targets) == 0 or rrr is None:
                log_signal(coin_id, "none", None, None, None, None,
                           tf, analysis_type, data_source,
                           reason="Invalid SL/TP/RRR")
                count_rejected += 1
                continue

            # --- ضدتکرار ۶ ساعته ---
            prev = last_signals.get(f"{coin_id}_{tf}")
            if prev:
                try:
                    prev_time = datetime.fromisoformat(prev.get("time", ""))
                    if prev_time and prev_time.tzinfo is None:
                        prev_time = prev_time.replace(tzinfo=timezone.utc)
                except Exception:
                    prev_time = None

                if prev_time is None:
                    prev_time = now

                diff_hours = (now - prev_time).total_seconds() / 3600.0
                last_status = prev.get("status", "active")

                if diff_hours < 6 and last_status != "stopped":
                    log_signal(
                        coin_id, "none", entry, stop, targets, rrr,
                        tf, analysis_type, data_source,
                        reason=f"Duplicate within 6h window (last {diff_hours:.2f}h ago)"
                    )
                    count_rejected += 1
                    continue

            # --- امتیازدهی کلاسیک + فیلتر MA20/50 ---
            if "کلاسیک" in analysis_type:
                if score_cls is None or score_cls < 4:
                    log_signal(coin_id, "none", entry, stop, targets, rrr,
                               tf, analysis_type, data_source,
                               reason="Classic score < 4 (رد شد)")
                    count_rejected += 1
                    continue

                if not ma_filter(df_tf, sig):
                    log_signal(coin_id, "none", entry, stop, targets, rrr,
                               tf, analysis_type, data_source,
                               reason="MA20/50 filter not passed")
                    count_rejected += 1
                    continue

                sig_reason = (sig_reason or "") + f" | ClassicScore={score_cls}"

            # --- ساخت پیام و ارسال ---
            msg = format_signal(coin_id, sig, entry, targets, stop, tf,
                                analysis_type, rrr, reason=sig_reason)
            send_signal(msg)
            log_signal(coin_id, sig, entry, stop, targets, rrr,
                       tf, analysis_type, data_source, reason=sig_reason)

            registry.mark_issued(coin_id)

            # ✅ اصلاح کلید: رشته‌ای به جای tuple
            last_signals[f"{coin_id}_{tf}"] = {
                "last_ts": last_ts.isoformat(),
                "direction": sig,
                "time": now.isoformat(),
                "status": "active"
            }
            save_last_signals()

            sig_id = make_signal_id(coin_id, tf, entry, last_ts.isoformat())
            registry.active_signals[sig_id] = SignalMetaAudit(
                coin_id=coin_id, side=sig, timeframe=tf, entry=entry,
                stop=stop, targets=targets, rrr=rrr,
                analysis_type=analysis_type, data_source=data_source
            )

            issued_pairs.append(f"{symbol_display.get(coin_id, coin_id.upper())}-{tf}")
            if "قوی" in analysis_type:
                count_strong += 1
            elif "کلاسیک" in analysis_type:
                count_classic += 1
            elif "اندیکاتوری" in analysis_type:
                count_indicator += 1

            any_signal = True
            used_symbols.add(coin_id)
            break  # پس از صدور یک سیگنال برای این کوین، به کوین بعدی برو

    # --- گزارش خلاصه ---
    print("📊 گزارش اجرای نوبت:")
    print(f"کل جفت‌ها: {total}")
    print(f"با داده: {with_data}, بدون داده: {no_data}")
    print(f"سیگنال قوی: {count_strong}, کلاسیک: {count_classic}, اندیکاتوری: {count_indicator}")
    print(f"رد شده: {count_rejected}")
    if issued_pairs:
        print("✅ سیگنال‌های صادر شده:", ", ".join(issued_pairs))
    if no_data_list:
        print("⚠️ بدون داده:", ", ".join(no_data_list))

    # --- پیام انگیزشی در صورت نبود سیگنال ---
    if not any_signal:
        send_motivation_message()

    print("⏹ پایان اجرای ربات.")

    # ================== بخش ۸/۳: گزارش‌های روزانه، هفتگی، ماهانه با جزئیات ==================

def load_log_df():
    """
    بارگذاری لاگ سیگنال‌ها به عنوان DataFrame.
    انتظار ستون‌ها:
    timestamp,symbol,signal,entry,stop,t1,t2,t3,rrr,tf,analysis_type,source,reason,result
    """
    try:
        df = pd.read_csv(LOG_FILE_PATH)
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        # هم‌نام‌سازی اگر ستونی متفاوت بود
        if "analysis" in df.columns and "analysis_type" not in df.columns:
            df = df.rename(columns={"analysis": "analysis_type"})
        return df
    except Exception:
        return None


def build_summary_message(title, df):
    """
    ساخت پیام گزارش با آمار کلی + جزئیات سیگنال‌ها برای بازه داده‌شده.
    """
    if df is None or df.empty:
        return f"{title}\nداده‌ای موجود نیست."

    # محافظت از ستون‌های مورد نیاز
    for col in ["result", "symbol", "analysis_type", "tf", "rrr"]:
        if col not in df.columns:
            df[col] = "" if col != "rrr" else pd.Series(dtype=float)

    res = df["result"].fillna("")
    is_active = (res == "") | (res == "active")
    is_hit = (res == "hit_target")
    is_stopped = (res == "stopped")

    total = len(df)
    hit = int(is_hit.sum())
    stopped = int(is_stopped.sum())
    active = int(is_active.sum())

    avg_rrr = pd.to_numeric(df["rrr"], errors="coerce").mean()
    winrate = (hit / (hit + stopped) * 100) if (hit + stopped) > 0 else 0.0

    lines = []
    if hit > 0:
        lines.append("✅ تارگت‌خورده‌ها:")
        for _, row in df[is_hit].iterrows():
            lines.append(f"- {row.get('symbol','?')} | {row.get('analysis_type','?')} | {row.get('tf','?')}")

    if stopped > 0:
        lines.append("❌ استاپ‌خورده‌ها:")
        for _, row in df[is_stopped].iterrows():
            lines.append(f"- {row.get('symbol','?')} | {row.get('analysis_type','?')} | {row.get('tf','?')}")

    if active > 0:
        lines.append("🔵 در جریان:")
        for _, row in df[is_active].iterrows():
            lines.append(f"- {row.get('symbol','?')} | {row.get('analysis_type','?')} | {row.get('tf','?')}")

    details_text = "\n".join(lines) if lines else "هیچ جزئیاتی موجود نیست."

    msg = (
        f"{title}\n"
        f"🧮 کل سیگنال‌ها: {total}\n"
        f"✅ هدف‌خورده: {hit} | ❌ استاپ: {stopped} | 🔵 در جریان: {active}\n"
        f"🏆 وین‌ریت: {winrate:.1f}% | 📐 میانگین RRR: {avg_rrr:.2f}\n\n"
        f"{details_text}"
    )
    return msg


def build_daily_message():
    df = load_log_df()
    if df is None or df.empty:
        return "📈 گزارش روزانه\nداده‌ای موجود نیست."

    cutoff = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(days=1)
    df_d = df[df["timestamp"] >= cutoff]
    msg = build_summary_message("📈 گزارش روزانه", df_d)
    append_report("daily", msg)
    return msg


def build_weekly_message():
    df = load_log_df()
    if df is None or df.empty:
        return "📊 گزارش هفتگی\nداده‌ای موجود نیست."

    cutoff = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(days=7)
    df_w = df[df["timestamp"] >= cutoff]
    msg = build_summary_message("📊 گزارش هفتگی", df_w)
    append_report("weekly", msg)
    return msg


def build_monthly_message():
    df = load_log_df()
    if df is None or df.empty:
        return "🗓️ گزارش ماهانه\nداده‌ای موجود نیست."

    cutoff = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(days=30)
    df_m = df[df["timestamp"] >= cutoff]
    msg = build_summary_message("🗓️ گزارش ماهانه", df_m)
    append_report("monthly", msg)
    return msg

    # ================== بخش :8/4 مدیریت گزارش‌ها (reports.csv) ================
REPORTS_BASE_NAME = "reports"
REPORTS_EXT = ".csv"
REPORTS_DIR = "logs"
MAX_REPORT_LINES = 5000

def ensure_reports_dir():
    if not os.path.exists(REPORTS_DIR):
        os.makedirs(REPORTS_DIR)

def get_reports_path():
    ensure_reports_dir()
    return os.path.join(REPORTS_DIR, f"{REPORTS_BASE_NAME}{REPORTS_EXT}")

def get_next_report_part(current_path):
    ensure_reports_dir()
    base = os.path.splitext(os.path.basename(current_path))[0]
    ext = os.path.splitext(current_path)[1]
    if "_part" in base:
        prefix, part = base.split("_part")
        try:
            n = int(part)
            return os.path.join(REPORTS_DIR, f"{prefix}_part{n+1}{ext}")
        except ValueError:
            return os.path.join(REPORTS_DIR, f"{REPORTS_BASE_NAME}_part2{ext}")
    else:
        return os.path.join(REPORTS_DIR, f"{REPORTS_BASE_NAME}_part2{ext}")

def init_reports_log(path):
    ensure_reports_dir()
    if not os.path.exists(path):
        with open(path, "w", encoding="utf-8") as f:
            f.write("timestamp,type,message\n")

def append_report(report_type: str, msg: str, path=None):
    if path is None:
        path = get_reports_path()
    init_reports_log(path)

    timestamp = now_dt().isoformat()
    line = f"{timestamp},{report_type},{json.dumps(msg, ensure_ascii=False)}"

    # نوشتن در فایل فعلی
    with open(path, "a", encoding="utf-8") as f:
        f.write(line + "\n")

    # بررسی طول و ایجاد پارت بعدی با هدر (برای خواندن‌های بعدی)
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        if len(lines) > MAX_REPORT_LINES:
            next_path = get_next_report_part(path)
            if not os.path.exists(next_path):
                with open(next_path, "w", encoding="utf-8") as nf:
                    nf.write(lines[0])  # فقط هدر
            print(f"ℹ️ گزارش‌ها از حد {MAX_REPORT_LINES} خط گذشت. ادامه در: {next_path}")
    except Exception:
        pass

def get_last_report(report_type: str):
    try:
        files = sorted([
            os.path.join(REPORTS_DIR, f) for f in os.listdir(REPORTS_DIR)
            if f.startswith(REPORTS_BASE_NAME) and f.endswith(REPORTS_EXT)
        ])
        if not files:
            return f"هیچ گزارشی از نوع {report_type} موجود نیست."

        # از جدیدترین فایل‌ها به عقب
        for path in reversed(files):
            df = pd.read_csv(path)
            if "type" not in df.columns or "message" not in df.columns:
                continue
            sel = df[df["type"] == report_type]
            if sel.empty:
                continue
            msg_raw = sel.iloc[-1]["message"]
            try:
                msg = json.loads(msg_raw)  # پیام‌ها json.dumps شده‌اند
            except Exception:
                msg = str(msg_raw)
            return msg

        return f"هیچ گزارشی از نوع {report_type} موجود نیست."
    except Exception as e:
        return f"خطا در خواندن گزارش {report_type}: {e}"

# ================== بخش ۸/۵: گزارش‌گیری روزانه، هفتگی و ماهانه ==================
from datetime import datetime, timedelta

# فلگ‌ها برای جلوگیری از تکرار (فقط یک‌بار در هر پنجره‌ی زمان)
daily_done_today = False
weekly_done_this_week = False
monthly_done_this_month = False

def _in_nightly_window(now: datetime) -> bool:
    """
    بازه مجاز اجرای گزارش‌گیری: از 00:20 تا 02:00
    - 00:20 تا 00:59
    - 01:00 تا 01:59
    - 02:00 دقیقاً (برای جبران تأخیر)
    """
    return ((now.hour == 0 and now.minute >= 20)
            or (now.hour == 1)
            or (now.hour == 2 and now.minute == 0))

def schedule_reports(now: datetime):
    global daily_done_today, weekly_done_this_week, monthly_done_this_month

    # ---------------- گزارش روزانه ----------------
    # دیروز را گزارش می‌کند؛ فقط یک‌بار در بازه 00:20 تا 02:00
    if _in_nightly_window(now) and not daily_done_today:
        try:
            yesterday = (now - timedelta(days=1)).date()

            # جمع‌آوری رکوردهای روز قبل
            daily_data = [
                row for row in load_reports()
                if datetime.strptime(row['date'], "%Y-%m-%d").date() == yesterday
            ]

            if daily_data:
                build_daily_report(daily_data, yesterday)
                print(f"✅ [DAILY] گزارش روزانه برای {yesterday} ساخته شد و در reports.csv ذخیره شد.")
            else:
                print(f"⚠️ [DAILY] داده‌ای برای {yesterday} پیدا نشد؛ گزارش روزانه ساخته نشد.")

            daily_done_today = True
        except Exception as e:
            print("❌ [DAILY] خطا در ساخت گزارش روزانه:", e)

    # ---------------- گزارش هفتگی ----------------
    # شنبه بامداد (weekday() == 5)؛ گزارش بازه کامل یکشنبه تا جمعه‌ی هفته‌ی قبل
    if (now.weekday() == 5) and _in_nightly_window(now) and not weekly_done_this_week:
        try:
            # جمعه‌ی همین هفته (روز قبلِ شنبه)
            last_week_end = (now - timedelta(days=1)).date()        # Friday
            # یکشنبه‌ی هفته‌ی قبل (شروع بازه 7 روزه)
            last_week_start = last_week_end - timedelta(days=6)     # Sunday → Friday

            weekly_data = [
                row for row in load_reports()
                if last_week_start <= datetime.strptime(row['date'], "%Y-%m-%d").date() <= last_week_end
            ]

            if weekly_data:
                build_weekly_report(weekly_data, last_week_start, last_week_end)
                print(f"✅ [WEEKLY] گزارش هفتگی برای بازه {last_week_start} تا {last_week_end} ساخته شد.")
            else:
                print("⚠️ [WEEKLY] داده‌ای برای هفته قبل پیدا نشد؛ گزارش هفتگی ساخته نشد.")

            weekly_done_this_week = True
        except Exception as e:
            print("❌ [WEEKLY] خطا در ساخت گزارش هفتگی:", e)

    # ---------------- گزارش ماهانه ----------------
    # اولین روز ماه (day == 1)؛ گزارش ماه کامل قبل با محاسبه‌ی دقیق تعداد روزها
    if (now.day == 1) and _in_nightly_window(now) and not monthly_done_this_month:
        try:
            # آخرین روز ماه قبل = روز قبل از روز اول ماه جاری
            last_day_last_month = (now.replace(day=1) - timedelta(days=1)).date()
            # اولین روز ماه قبل
            first_day_last_month = last_day_last_month.replace(day=1)

            monthly_data = [
                row for row in load_reports()
                if first_day_last_month <= datetime.strptime(row['date'], "%Y-%m-%d").date() <= last_day_last_month
            ]

            if monthly_data:
                build_monthly_report(monthly_data, first_day_last_month, last_day_last_month)
                print(f"✅ [MONTHLY] گزارش ماهانه برای بازه {first_day_last_month} تا {last_day_last_month} ساخته شد.")
            else:
                print("⚠️ [MONTHLY] داده‌ای برای ماه قبل پیدا نشد؛ گزارش ماهانه ساخته نشد.")

            monthly_done_this_month = True
        except Exception as e:
            print("❌ [MONTHLY] خطا در ساخت گزارش ماهانه:", e)
# ================== بخش ۸/۶: گزارش و بررسی گپ بیت‌کوین ==================
import csv
from datetime import datetime

BTC_GAP_FILE = "btc_gaps.csv"

# تشخیص گپ
def detect_btc_gap(friday_close, sunday_open, threshold=0.002):
    """
    تشخیص گپ بیت‌کوین بین قیمت بسته شدن جمعه و باز شدن یکشنبه
    threshold = 0.002 یعنی 0.2 درصد
    """
    diff = (sunday_open - friday_close) / friday_close
    if abs(diff) >= threshold:
        gap_type = "up" if diff > 0 else "down"
        return {
            "gap_type": gap_type,
            "gap_size": round(diff * 100, 2),
            "friday_close": friday_close,
            "sunday_open": sunday_open
        }
    return None

# ثبت گپ در فایل جدا
def log_btc_gap(gap_info):
    header = ["date", "gap_type", "gap_size", "friday_close", "sunday_open", "status", "fill_time"]
    file_exists = os.path.exists(BTC_GAP_FILE)

    with open(BTC_GAP_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(header)
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d"),
            gap_info["gap_type"],
            gap_info["gap_size"],
            gap_info["friday_close"],
            gap_info["sunday_open"],
            "pending",
            ""
        ])
    print(f"📊 گپ بیت‌کوین ثبت شد: {gap_info['gap_type']} {gap_info['gap_size']}%")

# بررسی پر شدن گپ
def check_gap_fill(current_price):
    if not os.path.exists(BTC_GAP_FILE):
        print("ℹ️ هیچ گزارشی برای بررسی گپ وجود ندارد.")
        return

    updated_rows = []
    filled = False

    with open(BTC_GAP_FILE, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        if row["status"] == "pending":
            friday_close = float(row["friday_close"])
            gap_type = row["gap_type"]
            if (gap_type == "up" and current_price <= friday_close) or \
               (gap_type == "down" and current_price >= friday_close):
                row["status"] = "filled"
                row["fill_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                filled = True
        updated_rows.append(row)

    with open(BTC_GAP_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(updated_rows)

    if filled:
        print("✅ گپ بیت‌کوین پر شد و گزارش آپدیت شد.")
    else:
        print("ℹ️ هنوز گپ بیت‌کوین پر نشده.")

        # ================== بخش ۸/۷: توابع کمکی ارسال و گزارش ==================
import csv
import os

def load_reports():
    """خواندن محتوای reports*.csv (logs/) به صورت لیست دیکشنری با پشتیبانی از پارت‌ها"""
    try:
        base_dir = REPORTS_DIR
        files = sorted([
            os.path.join(base_dir, f) for f in os.listdir(base_dir)
            if f.startswith(REPORTS_BASE_NAME) and f.endswith(REPORTS_EXT)
        ])
        if not files:
            return []
        rows = []
        for p in files:
            with open(p, "r", encoding="utf-8") as f:
                rows.extend(list(csv.DictReader(f)))
        return rows
    except Exception:
        return []

def send_daily_report(chat_id=None, reply_markup=None):
    """ارسال آخرین گزارش روزانه از reports.csv (فقط به چت خصوصی)"""
    reports = load_reports()
    if not reports:
        send_report("⚠️ گزارشی موجود نیست.")
        return
    daily = [r for r in reports if r.get("type") == "daily"]
    if not daily:
        send_report("⚠️ گزارش روزانه موجود نیست.")
        return
    last = daily[-1]
    text = f"📅 گزارش روزانه\nتاریخ: {last.get('timestamp','-')}\nخلاصه: {last.get('message','-')}"
    send_report(text)

def send_weekly_report(chat_id=None, reply_markup=None):
    """ارسال آخرین گزارش هفتگی از reports.csv (فقط به چت خصوصی)"""
    reports = load_reports()
    if not reports:
        send_report("⚠️ گزارشی موجود نیست.")
        return
    weekly = [r for r in reports if r.get("type") == "weekly"]
    if not weekly:
        send_report("⚠️ گزارش هفتگی موجود نیست.")
        return
    last = weekly[-1]
    text = f"📆 گزارش هفتگی\nتاریخ: {last.get('timestamp','-')}\nخلاصه: {last.get('message','-')}"
    send_report(text)

def send_monthly_report(chat_id=None, reply_markup=None):
    """ارسال آخرین گزارش ماهانه از reports.csv (فقط به چت خصوصی)"""
    reports = load_reports()
    if not reports:
        send_report("⚠️ گزارشی موجود نیست.")
        return
    monthly = [r for r in reports if r.get("type") == "monthly"]
    if not monthly:
        send_report("⚠️ گزارش ماهانه موجود نیست.")
        return
    last = monthly[-1]
    text = f"📊 گزارش ماهانه\nتاریخ: {last.get('timestamp','-')}\nخلاصه: {last.get('message','-')}"
    send_report(text)

# ================== بخش ۹: منوی گزارش‌ها + زمان‌بندی و لاگ شفاف ==================
from datetime import datetime

# اختیاری: خصوصی‌سازی دسترسی بات (فقط خودت)
ALLOWED_USERS = set()
try:
    if CHAT_ID_USER and CHAT_ID_USER.strip():
        ALLOWED_USERS = {int(CHAT_ID_USER)}
except Exception:
    ALLOWED_USERS = set()

def send_main_menu(chat_id: int):
    keyboard = {
        "keyboard": [
            [{"text": "📅 گزارش روزانه"}, {"text": "📆 گزارش هفتگی"}],
            [{"text": "📊 گزارش ماهانه"}, {"text": "📈 گزارش گپ بیت‌کوین"}],
            [{"text": "📊 گزارش ساده روزانه"}, {"text": "📆 گزارش ساده هفتگی"}],
            [{"text": "🗓️ گزارش ساده ماهانه"}]
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False
    }
    payload = {
        "chat_id": chat_id,
        "text": "منوی گزارش‌ها فعال است. از دکمه‌های زیر انتخاب کن:",
        "reply_markup": keyboard
    }
    try:
        requests.post(f"{BOT_API}/sendMessage", json=payload, timeout=12)
    except Exception as e:
        print("❌ خطا در ارسال منو:", e)

def handle_message(msg: dict):
    chat_id = msg.get("chat", {}).get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return

    # فیلتر دسترسی خصوصی
    if ALLOWED_USERS and chat_id not in ALLOWED_USERS:
        return

    if text == "/start":
        send_main_menu(chat_id)
        send_report("✋ خوش آمدی! منوی گزارش‌گیری فعال شد. از دکمه‌ها استفاده کن.")
        return

    if text.startswith("📅 گزارش روزانه"):
        send_daily_report()
        return

    if text.startswith("📆 گزارش هفتگی"):
        send_weekly_report()
        return

    if text.startswith("📊 گزارش ماهانه"):
        send_monthly_report()
        return

    if text.startswith("📈 گزارش گپ بیت‌کوین"):
        summary = summarize_gaps()
        send_report(summary)
        return

    if text.startswith("📊 گزارش ساده روزانه"):
        msg_txt = send_daily_summary()
        if isinstance(msg_txt, str) and msg_txt.strip():
            send_report(msg_txt)
        else:
            send_report(build_daily_message())
        return

    if text.startswith("📆 گزارش ساده هفتگی"):
        send_report(build_weekly_message())
        return

    if text.startswith("🗓️ گزارش ساده ماهانه"):
        send_report(build_monthly_message())
        return

    if text in ["🏁 منوی گزارش‌گیری", "منوی گزارش‌گیری", "منو"]:
        send_main_menu(chat_id)
        return

    send_report("برای گزارش‌ها از دکمه‌های پایین استفاده کن ✅")

    # ================== بخش ۹/۱: زمان‌بندی گزارش‌ها + لاگ شفاف (اصلاح‌شده برای ۷ تا ۹ صبح) ==================
from datetime import datetime, timedelta

daily_done_today = False
weekly_done_this_week = False
monthly_done_this_month = False

def collect_data(start: datetime, end: datetime):
    """جمع‌آوری داده‌ها از LOG_FILE_PATH و ساخت خلاصه با build_summary_message"""
    df = load_log_df()
    if df is None or df.empty:
        return {"summary": f"بازه: {start.date()} تا {end.date()}\nداده‌ای موجود نیست."}

    # اطمینان از timezone
    start_ts = pd.Timestamp(start, tz=timezone.utc)
    end_ts = pd.Timestamp(end, tz=timezone.utc)

    # محافظت از ستون‌ها
    needed_cols = ["timestamp", "result", "symbol", "analysis", "tf", "rrr"]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = "" if col != "rrr" else pd.Series(dtype=float)

    mask = (df["timestamp"] >= start_ts) & (df["timestamp"] <= end_ts)
    dfx = df[mask]
    title = f"بازه: {start.date()} تا {end.date()}"
    msg = build_summary_message(title, dfx)
    return {"summary": msg}

def save_report(rep_type: str, data: dict, start: datetime, end: datetime):
    """ذخیره خلاصه در reports.csv استاندارد (logs/...)"""
    title_map = {
        "daily": "📈 گزارش روزانه",
        "weekly": "📊 گزارش هفتگی",
        "monthly": "🗓️ گزارش ماهانه",
    }
    title = title_map.get(rep_type, "📄 گزارش")
    summary = data.get("summary", "هیچ")
    append_report(rep_type, f"{title}\n{summary}")

def build_daily_report(now: datetime):
    yesterday = now.date() - timedelta(days=1)
    start = datetime.combine(yesterday, datetime.min.time())
    end   = datetime.combine(yesterday, datetime.max.time())

    data = collect_data(start, end)
    save_report("daily", data, start, end)
    # ارسال گزارش به چت خصوصی
    send_report(f"📈 گزارش روزانه آماده شد:\n{data['summary']}")
    return data

def build_weekly_report(now: datetime):
    start_of_this_week = now.date() - timedelta(days=now.weekday())
    start_of_last_week = start_of_this_week - timedelta(days=7)
    end_of_last_week   = start_of_this_week - timedelta(seconds=1)

    start = datetime.combine(start_of_last_week, datetime.min.time())
    end   = datetime.combine(end_of_last_week, datetime.max.time())

    data = collect_data(start, end)
    save_report("weekly", data, start, end)
    send_report(f"📊 گزارش هفتگی آماده شد:\n{data['summary']}")
    return data

def build_monthly_report(now: datetime):
    first_of_this_month = now.replace(day=1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)

    start = datetime.combine(first_day_prev_month, datetime.min.time())
    end   = datetime.combine(last_day_prev_month, datetime.max.time())

    data = collect_data(start, end)
    save_report("monthly", data, start, end)
    send_report(f"🗓️ گزارش ماهانه آماده شد:\n{data['summary']}")
    return data

def schedule_reports(now: datetime):
    global daily_done_today, weekly_done_this_week, monthly_done_this_month

    # --- گزارش روزانه ---
    if 7 <= now.hour < 9 and not daily_done_today:
        try:
            build_daily_report(now)
            daily_done_today = True
            print("✅ [DAILY] گزارش روزانه ساخته و ارسال شد")
        except Exception as e:
            print("❌ [DAILY] خطا در ساخت گزارش روزانه:", e)

    if now.hour < 7:
        daily_done_today = False

    # --- گزارش هفتگی ---
    if now.weekday() == 5 and 7 <= now.hour < 9 and not weekly_done_this_week:
        try:
            build_weekly_report(now)
            weekly_done_this_week = True
            print("✅ [WEEKLY] گزارش هفتگی ساخته و ارسال شد")
        except Exception as e:
            print("❌ [WEEKLY] خطا در ساخت گزارش هفتگی:", e)

    if now.weekday() == 5 and now.hour < 7:
        weekly_done_this_week = False

    # --- گزارش ماهانه ---
    if now.day == 1 and 7 <= now.hour < 9 and not monthly_done_this_month:
        try:
            build_monthly_report(now)
            monthly_done_this_month = True
            print("✅ [MONTHLY] گزارش ماهانه ساخته و ارسال شد")
        except Exception as e:
            print("❌ [MONTHLY] خطا در ساخت گزارش ماهانه:", e)

    if now.day == 1 and now.hour < 7:
        monthly_done_this_month = False

    # --- لاگ شفاف ---
    log_report_check(now, daily_done_today, weekly_done_this_week, monthly_done_this_month)

def log_report_check(now, daily_done_today, weekly_done_this_week, monthly_done_this_month):
    stamp = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"⏳ [{stamp}] بررسی گزارش‌ها | "
          f"روزانه={'OK' if daily_done_today else 'WAIT'} | "
          f"هفتگی={'OK' if weekly_done_this_week else 'WAIT'} | "
          f"ماهانه={'OK' if monthly_done_this_month else 'WAIT'}")

          # ================== بخش ۱۰ (قسمت A): ابزار گزارش‌ها + خلاصه گپ‌ها + هندل منو ==================
import time
import csv
import requests
import os
from datetime import datetime
from telegram import ReplyKeyboardMarkup

# ------------ ابزارهای نمایش گزارش از reports.csv ------------
def _read_reports_by_type(rep_type):
    """خواندن آخرین گزارش‌ها از logs/reports*.csv بر اساس type ('daily' | 'weekly' | 'monthly')"""
    try:
        base_dir = REPORTS_DIR
        files = sorted([
            os.path.join(base_dir, f) for f in os.listdir(base_dir)
            if f.startswith(REPORTS_BASE_NAME) and f.endswith(REPORTS_EXT)
        ])
        rows = []
        for p in files:
            with open(p, "r", encoding="utf-8") as f:
                rows.extend([r for r in csv.DictReader(f)
                             if (r.get("type") or "").strip().lower() == rep_type])
        return rows
    except Exception:
        return []

def send_daily_report():
    rows = _read_reports_by_type("daily")
    if not rows:
        send_report("❌ گزارشی موجود نیست.")
        return
    last = rows[-1]
    date = last.get("timestamp", "").strip()
    summary = last.get("message", "").strip()
    msg = f"📅 گزارش روزانه ({date})\n{summary}"
    send_report(msg)

def send_weekly_report():
    rows = _read_reports_by_type("weekly")
    if not rows:
        send_report("❌ گزارشی موجود نیست.")
        return
    last = rows[-1]
    date = last.get("timestamp", "").strip()
    summary = last.get("message", "").strip()
    msg = f"📆 گزارش هفتگی ({date})\n{summary}"
    send_report(msg)

def send_monthly_report():
    rows = _read_reports_by_type("monthly")
    if not rows:
        send_report("❌ گزارشی موجود نیست.")
        return
    last = rows[-1]
    date = last.get("timestamp", "").strip()
    summary = last.get("message", "").strip()
    msg = f"📊 گزارش ماهانه ({date})\n{summary}"
    send_report(msg)

# ------------ خلاصه وضعیت گپ‌های بیت‌کوین ------------
def summarize_gaps():
    """خلاصه وضعیت گپ‌های بیت‌کوین از btc_gaps.csv"""
    try:
        with open("btc_gaps.csv", "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        total = len(rows)
        filled = sum(1 for r in rows if (r.get("status") or "").strip().lower() == "filled")
        pending = sum(1 for r in rows if (r.get("status") or "").strip().lower() == "pending")

        last_pending = None
        for r in reversed(rows):
            if (r.get("status") or "").strip().lower() == "pending":
                last_pending = r
                break

        msg = "📈 وضعیت گپ‌های بیت‌کوین:\n"
        msg += f"- کل گپ‌ها: {total}\n"
        msg += f"- پر شده: {filled}\n"
        msg += f"- باز: {pending}\n"
        if last_pending:
            msg += f"- آخرین گپ باز: {last_pending.get('date','')} ({last_pending.get('gap_type','')} Gap, {last_pending.get('gap_size','')}%)\n"
        return msg
    except Exception as e:
        return f"❌ خطا در خواندن گپ‌ها: {e}"

# ------------ هندل منوی تلگرام ------------
def handle_message(msg, reply_markup=None):
    text = (msg.get("text") or "").strip()

    if text == "📅 گزارش روزانه":
        send_daily_report(); return
    if text == "📆 گزارش هفتگی":
        send_weekly_report(); return
    if text == "📊 گزارش ماهانه":
        send_monthly_report(); return
    if text == "📈 گزارش گپ بیت‌کوین":
        summary = summarize_gaps()
        send_report(summary); return

    if text == "📊 گزارش ساده روزانه":
        msg_txt = send_daily_summary()
        send_report(msg_txt); return
    if text == "📆 گزارش ساده هفتگی":
        send_report(build_weekly_message()); return
    if text == "🗓️ گزارش ساده ماهانه":
        send_report(build_monthly_message()); return

    send_report("❓ گزینه نامعتبر است.")


# ================== بخش ۱۰ (قسمت B): حلقه اصلی + منو + ورود برنامه ==================
import time
import requests
from datetime import datetime
from telegram import ReplyKeyboardMarkup

def run_bot_loop():
    try:
        requests.get(f"{BOT_API}/deleteWebhook", timeout=10)
        print("🧹 Webhook حذف شد")
    except Exception as e:
        print("❌ خطا در deleteWebhook:", e)

    try:
        requests.get(f"{BOT_API}/getUpdates", params={"offset": -1}, timeout=10)
        print("✅ صف قدیمی پاک شد")
    except Exception as e:
        print("❌ خطا در پاک کردن آپدیت‌ها:", e)

    offset = None
    last_run = 0
    run_interval = 3600
    last_proxy_error = 0

    print("▶️ شروع اجرای حلقه ربات...")

    menu_keyboard = [
        ["📅 گزارش روزانه", "📆 گزارش هفتگی"],
        ["📊 گزارش ماهانه", "📈 گزارش گپ بیت‌کوین"],
        ["📊 گزارش ساده روزانه", "📆 گزارش ساده هفتگی"],
        ["🗓️ گزارش ساده ماهانه"]
    ]
    reply_markup = ReplyKeyboardMarkup(menu_keyboard, resize_keyboard=True)

    while True:
        try:
            now_ts = time.time()
            if now_ts - last_run >= run_interval:
                try:
                    run_once()
                    check_motivation_message()
                except Exception as e_run:
                    print("❌ خطا در run_once:", e_run)
                last_run = now_ts

            try:
                schedule_reports(datetime.now())
            except Exception as e_rep:
                print("❌ خطا در schedule_reports:", e_rep)

            url = f"{BOT_API}/getUpdates"
            params = {"timeout": 30}
            if offset is not None:
                params["offset"] = offset
            r = requests.get(url, params=params, timeout=35)
            data = r.json()

            for update in data.get("result", []):
                offset = update["update_id"] + 1
                msg = update.get("message")
                if msg and isinstance(msg.get("text"), str):
                    handle_message(msg, reply_markup=reply_markup)

        except Exception as e:
            if "proxy" in str(e).lower():
                if time.time() - last_proxy_error > 60:
                    print("⚠️ خطای پروکسی: اتصال به تلگرام ممکن نیست.")
                    last_proxy_error = time.time()
            else:
                print("❌ خطا در حلقه اصلی:", e)

        time.sleep(300)

if __name__ == "__main__":
    init_log(get_current_log_path())
    load_last_signals()
    run_bot_loop()