"""
========================================================================================
V40.5 Cloud Forex & Gold Engine - YFinance & Railway Ready ($10 to $10,000 Lot Matrix)
========================================================================================
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from threading import Thread
import aiohttp
import aiosqlite
import numpy as np
import pandas as pd
import yfinance as yf
from flask import Flask

# --- إعدادات التسجيل ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- بيانات التليجرام الخاصة بك ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8664695982:AAHMaTwCbX1aV1sZjKlie1jK5zJB4tXFSVo')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '8664695982')

# الأزواج المستهدفة في YFinance
SYMBOL_MAP = {
    "GC=F": "XAUUSD",     # عقود الذهب الآجلة
    "EURUSD=X": "EURUSD", # يورو / دولار
    "GBPUSD=X": "GBPUSD", # استرليني / دولار
    "JPY=X": "USDJPY"     # دولار / ين
}

DATABASE = 'forex_signals_cloud.db'
CHECK_INTERVAL = 30

MIN_RR_RATIO = 2.5
COOLDOWN_HOURS = 4.0
RISK_PER_TRADE = 0.01  # مخاطرة 1%

LOCAL_TZ = timezone(timedelta(hours=3))
signaled_history = {}
http_session = None

# ======================================================================================== #
# 1. FLASK KEEP-ALIVE SERVER (مهم جداً لمنع توقف Railway)
# ======================================================================================== #
app = Flask('')

@app.route('/')
def home():
    return "Forex & Gold Cloud Engine is ALIVE on Railway!", 200

def run_server():
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_server)
    t.daemon = True
    t.start()

# ======================================================================================== #
# 2. إدارة قاعدة البيانات والتليجرام
# ======================================================================================== #
async def init_database():
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute('''CREATE TABLE IF NOT EXISTS trades 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT, direction TEXT, entry REAL, stop REAL, 
             tp1 REAL, tp2 REAL, tp3 REAL, timestamp TEXT, status TEXT)''')
        await conn.commit()

def get_local_time():
    return datetime.now(LOCAL_TZ).strftime("%I:%M %p")

def format_price(symbol, price):
    if "JPY" in symbol:
        return f"{price:.3f}"
    elif "XAU" in symbol or "GC=F" in symbol:
        return f"{price:.2f}"
    else:
        return f"{price:.5f}"

async def send_telegram(message):
    global http_session
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        if http_session is None or http_session.closed:
            http_session = aiohttp.ClientSession()
        async with http_session.post(url, json=payload, timeout=8) as resp:
            return await resp.json()
    except Exception as e:
        logging.error(f"Telegram Error: {e}")

# ======================================================================================== #
# 3. جلب بيانات YFinance والمؤشرات الفنية
# ======================================================================================== #
def fetch_candles(yf_symbol, timeframe="15m", period="5d"):
    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=period, interval=timeframe)
        if df.empty:
            return None
        df.reset_index(inplace=True)
        df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error fetching data for {yf_symbol}: {e}")
        return None

def calculate_rsi(df, period=14):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_adx(df, period=14):
    df['up'] = df['high'] - df['high'].shift(1)
    df['down'] = df['low'].shift(1) - df['low']
    df['+dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0)
    df['-dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0)
    
    tr = np.maximum(df['high'] - df['low'], np.maximum(abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))))
    atr = pd.Series(tr).rolling(period).mean()
    
    plus_di = 100 * (pd.Series(df['+dm']).rolling(period).mean() / atr)
    minus_di = 100 * (pd.Series(df['-dm']).rolling(period).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)
    return dx.rolling(period).mean().iloc[-1]

def find_swings(df, length=5):
    highs, lows = [], []
    for i in range(length, len(df) - length - 1):
        if all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, length+1)) and \
           all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, length+1)):
            highs.append(df['high'].iloc[i])
        if all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, length+1)) and \
           all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, length+1)):
            lows.append(df['low'].iloc[i])
    return highs, lows

# ======================================================================================== #
# 4. حساب جدول أحجام العقود (Lot Matrix) من 10$ إلى 10,000$
# ======================================================================================== #
def calculate_lot_matrix(entry, stop, symbol):
    pips_at_risk = abs(entry - stop)
    if pips_at_risk == 0:
        return "• 0.01 Lot"

    accounts = [10, 50, 100, 500, 1000, 5000, 10000]
    matrix_lines = []

    for acc in accounts:
        risk_amount = acc * RISK_PER_TRADE
        if "XAU" in symbol or "GC=F" in symbol:
            lot = max(0.01, round(risk_amount / (pips_at_risk * 100), 2))
        else:
            pip_value = 10.0
            lot = max(0.01, round(risk_amount / (pips_at_risk * 10000 * pip_value), 2))

        matrix_lines.append(f"• <b>${acc:,}:</b> {lot} Lot")

    return "\n".join(matrix_lines)

def calculate_trade_setup(symbol, entry, direction, df_15m):
    high_low = df_15m['high'] - df_15m['low']
    atr = high_low.rolling(14).mean().iloc[-2]
    if pd.isna(atr) or atr == 0:
        atr = entry * 0.002

    if direction == "LONG":
        stop = entry - (atr * 1.5)
        risk = entry - stop
        tp1, tp2, tp3 = entry + (risk * 1.2), entry + (risk * 2.5), entry + (risk * 4.0)
    else:
        stop = entry + (atr * 1.5)
        risk = stop - entry
        tp1, tp2, tp3 = entry - (risk * 1.2), entry - (risk * 2.5), entry - (risk * 4.0)

    rr = abs(tp2 - entry) / abs(entry - stop) if abs(entry - stop) > 0 else 2.5
    lot_matrix = calculate_lot_matrix(entry, stop, symbol)

    return stop, tp1, tp2, tp3, round(rr, 2), lot_matrix

# ======================================================================================== #
# 5. تحليل الإشارة بالكامل
# ======================================================================================== #
async def analyze_symbol(yf_symbol):
    display_name = SYMBOL_MAP[yf_symbol]
    
    df_15m = await asyncio.to_thread(fetch_candles, yf_symbol, "15m", "5d")
    df_1h = await asyncio.to_thread(fetch_candles, yf_symbol, "1h", "7d")
    if df_15m is None or df_1h is None or len(df_15m) < 30: return None

    live_price = float(df_15m['close'].iloc[-1])
    highs, lows = find_swings(df_15m)
    if len(highs) < 2 or len(lows) < 2: return None

    rsi_val = calculate_rsi(df_15m).iloc[-1]
    adx_val = calculate_adx(df_15m)
    ema50_1h = df_1h['close'].ewm(span=50, adjust=False).mean().iloc[-1]

    direction = None
    if live_price > highs[-1] and live_price > ema50_1h and rsi_val < 65 and adx_val > 20:
        direction = "LONG"
    elif live_price < lows[-1] and live_price < ema50_1h and rsi_val > 35 and adx_val > 20:
        direction = "SHORT"

    if not direction: return None

    stop, tp1, tp2, tp3, rr, lot_matrix = calculate_trade_setup(display_name, live_price, direction, df_15m)
    if rr < MIN_RR_RATIO: return None

    return {
        "symbol": display_name, "direction": direction, "entry": live_price,
        "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3,
        "rr": rr, "lot_matrix": lot_matrix,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ======================================================================================== #
# 6. المحرك الرئيسي
# ======================================================================================== #
async def main():
    await init_database()
    await send_telegram("🚀 <b>تم تشغيل بوت الذهب والفوركس السحابي بنجاح!</b>\n• يعمل على Railway دون الحاجة لمنصة MT5")

    while True:
        try:
            for yf_symbol in SYMBOL_MAP.keys():
                now_ts = time.time()
                display_name = SYMBOL_MAP[yf_symbol]

                if display_name in signaled_history and (now_ts - signaled_history[display_name]) < (COOLDOWN_HOURS * 3600):
                    continue

                trade = await analyze_symbol(yf_symbol)
                if trade:
                    signaled_history[display_name] = now_ts
                    sym_title = "🏆 GOLD (XAUUSD)" if "XAU" in trade['symbol'] else f"💱 {trade['symbol']}"
                    
                    msg = (
                        f"🔱 <b>{sym_title}</b> ({trade['direction']})\n\n"
                        f"📌 <b>Entry Target:</b> {format_price(trade['symbol'], trade['entry'])}\n\n"
                        f"🎯 <b>Take-Profit Targets:</b>\n"
                        f"1) {format_price(trade['symbol'], trade['tp1'])}\n"
                        f"2) {format_price(trade['symbol'], trade['tp2'])}\n"
                        f"3) {format_price(trade['symbol'], trade['tp3'])}\n\n"
                        f"🛑 <b>Stop Target:</b>\n"
                        f"1) {format_price(trade['symbol'], trade['stop'])}\n\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📏 <b>Recommended Lot Sizes (1% Risk):</b>\n"
                        f"{trade['lot_matrix']}\n\n"
                        f"📊 <b>Risk-Reward:</b> {trade['rr']}:1\n"
                        f"⏰ <b>Time:</b> {get_local_time()}"
                    )
                    await send_telegram(msg)

            await asyncio.sleep(CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Main Loop Error: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    keep_alive()
    asyncio.run(main())
