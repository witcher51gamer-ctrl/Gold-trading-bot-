"""
========================================================================================
V50.9 Compact Forex & Gold Engine - With Direct MetaTrader Button
========================================================================================
"""

import os
import time
import logging
import requests
import numpy as np
import pandas as pd
import yfinance as yf
from threading import Thread
from datetime import datetime, timedelta, timezone
from flask import Flask

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8664695982:AAHMaTwCbX1aV1sZjKlie1jK5zJB4tXFSVo')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '6021016826')

SYMBOL_MAP = {
    "GC=F": "XAU/USD",
    "EURUSD=X": "EUR/USD",
    "GBPUSD=X": "GBP/USD",
    "JPY=X": "USD/JPY"
}

CHECK_INTERVAL = 20
MIN_RR_RATIO = 2.5
COOLDOWN_HOURS = 4.0

REPORT_INTERVAL = 7200
last_report_time = 0

LOCAL_TZ = timezone(timedelta(hours=3))
signaled_history = {}
active_trades = {}

app = Flask('')

@app.route('/')
def home():
    return "Forex & Gold V50 Engine is RUNNING!", 200

def keep_alive():
    t = Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.getenv('PORT', 8000))))
    t.daemon = True
    t.start()

def get_local_time():
    return datetime.now(LOCAL_TZ).strftime("%H:%M")

def format_price(symbol, price):
    if "JPY" in symbol: return f"{price:.3f}"
    elif "XAU" in symbol or "GC=F" in symbol: return f"{price:.2f}"
    else: return f"{price:.5f}"

# دالة إرسال الرسائل مع دعم أزرار Inline
def send_telegram_direct(message, reply_markup=None):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        res = requests.post(url, json=payload, timeout=10)
        return res.json()
    except Exception as e:
        logging.error(f"Telegram Direct Error: {e}")
        return None

def is_valid_session():
    now_utc = datetime.now(timezone.utc).hour
    return 7 <= now_utc <= 21

def check_volatility_spike(df):
    tr = np.maximum(df['high'] - df['low'], abs(df['close'] - df['close'].shift(1)))
    atr = tr.rolling(14).mean().iloc[-1]
    last_candle_body = abs(df['close'].iloc[-1] - df['open'].iloc[-1])
    return last_candle_body > (atr * 2.8)

def fetch_candles(yf_symbol, timeframe="15m", period="5d"):
    try:
        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=period, interval=timeframe)
        if df.empty: return None
        df.reset_index(inplace=True)
        df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        return df
    except Exception:
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

def calculate_trade_setup(symbol, entry, direction, df_15m):
    high_low = df_15m['high'] - df_15m['low']
    atr = high_low.rolling(14).mean().iloc[-2]
    if pd.isna(atr) or atr == 0: atr = entry * 0.002

    if direction == "LONG":
        stop = entry - (atr * 1.5)
        risk = entry - stop
        tp1, tp2, tp3 = entry + (risk * 1.2), entry + (risk * 2.5), entry + (risk * 4.0)
    else:
        stop = entry + (atr * 1.5)
        risk = stop - entry
        tp1, tp2, tp3 = entry - (risk * 1.2), entry - (risk * 2.5), entry - (risk * 4.0)

    rr = abs(tp2 - entry) / abs(entry - stop) if abs(entry - stop) > 0 else 2.5
    return stop, tp1, tp2, tp3, round(rr, 2)

def send_periodic_market_report():
    report_lines = [
        "📊 *[ MARKET SCAN ]*",
        f"🕒 `{get_local_time()}`\n"
    ]
    
    for yf_symbol, display_name in SYMBOL_MAP.items():
        df = fetch_candles(yf_symbol, "15m", "2d")
        if df is not None and not df.empty:
            price = df['close'].iloc[-1]
            rsi = calculate_rsi(df).iloc[-1]
            report_lines.append(f"• *{display_name}:* `{format_price(display_name, price)}` | RSI `{rsi:.0f}`")
            
    send_telegram_direct("\n".join(report_lines))

def analyze_symbol(yf_symbol):
    display_name = SYMBOL_MAP[yf_symbol]
    if not is_valid_session(): return None
    
    df_15m = fetch_candles(yf_symbol, "15m", "5d")
    df_1h = fetch_candles(yf_symbol, "1h", "7d")
    
    if df_15m is None or df_1h is None or len(df_15m) < 30: return None
    if check_volatility_spike(df_15m): return None

    live_price = float(df_15m['close'].iloc[-1])
    highs, lows = find_swings(df_15m)
    if len(highs) < 2 or len(lows) < 2: return None

    rsi_val = calculate_rsi(df_15m).iloc[-1]
    adx_val = calculate_adx(df_15m)
    ema50_1h = df_1h['close'].ewm(span=50, adjust=False).mean().iloc[-1]

    direction = None
    if live_price > highs[-1] and live_price > ema50_1h and rsi_val < 65 and adx_val > 20:
        direction = "BUY 🟢"
    elif live_price < lows[-1] and live_price < ema50_1h and rsi_val > 35 and adx_val > 20:
        direction = "SELL 🔴"

    if not direction: return None

    stop, tp1, tp2, tp3, rr = calculate_trade_setup(display_name, live_price, direction, df_15m)
    if rr < MIN_RR_RATIO: return None

    return {
        "symbol": display_name, "direction": direction, "entry": live_price,
        "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3, "rr": rr
    }

def track_active_trades():
    for symbol_name, trade in list(active_trades.items()):
        yf_symbol = [k for k, v in SYMBOL_MAP.items() if v == symbol_name][0]
        df = fetch_candles(yf_symbol, "1m", "1d")
        if df is None or df.empty: continue

        current_price = df['close'].iloc[-1]
        
        if "BUY" in trade['direction']:
            if not trade.get('tp1_hit') and current_price >= trade['tp1']:
                trade['tp1_hit'] = True
                send_telegram_direct(f"🎯 *TP1 Hit:* `{symbol_name}` @ `{format_price(symbol_name, trade['tp1'])}` (Move SL to Entry)")
            elif current_price <= trade['stop']:
                send_telegram_direct(f"🛑 *SL Hit:* `{symbol_name}` @ `{format_price(symbol_name, trade['stop'])}`")
                del active_trades[symbol_name]

        elif "SELL" in trade['direction']:
            if not trade.get('tp1_hit') and current_price <= trade['tp1']:
                trade['tp1_hit'] = True
                send_telegram_direct(f"🎯 *TP1 Hit:* `{symbol_name}` @ `{format_price(symbol_name, trade['tp1'])}` (Move SL to Entry)")
            elif current_price >= trade['stop']:
                send_telegram_direct(f"🛑 *SL Hit:* `{symbol_name}` @ `{format_price(symbol_name, trade['stop'])}`")
                del active_trades[symbol_name]

def main():
    global last_report_time
    send_telegram_direct("🚀 *Forex Bot Active!*")

    while True:
        try:
            current_time = time.time()
            
            if current_time - last_report_time > REPORT_INTERVAL:
                send_periodic_market_report()
                last_report_time = current_time

            track_active_trades()
            
            for yf_symbol in SYMBOL_MAP.keys():
                now_ts = time.time()
                display_name = SYMBOL_MAP[yf_symbol]

                if display_name in signaled_history and (now_ts - signaled_history[display_name]) < (COOLDOWN_HOURS * 3600):
                    continue

                trade = analyze_symbol(yf_symbol)
                if trade:
                    signaled_history[display_name] = now_ts
                    active_trades[display_name] = trade
                    
                    msg = (
                        f"🚨 *SIGNAL:* `{trade['symbol']}` ({trade['direction']})\n\n"
                        f"📍 *Entry:* `{format_price(trade['symbol'], trade['entry'])}`\n"
                        f"🎯 *TP1:* `{format_price(trade['symbol'], trade['tp1'])}`\n"
                        f"🎯 *TP2:* `{format_price(trade['symbol'], trade['tp2'])}`\n"
                        f"🎯 *TP3:* `{format_price(trade['symbol'], trade['tp3'])}`\n"
                        f"🛑 *SL:* `{format_price(trade['symbol'], trade['stop'])}`\n\n"
                        f"⚖️ *R:R:* `{trade['rr']}` | ⏰ `{get_local_time()}`"
                    )

                    # إضافة زر ينقلك لميتاترايدر مباشرة
                    keyboard = {
                        "inline_keyboard": [
                            [
                                {"text": "📲 Open MetaTrader 5", "url": "metatrader5://"},
                                {"text": "📲 Open MetaTrader 4", "url": "metatrader4://"}
                            ]
                        ]
                    }

                    send_telegram_direct(msg, reply_markup=keyboard)

            time.sleep(CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Main Loop Error: {e}")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    keep_alive()
    main()
