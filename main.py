"""
========================================================================================
V50.0 Master Forex & Gold Engine - Ultra Async & Database Edition
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

# --- إعدادات التسجيل (Logging) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- الثوابت والإعدادات العامة ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8664695982:AAHMaTwCbX1aV1sZjKlie1jK5zJB4tXFSVo')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '6021016826')

# خريطة الأصول المراد مسحها وتحليلها
SYMBOL_MAP = {
    "GC=F": "XAU/USD",
    "EURUSD=X": "EUR/USD",
    "GBPUSD=X": "GBP/USD",
    "USDJPY=X": "USD/JPY",
    "AUDUSD=X": "AUD/USD",
    "USDCAD=X": "USD/CAD"
}

TIMEFRAME_PRIMARY = "15m"
TIMEFRAME_CONFIRM = "1h"
DATABASE = 'forex_master_signals.db'

CHECK_INTERVAL = 15
SIGNAL_THRESHOLD = 80
MIN_RR_RATIO = 2.5
MAX_ACTIVE_TRADES = 5
COOLDOWN_HOURS = 4.0

LOCAL_TZ = timezone(timedelta(hours=3))
signaled_history = {}
active_live_trades = {}
http_session = None
last_telegram_update_id = 0

# ========================================================================================
# 1. خادم FLASK للحفاظ على استمرارية البوت (KEEP-ALIVE)
# ========================================================================================
app = Flask('')

@app.route('/', methods=['GET', 'HEAD', 'POST'])
def home():
    return "V50.0 Forex & Gold Master Engine is ALIVE!", 200

def run_server():
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_server)
    t.daemon = True
    t.start()

# ========================================================================================
# 2. إدارة قاعدة البيانات (DATABASE HELPERS)
# ========================================================================================
async def init_database():
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, direction TEXT, entry REAL, stop REAL,
            tp1 REAL, tp2 REAL, tp3 REAL, timestamp TEXT,
            hit_tp1 BOOLEAN, hit_tp2 BOOLEAN, sl REAL, msg_id TEXT,
            status TEXT, pnl REAL
        )''')
        await conn.commit()

async def save_trade(trade):
    async with aiosqlite.connect(DATABASE) as conn:
        cursor = await conn.execute('''INSERT INTO trades 
            (symbol, direction, entry, stop, tp1, tp2, tp3, timestamp, hit_tp1, hit_tp2, sl, status, pnl)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (trade['symbol'], trade['direction'], trade['entry'], trade['stop'],
             trade['tp1'], trade['tp2'], trade['tp3'], trade['timestamp'],
             False, False, trade['stop'], 'OPEN', 0.0))
        await conn.commit()
        return cursor.lastrowid

async def reload_active_trades():
    global active_live_trades
    async with aiosqlite.connect(DATABASE) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT * FROM trades WHERE status='OPEN'") as cursor:
            rows = await cursor.fetchall()
            for row in rows:
                trade_dict = dict(row)
                trade_dict['hit_tp1'] = bool(row['hit_tp1'])
                trade_dict['hit_tp2'] = bool(row['hit_tp2'])
                active_live_trades[row['id']] = trade_dict

async def update_trade_msg_id(row_id, msg_id):
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("UPDATE trades SET msg_id=? WHERE id=?", (str(msg_id), row_id))
        await conn.commit()

async def update_trade_progress(trade_id, hit_tp1=False, hit_tp2=False, new_sl=None):
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("UPDATE trades SET hit_tp1=?, hit_tp2=?, sl=? WHERE id=?", 
                           (hit_tp1, hit_tp2, new_sl, trade_id))
        await conn.commit()

async def update_trade(trade_id, status, pnl):
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("UPDATE trades SET status=?, pnl=? WHERE id=?", (status, pnl, trade_id))
        await conn.commit()

async def get_performance_summary():
    async with aiosqlite.connect(DATABASE) as conn:
        async with conn.execute("SELECT COUNT(*), SUM(pnl) FROM trades WHERE status != 'OPEN'") as cursor:
            total, total_pnl = await cursor.fetchone()
        async with conn.execute("SELECT COUNT(*) FROM trades WHERE status LIKE '%WIN%' OR status = 'TRAIL_PROFIT'") as cursor:
            wins = (await cursor.fetchone())[0] or 0
        return {"total": total or 0, "wins": wins, "total_pnl": total_pnl or 0.0}

# ========================================================================================
# 3. أدوات مساعدة وتنبيهات التليجرام التفاعلية
# ========================================================================================
def get_local_time():
    return datetime.now(LOCAL_TZ).strftime("%I:%M %p")

def format_price(symbol, price):
    if not price: return "0.00"
    if "JPY" in symbol: return f"{price:.3f}"
    elif "XAU" in symbol or "GC=F" in symbol: return f"{price:.2f}"
    else: return f"{price:.5f}"

async def send_telegram(message, reply_to_message_id=None, include_mt_buttons=True):
    global http_session
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return None
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    
    if include_mt_buttons:
        payload["reply_markup"] = {
            "inline_keyboard": [[
                {"text": "📲 Open MetaTrader 5", "url": "metatrader5://"},
                {"text": "📲 Open MetaTrader 4", "url": "metatrader4://"}
            ]]
        }
        
    if reply_to_message_id:
        payload["reply_to_message_id"] = reply_to_message_id

    try:
        if http_session is None or http_session.closed:
            http_session = aiohttp.ClientSession()
        async with http_session.post(url, json=payload, timeout=8) as response:
            data = await response.json()
            return data.get("result", {}).get("message_id")
    except Exception as e:
        logging.error(f"Telegram Send Error: {e}")
        return None

async def telegram_command_listener():
    global http_session, last_telegram_update_id
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    
    while True:
        try:
            params = {"offset": last_telegram_update_id + 1, "timeout": 5}
            if http_session is None or http_session.closed:
                http_session = aiohttp.ClientSession()
            
            async with http_session.get(url, params=params, timeout=10) as resp:
                data = await resp.json()
                if data.get("ok") and data.get("result"):
                    for update in data["result"]:
                        last_telegram_update_id = update["update_id"]
                        message = update.get("message", {})
                        text = message.get("text", "").strip()
                        chat_id = str(message.get("chat", {}).get("id", ""))
                        
                        if chat_id != TELEGRAM_CHAT_ID: continue
                        
                        command = text.lower()
                        if command in ["/start", "/help"]:
                            reply = (
                                f"🔱 *V50.0 Forex & Gold Master Engine*\n\n"
                                f"• `/status` - ملخص أداء الأرباح والصفقات المغلقة\n"
                                f"• `/active` - الصفقات النشطة والمفتوحة حالياً\n"
                                f"• `/help` - قائمة الأوامر"
                            )
                            await send_telegram(reply, include_mt_buttons=False)
                        elif command == "/status":
                            stats = await get_performance_summary()
                            total = stats["total"]
                            wins = stats["wins"]
                            pnl = stats["total_pnl"]
                            win_rate = (wins / total * 100) if total > 0 else 0
                            reply = (
                                f"📊 *Forex & Gold Performance Summary*\n"
                                f"━━━━━━━━━━━━━━━\n"
                                f"• Active Trades: `{len(active_live_trades)} / {MAX_ACTIVE_TRADES}`\n"
                                f"• Closed Signals: `{total}` | Win Rate: `{win_rate:.1f}%`\n"
                                f"• Net Profit Ratio: `{pnl:+.2f}R`\n"
                                f"⏰ Time: `{get_local_time()}`"
                            )
                            await send_telegram(reply, include_mt_buttons=False)
                        elif command == "/active":
                            if not active_live_trades:
                                await send_telegram("ℹ️ لا توجد توصيات مفتوحة حالياً.", include_mt_buttons=False)
                            else:
                                reply = "🔥 *Active Forex Signals:*\n\n"
                                for tid, tr in active_live_trades.items():
                                    reply += f"• *{tr['symbol']}* ({tr['direction']}) | Entry: `{format_price(tr['symbol'], tr['entry'])}` | SL: `{format_price(tr['symbol'], tr['sl'])}`\n"
                                await send_telegram(reply, include_mt_buttons=False)
        except Exception as e:
            logging.error(f"Command Listener Error: {e}")
        await asyncio.sleep(3)

# ========================================================================================
# 4. جلب البيانات غير المتزامن (ASYNC DATA FETCHING)
# ========================================================================================
async def fetch_candles_async(yf_symbol, timeframe="15m", period="5d"):
    def _fetch():
        try:
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(period=period, interval=timeframe)
            if df.empty: return None
            df.reset_index(inplace=True)
            df.rename(columns={'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
            return df
        except Exception:
            return None
    return await asyncio.to_thread(_fetch)

# ========================================================================================
# 5. المؤشرات التحليلية واستراتيجية SMC/ICT
# ========================================================================================
def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(period).mean().replace(0, 1e-10)
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_adx(df, period=14):
    df = df.copy()
    df['up'] = df['high'] - df['high'].shift(1)
    df['down'] = df['low'].shift(1) - df['low']
    df['+dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0)
    df['-dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0)
    tr = np.maximum(df['high'] - df['low'], np.maximum(abs(df['high'] - df['close'].shift(1)), abs(df['low'] - df['close'].shift(1))))
    atr = pd.Series(tr).rolling(period).mean().replace(0, 1e-10)
    plus_di = 100 * (pd.Series(df['+dm']).rolling(period).mean() / atr)
    minus_di = 100 * (pd.Series(df['-dm']).rolling(period).mean() / atr)
    dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, 1e-10)
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

def detect_market_structure(df):
    highs, lows = find_swings(df)
    if len(highs) < 2 or len(lows) < 2: return {"bos": "NONE", "choch": "NONE"}
    closed_price = df['close'].iloc[-1]
    bos, choch = "NONE", "NONE"
    if closed_price > highs[-1]: bos = "BULLISH_BOS"
    elif closed_price < lows[-1]: bos = "BEARISH_BOS"
    if lows[-2] < lows[-1] and closed_price > highs[-2]: choch = "BULLISH_CHOCH"
    elif highs[-2] > highs[-1] and closed_price < lows[-2]: choch = "BEARISH_CHOCH"
    return {"bos": bos, "choch": choch}

def calculate_trade_setup(symbol, entry, direction, df_15m):
    tr = np.maximum(df_15m['high'] - df_15m['low'], abs(df_15m['close'] - df_15m['close'].shift(1)))
    atr = tr.rolling(14).mean().iloc[-1]
    if pd.isna(atr) or atr == 0: atr = entry * 0.002

    if "BUY" in direction:
        stop = entry - (atr * 1.5)
        risk = entry - stop
        tp1, tp2, tp3 = entry + (risk * 1.2), entry + (risk * 2.5), entry + (risk * 4.0)
    else:
        stop = entry + (atr * 1.5)
        risk = stop - entry
        tp1, tp2, tp3 = entry - (risk * 1.2), entry - (risk * 2.5), entry - (risk * 4.0)

    rr = abs(tp2 - entry) / abs(entry - stop) if abs(entry - stop) > 0 else 2.5
    return stop, tp1, tp2, tp3, round(rr, 2)

# ========================================================================================
# 6. تحليل وتقييم الفرص
# ========================================================================================
async def analyze_symbol(yf_symbol):
    display_name = SYMBOL_MAP[yf_symbol]
    now_ts = time.time()
    
    for _, t in list(active_live_trades.items()):
        if t["symbol"] == display_name: return None
        
    if display_name in signaled_history and (now_ts - signaled_history[display_name]) < (COOLDOWN_HOURS * 3600):
        return None

    df_15m = await fetch_candles_async(yf_symbol, "15m", "5d")
    df_1h = await fetch_candles_async(yf_symbol, "1h", "7d")

    if df_15m is None or df_1h is None or len(df_15m) < 30: return None

    live_price = float(df_15m['close'].iloc[-1])
    rsi_val = calculate_rsi(df_15m['close']).iloc[-1]
    adx_val = calculate_adx(df_15m)
    ema50_1h = df_1h['close'].ewm(span=50, adjust=False).mean().iloc[-1]
    structure = detect_market_structure(df_15m)

    direction = None
    score = 0

    if (live_price > ema50_1h) and (adx_val > 20) and (rsi_val < 65):
        if structure["bos"] == "BULLISH_BOS": direction = "BUY 🟢"; score += 50
        elif structure["choch"] == "BULLISH_CHOCH": direction = "BUY 🟢"; score += 40
        else: direction = "BUY 🟢"; score += 30

    elif (live_price < ema50_1h) and (adx_val > 20) and (rsi_val > 35):
        if structure["bos"] == "BEARISH_BOS": direction = "SELL 🔴"; score += 50
        elif structure["choch"] == "BEARISH_CHOCH": direction = "SELL 🔴"; score += 40
        else: direction = "SELL 🔴"; score += 30

    if not direction or score < 30: return None

    stop, tp1, tp2, tp3, rr = calculate_trade_setup(display_name, live_price, direction, df_15m)
    if rr < MIN_RR_RATIO: return None

    return {
        "symbol": display_name, "direction": direction, "entry": live_price,
        "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3, "rr": rr, "score": score,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ========================================================================================
# 7. إدارة المتابعة التلقائية وتحديث الصفقات (WEBSOCKET SIMULATION)
# ========================================================================================
async def monitor_trades_loop():
    while True:
        try:
            if not active_live_trades:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            for trade_id, trade in list(active_live_trades.items()):
                yf_symbol = [k for k, v in SYMBOL_MAP.items() if v == trade['symbol']][0]
                df = await fetch_candles_async(yf_symbol, "1m", "1d")
                if df is None or df.empty: continue

                current_price = float(df['close'].iloc[-1])
                symbol_name = trade['symbol']
                msg_id = trade.get("msg_id")

                if "BUY" in trade['direction']:
                    if current_price >= trade['tp3']:
                        await update_trade(trade_id, "TP3_WIN", 4.0)
                        msg = f"🔱 *#{symbol_name}*\n✅ *TP3 HIT! (+4.0R)* 🔥🚀\n📊 Price: `{format_price(symbol_name, trade['tp3'])}`\n⏰ `{get_local_time()}`"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]
                    elif not trade.get('hit_tp2') and current_price >= trade['tp2']:
                        trade['hit_tp2'], trade['hit_tp1'] = True, True
                        trade['sl'] = trade['entry']
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=True, new_sl=trade['sl'])
                        msg = f"🔱 *#{symbol_name}*\n✅ *TP2 HIT! (+2.5R)* ⚡️\n📊 Price: `{format_price(symbol_name, trade['tp2'])}`\n🛡 Move SL to Entry!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif not trade.get('hit_tp1') and current_price >= trade['tp1']:
                        trade['hit_tp1'] = True
                        trade['sl'] = trade['entry'] - ((trade['entry'] - trade['stop']) * 0.5)
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=False, new_sl=trade['sl'])
                        msg = f"🔱 *#{symbol_name}*\n✅ *TP1 HIT! (+1.2R)* 🎯\n📊 Price: `{format_price(symbol_name, trade['tp1'])}`\n🛡 SL updated to secure partial profits."
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif current_price <= trade['sl']:
                        result = "STOP_LOSS" if not trade.get('hit_tp1') else "TRAIL_PROFIT"
                        pnl = -1.0 if not trade.get('hit_tp1') else 0.5
                        await update_trade(trade_id, result, pnl)
                        msg = f"🛑 *SL Hit:* `{symbol_name}` @ `{format_price(symbol_name, trade['sl'])}`"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]

                elif "SELL" in trade['direction']:
                    if current_price <= trade['tp3']:
                        await update_trade(trade_id, "TP3_WIN", 4.0)
                        msg = f"🔱 *#{symbol_name}*\n✅ *TP3 HIT! (+4.0R)* 🔥🚀\n📊 Price: `{format_price(symbol_name, trade['tp3'])}`\n⏰ `{get_local_time()}`"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]
                    elif not trade.get('hit_tp2') and current_price <= trade['tp2']:
                        trade['hit_tp2'], trade['hit_tp1'] = True, True
                        trade['sl'] = trade['entry']
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=True, new_sl=trade['sl'])
                        msg = f"🔱 *#{symbol_name}*\n✅ *TP2 HIT! (+2.5R)* ⚡️\n📊 Price: `{format_price(symbol_name, trade['tp2'])}`\n🛡 Move SL to Entry!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif not trade.get('hit_tp1') and current_price <= trade['tp1']:
                        trade['hit_tp1'] = True
                        trade['sl'] = trade['entry'] + ((trade['stop'] - trade['entry']) * 0.5)
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=False, new_sl=trade['sl'])
                        msg = f"🔱 *#{symbol_name}*\n✅ *TP1 HIT! (+1.2R)* 🎯\n📊 Price: `{format_price(symbol_name, trade['tp1'])}`\n🛡 SL updated to secure partial profits."
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif current_price >= trade['sl']:
                        result = "STOP_LOSS" if not trade.get('hit_tp1') else "TRAIL_PROFIT"
                        pnl = -1.0 if not trade.get('hit_tp1') else 0.5
                        await update_trade(trade_id, result, pnl)
                        msg = f"🛑 *SL Hit:* `{symbol_name}` @ `{format_price(symbol_name, trade['sl'])}`"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]

        except Exception as e:
            logging.error(f"Trade Monitor Error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# ========================================================================================
# 8. الحلقة الرئيسية للمحرك (MAIN ASYNC LOOP)
# ========================================================================================
async def main():
    await init_database()
    await reload_active_trades()
    
    await send_telegram("🚀 *V50.0 Master Forex & Gold Engine Active!*\n• Real-Time SMC Logic\n• Dynamic Database Tracking Enabled", include_mt_buttons=False)

    asyncio.create_task(monitor_trades_loop())
    asyncio.create_task(telegram_command_listener())

    while True:
        try:
            if len(active_live_trades) < MAX_ACTIVE_TRADES:
                for yf_symbol in SYMBOL_MAP.keys():
                    trade = await analyze_symbol(yf_symbol)
                    if trade and len(active_live_trades) < MAX_ACTIVE_TRADES:
                        row_id = await save_trade(trade)
                        if row_id:
                            display_name = trade['symbol']
                            signaled_history[display_name] = time.time()

                            msg = (
                                f"🚨 *SIGNAL:* `{trade['symbol']}` ({trade['direction']})\n\n"
                                f"📍 *Entry:* `{format_price(display_name, trade['entry'])}`\n"
                                f"🎯 *TP1:* `{format_price(display_name, trade['tp1'])}`\n"
                                f"🎯 *TP2:* `{format_price(display_name, trade['tp2'])}`\n"
                                f"🎯 *TP3:* `{format_price(display_name, trade['tp3'])}`\n"
                                f"🛑 *SL:* `{format_price(display_name, trade['stop'])}`\n\n"
                                f"⚖️ *R:R:* `{trade['rr']}` | ⏰ `{get_local_time()}`"
                            )

                            msg_id = await send_telegram(msg, include_mt_buttons=True)
                            if msg_id:
                                await update_trade_msg_id(row_id, msg_id)
                                trade["msg_id"] = msg_id
                                trade["sl"] = trade["stop"]
                                active_live_trades[row_id] = trade

            await asyncio.sleep(CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Main Event Loop Exception: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    keep_alive()
    asyncio.run(main())
