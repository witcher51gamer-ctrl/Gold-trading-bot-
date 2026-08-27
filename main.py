"""
========================================================================================
V52.0 Master Forex & Gold Cloud Engine - Ultra Precision & Twelve Data (Fast & Live)
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
from flask import Flask

# --- إعدادات التسجيل (Logging) ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- الثوابت والإعدادات العامة ---
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN', '8664695982:AAHMaTwCbX1aV1sZjKlie1jK5zJB4tXFSVo')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '6021016826')
TWELVE_DATA_API_KEY = os.getenv('TWELVE_DATA_API_KEY', 'YOUR_TWELVE_DATA_API_KEY') # ضع مفتاحك هنا أو في متغيرات البيئة

TIMEFRAME_PRIMARY = "15min"   # فريم الدخول الأساسي (موافق لصيغة Twelve Data)
TIMEFRAME_CONFIRM = "1h"    # فريم التأكيد الاتجاهي
DATABASE = 'forex_master_signals_v52.db'

CHECK_INTERVAL = 15
BATCH_SIZE = 4
BATCH_DELAY = 1.0

SIGNAL_THRESHOLD = 95       # عتبة قوة الإشارة العالية
MIN_RR_RATIO = 2.5           # حد أدنى للعائد مقابل المخاطرة
MAX_ACTIVE_TRADES = 8        # أقصى عدد صفقات نشطة
COOLDOWN_HOURS = 6.0         # فترة الانتظار لتكرار نفس الزوج

# رموز Twelve Data المباشرة
SYMBOL_MAP = {
    "XAU/USD": "XAU/USD",       # الذهب
    "XAG/USD": "XAG/USD",       # الفضة
    "WTI/USD": "USOIL",         # النفط (أو حسب الرمز المعتمد لديهم)
    "EUR/USD": "EUR/USD",
    "GBP/USD": "GBP/USD",
    "USD/JPY": "USD/JPY",
    "AUD/USD": "AUD/USD",
    "USD/CAD": "USD/CAD",
    "USD/CHF": "USD/CHF",
    "NZD/USD": "NZD/USD",
    "EUR/GBP": "EUR/GBP",
    "EUR/JPY": "EUR/JPY",
    "GBP/JPY": "GBP/JPY"
}

LOCAL_TZ = timezone(timedelta(hours=3))
signaled_history = {}
active_live_trades = {}
http_session = None
last_telegram_update_id = 0
news_blackout = False

# ========================================================================================
# 1. خادم FLASK (KEEP-ALIVE SERVER)
# ========================================================================================
app = Flask('')

@app.route('/', methods=['GET', 'HEAD', 'POST'])
def home():
    return "V52.0 Forex & Gold Ultra Precision Cloud Engine (Twelve Data) is ALIVE!", 200

def run_server():
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_server)
    t.daemon = True
    t.start()

# ========================================================================================
# 2. فلتر الأخبار الاقتصادية المتقدم
# ========================================================================================
async def check_economic_news():
    global news_blackout, http_session
    url = "https://forexfactory-api.com/api/today"
    try:
        if http_session is None or http_session.closed:
            http_session = aiohttp.ClientSession()
        async with http_session.get(url, timeout=5) as response:
            if response.status == 200:
                data = await response.json()
                now = datetime.now(timezone.utc)
                for item in data:
                    impact = str(item.get("impact", "")).lower()
                    if "high" in impact or impact == "3":
                        news_time_str = item.get("date")
                        if news_time_str:
                            news_time = datetime.fromisoformat(news_time_str.replace("Z", "+00:00"))
                            time_diff = abs((news_time - now).total_seconds()) / 60
                            if time_diff <= 30:
                                news_blackout = True
                                logging.warning(f"⚠️ حظر التداول مفعل بسبب خبر اقتصادي هام: {item.get('title')}")
                                return True
    except Exception as e:
        logging.debug(f"فحص الأخبار ينتهي بدون حظر: {e}")
    news_blackout = False
    return False

# ========================================================================================
# 3. إدارة قاعدة البيانات (DATABASE HELPERS)
# ========================================================================================
async def init_database():
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute('''CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT, direction TEXT, entry REAL, stop REAL,
            tp1 REAL, tp2 REAL, tp3 REAL, timestamp TEXT,
            hit_tp1 BOOLEAN, hit_tp2 BOOLEAN, sl REAL, msg_id TEXT,
            status TEXT, pnl REAL, highest REAL, lowest REAL
        )''')
        await conn.commit()

async def save_trade(trade):
    async with aiosqlite.connect(DATABASE) as conn:
        cursor = await conn.execute('''INSERT INTO trades 
            (symbol, direction, entry, stop, tp1, tp2, tp3, timestamp, hit_tp1, hit_tp2, sl, status, pnl, highest, lowest)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (trade['symbol'], trade['direction'], trade['entry'], trade['stop'],
             trade['tp1'], trade['tp2'], trade['tp3'], trade['timestamp'],
             False, False, trade['stop'], 'OPEN', 0.0, trade['entry'], trade['entry']))
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
                trade_dict['highest'] = row['highest'] if row['highest'] else row['entry']
                trade_dict['lowest'] = row['lowest'] if row['lowest'] else row['entry']
                active_live_trades[row['id']] = trade_dict

async def update_trade_msg_id(row_id, msg_id):
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("UPDATE trades SET msg_id=? WHERE id=?", (str(msg_id), row_id))
        await conn.commit()

async def update_trade_progress(trade_id, hit_tp1=False, hit_tp2=False, new_sl=None, highest=None, lowest=None):
    async with aiosqlite.connect(DATABASE) as conn:
        await conn.execute("UPDATE trades SET hit_tp1=?, hit_tp2=?, sl=?, highest=?, lowest=? WHERE id=?", 
                           (hit_tp1, hit_tp2, new_sl, highest, lowest, trade_id))
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
        async with conn.execute("SELECT symbol, pnl, direction FROM trades WHERE status != 'OPEN' ORDER BY pnl DESC LIMIT 1") as cursor:
            best_trade = await cursor.fetchone()
        return {
            "total": total or 0,
            "wins": wins,
            "total_pnl": total_pnl or 0.0,
            "best_trade": best_trade
        }

# ========================================================================================
# 4. الأدوات والتنبيهات للتليجرام
# ========================================================================================
def get_local_time():
    return datetime.now(LOCAL_TZ).strftime("%I:%M %p")

def format_price(symbol, price):
    if not price or price == 0: return "0.00"
    if "JPY" in symbol: return f"{price:.3f}"
    elif "XAU" in symbol or "XAG" in symbol or "OIL" in symbol: return f"{price:.2f}"
    else: return f"{price:.5f}"

def format_duration(start_time_iso):
    try:
        start_time = datetime.fromisoformat(start_time_iso)
        now = datetime.now(timezone.utc)
        diff_seconds = int((now - start_time).total_seconds())
        hours = diff_seconds // 3600
        minutes = (diff_seconds % 3600) // 60
        return f"{hours}h {minutes}m" if hours > 0 else f"{minutes}m"
    except Exception:
        return "0m"

async def send_telegram(message, reply_to_message_id=None, include_mt_buttons=True):
    global http_session
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return None
    
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
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
        logging.error(f"Telegram Send Exception: {e}")
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
                        
                        command = text.split('@')[0].lower() if text.startswith('/') else text.lower()
                        if command in ["/start", "/help"]:
                            reply = (
                                f"🔱 <b>V52.0 Forex & Gold Twelve Data Engine</b> (Strict Threshold: {SIGNAL_THRESHOLD})\n\n"
                                f"• /status - ملخص الأداء القياسي والأرباح\n"
                                f"• /active - الصفقات النشطة والمفتوحة حالياً\n"
                                f"• /news - حالة حظر الأخبار الاقتصادية\n"
                                f"• /help - قائمة الأوامر المتاحة"
                            )
                            await send_telegram(reply, include_mt_buttons=False)
                        elif command == "/news":
                            state = "⚠️ حظر مفعل (خبر قريب)" if news_blackout else "🟢 السوق آمن للتداول"
                            await send_telegram(f"📰 <b>News Status:</b> {state}", include_mt_buttons=False)
                        elif command == "/status":
                            stats = await get_performance_summary()
                            total = stats["total"]
                            wins = stats["wins"]
                            pnl = stats["total_pnl"]
                            win_rate = (wins / total * 100) if total > 0 else 0
                            best_info = "لا يوجد"
                            if stats["best_trade"] and stats["best_trade"][0]:
                                best_info = f"#{stats['best_trade'][0]} (+{stats['best_trade'][1]:.1f}R)"
                            
                            reply = (
                                f"📊 <b>Forex & Gold Performance Summary</b>\n"
                                f"━━━━━━━━━━━━━━━\n"
                                f"• Active Trades: <code>{len(active_live_trades)} / {MAX_ACTIVE_TRADES}</code>\n"
                                f"• Total Signals: <code>{total}</code> | Win Rate: <code>{win_rate:.1f}%</code>\n"
                                f"• Net Profit Ratio: <code>{pnl:+.2f}R</code>\n"
                                f"• 🏆 Top Trade: <b>{best_info}</b>\n"
                                f"⏰ Time: <code>{get_local_time()}</code>"
                            )
                            await send_telegram(reply, include_mt_buttons=False)
                        elif command == "/active":
                            if not active_live_trades:
                                await send_telegram("ℹ️ لا توجد توصيات مفتوحة حالياً.", include_mt_buttons=False)
                            else:
                                reply = "🔥 <b>Active Signals:</b>\n\n"
                                for tid, tr in active_live_trades.items():
                                    dur = format_duration(tr['timestamp'])
                                    reply += f"• <b>#{tr['symbol']}</b> ({tr['direction']}) | Entry: <code>{format_price(tr['symbol'], tr['entry'])}</code> | SL: <code>{format_price(tr['symbol'], tr['sl'])}</code> | ⏱ {dur}\n"
                                await send_telegram(reply, include_mt_buttons=False)
        except Exception as e:
            logging.error(f"Telegram Listener Error: {e}")
        await asyncio.sleep(3)

# ========================================================================================
# 5. جلب البيانات عبر TWELVE DATA API
# ========================================================================================
async def fetch_candles_async(symbol, timeframe="15min", outputsize=100):
    global http_session
    url = f"https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": timeframe,
        "outputsize": outputsize,
        "apikey": TWELVE_DATA_API_KEY,
        "format": "JSON"
    }
    try:
        if http_session is None or http_session.closed:
            http_session = aiohttp.ClientSession()
        async with http_session.get(url, params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                if "values" in data:
                    df = pd.DataFrame(data["values"])
                    # إعادة ترتيب الأعمدة وتصحيح الأنواع
                    df = df.iloc[::-1].reset_index(drop=True) # ترتيب تصاعدي حسب الوقت
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'}, inplace=True)
                    if len(df) < 30: return None
                    return df
    except Exception as e:
        logging.error(f"Twelve Data Fetch Error for {symbol}: {e}")
    return None

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).ewm(alpha=1/period, adjust=False).mean()
    loss = (-delta.where(delta < 0, 0)).ewm(alpha=1/period, adjust=False).mean().replace(0, 1e-10)
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_adx(df, period=14):
    df = df.copy()
    df['up'] = df['high'] - df['high'].shift(1)
    df['down'] = df['low'].shift(1) - df['low']
    df['+dm'] = np.where((df['up'] > df['down']) & (df['up'] > 0), df['up'], 0)
    df['-dm'] = np.where((df['down'] > df['up']) & (df['down'] > 0), df['down'], 0)
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False).mean().replace(0, 1e-10)
    plus_di = 100 * (pd.Series(df['+dm']).ewm(alpha=1/period, adjust=False).mean() / atr)
    minus_di = 100 * (pd.Series(df['-dm']).ewm(alpha=1/period, adjust=False).mean() / atr)
    denom = (plus_di + minus_di).replace(0, 1e-10)
    dx = (np.abs(plus_di - minus_di) / denom) * 100
    adx = dx.ewm(alpha=1/period, adjust=False).mean().fillna(20.0)
    return adx

def find_swing_points(df, length=7):
    highs, lows = [], []
    for i in range(length, len(df) - length - 1):
        if all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, length+1)) and \
           all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, length+1)):
            highs.append({"index": i, "price": df['high'].iloc[i]})
        if all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, length+1)) and \
           all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, length+1)):
            lows.append({"index": i, "price": df['low'].iloc[i]})
    return highs, lows

def detect_market_structure(df):
    highs, lows = find_swing_points(df, length=7)
    if len(highs) < 2 or len(lows) < 2: return {"bos": "NONE", "choch": "NONE"}
    last_high, previous_high = highs[-1]["price"], highs[-2]["price"]
    last_low, previous_low = lows[-1]["price"], lows[-2]["price"]
    closed_price = df['close'].iloc[-2]
    
    bos, choch = "NONE", "NONE"
    if closed_price > last_high: bos = "BULLISH_BOS"
    elif closed_price < last_low: bos = "BEARISH_BOS"
    
    if previous_low < last_low and closed_price > previous_high: choch = "BULLISH_CHOCH"
    elif previous_high > last_high and closed_price < previous_low: choch = "BEARISH_CHOCH"
    return {"bos": bos, "choch": choch}

def detect_liquidity_sweep(df):
    if len(df) < 30: return "NONE"
    highs, lows = find_swing_points(df, length=7)
    if not highs or not lows: return "NONE"
    recent_high, recent_low = highs[-1]["price"], lows[-1]["price"]
    candle = df.iloc[-2]
    if candle['high'] > recent_high and candle['close'] < recent_high: return "BEARISH_SWEEP"
    if candle['low'] < recent_low and candle['close'] > recent_low: return "BULLISH_SWEEP"
    return "NONE"

def calculate_dynamic_sl_tp(symbol, live_entry, direction, df_15m):
    high_low = df_15m['high'] - df_15m['low']
    high_close = np.abs(df_15m['high'] - df_15m['close'].shift())
    low_close = np.abs(df_15m['low'] - df_15m['close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(14).mean().iloc[-2]
    if pd.isna(atr) or atr == 0: atr = live_entry * 0.002

    highs, lows = find_swing_points(df_15m, length=7)
    
    is_gold_or_oil = "XAU" in symbol or "XAG" in symbol or "OIL" in symbol
    multiplier = 2.2 if is_gold_or_oil else 1.2
    spread_buffer = atr * 0.3 if is_gold_or_oil else 0.0
    atr_buffer = (atr * multiplier) + spread_buffer

    if "BUY" in direction or "LONG" in direction:
        recent_low = lows[-1]["price"] if lows else (live_entry - atr * 2.0)
        stop = min(recent_low - atr_buffer, live_entry - (atr * 1.5))
        risk = live_entry - stop
        tp1 = live_entry + (risk * 1.2)
        tp2 = live_entry + (risk * 2.5)
        tp3 = live_entry + (risk * 4.5)
    else:
        recent_high = highs[-1]["price"] if highs else (live_entry + atr * 2.0)
        stop = max(recent_high + atr_buffer, live_entry + (atr * 1.5))
        risk = stop - live_entry
        tp1 = live_entry - (risk * 1.2)
        tp2 = live_entry - (risk * 2.5)
        tp3 = live_entry - (risk * 4.5)

    risk_val = abs(live_entry - stop)
    reward_val = abs(tp2 - live_entry)
    rr_ratio = reward_val / risk_val if risk_val > 0 else 2.5
    return stop, tp1, tp2, tp3, round(rr_ratio, 2)

def calculate_score(direction, structure_15m, sweep_15m, volume_ok):
    score = 0
    if "BUY" in direction or "LONG" in direction:
        if structure_15m["bos"] == "BULLISH_BOS": score += 40
        elif structure_15m["choch"] == "BULLISH_CHOCH": score += 35
    else:
        if structure_15m["bos"] == "BEARISH_BOS": score += 40
        elif structure_15m["choch"] == "BEARISH_CHOCH": score += 35

    if volume_ok: score += 30
    if sweep_15m != "NONE": score += 20
    score += 15  
    return max(0, score)

# ========================================================================================
# 6. محرك التحليل الشامل
# ========================================================================================
async def analyze_symbol(api_symbol):
    if news_blackout: return None

    display_name = SYMBOL_MAP[api_symbol]
    now_ts = time.time()
    
    for _, t in list(active_live_trades.items()):
        if t["symbol"] == display_name: return None
        
    if display_name in signaled_history and (now_ts - signaled_history[display_name]) < (COOLDOWN_HOURS * 3600):
        return None

    df_15m = await fetch_candles_async(api_symbol, "15min", 100)
    df_1h = await fetch_candles_async(api_symbol, "1h", 100)

    if df_15m is None or df_1h is None or len(df_15m) < 50 or len(df_1h) < 50: return None

    live_price = float(df_15m['close'].iloc[-1])
    
    adx_series = calculate_adx(df_15m, 14)
    if adx_series.iloc[-1] < 30.0: return None

    structure_15m = detect_market_structure(df_15m)
    sweep_15m = detect_liquidity_sweep(df_15m)
    
    vol_col = 'volume' if 'volume' in df_15m.columns else None
    volume_ok = True
    if vol_col and df_15m[vol_col].sum() > 0:
        vol_ma = df_15m[vol_col].rolling(20).mean().iloc[-2]
        volume_ok = df_15m[vol_col].iloc[-2] > (vol_ma * 1.5) if vol_ma > 0 else True

    direction = None
    if structure_15m["bos"] == "BULLISH_BOS" or structure_15m["choch"] == "BULLISH_CHOCH":
        direction = "BUY 🟢"
    elif structure_15m["bos"] == "BEARISH_BOS" or structure_15m["choch"] == "BEARISH_CHOCH":
        direction = "SELL 🔴"

    if not direction: return None

    rsi_series = calculate_rsi(df_15m['close'], 14)
    live_rsi = rsi_series.iloc[-1]
    if "BUY" in direction and not (50.0 <= live_rsi <= 70.0): return None
    if "SELL" in direction and not (30.0 <= live_rsi <= 50.0): return None

    ema20_1h = df_1h['close'].ewm(span=20, adjust=False).mean().iloc[-1]
    ema200_1h = df_1h['close'].ewm(span=200, adjust=False).mean().iloc[-1]

    if "BUY" in direction and (df_1h['close'].iloc[-1] < ema20_1h or live_price < ema200_1h): return None
    if "SELL" in direction and (df_1h['close'].iloc[-1] > ema20_1h or live_price > ema200_1h): return None

    stop, tp1, tp2, tp3, rr = calculate_dynamic_sl_tp(display_name, live_price, direction, df_15m)
    score = calculate_score(direction, structure_15m, sweep_15m, volume_ok)

    if score < SIGNAL_THRESHOLD or rr < MIN_RR_RATIO: return None

    return {
        "symbol": display_name, "direction": direction, "entry": live_price,
        "stop": stop, "tp1": tp1, "tp2": tp2, "tp3": tp3, "rr": rr, "score": score,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

# ========================================================================================
# 7. المتابعة التلقائية المباشرة للصفقات المفتوحة
# ========================================================================================
async def monitor_trades_loop():
    while True:
        try:
            if not active_live_trades:
                await asyncio.sleep(CHECK_INTERVAL)
                continue

            for trade_id, trade in list(active_live_trades.items()):
                api_symbol = [k for k, v in SYMBOL_MAP.items() if v == trade['symbol']][0]
                df = await fetch_candles_async(api_symbol, "1min", 30)
                if df is None or df.empty: continue

                current_price = float(df['close'].iloc[-1])
                symbol_name = trade['symbol']
                msg_id = trade.get("msg_id")
                local_now = get_local_time()
                duration = format_duration(trade['timestamp'])

                trade["highest"] = max(trade.get("highest", current_price), current_price)
                trade["lowest"] = min(trade.get("lowest", current_price), current_price)

                if "BUY" in trade['direction']:
                    if current_price >= trade['tp3']:
                        await update_trade(trade_id, "TP3_WIN", 4.5)
                        msg = f"🔱 <b>#{symbol_name}</b>\n✅ <b>TP3 HIT! (+4.5R) 🔥🚀</b>\n📊 Price: <code>{format_price(symbol_name, trade['tp3'])}</code>\n⏱ {duration}\n⏰ {local_now}\n\n💡 إغلاق الصفقة بالكامل!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]
                    elif not trade.get('hit_tp2') and current_price >= trade['tp2']:
                        trade['hit_tp2'], trade['hit_tp1'] = True, True
                        trade['sl'] = trade['entry']
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=True, new_sl=trade['sl'], highest=trade['highest'], lowest=trade['lowest'])
                        msg = f"🔱 <b>#{symbol_name}</b>\n✅ <b>TP2 HIT! (+2.5R) ⚡️</b>\n📊 Price: <code>{format_price(symbol_name, trade['tp2'])}</code>\n⏱ {duration}\n⏰ {local_now}\n\n🛡 تم نقل الـ SL لنقطة الدخول!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif not trade.get('hit_tp1') and current_price >= trade['tp1']:
                        trade['hit_tp1'] = True
                        trade['sl'] = trade['entry'] + ((trade['tp1'] - trade['entry']) * 0.5)
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=False, new_sl=trade['sl'], highest=trade['highest'], lowest=trade['lowest'])
                        msg = f"🔱 <b>#{symbol_name}</b>\n✅ <b>TP1 HIT! (+1.2R) 🎯</b>\n📊 Price: <code>{format_price(symbol_name, trade['tp1'])}</code>\n⏱ {duration}\n⏰ {local_now}\n\n🛡 تفعيل التأمين الذكي!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif current_price <= trade['sl']:
                        result = "TRAIL_PROFIT" if trade.get('hit_tp1') else "STOP_LOSS"
                        pnl = 0.5 if trade.get('hit_tp1') else -1.0
                        await update_trade(trade_id, result, pnl)
                        msg = f"🛑 <b>SL Hit:</b> <code>{symbol_name}</code> @ <code>{format_price(symbol_name, trade['sl'])}</code>\n⏱ {duration}\n⏰ {local_now}"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]

                elif "SELL" in trade['direction']:
                    if current_price <= trade['tp3']:
                        await update_trade(trade_id, "TP3_WIN", 4.5)
                        msg = f"🔱 <b>#{symbol_name}</b>\n✅ <b>TP3 HIT! (+4.5R) 🔥🚀</b>\n📊 Price: <code>{format_price(symbol_name, trade['tp3'])}</code>\n⏱ {duration}\n⏰ {local_now}\n\n💡 إغلاق الصفقة بالكامل!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]
                    elif not trade.get('hit_tp2') and current_price <= trade['tp2']:
                        trade['hit_tp2'], trade['hit_tp1'] = True, True
                        trade['sl'] = trade['entry']
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=True, new_sl=trade['sl'], highest=trade['highest'], lowest=trade['lowest'])
                        msg = f"🔱 <b>#{symbol_name}</b>\n✅ <b>TP2 HIT! (+2.5R) ⚡️</b>\n📊 Price: <code>{format_price(symbol_name, trade['tp2'])}</code>\n⏱ {duration}\n⏰ {local_now}\n\n🛡 تم نقل الـ SL لنقطة الدخول!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif not trade.get('hit_tp1') and current_price <= trade['tp1']:
                        trade['hit_tp1'] = True
                        trade['sl'] = trade['entry'] - ((trade['entry'] - trade['tp1']) * 0.5)
                        await update_trade_progress(trade_id, hit_tp1=True, hit_tp2=False, new_sl=trade['sl'], highest=trade['highest'], lowest=trade['lowest'])
                        msg = f"🔱 <b>#{symbol_name}</b>\n✅ <b>TP1 HIT! (+1.2R) 🎯</b>\n📊 Price: <code>{format_price(symbol_name, trade['tp1'])}</code>\n⏱ {duration}\n⏰ {local_now}\n\n🛡 تفعيل التأمين الذكي!"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                    elif current_price >= trade['sl']:
                        result = "TRAIL_PROFIT" if trade.get('hit_tp1') else "STOP_LOSS"
                        pnl = 0.5 if trade.get('hit_tp1') else -1.0
                        await update_trade(trade_id, result, pnl)
                        msg = f"🛑 <b>SL Hit:</b> <code>{symbol_name}</code> @ <code>{format_price(symbol_name, trade['sl'])}</code>\n⏱ {duration}\n⏰ {local_now}"
                        await send_telegram(msg, reply_to_message_id=msg_id)
                        del active_live_trades[trade_id]

        except Exception as e:
            logging.error(f"Trade Monitor Exception: {e}")
        await asyncio.sleep(CHECK_INTERVAL)

# ========================================================================================
# 8. الحلقة التشغيلية الرئيسية
# ========================================================================================
async def main():
    await init_database()
    await reload_active_trades()
    
    await send_telegram(
        f"🚀 <b>V52.0 Forex & Gold Twelve Data Cloud Engine Active!</b>\n"
        f"• Strict Threshold: <b>{SIGNAL_THRESHOLD}</b> | Min R:R: <b>{MIN_RR_RATIO}</b>\n"
        f"• Real-time Data Stream Enabled ⚡️", 
        include_mt_buttons=False
    )

    asyncio.create_task(monitor_trades_loop())
    asyncio.create_task(telegram_command_listener())

    symbols = list(SYMBOL_MAP.keys())

    while True:
        try:
            await check_economic_news()

            if len(active_live_trades) < MAX_ACTIVE_TRADES and not news_blackout:
                for i in range(0, len(symbols), BATCH_SIZE):
                    if len(active_live_trades) >= MAX_ACTIVE_TRADES: break
                    batch = symbols[i:i + BATCH_SIZE]
                    
                    clean_batch = [SYMBOL_MAP[s] for s in batch]
                    logging.info(f"🔍 جاري فحص دفعة أسواق الفوركس والذهب عبر Twelve Data: {clean_batch}")

                    tasks = [analyze_symbol(sym) for sym in batch]
                    results = await asyncio.gather(*tasks)

                    for trade in results:
                        if trade and len(active_live_trades) < MAX_ACTIVE_TRADES:
                            row_id = await save_trade(trade)
                            if row_id:
                                display_name = trade['symbol']
                                signaled_history[display_name] = time.time()
                                local_now = get_local_time()

                                msg = (
                                    f"<b>#{display_name.replace('/', '')}</b> ({trade['direction']})\n"
                                    f"Exchanges: Twelve Data Cloud Masters\n\n"
                                    f"Entry Targets:\n"
                                    f"1) <code>{format_price(display_name, trade['entry'])}</code>\n\n"
                                    f"Take-Profit Targets:\n"
                                    f"1) <code>{format_price(display_name, trade['tp1'])}</code>\n"
                                    f"2) <code>{format_price(display_name, trade['tp2'])}</code>\n"
                                    f"3) <code>{format_price(display_name, trade['tp3'])}</code>\n\n"
                                    f"Stop Target:\n"
                                    f"1) <code>{format_price(display_name, trade['stop'])}</code>\n\n"
                                    f"━━━━━━━━━━━━━━━\n"
                                    f"📊 Score: <b>{trade['score']}/105</b> | R:R: <b>{trade['rr']}:1</b>\n"
                                    f"⏰ Time: <code>{local_now}</code>"
                                )

                                msg_id = await send_telegram(msg, include_mt_buttons=True)
                                if msg_id:
                                    await update_trade_msg_id(row_id, msg_id)
                                    trade["msg_id"] = msg_id
                                    trade["sl"] = trade["stop"]
                                    trade["highest"] = trade["entry"]
                                    trade["lowest"] = trade["entry"]
                                    active_live_trades[row_id] = trade

                    await asyncio.sleep(BATCH_DELAY)

            await asyncio.sleep(CHECK_INTERVAL)
        except Exception as e:
            logging.error(f"Main Event Loop Exception: {e}")
            await asyncio.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    keep_alive()
    while True:
        try:
            print("\n🚀 Starting V52.0 Forex & Gold Twelve Data Cloud Engine...")
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.critical(f"Fatal Crash: {e}. Restarting in 10s...")
            time.sleep(10)
