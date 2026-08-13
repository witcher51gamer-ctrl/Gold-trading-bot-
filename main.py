import os
import time
import requests
import pandas as pd
import numpy as np
import pandas_ta as ta
from datetime import datetime, timezone, timedelta
from flask import Flask
from threading import Thread

# === 1. خادم Flask لضمان استمرارية البوت على Railway (Keep-Alive) ===
app = Flask('')

@app.route('/')
def home():
    return "Binance Gold Automation Engine is ALIVE!", 200

def run_server():
    port = int(os.getenv('PORT', 8000))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run_server)
    t.daemon = True
    t.start()

# === 2. المتغيرات والبيانات الأساسية (تم دمج التوكين ومعرف القناة) ===
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "8905400473:AAGR8W1MrSBe42CNiB7gjLmVgZbaDJ8sjyc")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-1004342900700")
LOCAL_TZ = timezone(timedelta(hours=3))

CHECK_INTERVAL = 60      # فحص الشارت كل دقيقة
COOLDOWN_MINUTES = 45    # زمن انتظار لمنع تكرار التوصيات المتتابعة
last_signal_time = None

def get_local_time():
    return datetime.now(LOCAL_TZ).strftime("%I:%M %p")

def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ خطأ: يرجي ضبط TELEGRAM_TOKEN و TELEGRAM_CHAT_ID!")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "disable_web_page_preview": True
    }
    try:
        res = requests.post(url, json=payload, timeout=10)
        if res.status_code != 200:
            print(f"فشل إرسال الرسالة إلى تليجرام: {res.text}")
    except Exception as e:
        print(f"خطأ أثناء الاتصال بتليجرام: {e}")

# === 3. جلب بيانات XAUUSDT المباشرة ===
def fetch_gold_candles():
    url = "https://api.bitget.com/api/v2/mix/market/candles?symbol=XAUUSDT&productType=USDT-FUTURES&granularity=15m&limit=100"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers, timeout=10).json()
        if res.get("code") == "00000" and res.get("data"):
            raw_data = res["data"]
            df = pd.DataFrame(raw_data, columns=['ts', 'open', 'high', 'low', 'close', 'vol', 'base_vol'])
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['vol'] = df['vol'].astype(float)
            return df
        return None
    except Exception as e:
        print(f"خطأ في جلب شموع الذهب: {e}")
        return None

# === 4. تحليل الهيكل ومؤشرات الزخم (SMC + RSI + ATR) ===
def find_swing_points(df, length=3):
    highs, lows = [], []
    for i in range(length, len(df) - length - 1):
        is_high = all(df['high'].iloc[i] > df['high'].iloc[i-j] for j in range(1, length+1)) and \
                  all(df['high'].iloc[i] > df['high'].iloc[i+j] for j in range(1, length+1))
        is_low = all(df['low'].iloc[i] < df['low'].iloc[i-j] for j in range(1, length+1)) and \
                 all(df['low'].iloc[i] < df['low'].iloc[i+j] for j in range(1, length+1))
        if is_high:
            highs.append(df['high'].iloc[i])
        if is_low:
            lows.append(df['low'].iloc[i])
    return highs, lows

def analyze_gold_market(df):
    if len(df) < 50:
        return None

    df['RSI'] = ta.rsi(df['close'], length=14)
    df['ATR'] = ta.atr(df['high'], df['low'], df['close'], length=14)
    
    latest = df.iloc[-1]
    prev = df.iloc[-2]
    
    live_price = latest['close']
    atr = latest['ATR'] if not pd.isna(latest['ATR']) else 2.5
    rsi = latest['RSI']

    highs, lows = find_swing_points(df, length=3)
    if not highs or not lows:
        return None

    last_high = highs[-1]
    last_low = lows[-1]

    direction = None

    # شروط دخول LONG
    if prev['close'] > last_high and (45.0 <= rsi <= 70.0):
        direction = "LONG"
    # شروط دخول SHORT
    elif prev['close'] < last_low and (30.0 <= rsi <= 55.0):
        direction = "SHORT"

    if not direction:
        return None

    # مسافة إدارة المخاطر
    risk_dist = max(atr * 1.5, 2.5)
    
    if direction == "LONG":
        sl = live_price - risk_dist
        tp1 = live_price + (risk_dist * 1.2)
        tp2 = live_price + (risk_dist * 2.5)
        tp3 = live_price + (risk_dist * 4.0)
    else:
        sl = live_price + risk_dist
        tp1 = live_price - (risk_dist * 1.2)
        tp2 = live_price - (risk_dist * 2.5)
        tp3 = live_price - (risk_dist * 4.0)

    rr_ratio = round((abs(tp2 - live_price) / abs(live_price - sl)), 2)

    return {
        "symbol": "XAUUSDT",
        "direction": direction,
        "entry": live_price,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "rsi": rsi,
        "rr": rr_ratio
    }

# === 5. صياغة التوصية بالشكل المعتمد للبوتات الآلية ===
def build_automated_signal_text(signal):
    msg = (
        f"#{signal['symbol']}\n"
        f"Exchange: Binance Futures\n"
        f"Signal Type: Regular ({signal['direction']})\n"
        f"Leverage: Cross (20x)\n\n"
        f"Entry Targets:\n"
        f"1) {signal['entry']:.2f}\n\n"
        f"Take-Profit Targets:\n"
        f"1) {signal['tp1']:.2f}\n"
        f"2) {signal['tp2']:.2f}\n"
        f"3) {signal['tp3']:.2f}\n\n"
        f"Stop Targets:\n"
        f"1) {signal['sl']:.2f}\n\n"
        f"📊 RSI: {signal['rsi']:.1f} | R:R: {signal['rr']}:1\n"
        f"⏰ {get_local_time()}"
    )
    return msg

# === 6. المحرك التشغيلي للبوت ===
def main():
    global last_signal_time
    print("🚀 تم تشغيل محرك تداول الذهب لبيئة بينانس...")
    
    # رسالة تأكيد التشغيل داخل القناة
    send_telegram("🔱 <b>تم تشغيل بوت توصيات الذهب للربط الآلي بنجاح!</b>")
    
    while True:
        try:
            df = fetch_gold_candles()
            if df is not None:
                signal = analyze_gold_market(df)
                
                now = datetime.now()
                allow_send = True
                if last_signal_time and (now - last_signal_time).total_seconds() < (COOLDOWN_MINUTES * 60):
                    allow_send = False

                if signal and allow_send:
                    last_signal_time = now
                    formatted_signal = build_automated_signal_text(signal)
                    send_telegram(formatted_signal)
                    print(f"[{get_local_time()}] 🟢 تم إرسال توصية {signal['symbol']} إلى القناة!")

            time.sleep(CHECK_INTERVAL)

        except Exception as e:
            print(f"خطأ في دورة الفحص: {e}")
            time.sleep(10)

if __name__ == "__main__":
    keep_alive()
    main()
