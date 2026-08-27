import os
import requests
import logging

logging.basicConfig(level=logging.INFO)

TELEGRAM_TOKEN = "8664695982:AAHMaTwCbX1aV1sZjKlie1jK5zJB4tXFSVo"
TELEGRAM_CHAT_ID = "6435071066"

url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
payload = {
    "chat_id": TELEGRAM_CHAT_ID,
    "text": "اختبار فحص الاتصال المباشر"
}

response = requests.post(url, json=payload)
print("=== TELEGRAM API RESPONSE ===")
print(f"Status Code: {response.status_code}")
print(f"Full Body: {response.text}")
print("=============================")
