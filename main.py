import os
import requests
import json

TELEGRAM_TOKEN = "8664695982:AAHMaTwCbX1aV1sZjKlie1jK5zJB4tXFSVo"
TELEGRAM_CHAT_ID = "6435071066"

url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
payload = {
    "chat_id": TELEGRAM_CHAT_ID,
    "text": "test"
}

res = requests.post(url, json=payload)
data = res.json()

print("\n" + "="*40)
print("ERROR REASON:", data.get("description", "UNKNOWN ERROR"))
print("="*40 + "\n")
