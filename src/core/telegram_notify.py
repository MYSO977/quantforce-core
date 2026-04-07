import requests
import os

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "7019506529:AAHGd21YXchNiqaJrMYAH7qNE3O7TmNRRB8")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "6318635327")

def send(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=8)
        return r.json().get("ok", False)
    except Exception as e:
        print(f"Telegram error: {e}")
        return False
