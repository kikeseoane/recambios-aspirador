"""Send an already-published improvement summary, without exposing bot secrets."""
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def main():
    message = Path(os.environ["GROWTH_NOTIFICATION_FILE"]).read_text(encoding="utf-8")
    token = os.environ["TELEGRAM_TOKEN"]
    chat = os.environ["TELEGRAM_CHAT_ID"]
    if not token or not chat or not message.strip():
        raise ValueError("Missing notification configuration")
    request = Request(f"https://api.telegram.org/bot{token}/sendMessage",
        data=urlencode({"chat_id": chat, "text": message,
                        "disable_web_page_preview": "true"}).encode(), method="POST")
    with urlopen(request, timeout=30) as response:
        if not json.load(response).get("ok"):
            raise RuntimeError("Telegram rejected the notification")
    print("Resumen de mejora enviado a Telegram")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        # HTTP exceptions can contain the token-bearing URL: never log them.
        print("No se pudo enviar el resumen a Telegram; la mejora ya esta publicada.", file=sys.stderr)
        raise SystemExit(1)
