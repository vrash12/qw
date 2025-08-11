# backend/utils/push.py
import os
from typing import List, Optional
from flask import current_app
import json

# Requests is optional; we keep import inside function to avoid startup crashes
EXPO_URL = "https://exp.host/--/api/v2/push/send"
DISABLE_PUSH = os.getenv("DISABLE_PUSH", "0") == "1"

def _chunk(xs: List[str], n: int):
    for i in range(0, len(xs), n):
        yield xs[i:i+n]

def send_push(tokens: List[str], title: str, body: str, data: Optional[dict] = None) -> bool:
    """
    Fires Expo push to given tokens. If DISABLE_PUSH=1 or requests/SSL fail, it logs and returns False.
    Never raises at import-time; requests imported lazily here.
    """
    if not tokens:
        return False
    if DISABLE_PUSH:
        current_app.logger.info("[push] disabled by env; skipping send")
        return False

    try:
        import requests  # lazy import to avoid cert errors on startup
    except Exception as e:
        current_app.logger.warning(f"[push] requests not available: {e}")
        return False

    ok_any = False
    payload_base = {
        "title": title,
        "body": body,
        "sound": "default",
        "data": data or {},
    }

    for batch in _chunk(tokens, 100):  # Expo: 100 messages per call
        msgs = [{**payload_base, "to": t} for t in batch]
        try:
            r = requests.post(EXPO_URL, json=msgs, timeout=10)
            if r.status_code == 200:
                ok_any = True
            else:
                current_app.logger.error("Expo push error %s – %s", r.status_code, r.text)
        except Exception as e:
            current_app.logger.warning(f"[push] send failed: {e}")

    return ok_any


def push_to_bus(db, User, DeviceToken, bus_id: int, title: str, body: str, extra: Optional[dict] = None) -> bool:
    """
    Sends push to all PAOs assigned to a bus. Models are passed in to avoid circular imports.
    """
    try:
        pao_ids = [u.id for u in User.query.filter_by(role="pao", assigned_bus_id=bus_id)]
        tokens  = [t.token for t in DeviceToken.query.filter(DeviceToken.user_id.in_(pao_ids))]
        return send_push(tokens, title, body, extra)
    except Exception as e:
        current_app.logger.warning(f"[push_to_bus] failed: {e}")
        return False
