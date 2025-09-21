# utils/notify_user.py
from __future__ import annotations
import time as _time
from typing import Dict, Any
from flask import current_app

from db import db
from models.user import User
from models.device_token import DeviceToken
from utils.push import push_to_user

try:
    from mqtt_ingest import publish as mqtt_publish  # def publish(topic, payload) -> bool
except Exception:
    mqtt_publish = None


def _title_body(payload: Dict[str, Any]) -> tuple[str, str]:
    t = (payload.get("type") or "").lower()
    if t == "wallet_topup":
        amt  = int(payload.get("amount_php") or 0)
        newb = int(payload.get("new_balance_php") or 0)
        return ("Wallet top-up successful", f"+₱{amt:,} • New balance: ₱{newb:,}")
    if t == "wallet_topup_rejected":
        amt  = int(payload.get("amount_php") or 0)
        meth = (payload.get("method") or "gcash").title()
        rsn  = (payload.get("reason") or "Tap to view details").strip()
        return ("Top-up rejected", f"₱{amt:,} via {meth} • {rsn}")
    # Fallback
    return ("PGT", payload.get("message") or "You have a new notification")


def notify_user(user_id: int, payload: Dict[str, Any]) -> bool:
    """
    Sends BOTH:
      1) Realtime MQTT (best effort) to topics the app listens to:
         user/{uid}/events AND user/{uid}/notify (also 'users/' alias)
      2) Mobile push (FCM/APNs) via stored DeviceToken(s) using utils.push
    """
    ok = True

    # MQTT best-effort
    try:
        if mqtt_publish:
            enriched = dict(payload)
            enriched.setdefault("sentAt", int(_time.time() * 1000))
            for root in ("user", "users"):
                mqtt_publish(f"{root}/{int(user_id)}/events", enriched)
                mqtt_publish(f"{root}/{int(user_id)}/notify", enriched)
            current_app.logger.info("[notify_user] mqtt fanout uid=%s type=%s", user_id, payload.get("type"))
        else:
            current_app.logger.warning("[notify_user] mqtt disabled")
    except Exception:
        ok = False
        current_app.logger.exception("[notify_user] mqtt publish failed uid=%s", user_id)

    # OS push
    try:
        title, body = _title_body(payload)
        deeplink = payload.get("deeplink") or "/(tabs)/commuter/wallet"
        push_ok = push_to_user(
            db, DeviceToken, int(user_id),
            title, body,
            {**payload, "deeplink": deeplink},
            channelId="wallet", priority="high", ttl=600,
        )
        ok = bool(push_ok) and ok
        current_app.logger.info("[notify_user] push uid=%s ok=%s", user_id, push_ok)
    except Exception:
        ok = False
        current_app.logger.exception("[notify_user] push failed uid=%s", user_id)

    return ok


def notify_tellers(payload: Dict[str, Any]) -> bool:
    """
    Broadcast to all tellers via push + optional MQTT 'tellers/topups'.
    """
    ok = True
    # Push to each teller
    try:
        teller_ids = [uid for (uid,) in db.session.query(User.id).filter(User.role == "teller").all()]
        for uid in teller_ids:
            try:
                title = payload.get("title") or "🧾 New top-up request"
                body  = payload.get("body")  or payload.get("message") or "Open the Teller console"
                push_ok = push_to_user(
                    db, DeviceToken, int(uid),
                    title, body, payload,
                    channelId="topups", priority="high", ttl=600,
                )
                ok = bool(push_ok) and ok
            except Exception:
                current_app.logger.exception("[notify_tellers] push failed uid=%s", uid)
    except Exception:
        ok = False
        current_app.logger.exception("[notify_tellers] enumerate tellers failed")

    # MQTT broadcast (optional)
    try:
        if mqtt_publish:
            mqtt_publish("tellers/topups", {**payload, "sentAt": int(_time.time() * 1000)})
    except Exception:
        ok = False
        current_app.logger.exception("[notify_tellers] mqtt publish failed")

    return ok
