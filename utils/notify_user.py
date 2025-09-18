# utils/notify_user.py
from __future__ import annotations

from typing import Dict, Iterable, Optional
from flask import current_app

# DB models for push fallback
from db import db
from models.device_token import DeviceToken
from models.user import User

# Your existing push helper (FCM/APNs/Expo)
from utils.push import push_to_user

# Optional MQTT publisher (best-effort)
try:
    from mqtt_ingest import publish as mqtt_publish  # def publish(topic: str, payload: dict) -> bool
except Exception:
    mqtt_publish = None


def _log(msg: str, *args):
    try:
        current_app.logger.info(msg, *args)
    except Exception:
        pass


def _teller_ids() -> Iterable[int]:
    """All users who should receive teller broadcasts."""
    rows = db.session.query(User.id).filter(User.role.in_(["teller", "pao"])).all()
    return [int(r[0]) for r in rows]


def notify_user(user_id: int, payload: Dict) -> bool:
    """
    Stateless notify to a single commuter user.
    1) Try MQTT (topic: user/<uid>/events)
    2) Fallback to device push via push_to_user()
    """
    ok = False

    # 1) MQTT (best-effort)
    if mqtt_publish is not None:
        try:
            topic = f"user/{int(user_id)}/events"
            mqtt_publish(topic, dict(payload))
            _log("[notify_user] mqtt publish topic=%s keys=%s", topic, list(payload.keys()))
            ok = True
        except Exception:
            current_app.logger.exception("[notify_user] mqtt publish failed uid=%s", user_id)

    # 2) Push fallback (if no MQTT or you also want a banner)
    try:
        title = payload.get("title") or "Notification"
        body  = payload.get("body")  or (payload.get("message") or "")
        ok_push = push_to_user(
            db, DeviceToken, int(user_id),
            title, body, dict(payload),
            channelId=payload.get("channelId") or "topups",
            priority="high",
            ttl=payload.get("ttl") or 600,
        )
        ok = bool(ok or ok_push)
        _log("[notify_user] push fallback uid=%s ok=%s", user_id, ok_push)
    except Exception:
        current_app.logger.exception("[notify_user] push fallback failed uid=%s", user_id)

    return bool(ok)


def notify_tellers(payload: Dict) -> int:
    """
    Broadcast to all tellers/PAOs.
    MQTT topics:
      • teller/broadcast (fanout)
      • teller/<uid>/events (per-user, optional)
    Also falls back to device push for each teller.
    Returns the count of successful push deliveries (best-effort).
    """
    delivered = 0
    uids = list(_teller_ids())

    # 1) MQTT fanout (best-effort)
    if mqtt_publish is not None:
        try:
            mqtt_publish("teller/broadcast", dict(payload))
            _log("[notify_tellers] mqtt broadcast keys=%s", list(payload.keys()))
        except Exception:
            current_app.logger.exception("[notify_tellers] mqtt broadcast failed")

        # optional per-user topics
        for uid in uids:
            try:
                mqtt_publish(f"teller/{uid}/events", dict(payload))
            except Exception:
                # don’t spam logs per user
                pass

    # 2) Push fallback to each teller device
    for uid in uids:
        try:
            title = payload.get("title") or "PGT"
            body  = payload.get("body")  or (payload.get("message") or "New event")
            ok_push = push_to_user(
                db, DeviceToken, int(uid),
                title, body, dict(payload),
                channelId=payload.get("channelId") or "topups",
                priority="high",
                ttl=payload.get("ttl") or 600,
            )
            if ok_push:
                delivered += 1
        except Exception:
            current_app.logger.exception("[notify_tellers] push failed uid=%s", uid)

    _log("[notify_tellers] delivered=%s (uids=%s)", delivered, len(uids))
    return delivered
