# services/notify.py
from __future__ import annotations

import time as _time
from typing import Any

from flask import current_app

from models.user import User
from models.wallet import TopUp
from models.device_token import DeviceToken
from utils.push import push_to_user

# Optional realtime broadcast (best-effort)
try:
    from mqtt_ingest import publish as mqtt_publish
except Exception:
    mqtt_publish = None


def notify_tellers_new_topup(db, UserModel: type[User], DeviceTokenModel: type[DeviceToken],
                             push_to_user_fn, *, topup: TopUp, commuter: User) -> None:
    """
    Broadcast a 'topup_request' to all tellers.
    Called after a commuter posts a pending GCash receipt.

    Arguments are passed explicitly so this stays import-light:
      - db: SQLAlchemy db module (with session)
      - UserModel, DeviceTokenModel: your mapped models
      - push_to_user_fn: usually utils.push.push_to_user
      - topup: the TopUp row that was created
      - commuter: the g.user (commuter) who submitted the request
    """
    try:
        teller_ids = [uid for (uid,) in db.session.query(UserModel.id).filter(UserModel.role == "teller").all()]
        if not teller_ids:
            current_app.logger.info("[notify] no tellers to notify")
            return

        amount = int(getattr(topup, "amount_pesos", 0) or 0)
        name = f"{(commuter.first_name or '').strip()} {(commuter.last_name or '').strip()}".strip() or \
               (commuter.username or f"User #{commuter.id}")

        payload = {
            "type": "topup_request",
            "topup_id": int(topup.id),
            "amount_php": amount,
            "method": getattr(topup, "method", "gcash"),
            "commuter_id": int(getattr(commuter, "id", 0) or 0),
            "commuter_name": name,
            "deeplink": "/teller/pending",
            "sentAt": int(_time.time() * 1000),
        }

        # Optional MQTT broadcast (UI can listen and refresh list)
        if mqtt_publish:
            try:
                mqtt_publish("teller/all", dict(payload))
            except Exception:
                current_app.logger.exception("[notify] mqtt publish failed")

        # Mobile push to each teller
        title = "🧾 New top-up request"
        body = f"₱{amount:,} via GCash from {name}"
        for uid in teller_ids:
            try:
                push_to_user_fn(
                    db, DeviceTokenModel, uid, title, body, dict(payload),
                    channelId="topups", priority="high", ttl=600
                )
            except Exception:
                current_app.logger.exception("[notify] push to teller failed uid=%s", uid)

    except Exception:
        current_app.logger.exception("[notify] notify_tellers_new_topup failed")
