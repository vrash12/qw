# services/notify.py
from __future__ import annotations

import time as _time
from typing import Optional
from flask import current_app

def _safe_name(user) -> str:
    if not user:
        return "Unknown user"
    fn = (getattr(user, "first_name", "") or "").strip()
    ln = (getattr(user, "last_name", "") or "").strip()
    name = (fn + " " + ln).strip()
    return name or (getattr(user, "username", None) or f"User #{getattr(user, 'id', '?')}")

def notify_tellers_new_topup(db, User, DeviceToken, push_to_user, *, topup, commuter) -> int:
    """
    Notify all teller-like operators that a commuter submitted a new GCash top-up request.
    Returns the count of users we attempted to notify.
    """
    try:
        # target both roles
        q = db.session.query(User.id).filter(User.role.in_(["teller", "pao"]))
        ids = [uid for (uid,) in q.all()]
        if not ids:
            return 0

        amount = int(getattr(topup, "amount_pesos", 0) or 0)
        method = (getattr(topup, "method", None) or "gcash").lower()
        title = "🧾 New top-up request"
        body  = f"₱{amount:,} via {method.title()} from {_safe_name(commuter)}"

        payload = {
            "type": "topup_request",
            "topup_id": int(getattr(topup, "id")),
            "amount_php": amount,
            "method": method,
            # Your app switches to the Pending tab on receipt; deeplink is optional/helpful:
            "deeplink": "/(tabs)/teller/pending",
            "sentAt": int(_time.time() * 1000),
        }

        attempted = 0
        for uid in ids:
            try:
                push_to_user(
                    db, DeviceToken, uid,
                    title, body, payload,
                    channelId="topups", priority="high", ttl=600
                )
                attempted += 1
            except Exception:
                # Don't fail the whole loop if one user’s push errors
                current_app.logger.exception("[push] notify teller uid=%s failed", uid)

        return attempted

    except Exception:
        current_app.logger.exception("[notify] notify_tellers_new_topup failed")
        return 0
