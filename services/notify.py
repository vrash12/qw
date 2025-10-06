# services/notify.py
from flask import current_app
from db import db
from models.user import User
from models.device_token import DeviceToken
from utils.push import send_push_async

def notify_commuters_announcement(*, bus_id: int, message: str) -> bool:
    # Pull ALL commuter tokens for now (you can scope by bus later)
    rows = (
        db.session.query(DeviceToken.token)
        .join(User, User.id == DeviceToken.user_id)
        .filter(User.role == "commuter")
        .all()
    )
    tokens = [t for (t,) in rows if t]

    current_app.logger.info(
        "[push] commuters: %d tokens fetched (bus_id=%s)", len(tokens), bus_id
    )

    if not tokens:
        current_app.logger.warning(
            "[push] no commuter device tokens to notify (bus_id=%s)", bus_id
        )
        return False

    payload = {
        "type": "announcement",
        "bus_id": int(bus_id),
        "deeplink": "/commuter/announcements",
    }

    # Use a channel that you also create on Android (see UI below)
    send_push_async(tokens, "🚌 Bus announcement", message, payload, channelId="announcements")

    current_app.logger.info(
        "[push] commuters notified: %d tokens (bus_id=%s)", len(tokens), bus_id
    )
    return True

# ---- Back-compat shim for older code paths ----
def notify_tellers_new_topup(*, bus_id: int | None = None,
                             commuter_id: int | None = None,
                             amount_php: float | int | None = None,
                             pao_id: int | None = None,
                             **kwargs) -> int:
    """
    Notify PAO/tellers about a new wallet top-up.
    Accepts flexible kwargs so older callers don't crash.
    Filters to a bus if bus_id is provided.
    Returns number of device tokens notified.
    """
    try:
        q = (
            db.session.query(DeviceToken.token)
            .join(User, User.id == DeviceToken.user_id)
            .filter(User.role == "pao")
        )
        if bus_id is not None:
            q = q.filter(User.assigned_bus_id == bus_id)

        tokens = [t for (t,) in q.all() if t]
        if not tokens:
            current_app.logger.info(
                "[push] notify_tellers_new_topup: no PAO tokens (bus_id=%s)", bus_id
            )
            return 0

        title = "💸 New Wallet Top-up"
        parts = []
        if amount_php is not None:
            try:
                parts.append(f"+₱{float(amount_php):.2f}")
            except Exception:
                parts.append(f"+₱{amount_php}")
        if commuter_id is not None:
            parts.append(f"commuter #{int(commuter_id)}")
        if bus_id is not None:
            parts.append(f"bus {int(bus_id)}")
        body = " • ".join(parts) if parts else "A wallet has been topped up."

        payload = {
            "type": "wallet_topup",
            "bus_id": (int(bus_id) if bus_id is not None else None),
            "commuter_id": (int(commuter_id) if commuter_id is not None else None),
            "amount_php": (float(amount_php) if amount_php is not None else None),
            **{k: v for k, v in kwargs.items() if k not in {"tokens"}},
        }

        send_push_async(tokens, title, body, payload, channelId="payments")
        current_app.logger.info(
            "[push] notify_tellers_new_topup: sent tokens=%d bus_id=%s commuter_id=%s amount=%s",
            len(tokens), bus_id, commuter_id, amount_php
        )
        return len(tokens)
    except Exception:
        current_app.logger.exception("notify_tellers_new_topup failed")
        return 0
