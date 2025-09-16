# services/notify.py
from datetime import timezone, datetime
# services/notify.py (or use your local helper)
def notify_tellers_new_topup(db, User, DeviceToken, push_to_user, *, topup, commuter):
    # target both 'teller' and 'pao' so you can't miss
    ids = [uid for (uid,) in db.session.query(User.id)
           .filter(User.role.in_(["teller", "pao"])).all()]
    if not ids:
        return

    amount = int(getattr(topup, "amount_pesos", 0) or 0)
    name = f"{(commuter.first_name or '').strip()} {(commuter.last_name or '').strip()}".strip() \
           or (commuter.username or f"User #{commuter.id}")
    title = "🧾 New top-up request"
    body  = f"₱{amount:,} via GCash from {name}"
    payload = {
        "type": "topup_request",
        "topup_id": int(topup.id),
        "amount_php": amount,
        "method": "gcash",
        "deeplink": "/teller/pending",
        "sentAt": int(_time.time() * 1000),
    }
    for uid in ids:
        push_to_user(db, DeviceToken, uid, title, body, payload,
                     channelId="topups", priority="high", ttl=600)
