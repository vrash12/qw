# services/notify_fcm.py
import os, json
import firebase_admin
from firebase_admin import messaging, credentials

# NEW: per-user topic
TOPIC_USER_TICKETS = "users.{user_id}"
TOPIC_COMMUTERS_BUS = "commuters.bus.{bus_id}"

def _ensure_firebase_app():
    """Initialize Firebase Admin once, with a guaranteed projectId."""
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass
    cred = None
    project_id = (
        os.environ.get("GOOGLE_CLOUD_PROJECT")
        or os.environ.get("GCLOUD_PROJECT")
        or os.environ.get("FIREBASE_PROJECT_ID")
        or None
    )
    cred_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not cred_path:
        cred_path = os.path.normpath(os.path.join(os.getcwd(), "etc", "secrets", "firebase-sa.json"))
    if cred_path and os.path.exists(cred_path):
        with open(cred_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        project_id = project_id or data.get("project_id")
        cred = credentials.Certificate(data)
    else:
        inline = os.environ.get("FIREBASE_CREDENTIALS_JSON")
        if inline:
            data = json.loads(inline)
            project_id = project_id or data.get("project_id")
            cred = credentials.Certificate(data)
        else:
            cred = credentials.ApplicationDefault()
    opts = {"projectId": project_id} if project_id else None
    return firebase_admin.initialize_app(cred, opts)

# ---------- NEW: ticket notification helpers ----------

def notify_user_ticket_received_fcm(
    *,
    user_id: int,
    ticket_id: int,
    reference_no: str,
    amount_php: float,
    bus_identifier: str | None,
    origin_name: str | None,
    destination_name: str | None,
) -> str:
    """
    Sends a 'Ticket received' push to the commuter's per-user topic.
    Client must subscribe to topic 'users.{user_id}'.
    """
    _ensure_firebase_app()

    topic = TOPIC_USER_TICKETS.format(user_id=int(user_id))
    title = "🎟️ Ticket received"
    route = f"/commuter/receipt/{int(ticket_id)}"
    parts = []
    if bus_identifier:
        parts.append(f"Bus {bus_identifier}")
    if origin_name and destination_name:
        parts.append(f"{origin_name} → {destination_name}")
    body = " • ".join(parts) or "Your receipt is ready."

    msg = messaging.Message(
        notification=messaging.Notification(title=title, body=body),
        data={
            "type": "ticket_received",
            "ticket_id": str(int(ticket_id)),
            "reference_no": str(reference_no),
            "amount_php": f"{amount_php:.2f}",
            "deeplink": route,
        },
        topic=topic,
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(channel_id="payments")
        ),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(aps=messaging.Aps(sound="default"))
        ),
    )
    return messaging.send(msg)

# Optional fallback: direct-token multicast (only if you store **FCM** tokens server-side)
def notify_user_ticket_received_fcm_direct_tokens(
    *,
    user_id: int,
    ticket_id: int,
    reference_no: str,
    amount_php: float,
    bus_identifier: str | None,
    origin_name: str | None,
    destination_name: str | None,
) -> int:
    """
    Fallback if you want to push directly to device tokens (must be FCM tokens, not Expo push tokens).
    Returns count of successful sends.
    """
    _ensure_firebase_app()
    try:
        from models.device_token import DeviceToken
    except Exception:
        return 0

    tokens = [
        t.token
        for t in DeviceToken.query.filter(DeviceToken.user_id == int(user_id)).all()
        # crude filter: skip Expo push tokens if present in this table
        if not t.token.startswith("ExponentPushToken")
    ]
    if not tokens:
        return 0

    title = "🎟️ Ticket received"
    route = f"/commuter/receipt/{int(ticket_id)}"
    parts = []
    if bus_identifier:
        parts.append(f"Bus {bus_identifier}")
    if origin_name and destination_name:
        parts.append(f"{origin_name} → {destination_name}")
    body = " • ".join(parts) or "Your receipt is ready."

    # chunk to 500 (FCM limit)
    sent = 0
    for i in range(0, len(tokens), 500):
        batch = tokens[i : i + 500]
        mm = messaging.MulticastMessage(
            tokens=batch,
            notification=messaging.Notification(title=title, body=body),
            data={
                "type": "ticket_received",
                "ticket_id": str(int(ticket_id)),
                "reference_no": str(reference_no),
                "amount_php": f"{amount_php:.2f}",
                "deeplink": route,
            },
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(channel_id="payments")
            ),
            apns=messaging.APNSConfig(
                headers={"apns-priority": "10"},
                payload=messaging.APNSPayload(aps=messaging.Aps(sound="default"))
            ),
        )
        resp = messaging.send_multicast(mm)
        sent += resp.success_count
    return sent
