# services/notify_ticket.py
from __future__ import annotations
from typing import Optional
from flask import current_app, url_for
from models.ticket_sale import TicketSale
from models.bus import Bus
from models.ticket_stop import TicketStop
from models.user import User
from utils.push import push_to_user  # uses DeviceToken table
from db import db

# optional: if you added topic-based FCM in services/notify_fcm.py
try:
    from services.notify_fcm import notify_user_ticket_received_fcm
except Exception:
    notify_user_ticket_received_fcm = None  # type: ignore


def notify_commuter_ticket_received(ticket_id: int) -> None:
    """
    Fire-and-forget push to the ticket owner IF the ticket is paid and has a user_id.
    Sends both a direct user push (via DeviceToken tokens) and, if available,
    a topic push to users.{user_id}.
    """
    t: Optional[TicketSale] = TicketSale.query.get(ticket_id)
    if not t or not getattr(t, "paid", False) or not getattr(t, "user_id", None):
        return

    # Resolve origin/destination names (StopTime or TicketStop)
    def _name(stop_time_id: Optional[int]) -> str:
        if not stop_time_id:
            return ""
        ts = TicketStop.query.get(stop_time_id)
        return (ts.stop_name if ts else "") or ""

    origin = _name(getattr(t, "origin_stop_time_id", None))
    dest   = _name(getattr(t, "destination_stop_time_id", None))

    bus: Optional[Bus] = Bus.query.get(getattr(t, "bus_id", None))
    bus_ident = getattr(bus, "identifier", None)

    # Compose message + deeplink
    amount = int(round(float(getattr(t, "price", 0) or 0)))
    title  = "🎟️ Ticket received"
    body   = " • ".join([p for p in [f"Bus {bus_ident}" if bus_ident else None, f"{origin} → {dest}"] if p]) or "Your receipt is ready."
    deeplink = f"/commuter/receipt/{int(t.id)}"

    payload = {
        "type": "ticket_received",
        "ticket_id": str(int(t.id)),
        "reference_no": str(getattr(t, "reference_no", "") or f"T{t.id:06d}"),
        "amount_php": str(amount),
        "deeplink": deeplink,
    }

    # 1) Direct push to this user (DeviceToken table)
    try:
        ok = push_to_user(db, __import__("models.device_token").models.device_token.DeviceToken, int(t.user_id),
                          title, body, payload, channelId="payments", priority="high", ttl=3600)
        current_app.logger.info("[push] ticket_received direct=%s ticket_id=%s user_id=%s", ok, t.id, t.user_id)
    except Exception:
        current_app.logger.exception("[push] ticket_received direct failed (ticket_id=%s)", t.id)

    # 2) (Optional) topic push: users.{user_id}
    if notify_user_ticket_received_fcm:
        try:
            notify_user_ticket_received_fcm(
                user_id=int(t.user_id),
                ticket_id=int(t.id),
                reference_no=str(payload["reference_no"]),
                amount_php=float(amount),
                bus_identifier=bus_ident,
                origin_name=origin,
                destination_name=dest,
            )
            current_app.logger.info("[push] ticket_received topic ok ticket_id=%s user_id=%s", t.id, t.user_id)
        except Exception:
            current_app.logger.exception("[push] ticket_received topic failed (ticket_id=%s)", t.id)
