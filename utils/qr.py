# utils/qr.py
import json, os
from datetime import timezone
from flask import url_for

def build_qr_payload(ticket, *, origin_name=None, destination_name=None) -> str:
    link = url_for("commuter.qr_image_for_ticket", ticket_id=ticket.id, _external=True)
    data = {
        "schema": "pgt.ticket.v1",
        "ref": ticket.reference_no,
        "uuid": str(ticket.ticket_uuid),
        "type": ticket.passenger_type,
        "fare": round(float(ticket.price), 2),
        "paid": bool(ticket.paid),
        "createdAt": ticket.created_at.replace(tzinfo=timezone.utc).isoformat(),
        "busId": ticket.bus_id,
        "userId": ticket.user_id,
        "originId": getattr(ticket, "origin_stop_time_id", None),
        "destinationId": getattr(ticket, "destination_stop_time_id", None),
        "origin": origin_name,
        "destination": destination_name,
        "link": link,  # 👈 the image URL
    }
    return json.dumps(data, separators=(",", ":"))
