# utils/qr.py
from __future__ import annotations
import json
from datetime import timezone
from flask import url_for
from routes.tickets_static import jpg_name

def _iso_z(dt):
    dt = dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def build_qr_payload(ticket, *, origin_name=None, destination_name=None) -> str:
    # Try dynamic receipt QR first
    try:
        link = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=ticket.id, _external=True)
    except Exception:
        # Fallback to shipped JPG
        prefix = "discount" if (ticket.passenger_type or "").lower() == "discount" else "regular"
        fname  = jpg_name(int(round(float(getattr(ticket, "price", 0) or 0))), prefix)
        link   = url_for("static", filename=f"qr/{fname}", _external=True)

    payload = {
        "version": 1,
        "type": "ticket",
        "id": int(ticket.id),
        "ref": ticket.reference_no,
        "fare": float(getattr(ticket, "price", 0) or 0),
        "passengerType": (ticket.passenger_type or "").lower(),
        "origin": origin_name,
        "destination": destination_name,
        "issuedAt": _iso_z(ticket.created_at),
        "link": link,
    }
    return json.dumps(payload, separators=(",", ":"))
