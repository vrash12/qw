# routes/tickets_static.py
from datetime import datetime, timedelta
from flask import Blueprint, jsonify, request, url_for, current_app, g
from db import db
from models.ticket_stop import TicketStop           # ← use TicketStop (not StopTime)
from models.ticket_sale import TicketSale
from routes.auth import require_role

tickets_bp = Blueprint("tickets", __name__)

# Keep this as an absolute web path for convenience in PAO (used to build qr_bg_url)
# NOTE: when calling url_for('static', filename=...), pass ONLY the path relative to /static
QR_PATH = "static/qr"

# ─────────── QR image catalog (snapping) ──────────────────────────────────────
REGULAR_VALUES  = [10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44]
DISCOUNT_VALUES = [ 8,10,13,14,16,18,19,21,22,24,26,27,29,30,32,34,35]

def _nearest(value: int, allowed: list[int]) -> int:
    return min(allowed, key=lambda v: abs(v - value))

def jpg_name(peso: int, passenger_type: str) -> str:
    """
    Choose the closest existing fare image; keeps working even if an odd fare
    is computed (e.g., very long route) but only a discrete set of JPGs exists.
    """
    prefix  = "discount" if (passenger_type or "").lower() == "discount" else "regular"
    allowed = DISCOUNT_VALUES if prefix == "discount" else REGULAR_VALUES
    snap    = _nearest(int(round(float(peso or 0))), allowed)
    return f"{prefix}_{snap}.jpg"

# ─────────── helpers ──────────────────────────────────────────────────────────
def hops_between(a: TicketStop, b: TicketStop) -> int:
    return abs(int(a.seq) - int(b.seq))

def base_fare(hops: int) -> int:
    # first hop ₱10, then +₱2 per additional hop
    return 10 + max(hops - 1, 0) * 2

def calc_fare(hops: int, passenger_type: str) -> int:
    reg = base_fare(hops)
    return reg if (passenger_type or "").lower() == "regular" else round(reg * 0.8)

def gen_reference() -> str:
    last = db.session.query(TicketSale).order_by(TicketSale.id.desc()).first()
    nxt = (last.id if last else 0) + 1
    return f"BUS1-{nxt:04d}"

def _resolve_stops_from_payload(data: dict):
    """
    Accept both new and legacy keys. We now store/use TicketStop ids.
    - preferred: origin_stop_id / destination_stop_id
    - legacy fallback: origin_stop_time_id / destination_stop_time_id
    """
    o_id = data.get("origin_stop_id") or data.get("origin_stop_time_id")
    d_id = data.get("destination_stop_id") or data.get("destination_stop_time_id")
    if not o_id or not d_id:
        return None, None
    o = TicketStop.query.get(o_id)
    d = TicketStop.query.get(d_id)
    return o, d

# ─────────── 1) fare preview ─────────────────────────────────────────────────
@tickets_bp.route("/tickets/preview", methods=["POST"])
@require_role("commuter")
def preview():
    data  = request.get_json() or {}
    ptype = (data.get("passenger_type") or "").lower()
    if ptype not in ("regular", "discount"):
        return jsonify(error="invalid passenger_type"), 400

    o, d = _resolve_stops_from_payload(data)
    if not o or not d:
        return jsonify(error="invalid stops"), 400

    fare = calc_fare(hops_between(o, d), ptype)
    return jsonify(fare=f"{fare:.2f}"), 200

# ─────────── 2) issue ticket (UNPAID, QR background only) ────────────────────
@tickets_bp.route("/tickets", methods=["POST"])
@require_role("commuter")
def create_ticket():
    data  = request.get_json() or {}
    ptype = (data.get("passenger_type") or "").lower()
    if ptype not in ("regular", "discount"):
        return jsonify(error="invalid passenger_type"), 400

    o, d = _resolve_stops_from_payload(data)
    if not o or not d:
        return jsonify(error="invalid stops"), 400

    hops      = hops_between(o, d)
    fare_peso = calc_fare(hops, ptype)

    ticket = TicketSale(
        user_id                  = g.user.id,
        origin_stop_time_id      = o.id,   # column name kept for compatibility
        destination_stop_time_id = d.id,   # (stores TicketStop ids)
        price                    = fare_peso,
        passenger_type           = ptype,
        reference_no             = gen_reference(),
        paid                     = False
    )
    db.session.add(ticket)
    db.session.commit()

    # Build a URL to the nearest existing JPG under /static/qr/
    img_file = jpg_name(fare_peso, ptype)
    qr_url   = url_for("static", filename=f"qr/{img_file}", _external=True)
    current_app.logger.info(f"[tickets_static:create_ticket] qr_url → {qr_url}")

    return jsonify({
        "id":            ticket.id,
        "referenceNo":   ticket.reference_no,
        "qr_url":        qr_url,
        "origin":        o.stop_name,
        "destination":   d.stop_name,
        "passengerType": ptype,
        "fare":          f"{fare_peso:.2f}",
        "paid":          False
    }), 201

# ─────────── 3) commuter’s own receipts ──────────────────────────────────────
@tickets_bp.route("/tickets/mine", methods=["GET"])
@require_role("commuter")
def my_receipts():
    days  = request.args.get("days", default=30, type=int)
    end   = datetime.utcnow()
    start = end - timedelta(days=days)

    rows = (
        TicketSale.query
          .filter(
              TicketSale.user_id == g.user.id,
              TicketSale.created_at.between(start, end)
          )
          .order_by(TicketSale.created_at.desc())
    )

    out = []
    for t in rows:
        # Resolve O/D directly via TicketStop ids stored in the ticket
        o = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        d = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        if not o or not d:
            # skip malformed rows gracefully
            continue

        hops      = hops_between(o, d)
        fare_peso = calc_fare(hops, (t.passenger_type or "").lower())
        img_file  = jpg_name(fare_peso, (t.passenger_type or "").lower())
        qr_url    = url_for("static", filename=f"qr/{img_file}", _external=True)

        out.append({
            "id":          t.id,
            "referenceNo": t.reference_no,
            "date":        t.created_at.strftime("%B %d, %Y"),
            "time":        t.created_at.strftime("%-I:%M %p").lower(),
            "fare":        f"{float(t.price):.2f}",
            "paid":        bool(t.paid),
            "qr_url":      qr_url,
        })

    return jsonify(out), 200
