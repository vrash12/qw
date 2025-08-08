# routes/tickets_static.py
from datetime import datetime, timedelta
from flask import Blueprint, jsonify, request, url_for, current_app, g
from db import db
from models.schedule    import StopTime
from models.ticket_sale import TicketSale
from routes.auth        import require_role
from sqlalchemy import func

tickets_bp = Blueprint("tickets", __name__)
QR_PATH    = "qr"  # matches <project_root>/static/qr/*.jpg

# ─────────── helpers ───────────────────────────────────────────────────
def hops_between(a: StopTime, b: StopTime) -> int:
    return abs(a.seq - b.seq)

def base_fare(hops: int) -> int:
    return 10 + max(hops - 1, 0) * 2

def calc_fare(hops: int, passenger_type: str) -> int:
    reg = base_fare(hops)
    return reg if passenger_type == "regular" else round(reg * 0.8)

def jpg_name(peso: int, passenger_type: str) -> str:
    """
    Return the exact filename you already have:
      regular →  'regular_12.jpg'
      discount → 'discount_14.jpg'
    """
    prefix = "regular" if passenger_type == "regular" else "discount"
    return f"{prefix}_{peso}.jpg"

def gen_reference() -> str:
    last = (
        db.session.query(TicketSale)
          .order_by(TicketSale.id.desc())
          .first()
    )
    nxt = (last.id if last else 0) + 1
    return f"BUS1-{nxt:04d}"

# ─────────── 1. fare preview ───────────────────────────────────────────
@tickets_bp.route("/tickets/preview", methods=["POST"])
@require_role("commuter")
def preview():
    data  = request.get_json() or {}
    ptype = data.get("passenger_type")
    if ptype not in ("regular", "discount"):
        return jsonify(error="invalid passenger_type"), 400

    o = StopTime.query.get(data.get("origin_stop_time_id"))
    d = StopTime.query.get(data.get("destination_stop_time_id"))
    if not o or not d or o.trip_id != d.trip_id:
        return jsonify(error="invalid stops"), 400

    fare = calc_fare(hops_between(o, d), ptype)
    return jsonify(fare=f"{fare:.2f}"), 200

# ─────────── 2. issue ticket ───────────────────────────────────────────
@tickets_bp.route("/tickets", methods=["POST"])
@require_role("commuter")
def create_ticket():
    data  = request.get_json() or {}
    ptype = data.get("passenger_type")
    if ptype not in ("regular", "discount"):
        return jsonify(error="invalid passenger_type"), 400

    o = StopTime.query.get(data.get("origin_stop_time_id"))
    d = StopTime.query.get(data.get("destination_stop_time_id"))
    if not o or not d or o.trip_id != d.trip_id:
        return jsonify(error="invalid stops"), 400

    hops      = hops_between(o, d)
    reg_peso  = base_fare(hops)
    fare_peso = calc_fare(hops, ptype)

    ticket = TicketSale(
        user_id                  = g.user.id,
        origin_stop_time_id      = o.id,
        destination_stop_time_id = d.id,
        price                    = fare_peso,
        passenger_type           = ptype,
        reference_no             = gen_reference(),
        paid                     = False
    )
    db.session.add(ticket)
    db.session.commit()

    # Build the URL for your existing JPEG
    img_file = jpg_name(fare_peso, ptype)
    qr_url   = url_for("static", filename=f"{QR_PATH}/{img_file}", _external=True)
    current_app.logger.info(f"[create_ticket] qr_url → {qr_url}")

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

# ─────────── 4. commuter’s own receipts ───────────────────────────────
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
        # Recompute base so we pick the correct image
        hops      = hops_between(t.origin_stop_time, t.destination_stop_time)
        fare_peso = calc_fare(hops, t.passenger_type)
        img_file  = jpg_name(fare_peso, t.passenger_type)
        qr_url    = url_for("static", filename=f"{QR_PATH}/{img_file}", _external=True)

        out.append({
            "id":          t.id,
            "referenceNo": t.reference_no,
            "date":        t.created_at.strftime("%B %d, %Y"),
            "time":        t.created_at.strftime("%-I:%M %p").lower(),
            "fare":        f"{float(t.price):.2f}",
            "paid":        bool(t.paid),
            "qr_url":      qr_url,     # front-end will pick this up and render the <Image/>
        })

    return jsonify(out), 200
