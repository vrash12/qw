# routes/tickets_static.py
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, url_for, g
from db import db
from models.schedule   import StopTime
from models.ticket_sale import TicketSale
from routes.auth        import require_role
from datetime import timedelta
from flask import g, url_for   # g.user is set by require_role

tickets_bp = Blueprint("tickets", __name__)
QR_PATH = "qr"              # where your PNGs live under /static

# ---------- helpers --------------------------------------
def hops_between(a: StopTime, b: StopTime) -> int:
    """Absolute hop distance between two stops on the same trip."""
    return abs(a.seq - b.seq)

def calc_fare(hops: int, passenger_type: str) -> float:
    base = 10 + max(hops - 1, 0) * 2
    return round(base * 0.8) if passenger_type == "discount" else base

def png_name(base: int, passenger_type: str) -> str:
    return f"fare_{base}{'_disc' if passenger_type == 'discount' else ''}.png"

def gen_reference() -> str:
    last = db.session.query(TicketSale).order_by(TicketSale.id.desc()).first()
    nxt  = (last.id if last else 0) + 1
    return f"PGT-{nxt:03d}"

# ---------- 1. fare preview --------------------------------
@tickets_bp.route("/tickets/preview", methods=["POST"])
@require_role("commuter")
def preview():
    """
    Body:
      {
        "origin_stop_time_id": 1,
        "destination_stop_time_id": 5,
        "passenger_type": "regular" | "discount"
      }
    """
    data = request.get_json() or {}
    ptype = data.get("passenger_type")
    if ptype not in ("regular", "discount"):
        return jsonify(error="invalid passenger_type"), 400

    o = StopTime.query.get(data.get("origin_stop_time_id"))
    d = StopTime.query.get(data.get("destination_stop_time_id"))
    if not o or not d or o.trip_id != d.trip_id:
        return jsonify(error="invalid stops"), 400

    hops = hops_between(o, d)
    fare = calc_fare(hops, ptype)
    return jsonify(fare=f"{fare:.2f}"), 200

# ---------- 2. issue ticket --------------------------------
@tickets_bp.route("/tickets", methods=["POST"])
@require_role("commuter")
def create_ticket():
    data = request.get_json() or {}
    ptype = data.get("passenger_type")
    if ptype not in ("regular", "discount"):
        return jsonify(error="invalid passenger_type"), 400

    o = StopTime.query.get(data.get("origin_stop_time_id"))
    d = StopTime.query.get(data.get("destination_stop_time_id"))
    if not o or not d or o.trip_id != d.trip_id:
        return jsonify(error="invalid stops"), 400

    hops = hops_between(o, d)
    full_base = 10 + max(hops - 1, 0) * 2        # integer base (10,12,…,44)
    fare = calc_fare(hops, ptype)

    ticket = TicketSale(
        user_id                  = g.user.id,
        origin_stop_time_id      = o.id,
        destination_stop_time_id = d.id,
        price                    = fare,
        passenger_type           = ptype,
        reference_no             = gen_reference(),
        paid                     = 0
    )
    db.session.add(ticket)
    db.session.commit()

    # static PNG path
    img = png_name(full_base, ptype)
    qr_url = url_for("static", filename=f"{QR_PATH}/{img}", _external=True)

    return jsonify(
        id=ticket.id,
        referenceNo=ticket.reference_no,
        qr_url=qr_url,
        origin=o.stop_name,
        destination=d.stop_name,
        passengerType=ptype,
        fare=f"{fare:.2f}",
        paid=False
    ), 201

# ---------- 3. list tickets --------------------------------
@tickets_bp.route("/tickets", methods=["GET"])
@require_role("pao")   # commuters would filter by their own user_id
def list_tickets():
    day = request.args.get("date")
    try:
        day = datetime.strptime(day, "%Y-%m-%d").date() if day else datetime.utcnow().date()
    except ValueError:
        return jsonify(error="bad date"), 400

    start = datetime.combine(day, datetime.min.time())
    end   = datetime.combine(day, datetime.max.time())

    qs = TicketSale.query.filter(TicketSale.created_at.between(start, end)).order_by(TicketSale.id.asc())
    out = []
    for t in qs:
        out.append({
            "referenceNo": t.reference_no,
            "commuter":    f"{t.user.first_name} {t.user.last_name}",
            "date":        t.created_at.strftime("%B %d, %Y"),
            "time":        t.created_at.strftime("%-I:%M %p"),
            "fare":        f"{t.price:.2f}",
            "paid":        bool(t.paid)
        })
    return jsonify(out), 200

@tickets_bp.route('/tickets/mine', methods=['GET'])
@require_role('commuter')
def my_receipts():
    print("DEBUG: g.user.id →", g.user.id)
    qs = TicketSale.query.filter_by(user_id=g.user.id).all()
    """
    Returns the authenticated commuter’s tickets.
    Optional filters:
      • days=<int>            – last N days (default 30)
      • from=<YYYY-MM-DD>     – start date (inclusive)
      • to=<YYYY-MM-DD>       – end   date (inclusive)
    """
    # ---------- resolve date range ----------
    try:
        if 'from' in request.args or 'to' in request.args:
            start = datetime.strptime(request.args.get('from', '1970-01-01'), "%Y-%m-%d")
            end   = datetime.strptime(request.args.get('to',   datetime.utcnow().strftime("%Y-%m-%d")), "%Y-%m-%d")
            end   = datetime.combine(end, datetime.max.time())       # include entire end-day
        else:
            days  = int(request.args.get('days', 30))
            end   = datetime.utcnow()
            start = end - timedelta(days=days)
    except ValueError:
        return jsonify(error="invalid date / days parameter"), 400

    # ---------- query ----------
    qs = TicketSale.query.filter(
            TicketSale.user_id == g.user.id,
            TicketSale.created_at.between(start, end)
         ).order_by(TicketSale.created_at.desc())

    # ---------- shape response ----------
    out = []
    for t in qs:
        out.append({
            "id":           t.id,
            "referenceNo":  t.reference_no,
            "date":         t.created_at.strftime("%B %d, %Y"),
            "time":         t.created_at.strftime("%-I:%M %p"),
            "fare":         f"{float(t.price):.2f}",
            "paid":         bool(t.paid),
            "qr":           str(t.ticket_uuid),               # let the RN app render QR locally
            # absolute URL to a static PNG if you prefer images:
            # "qr_url": url_for('static', filename=f"qr/{t.ticket_uuid}.png", _external=True),
        })
    return jsonify(out), 200