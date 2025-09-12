# backend/models/tickets.py
import uuid
from datetime import datetime

from flask import Blueprint, request, jsonify
from db import db
from models.schedule import FareSegment, StopTime
from models.ticket_sale import TicketSale
from routes.auth import require_role   # protects commuters/PAO only

tickets_bp = Blueprint('tickets', __name__)

# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def gen_reference(bus) -> str:
    """
    BUS-scoped ticket code, e.g. BUS1-0001
    Guarantees uniqueness *per bus*.
    """
    last = (
        db.session.query(TicketSale)
        .filter_by(bus_id=bus.id)
        .order_by(TicketSale.id.desc())
        .first()
    )
    next_num = (last.id if last else 0) + 1
    return f"{bus.identifier}-{next_num:04d}"


def to_peso(x) -> int:
    """
    Normalize any numeric to a whole-peso integer.
    """
    try:
        return int(round(float(x)))
    except Exception:
        return 0


def fare_for(segment: FareSegment, passenger_type: str) -> int:
    """
    Compute the fare for the given segment and passenger type (whole pesos).
    Discount is a simple 20% off, then rounded to the nearest peso.
    """
    base = float(segment.price)
    if passenger_type == 'discount':
        base = base * 0.80
    return to_peso(base)

# ──────────────────────────────────────────────────────────────────────────────
# 1) Fare preview (commuter)
# ──────────────────────────────────────────────────────────────────────────────

@tickets_bp.route('/tickets/preview', methods=['POST'])
@require_role('commuter')
def preview_fare():
    """
    Body: { "fare_segment_id": 7, "passenger_type": "regular" }
    Returns: { "fare": 15 }    # whole pesos, no cents
    """
    data = request.get_json() or {}
    seg  = FareSegment.query.get(data.get('fare_segment_id'))
    if not seg or data.get('passenger_type') not in ('regular','discount'):
        return jsonify(error="invalid parameters"), 400

    amount = fare_for(seg, data['passenger_type'])  # int pesos
    return jsonify(fare=amount), 200

# ──────────────────────────────────────────────────────────────────────────────
# 2) Issue ticket (commuter)
# ──────────────────────────────────────────────────────────────────────────────

@tickets_bp.route('/tickets', methods=['POST'])
@require_role('commuter')
def create_ticket():
    """
    Body: { "fare_segment_id": 7, "passenger_type": "regular" }
    """
    user = request.ctx.user      # from auth layer (set by middleware)
    data = request.get_json() or {}
    seg  = FareSegment.query.get(data.get('fare_segment_id'))
    ptype = data.get('passenger_type')

    if not seg or ptype not in ('regular','discount'):
        return jsonify(error="invalid parameters"), 400

    amount = fare_for(seg, ptype)          # int pesos
    bus    = seg.trip.bus
    ref    = gen_reference(bus)

    ticket = TicketSale(
        user_id                    = user.id,
        fare_segment_id            = seg.id,
        origin_stop_time_id        = seg.origin_stop_time_id,
        destination_stop_time_id   = seg.destination_stop_time_id,
        price                      = amount,     # store as whole pesos
        passenger_type             = ptype,
        reference_no               = ref,
        paid                       = 0,         # unpaid until PAO scans
    )
    db.session.add(ticket)
    db.session.commit()

    # generate QR payload – can be as simple as the ticket UUID
    qr_payload = str(ticket.ticket_uuid)

    return jsonify(
        id=ticket.id,
        referenceNo=ref,
        qr=qr_payload,
        busCode=bus.identifier,
        origin=StopTime.query.get(seg.origin_stop_time_id).stop_name,
        destination=StopTime.query.get(seg.destination_stop_time_id).stop_name,
        passengerType=ptype,
        fare=amount,         # whole pesos, no decimals
        paid=False
    ), 201

# ──────────────────────────────────────────────────────────────────────────────
# 3) Daily records (PAO)
# ──────────────────────────────────────────────────────────────────────────────

@tickets_bp.route('/tickets', methods=['GET'])
@require_role('pao')   # PAO or manager can view all; commuters would filter self
def list_tickets():
    """
    /tickets?date=2025-04-04
    Lists the day’s issued tickets.
    """
    date_str = request.args.get('date')
    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.utcnow().date()
    except ValueError:
        return jsonify(error="bad date"), 400

    qs = TicketSale.query.filter(
        TicketSale.created_at.between(day, datetime.combine(day, datetime.max.time()))
    ).order_by(TicketSale.id.asc())

    out = []
    for t in qs:
        out.append({
            "referenceNo": t.reference_no,
            "commuter":    f"{t.user.first_name} {t.user.last_name}",
            "date":        t.created_at.strftime("%B %d, %Y"),
            "time":        t.created_at.strftime('%-I:%M %p'),
            "fare":        int(round(float(t.price or 0))),  # whole pesos
            "paid":        bool(t.paid)
        })
    return jsonify(out), 200

