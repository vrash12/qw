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

# backend/models/tickets.py
@tickets_bp.route('/tickets/preview', methods=['POST'])
@require_role('commuter')
def preview_fare():
    """
    SOLO:
      { "fare_segment_id": 7, "passenger_type": "regular" }
      -> { fare: 15 }

    GROUP / BATCH:
      {
        "fare_segment_id": 7,                            # or use origin/destination below
        "origin_stop_time_id": 123,                      # optional
        "destination_stop_time_id": 456,                 # optional
        "items": [
          {"passenger_type":"regular","quantity":3},
          {"passenger_type":"discount","quantity":2}
        ]
      }
      -> {
           total_fare: 75,
           items: [
             { passenger_type:"regular", quantity:3, fare_each:15, subtotal:45 },
             { passenger_type:"discount", quantity:2, fare_each:15*0.8≈12, subtotal:24 }
           ]
         }
    """
    data = request.get_json() or {}
    seg = None

    fare_segment_id = data.get('fare_segment_id')
    if fare_segment_id:
        seg = FareSegment.query.get(fare_segment_id)
    else:
        # allow O/D selection, if that’s what your UI sends
        oid = data.get('origin_stop_time_id')
        did = data.get('destination_stop_time_id')
        if oid and did:
            seg = (
                db.session.query(FareSegment)
                .filter_by(origin_stop_time_id=oid, destination_stop_time_id=did)
                .first()
            )

    if not seg:
        return jsonify(error="invalid or missing fare segment"), 400

    items = data.get('items')
    if not items:
        # SOLO
        ptype = data.get('passenger_type')
        if ptype not in ('regular', 'discount'):
            return jsonify(error="invalid passenger_type"), 400
        amount = fare_for(seg, ptype)
        return jsonify(fare=amount), 200

    # GROUP
    out_items = []
    total = 0
    for it in items:
        ptype = (it.get('passenger_type') or '').lower()
        qty   = max(0, int(it.get('quantity') or 0))
        if ptype not in ('regular','discount') or qty <= 0:
            return jsonify(error="invalid items"), 400
        each = fare_for(seg, ptype)
        subtotal = each * qty
        total += subtotal
        out_items.append({
            "passenger_type": ptype,
            "quantity": qty,
            "fare_each": each,
            "subtotal": subtotal
        })
    return jsonify(total_fare=to_peso(total), items=out_items), 200


@tickets_bp.route('/tickets', methods=['POST'])
@require_role('commuter')
def create_ticket():
    """
    SOLO:
      { "fare_segment_id": 7, "passenger_type": "regular" }

    GROUP:
      {
        "fare_segment_id": 7,                            # or origin/destination pair
        "origin_stop_time_id": 123,                      # optional
        "destination_stop_time_id": 456,                 # optional
        "items": [
          {"passenger_type":"regular","quantity":3},
          {"passenger_type":"discount","quantity":2}
        ],
        "primary_type": "regular"                        # used for display/compat; stored in passenger_type
      }

    Creates exactly ONE TicketSale row for both solo and group.
    """
    user = request.ctx.user
    data = request.get_json() or {}

    # pick segment either by id or O/D
    seg = None
    fare_segment_id = data.get('fare_segment_id')
    if fare_segment_id:
        seg = FareSegment.query.get(fare_segment_id)
    else:
        oid = data.get('origin_stop_time_id')
        did = data.get('destination_stop_time_id')
        if oid and did:
            seg = (
                db.session.query(FareSegment)
                .filter_by(origin_stop_time_id=oid, destination_stop_time_id=did)
                .first()
            )

    if not seg:
        return jsonify(error="invalid or missing fare segment"), 400

    items = data.get('items')
    if not items:
        # SOLO
        ptype = data.get('passenger_type')
        if ptype not in ('regular','discount'):
            return jsonify(error="invalid passenger_type"), 400
        total_amount = fare_for(seg, ptype)
        is_group = False
        n_regular = 1 if ptype == 'regular' else 0
        n_discount = 1 if ptype == 'discount' else 0
        stored_ptype = ptype
        count = 1
    else:
        # GROUP — one ticket, multiple passengers
        total_amount = 0
        n_regular = 0
        n_discount = 0
        for it in items:
            ptype = (it.get('passenger_type') or '').lower()
            qty   = max(0, int(it.get('quantity') or 0))
            if ptype not in ('regular','discount') or qty <= 0:
                return jsonify(error="invalid items"), 400
            if ptype == 'regular':
                n_regular += qty
            else:
                n_discount += qty
            total_amount += fare_for(seg, ptype) * qty

        count = n_regular + n_discount
        if count <= 0:
            return jsonify(error="items sum to zero"), 400

        # store the "primary" for compatibility with old fields
        stored_ptype = (data.get('primary_type') or 'regular').lower()
        if stored_ptype not in ('regular','discount'):
            stored_ptype = 'regular'
        is_group = True

    bus = seg.trip.bus
    ref = gen_reference(bus)

    ticket = TicketSale(
        user_id                  = user.id,
        fare_segment_id          = seg.id,
        origin_stop_time_id      = seg.origin_stop_time_id,
        destination_stop_time_id = seg.destination_stop_time_id,
        price                    = to_peso(total_amount),   # integer pesos
        passenger_type           = stored_ptype,            # keep for compatibility
        reference_no             = ref,
        paid                     = 0,
        bus_id                   = bus.id,
        # group meta
        is_group                 = bool(is_group),
        group_regular            = int(n_regular),
        group_discount           = int(n_discount),
    )
    db.session.add(ticket)
    db.session.commit()

    qr_payload = str(ticket.ticket_uuid)

    origin_name      = StopTime.query.get(seg.origin_stop_time_id).stop_name
    destination_name = StopTime.query.get(seg.destination_stop_time_id).stop_name

    return jsonify(
        id=ticket.id,
        referenceNo=ref,
        qr=qr_payload,
        busCode=bus.identifier,
        origin=origin_name,
        destination=destination_name,
        passengerType=stored_ptype,
        fare=to_peso(total_amount),     # integer pesos
        paid=False,
        # helpful extras for UIs that expect batch
        count=count,
        group={
          "regular": n_regular,
          "discount": n_discount,
          "total": count,
        },
        receipt_image=url_for("commuter.commuter_ticket_image", ticket_id=ticket.id, _external=True),
    ), 201


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

