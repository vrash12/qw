from flask import Blueprint, request, jsonify, g
from db import db
from models.ticket_sale import TicketSale
from models.fare_segment import FareSegment
from routes.auth import require_role

ticket_sales_bp = Blueprint('ticket_sales', __name__, url_prefix='/ticket-sales')

@ticket_sales_bp.route('', methods=['POST'])
@require_role('commuter')
def purchase_ticket():
    """
    Purchase a ticket for a given fare segment.
    Expects JSON: { "fare_segment_id": 123 }
    """
    data = request.get_json() or {}
    try:
        seg_id = int(data['fare_segment_id'])
    except (KeyError, ValueError):
        return jsonify({"error": "fare_segment_id is required and must be an integer"}), 400

    segment = FareSegment.query.get_or_404(seg_id)
    price   = segment.price
    user_id = g.user.id

    ticket = TicketSale(
        user_id=user_id,
        fare_segment_id=segment.id,
        price=price
    )
    db.session.add(ticket)
    db.session.commit()

    return jsonify({
        "id":         ticket.id,
        "uuid":       ticket.ticket_uuid,
        "price":      f"{price:.2f}",
        "created_at": ticket.created_at.isoformat()
    }), 201

@ticket_sales_bp.route('', methods=['GET'])
@require_role('commuter')
def list_tickets():
    """
    List all tickets purchased by the current commuter.
    """
    user_id = g.user.id
    tickets = (TicketSale
               .query
               .filter_by(user_id=user_id)
               .order_by(TicketSale.created_at.desc())
               .all())

    out = []
    for t in tickets:
        seg   = t.fare_segment
        label = f"{seg.trip.number}: {seg.origin.stop_name} → {seg.destination.stop_name}"
        out.append({
            "id":                t.id,
            "uuid":              t.ticket_uuid,
            "price":             f"{t.price:.2f}",
            "created_at":        t.created_at.isoformat(),
            "fare_segment_id":   seg.id,
            "segment_label":     label
        })

    return jsonify(out), 200
