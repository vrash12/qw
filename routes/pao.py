from flask import Blueprint, request, jsonify, g
from datetime import datetime
from routes.auth import require_role
from models.schedule import Trip, StopTime
from flask import current_app
import traceback

pao_bp = Blueprint('pao', __name__, url_prefix='/pao')

@pao_bp.route('/bus-trips', methods=['GET'])
@require_role('pao')
def pao_bus_trips():
    """
    GET /pao/bus-trips?date=YYYY-MM-DD
    Returns all trips for the PAO’s assigned bus on that date.
    """
    try:
        # ✅ FIX: First, check if the user object exists at all.
        if not hasattr(g, 'user'):
            current_app.logger.error("!!! [pao_bus_trips] Authentication failed: g.user not set by decorator.")
            return jsonify(error="Not authorized: Could not identify user from token."), 401

        # Now we can safely get the parameters
        bus_id = getattr(g.user, 'assigned_bus_id', None)
        date_str = request.args.get('date')
        
        if not bus_id or not date_str:
            return jsonify(error="PAO is not assigned to a bus or date is missing"), 400

        svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()

        trips = (
            Trip.query
            .filter_by(bus_id=bus_id, service_date=svc_date)
            .order_by(Trip.start_time.asc())
            .all()
        )
        
        result = [
            {
                "id":         t.id,
                "number":     t.number,
                "start_time": t.start_time.strftime("%H:%M"),
                "end_time":   t.end_time.strftime("%H:%M"),
            }
            for t in trips
        ]
        return jsonify(result), 200

    except Exception as e:
        current_app.logger.error("!!! [pao_bus_trips] An unexpected error occurred !!!")
        current_app.logger.error(traceback.format_exc())
        return jsonify(error=f"An internal server error occurred: {e}"), 500
        
@pao_bp.route('/stop-times', methods=['GET'])
@require_role('pao')
def pao_stop_times():
    """
    GET /pao/stop-times?trip_id=<int>
    Returns the stop list for that trip.
    """
    trip_id = request.args.get('trip_id', type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = (
        StopTime.query
        .filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc())
        .all()
    )
    return jsonify([
        {
            "stop_name":   st.stop_name,
            "arrive_time": st.arrive_time.strftime("%H:%M"),
            "depart_time": st.depart_time.strftime("%H:%M")
        } for st in sts
    ]), 200


@pao_bp.route('/tickets', methods=['POST'])
@require_role('pao')
def create_ticket():
    data = request.get_json(silent=True) or {}
    current_app.logger.debug(f"[PAO:tickets POST] user={g.user.id} payload={data!r}")

    try:
        # ─── parse & validate payload ──────────────────────────
        o_id = data.get('origin_stop_id')      or data.get('origin_stop_time_id')
        d_id = data.get('destination_stop_id') or data.get('destination_stop_time_id')
        p    = data.get('passenger_type')
        uid  = data.get('commuter_id')
        client_ts = data.get("created_at")          # ISO string from the app
        try:
            ticket_dt = dtparse.parse(client_ts) if client_ts else datetime.now()
        except Exception:
            ticket_dt = datetime.now()
        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)
        if not o or not d:
            return jsonify(error="origin or destination not found"), 400

        if p not in ('regular', 'discount'):
            return jsonify(error="invalid passenger_type"), 400

        user = User.query.get(uid)
        if not user:
            return jsonify(error="invalid commuter_id"), 400

        # ─── fare calculation ──────────────────────────────────
        hops = abs(o.seq - d.seq)
        base = 10 + max(hops - 1, 0) * 2
        fare = round(base * 0.8) if p == 'discount' else base

        # ─── ensure PAO has an assigned bus ────────────────────
        pao    = g.user
        bus_id = pao.assigned_bus_id
        if not bus_id:
            return jsonify(error="PAO has no assigned bus"), 400

        # ─── create & commit TicketSale ────────────────────────
        ref = _gen_reference(bus_id)
        ticket = TicketSale(
            bus_id         = bus_id,        # ← tie to this PAO’s bus
            user_id        = user.id,
            price          = fare,
            passenger_type = p,
            reference_no   = ref,
            paid           = False,
            created_at     = ticket_dt,
        )
        db.session.add(ticket)
        db.session.commit()

        # ─── build QR URL & final response ────────────────────
        qr_url = url_for(
            'static',
            filename=f"qr/{_png_name(base, p=='discount')}",
            _external=True
        )
        return jsonify({
            "id":            ticket.id,
            "referenceNo":   ref,               # now BUS1-0001 style
            "qr_url":        qr_url,
            "origin":        o.stop_name,
            "destination":   d.stop_name,
            "passengerType": p,
            "fare":          f"{fare:.2f}",
            "paid":          False
        }), 201

    except Exception as e:
        current_app.logger.exception("!! create_ticket unexpected error")
        traceback.print_exc()
        return jsonify(error=str(e)), 500
@pao_bp.route('/stops', methods=['GET'])
@require_role('pao')    # or commuter, depending on who needs to load stops
def list_stops():
    # return an array of {id, name} sorted by your seq field
    rows = TicketStop.query.order_by(TicketStop.seq).all()
    return jsonify([{"id": r.id, "name": r.stop_name, "seq": r.seq} for r in rows]), 200

@pao_bp.route('/commuters', methods=['GET'])
@require_role('pao')
def list_commuters():
    users = User.query.filter_by(role='commuter') \
                     .order_by(User.first_name, User.last_name).all()
    return jsonify([
        {"id": u.id, "name": f"{u.first_name} {u.last_name}"}
        for u in users
    ]), 200

@pao_bp.route('/timetable', methods=['GET'])
@require_role('pao')
def timetable():
    # TODO: pull this PAO’s route timetable from your data store
    sample = [
        {"trip_id": 1, "route": "A→B", "departure": "06:00", "arrival": "07:00"},
        {"trip_id": 2, "route": "B→C", "departure": "07:30", "arrival": "08:30"},
    ]
    return jsonify(sample), 200

@pao_bp.route('/monitor-commuter', methods=['GET'])
@require_role('pao')
def live_user_locations():
    # TODO: return real commuter locations via MQTT subscription
    sample = [
        {"user_id": 42, "lat": 14.7002,   "lng": 121.0456, "last_seen": "2025-04-01T11:59:00Z"},
        {"user_id": 73, "lat": 14.7025,   "lng": 121.0501, "last_seen": "2025-04-01T11:59:30Z"},
    ]
    return jsonify(sample), 200

@pao_bp.route('/broadcast', methods=['POST'])
@require_role('pao')
def broadcast():
    data = request.get_json() or {}
    message = data.get('message', '').strip()
    if not message:
        return jsonify({"error": "message is required"}), 400

    ann = Announcement(
        message    = message,
        created_by = g.user.id
    )
    db.session.add(ann)
    db.session.commit()

    return jsonify({
        "id":         ann.id,
        "message":    ann.message,
        "timestamp":  ann.timestamp.isoformat(),
        "created_by": ann.created_by
    }), 201

@pao_bp.route('/broadcast', methods=['GET'])
@require_role('pao')
def list_broadcasts():
    anns = Announcement.query.order_by(Announcement.timestamp.desc()).all()
    return jsonify([
        {
            "id":         a.id,
            "message":    a.message,
            "timestamp":  a.timestamp.isoformat(),
            "created_by": a.created_by
        } for a in anns
    ]), 200

@pao_bp.route('/validate-fare', methods=['POST'])
@require_role('pao')
def validate_fare():
    data     = request.get_json() or {}
    user_id  = data.get('user_id')
    fare_amt = data.get('fare_amount')
    # TODO: real validation logic (e.g., check scanned QR → TicketSale)
    valid = True

    return jsonify({
        "user_id":     user_id,
        "fare_amount": fare_amt,
        "valid":       valid
    }), 200


# — ELECTRONIC TICKETING (PAO) ——————————————————————————————

def _gen_reference(bus_id: int) -> str:
    """
    BUS-scoped ticket code, e.g. BUS1-0001 or BUS2-0023.
    """
    last = (
        TicketSale.query
        .filter_by(bus_id=bus_id)
        .order_by(TicketSale.id.desc())
        .first()
    )
    next_idx = (last.id if last else 0) + 1
    # Always prefix with BUS<bus_id>
    return f"BUS{bus_id}-{next_idx:04d}"



@pao_bp.route('/tickets/preview', methods=['POST'])
@require_role('pao')
def preview_ticket():
    data = request.get_json() or {}
    try:
        o_id = data.get('origin_stop_id')       or data.get('origin_stop_time_id')
        d_id = data.get('destination_stop_id')  or data.get('destination_stop_time_id')
        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)
        p = data.get('passenger_type')

        if not o or not d:
            return jsonify(error="origin or destination not found"), 400
   
        if p not in ('regular','discount'):
            return jsonify(error="invalid passenger_type"), 400

        hops = abs(o.seq - d.seq)
        base = 10 + max(hops - 1, 0) * 2
        fare = round(base * 0.8) if p == 'discount' else base

        return jsonify(fare=f"{fare:.2f}"), 200

    except Exception as e:
        traceback.print_exc()
        return jsonify(error=str(e)), 500



@pao_bp.route('/tickets', methods=['GET'])
@require_role('pao')
def list_tickets():
    """
    /pao/tickets?date=2025-03-26
    Returns only tickets issued today on this PAO’s assigned bus.
    """
    date_str = request.args.get('date')
    try:
        day = (
            datetime.strptime(date_str, "%Y-%m-%d").date()
            if date_str else
            datetime.utcnow().date()
        )
    except ValueError:
        return jsonify(error="invalid date"), 400

    # compute the day's bounds
    start_dt = datetime.combine(day, datetime.min.time())
    end_dt   = datetime.combine(day, datetime.max.time())

    # only tickets for this PAO’s bus, within today’s range
    bus_id = g.user.assigned_bus_id
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    qs = (
        TicketSale.query
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at.between(start_dt, end_dt)
        )
        .order_by(TicketSale.id.asc())
    )

    out = []
    for t in qs:
        out.append({
            "referenceNo": t.reference_no,
            "commuter":    f"{t.user.first_name} {t.user.last_name}",
            "date":        t.created_at.strftime("%B %d, %Y"),
            "time":        t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "fare":        f"{float(t.price):.2f}",
            "paid":        bool(t.paid)
        })
    return jsonify(out), 200

@pao_bp.route('/tickets/<int:ticket_id>', methods=['PATCH'])
@require_role('pao')
def mark_ticket_paid(ticket_id):
    """
    Body: { "paid": true }
    """
    data = request.get_json(silent=True) or {}
    current_app.logger.debug(f"[PAO:PATCH /tickets/{ticket_id}] payload={data!r}")
    paid = data.get('paid')
    if paid not in (True, False, 1, 0):
        current_app.logger.debug(f" → invalid paid flag: {paid!r}")
        return jsonify(error="invalid paid flag"), 400

    ticket = TicketSale.query.get(ticket_id)
    if not ticket:
        current_app.logger.debug(f" → ticket not found id={ticket_id}")
        return jsonify(error="ticket not found"), 404
    
    current_app.logger.debug(f" → before update: ticket.paid={ticket.paid}")
    ticket.paid = 1 if paid else 0
    try:
        db.session.commit()
        current_app.logger.debug(f" ← after commit: ticket.paid={ticket.paid}")
        return jsonify(id=ticket.id, paid=bool(ticket.paid)), 200
    except Exception as e:
        current_app.logger.exception("!! mark_ticket_paid commit failed")
        return jsonify(error=str(e)), 500