#backend/routes/pao.py
from flask import Blueprint, request, jsonify, g
from datetime import datetime
from routes.auth import require_role
from models.schedule import Trip, StopTime
from flask import current_app
import traceback
from models.announcement import Announcement
from models.schedule import StopTime
from datetime import datetime
from flask import current_app
import traceback
from db import db
from models.user import User 
from datetime import datetime, timezone
from models.ticket_sale import TicketSale
from flask import url_for, current_app
from dateutil import parser as dtparse
from sqlalchemy import func
from routes.tickets_static import jpg_name, QR_PATH
from models.ticket_stop import TicketStop  
from models.device_token import DeviceToken
from push import send_expo_push
from mqtt_ingest import publish 


pao_bp = Blueprint('pao', __name__, url_prefix='/pao')

from models.bus import Bus



@pao_bp.route('/pickup-request', methods=['POST'])
@require_role('commuter')
def pickup_request():
    """
    commuter calls this (instead of, or in addition to, MQTT) 
    {
      "bus_id": 2,
      "commuter_id": 17
    }
    """
    data = request.get_json() or {}
    bus_id      = data.get('bus_id')
    commuter_id = data.get('commuter_id')

    if not bus_id or not commuter_id:
        return jsonify(error="bus_id & commuter_id required"), 400

    # — you can log it, insert into a table, etc. —
    current_app.logger.info(f"[PICKUP] bus={bus_id} commuter={commuter_id}")

    # ── now PUSH to all PAO devices on that bus ─────────────────────
    tokens = [
      t.token
      for t in DeviceToken.query
         .join(User, User.id == DeviceToken.user_id)
         .filter(User.role == 'pao',
                 User.assigned_bus_id == bus_id)
         .all()
    ]
    send_expo_push(
      tokens,
      "🚍 New Pickup Request",
      f"Commuter #{commuter_id} is waiting.",
      {"commuterId": commuter_id}
    )

    return jsonify(success=True), 201
def _void_ticket(ticket: TicketSale, reason: str | None):
    ticket.voided      = True
    ticket.paid        = False           # cannot stay paid once voided
    ticket.void_reason = (reason or "").strip() or None
    db.session.commit()

@pao_bp.route('/tickets/<int:ticket_id>/void', methods=['PATCH'])
@require_role('pao')
def void_ticket(ticket_id: int):
    """
    Body { "reason": "wrong route" }   # reason is optional
    """
    t = TicketSale.query.get(ticket_id)
    if not t:
        return jsonify(error="ticket not found"), 404
    if t.voided:
        return jsonify(message="already voided"), 200

    data   = request.get_json(silent=True) or {}
    reason = data.get("reason")
    _void_ticket(t, reason)
    current_app.logger.info(f"[PAO] voided ticket {t.reference_no} – {reason or 'no reason'}")
    return jsonify(id=t.id, voided=True), 200

@pao_bp.route('/bus-trips', methods=['GET'])
@require_role('pao')
def pao_bus_trips():
    bus_id = g.user.assigned_bus_id
    date_str = request.args.get('date')
    
    if not bus_id or not date_str:
        return jsonify(error="PAO is not assigned to a bus or date is missing"), 400

    svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    trips = Trip.query.filter_by(bus_id=bus_id, service_date=svc_date).order_by(Trip.start_time.asc()).all()
    
    result = [{
        "id": t.id, "number": t.number,
        "start_time": t.start_time.strftime("%H:%M"), "end_time": t.end_time.strftime("%H:%M"),
    } for t in trips]
    return jsonify(result), 200

@pao_bp.route('/stop-times', methods=['GET'])
@require_role('pao') # ✅ Re-enable the decorator
def pao_stop_times():
    trip_id = request.args.get('trip_id', type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = StopTime.query.filter_by(trip_id=trip_id).order_by(StopTime.seq.asc()).all()
    
    return jsonify([{
        "stop_name": st.stop_name,
        "arrive_time": st.arrive_time.strftime("%H:%M"),
        "depart_time": st.depart_time.strftime("%H:%M")
    } for st in sts]), 200


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
            origin_stop_time_id      = o.id,
            destination_stop_time_id = d.id,
        )
        db.session.add(ticket)
        db.session.commit()

        # ─── build QR URL & final response ────────────────────
        img = jpg_name(fare, p)
        qr_url = url_for("static", filename=f"{QR_PATH}/{img}", _external=True)
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
@require_role('pao')
def list_stops():
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



# ── new helper, reuse everywhere we need the current bus
def _current_bus_id() -> int | None:
    return getattr(g.user, "assigned_bus_id", None)

# --------------------------------------------------------------------
@pao_bp.route("/broadcast", methods=["POST"])
@require_role("pao")
def broadcast():
    """
    Create an announcement visible only to PAOs (and commuters) on *this* bus.
    """
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400      # <- guard early
    bus_row        = Bus.query.get(bus_id)
    bus_identifier = (bus_row.identifier or f"bus-{bus_id:02d}") if bus_row else f
    data     = request.get_json() or {}
    message  = (data.get("message") or "").strip()
    if not message:
        return jsonify(error="message is required"), 400

    try:
        ann = Announcement(
            message    = message,
            created_by = g.user.id,          # FK to users.id
        )
        db.session.add(ann)
        db.session.commit()

        return jsonify({
            "id":          ann.id,
            "message":     ann.message,
            "timestamp":   ann.timestamp.replace(tzinfo=timezone.utc).isoformat(),
            "created_by":  ann.created_by,
            "author_name": f"{g.user.first_name} {g.user.last_name}",
            "bus":         bus_identifier,
        }), 201

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("broadcast failed")
        return jsonify(error="internal server error"), 500


# --------------------------------------------------------------------
@pao_bp.route("/broadcast", methods=["GET"])
@require_role("pao")
def list_broadcasts():
    """
    Return only the announcements whose author is assigned to *my* bus.
    """
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    rows = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier")
        )
        .join(User, Announcement.created_by == User.id)
        .join(Bus, User.assigned_bus_id == Bus.id)
        .filter(User.assigned_bus_id == _current_bus_id())
        .order_by(Announcement.timestamp.desc())
        .all()
    )

    anns = [{
        "id":          ann.id,
        "message":     ann.message,
        "timestamp":   ann.timestamp.replace(tzinfo=timezone.utc).isoformat(),
        "created_by":  ann.created_by,
        "author_name": f"{first} {last}",
        "bus":         bus_identifier,
    } for ann, first, last, bus_identifier in rows]

    return jsonify(anns), 200


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
        o = StopTime.query.get(o_id)
        d = StopTime.query.get(d_id)
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
            "id":           t.id,
            "referenceNo": t.reference_no,
            "commuter":    f"{t.user.first_name} {t.user.last_name}",
            "date":        t.created_at.strftime("%B %d, %Y"),
            "time":        t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "fare":        f"{float(t.price):.2f}",
            "paid":        bool(t.paid),
            "voided":      bool(t.voided),
            "void_reason": t.void_reason
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

        # Recompute today's total paid tickets for this bus
        from sqlalchemy import func
        from datetime import date

        cnt = (
            TicketSale.query
            .filter_by(bus_id=ticket.bus_id, paid=True)
            .filter(func.date(TicketSale.created_at) == date.today())
            .count()
        )

        # Publish the updated count
        topic = f"device/{ticket.bus.identifier}/fare"
        publish(topic, {"paid": cnt})
        current_app.logger.info(f"MQTT fare update → {topic}: {cnt}")

        return jsonify(id=ticket.id, paid=bool(ticket.paid)), 200

    except Exception as e:
        current_app.logger.exception("!! mark_ticket_paid commit failed")
        return jsonify(error=str(e)), 500
# ─── Read a single ticket (receipt) ───────────────────────
@pao_bp.route('/tickets/<int:ticket_id>', methods=['GET'])
@require_role('pao')
def get_ticket(ticket_id):
    """
    Return full details for one ticket (used as a receipt).
    """
    ticket = TicketSale.query.get(ticket_id)
    if not ticket:
        return jsonify(error="ticket not found"), 404

    # build the same shape as list_tickets but full detail
    return jsonify({
        "id":           ticket.id,
        "referenceNo":  ticket.reference_no,
        "commuter":     f"{ticket.user.first_name} {ticket.user.last_name}",
        "date":         ticket.created_at.strftime("%B %d, %Y"),
        "time":         ticket.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "fare":         f"{float(ticket.price):.2f}",
        "passengerType": ticket.passenger_type,
        "paid":         bool(ticket.paid),
        "busId":        ticket.bus_id,
        "ticketUuid":   ticket.ticket_uuid
    }), 200

@pao_bp.route('/tickets/<int:ticket_id>', methods=['PUT'])
@require_role('pao')
def update_ticket(ticket_id):
    data   = request.get_json(silent=True) or {}
    ticket = TicketSale.query.get(ticket_id)
    if not ticket:
        return jsonify(error="ticket not found"), 404

    # passenger / commuter
    if name := data.get("commuter_name"):
        user = (
            db.session.query(User)
            .filter(db.func.trim(db.func.concat(User.first_name, " ", User.last_name)) == name.strip())
            .first()
        )
        if not user:
            return jsonify(error="commuter not found"), 400
        ticket.user_id = user.id

    # datetime
    if iso := data.get("created_at"):
        try:
            ticket.created_at = dtparse.parse(iso)
        except Exception:
            return jsonify(error="invalid created_at"), 400

    # fare
    if "fare" in data:
        try:
            ticket.price = float(data["fare"])
        except ValueError:
            return jsonify(error="invalid fare"), 400

    # passenger type
    if pt := data.get("passenger_type"):
        if pt not in ("regular", "discount"):
            return jsonify(error="invalid passenger_type"), 400
        ticket.passenger_type = pt

    # paid flag
    if "paid" in data:
        ticket.paid = bool(data["paid"])

    try:
        db.session.commit()
        return jsonify(success=True), 200
    except Exception as e:
        db.session.rollback()
        return jsonify(error=str(e)), 500

# routes/pao.py  (add at the end)
@pao_bp.route("/device-token", methods=["POST"])
@require_role("pao")
def save_device_token():
    data  = request.get_json() or {}
    token = data.get("token")
    if not token:
        return jsonify(error="token required"), 400

    row = (DeviceToken.query.filter_by(token=token).first()
           or DeviceToken(user_id=g.user.id, token=token))
    row.platform = data.get("platform")          # update platform each time
    db.session.add(row)
    db.session.commit()
    return jsonify(success=True), 200

