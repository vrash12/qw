# backend/routes/pao.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional

from dateutil import parser as dtparse
from flask import Blueprint, request, jsonify, g, current_app, url_for
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from db import db
from models.announcement import Announcement
from models.bus import Bus
from models.schedule import Trip, StopTime
from models.ticket_sale import TicketSale
from models.ticket_stop import TicketStop
from models.user import User
from models.device_token import DeviceToken
from mqtt_ingest import publish

from routes.auth import require_role
from routes.tickets_static import jpg_name, QR_PATH
from utils.qr import build_qr_payload
from utils.push import send_push  # ← safe wrapper for push



pao_bp = Blueprint("pao", __name__, url_prefix="/pao")


@pao_bp.route("/summary", methods=["GET"])
@require_role("pao")
def pao_summary():
    date_str = request.args.get("date")
    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.utcnow().date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    start_dt = datetime.combine(day, datetime.min.time())
    end_dt   = datetime.combine(day, datetime.max.time())
    bus_id   = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    total = (
        db.session.query(func.count(TicketSale.id))
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at.between(start_dt, end_dt),
            TicketSale.voided.is_(False),
        )
        .scalar()
        or 0
    )

    paid_count = (
        db.session.query(func.count(TicketSale.id))
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at.between(start_dt, end_dt),
            TicketSale.paid.is_(True),
            TicketSale.voided.is_(False),
        )
        .scalar()
        or 0
    )

    revenue_total = (
        db.session.query(func.coalesce(func.sum(TicketSale.price), 0.0))
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at.between(start_dt, end_dt),
            TicketSale.paid.is_(True),
            TicketSale.voided.is_(False),
        )
        .scalar()
        or 0.0
    )

    last_row = (
        db.session.query(Announcement, User.first_name, User.last_name)
        .join(User, Announcement.created_by == User.id)
        .filter(User.assigned_bus_id == bus_id)
        .order_by(Announcement.timestamp.desc())
        .first()
    )

    last_announcement = None
    if last_row:
        ann, first, last = last_row
        last_announcement = {
            "message": ann.message,
            "timestamp": ann.timestamp.replace(tzinfo=timezone.utc).isoformat(),
            "author_name": f"{first} {last}",
        }

    return jsonify(
        tickets_total = int(total),
        paid_count    = int(paid_count),
        pending_count = int(max(0, total - paid_count)),
        revenue_total = float(round(revenue_total, 2)),
        last_announcement = last_announcement
    ), 200


@pao_bp.route("/recent-tickets", methods=["GET"])
@require_role("pao")
def recent_tickets():
    limit = request.args.get("limit", type=int) or 5
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    rows = (
        TicketSale.query.options(joinedload(TicketSale.user))
        .filter(TicketSale.bus_id == bus_id, TicketSale.voided.is_(False))
        .order_by(TicketSale.id.desc())
        .limit(limit)
        .all()
    )

    out = []
    for t in rows:
        out.append({
            "id": t.id,
            "referenceNo": t.reference_no,
            "commuter": f"{t.user.first_name} {t.user.last_name}",
            "fare": f"{float(t.price):.2f}",
            "paid": bool(t.paid),
            "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        })
    return jsonify(out), 200


def _current_bus_id() -> Optional[int]:
    return getattr(g.user, "assigned_bus_id", None)


def _void_ticket(ticket: TicketSale, reason: Optional[str]) -> None:
    ticket.voided = True
    ticket.paid = False
    ticket.void_reason = (reason or "").strip() or None
    db.session.commit()


@pao_bp.route("/pickup-request", methods=["POST"])
@require_role("commuter")
def pickup_request():
    data = request.get_json() or {}
    bus_id = data.get("bus_id")
    commuter_id = data.get("commuter_id")

    if not bus_id or not commuter_id:
        return jsonify(error="bus_id & commuter_id required"), 400

    current_app.logger.info(f"[PICKUP] bus={bus_id} commuter={commuter_id}")

    tokens = [
        t.token
        for t in DeviceToken.query
        .join(User, User.id == DeviceToken.user_id)
        .filter(User.role == "pao", User.assigned_bus_id == bus_id)
        .all()
    ]

    # SAFE push (no import-time coupling to Expo SDK)
    send_push(
        tokens,
        "🚍 New Pickup Request",
        f"Commuter #{commuter_id} is waiting.",
        {"commuterId": commuter_id},
    )

    return jsonify(success=True), 201


@pao_bp.route("/tickets/<int:ticket_id>/void", methods=["PATCH"])
@require_role("pao")
def void_ticket(ticket_id: int):
    t = TicketSale.query.get(ticket_id)
    if not t:
        return jsonify(error="ticket not found"), 404
    if t.voided:
        return jsonify(message="already voided"), 200

    data = request.get_json(silent=True) or {}
    reason = data.get("reason")
    _void_ticket(t, reason)
    current_app.logger.info(f"[PAO] voided ticket {t.reference_no} – {reason or 'no reason'}")
    return jsonify(id=t.id, voided=True), 200


@pao_bp.route("/bus-trips", methods=["GET"])
@require_role("pao")
def pao_bus_trips():
    bus_id = g.user.assigned_bus_id
    date_str = request.args.get("date")

    if not bus_id or not date_str:
        return jsonify(error="PAO is not assigned to a bus or date is missing"), 400

    svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    trips = (
        Trip.query.filter_by(bus_id=bus_id, service_date=svc_date)
        .order_by(Trip.start_time.asc())
        .all()
    )

    result = [
        {
            "id": t.id,
            "number": t.number,
            "start_time": t.start_time.strftime("%H:%M"),
            "end_time": t.end_time.strftime("%H:%M"),
        }
        for t in trips
    ]
    return jsonify(result), 200


@pao_bp.route("/stop-times", methods=["GET"])
@require_role("pao")
def pao_stop_times():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = StopTime.query.filter_by(trip_id=trip_id).order_by(StopTime.seq.asc()).all()

    return jsonify(
        [
            {
                "stop_name": st.stop_name,
                "arrive_time": st.arrive_time.strftime("%H:%M"),
                "depart_time": st.depart_time.strftime("%H:%M"),
            }
            for st in sts
        ]
    ), 200

@pao_bp.route("/tickets", methods=["POST"])
@require_role("pao")
def create_ticket():
    data = request.get_json(silent=True) or {}
    current_app.logger.debug(f"[PAO:/tickets] raw payload: {data}")

    try:
        o_id = data.get("origin_stop_id") or data.get("origin_stop_time_id")
        d_id = data.get("destination_stop_id") or data.get("destination_stop_time_id")
        p = data.get("passenger_type")
        uid = data.get("commuter_id")
        client_ts = data.get("created_at")

        try:
            ticket_dt = dtparse.parse(client_ts) if client_ts else datetime.now()
        except Exception:
            ticket_dt = datetime.now()

        # Use TicketStop to compute hops (has seq)
        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)

        if not o or not d:
            return jsonify(error="origin or destination not found"), 400
        if p not in ("regular", "discount"):
            return jsonify(error="invalid passenger_type"), 400

        user = User.query.get(uid)
        if not user:
            return jsonify(error="invalid commuter_id"), 400

        hops = abs(o.seq - d.seq)
        base = 10 + max(hops - 1, 0) * 2
        fare = round(base * 0.8) if p == "discount" else base

        bus_id = _current_bus_id()
        if not bus_id:
            return jsonify(error="PAO has no assigned bus"), 400

        ref = _gen_reference(bus_id)
        ticket = TicketSale(
            bus_id=bus_id,
            user_id=user.id,
            price=fare,
            passenger_type=p,
            reference_no=ref,
            paid=False,
            created_at=ticket_dt,
            origin_stop_time_id=o.id,
            destination_stop_time_id=d.id,
        )
        db.session.add(ticket)
        db.session.commit()

        payload = build_qr_payload(ticket, origin_name=o.stop_name, destination_name=d.stop_name)
        qr_link = url_for("commuter.qr_image_for_ticket", ticket_id=ticket.id, _external=True)

        # background image for the ticket (static JPG)
        img = jpg_name(fare, p)
        qr_bg_url = f"{request.url_root.rstrip('/')}/{QR_PATH}/{img}"

        # 🔔 push notification to the commuter's device(s)
        try:
            tokens = [t.token for t in DeviceToken.query.filter_by(user_id=user.id).all()]
            if tokens:
                send_push(
                    tokens,
                    "🎟️ Ticket Created",
                    f"Ref {ref} • ₱{fare:.2f} • {o.stop_name} → {d.stop_name}",
                    {"ticketId": ticket.id, "ref": ref}
                )
        except Exception:
            current_app.logger.exception("push to commuter failed")

        return jsonify({
            "id": ticket.id,
            "referenceNo": ref,
            "qr": payload,
            "qr_link": qr_link,
            "qr_bg_url": qr_bg_url,
            "origin": o.stop_name,
            "destination": d.stop_name,
            "passengerType": p,
            "commuter": f"{user.first_name} {user.last_name}",
            "fare": f"{fare:.2f}",
            "paid": False,
        }), 201

    except Exception as e:
        current_app.logger.exception("!! create_ticket unexpected error")
        return jsonify(error=str(e)), 500


@pao_bp.route("/tickets/preview", methods=["POST"])
@require_role("pao")
def preview_ticket():
    data = request.get_json() or {}
    try:
        o_id = data.get("origin_stop_id") or data.get("origin_stop_time_id")
        d_id = data.get("destination_stop_id") or data.get("destination_stop_time_id")
        p = data.get("passenger_type")

        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)
        if not o or not d:
            return jsonify(error="origin or destination not found"), 400
        if p not in ("regular", "discount"):
            return jsonify(error="invalid passenger_type"), 400

        hops = abs(o.seq - d.seq)
        base = 10 + max(hops - 1, 0) * 2
        fare = round(base * 0.8) if p == "discount" else base

        return jsonify(fare=f"{fare:.2f}"), 200

    except Exception as e:
        current_app.logger.exception("preview_ticket failed")
        return jsonify(error=str(e)), 500


@pao_bp.route("/tickets", methods=["GET"])
@require_role("pao")
def list_tickets():
    date_str = request.args.get("date")
    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else datetime.utcnow().date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    start_dt = datetime.combine(day, datetime.min.time())
    end_dt = datetime.combine(day, datetime.max.time())

    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    qs = (
        TicketSale.query.options(joinedload(TicketSale.user))
        .filter(TicketSale.bus_id == bus_id, TicketSale.created_at.between(start_dt, end_dt))
        .order_by(TicketSale.id.asc())
    )

    out = []
    for t in qs:
        # Resolve names even if relationship not eager-loaded
        if t.origin_stop_time:
            origin_name = t.origin_stop_time.stop_name
        else:
            ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
            origin_name = ts.stop_name if ts else ""

        if t.destination_stop_time:
            destination_name = t.destination_stop_time.stop_name
        else:
            tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
            destination_name = tsd.stop_name if tsd else ""

        out.append(
            {
                "id": t.id,
                "referenceNo": t.reference_no,
                "commuter": f"{t.user.first_name} {t.user.last_name}",
                "date": t.created_at.strftime("%B %d, %Y"),
                "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
                "origin": origin_name,
                "destination": destination_name,
                "fare": f"{float(t.price):.2f}",
                "paid": bool(t.paid),
                "voided": bool(t.voided),
                "void_reason": t.void_reason,
            }
        )
    return jsonify(out), 200


@pao_bp.route("/tickets/<int:ticket_id>", methods=["GET"])
@require_role("pao")
def get_ticket(ticket_id):
    ticket = (
        TicketSale.query.options(joinedload(TicketSale.user))
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not ticket:
        return jsonify(error="ticket not found"), 404

    origin_name = ticket.origin_stop_time.stop_name if ticket.origin_stop_time else (
        TicketStop.query.get(getattr(ticket, "origin_stop_time_id", None)).stop_name
        if getattr(ticket, "origin_stop_time_id", None) else ""
    )
    destination_name = ticket.destination_stop_time.stop_name if ticket.destination_stop_time else (
        TicketStop.query.get(getattr(ticket, "destination_stop_time_id", None)).stop_name
        if getattr(ticket, "destination_stop_time_id", None) else ""
    )

    return jsonify({
        "id": ticket.id,
        "referenceNo": ticket.reference_no,
        "date": ticket.created_at.strftime("%B %d, %Y"),
        "time": ticket.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "fare": f"{float(ticket.price):.2f}",
        "passengerType": ticket.passenger_type,
        "paid": bool(ticket.paid),
        "busId": ticket.bus_id,
        "ticketUuid": ticket.ticket_uuid,
        "origin": origin_name,
        "destination": destination_name,
    }), 200


@pao_bp.route("/tickets/<int:ticket_id>", methods=["PATCH"])
@require_role("pao")
def mark_ticket_paid(ticket_id: int):
    data = request.get_json(silent=True) or {}
    current_app.logger.debug(f"[PAO:PATCH /tickets/{ticket_id}] payload={data!r}")
    paid = data.get("paid")
    if paid not in (True, False, 1, 0):
        current_app.logger.debug(f" → invalid paid flag: {paid!r}")
        return jsonify(error="invalid paid flag"), 400

    ticket = (
        TicketSale.query.options(joinedload(TicketSale.bus))
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not ticket:
        current_app.logger.debug(f" → ticket not found id={ticket_id}")
        return jsonify(error="ticket not found"), 404

    current_app.logger.debug(f" → before update: ticket.paid={ticket.paid}")
    ticket.paid = 1 if paid else 0
    try:
        db.session.commit()

        from datetime import date as _date

        cnt = (
            TicketSale.query.filter_by(bus_id=ticket.bus_id, paid=True)
            .filter(func.date(TicketSale.created_at) == _date.today())
            .count()
        )

        topic = f"device/{ticket.bus.identifier}/fare"
        publish(topic, {"paid": cnt})
        current_app.logger.info(f"MQTT fare update → {topic}: {cnt}")

        return jsonify(id=ticket.id, paid=bool(ticket.paid)), 200

    except Exception as e:
        current_app.logger.exception("!! mark_ticket_paid commit failed")
        return jsonify(error=str(e)), 500


@pao_bp.route("/tickets/<int:ticket_id>", methods=["PUT"])
@require_role("pao")
def update_ticket(ticket_id):
    data = request.get_json(silent=True) or {}
    ticket = TicketSale.query.get(ticket_id)
    if not ticket:
        return jsonify(error="ticket not found"), 404

    if name := data.get("commuter_name"):
        user = (
            db.session.query(User)
            .filter(db.func.trim(db.func.concat(User.first_name, " ", User.last_name)) == name.strip())
            .first()
        )
        if not user:
            return jsonify(error="commuter not found"), 400
        ticket.user_id = user.id

    if iso := data.get("created_at"):
        try:
            ticket.created_at = dtparse.parse(iso)
        except Exception:
            return jsonify(error="invalid created_at"), 400

    if "fare" in data:
        try:
            ticket.price = float(data["fare"])
        except ValueError:
            return jsonify(error="invalid fare"), 400

    if pt := data.get("passenger_type"):
        if pt not in ("regular", "discount"):
            return jsonify(error="invalid passenger_type"), 400
        ticket.passenger_type = pt

    if "paid" in data:
        ticket.paid = bool(data["paid"])

    try:
        db.session.commit()
        return jsonify(success=True), 200
    except Exception as e:
        db.session.rollback()
        return jsonify(error=str(e)), 500


@pao_bp.route("/stops", methods=["GET"])
@require_role("pao")
def list_stops():
    rows = TicketStop.query.order_by(TicketStop.seq).all()
    return jsonify([{"id": r.id, "name": r.stop_name, "seq": r.seq} for r in rows]), 200


@pao_bp.route("/commuters", methods=["GET"])
@require_role("pao")
def list_commuters():
    users = User.query.filter_by(role="commuter").order_by(User.first_name, User.last_name).all()
    return jsonify([{"id": u.id, "name": f"{u.first_name} {u.last_name}"} for u in users]), 200


@pao_bp.route("/broadcast", methods=["POST"])
@require_role("pao")
def broadcast():
    """
    Creates an announcement and emits it in real-time to:
      1) All connected clients (broadcast)
      2) The specific bus room (bus:<bus_id>) for subscribers
    """
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    bus_row = Bus.query.get(bus_id)
    bus_identifier = (bus_row.identifier or f"bus-{bus_id:02d}") if bus_row else f"bus-{bus_id:02d}"

    data = request.get_json() or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify(error="message is required"), 400

    try:
        ann = Announcement(message=message, created_by=g.user.id)
        db.session.add(ann)
        db.session.commit()

        payload = {
            "id": ann.id,
            "message": ann.message,
            "timestamp": ann.timestamp.replace(tzinfo=timezone.utc).isoformat(),
            "bus_identifier": bus_identifier,
        }

    
        return jsonify(
            {
                "id": ann.id,
                "message": ann.message,
                "timestamp": ann.timestamp.replace(tzinfo=timezone.utc).isoformat(),
                "created_by": ann.created_by,
                "author_name": f"{g.user.first_name} {g.user.last_name}",
                "bus": bus_identifier,
            }
        ), 201

    except Exception:
        db.session.rollback()
        current_app.logger.exception("broadcast failed")
        return jsonify(error="internal server error"), 500


@pao_bp.route("/broadcast", methods=["GET"])
@require_role("pao")
def list_broadcasts():
    """
    Return announcements.
    By default: only messages authored by PAOs on *my* bus.
    When ?scope=all: include messages from PAOs on *all* buses.
    """
    scope  = (request.args.get("scope") or "bus").lower()
    bus_id = _current_bus_id()
    if not bus_id and scope != "all":
        return jsonify(error="PAO has no assigned bus"), 400

    # Base query: join author and the bus the author is assigned to
    q = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)
        .order_by(Announcement.timestamp.desc())
    )

    # Default = restrict to my bus; scope=all = show everything
    if scope != "all":
        q = q.filter(User.assigned_bus_id == bus_id)

    rows = q.all()

    anns = [
        {
            "id": ann.id,
            "message": ann.message,
            "timestamp": ann.timestamp.replace(tzinfo=timezone.utc).isoformat(),
            "created_by": ann.created_by,
            "author_name": f"{first} {last}",
            "bus": bus_identifier or "—",
        }
        for ann, first, last, bus_identifier in rows
    ]
    return jsonify(anns), 200


@pao_bp.route("/validate-fare", methods=["POST"])
@require_role("pao")
def validate_fare():
    data = request.get_json() or {}
    user_id = data.get("user_id")
    fare_amt = data.get("fare_amount")
    valid = True
    return jsonify({"user_id": user_id, "fare_amount": fare_amt, "valid": valid}), 200


def _gen_reference(bus_id: int) -> str:
    last = TicketSale.query.filter_by(bus_id=bus_id).order_by(TicketSale.id.desc()).first()
    next_idx = (last.id if last else 0) + 1
    return f"BUS{bus_id}-{next_idx:04d}"
