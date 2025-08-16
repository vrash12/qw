#backend/routes/commuter.py
from datetime import datetime, timedelta
from flask import Blueprint, request, jsonify, g, current_app, url_for
from sqlalchemy import func

from routes.auth import require_role
from db import db
from models.schedule import Trip, StopTime
from models.sensor_reading import SensorReading
from models.announcement import Announcement
from models.ticket_sale import TicketSale
from models.bus import Bus
from models.user import User
from utils.qr import build_qr_payload
from models.ticket_stop import TicketStop
from sqlalchemy.orm import joinedload
from flask import send_from_directory, redirect
from models.device_token import DeviceToken

commuter_bp = Blueprint("commuter", __name__, url_prefix="/commuter")



@commuter_bp.route("/device-token", methods=["POST"])
@require_role("commuter")
def save_device_token():
    data = request.get_json(silent=True) or {}
    tok = (data.get("token") or "").strip()
    if not tok:
        return jsonify(error="token required"), 400
    exists = DeviceToken.query.filter_by(user_id=g.user.id, token=tok).first()
    if not exists:
        db.session.add(DeviceToken(user_id=g.user.id, token=tok))
        db.session.commit()
        return jsonify(success=True, created=True), 201
    return jsonify(success=True, created=False), 200


@commuter_bp.route("/qr/ticket/<int:ticket_id>.jpg", methods=["GET"])
def qr_image_for_ticket(ticket_id: int):
    t = TicketSale.query.get_or_404(ticket_id)

    # figure out which static file matches this ticket (your existing logic)
    if t.passenger_type == "discount":
        base = round(float(t.price) / 0.8) if t.price else 0
        prefix = "discount"
    else:
        base = int(t.price)
        prefix = "regular"

    filename = f"{prefix}_{base}.jpg"

    # Option A: redirect to the static asset URL
    return redirect(url_for("static", filename=f"qr/{filename}", _external=True), code=302)


@commuter_bp.route("/tickets/<int:ticket_id>", methods=["GET"])
@require_role("commuter")
def commuter_get_ticket(ticket_id: int):
    # Only allow the logged-in commuter to view their ticket
    t = (
        TicketSale.query.options(joinedload(TicketSale.user))
        .filter(TicketSale.id == ticket_id, TicketSale.user_id == g.user.id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Resolve names (works whether IDs point to StopTime or TicketStop)
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

    # Optional QR (same logic you use elsewhere)
    if t.passenger_type == "discount":
        base = round(float(t.price) / 0.8) if t.price else 0
        prefix = "discount"
    else:
        base = int(t.price)
        prefix = "regular"
    filename = f"{prefix}_{base}.jpg"
    payload = build_qr_payload(
        t,
        origin_name=origin_name,
        destination_name=destination_name,
    )
    qr_link = url_for("commuter.qr_image_for_ticket", ticket_id=t.id, _external=True)

    return jsonify({
        "id": t.id,
        "referenceNo": t.reference_no,
        "date": t.created_at.strftime("%B %d, %Y"),
        "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "origin": origin_name,
        "destination": destination_name,
        "passengerType": t.passenger_type.title(),
        "commuter": f"{t.user.first_name} {t.user.last_name}",
        "fare": f"{float(t.price):.2f}",
        "paid": bool(t.paid),

        # 👇 important
        "qr": payload,          # JSON string (contains schema, ids, names, link)
        "qr_link": qr_link,     # plain URL — scanners will auto-open the image
        "qr_url": qr_url,       # same static image you already use inside the app
    }), 200

@commuter_bp.route("/dashboard", methods=["GET"])
@require_role("commuter")
def dashboard():
    """
    Compact, single-hop dashboard payload for the commuter app.
    Includes a tiny "mini_schedules" teaser per bus (max 2 trips each).
    """
    from datetime import date as _date
    from sqlalchemy import case

    def _choose_greeting() -> str:
        hr = datetime.utcnow().hour
        if hr < 12:
            return "Good morning"
        elif hr < 18:
            return "Good afternoon"
        return "Good evening"

    now = datetime.utcnow()
    today = now.date()

    # ── next trip today (first one today)
    next_trip_row = (
        db.session.query(Trip, Bus.identifier.label("bus_identifier"))
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(Trip.service_date == today)
        .order_by(Trip.start_time.asc())
        .first()
    )

    # ── recent tickets (last 7 days for this user)
    seven_days_ago = now - timedelta(days=7)
    recent_tix = (
        TicketSale.query.filter(
            TicketSale.user_id == g.user.id,
            TicketSale.created_at >= seven_days_ago,
        ).count()
    )

    # ── unread messages (simple total for now)
    unread_msgs = Announcement.query.count()

    # ── active buses (seen within last 5 minutes, ignore future-dated rows)
    cutoff = datetime.utcnow() - timedelta(minutes=5)
    now_utc = datetime.utcnow()
    last_per_bus = (
        db.session.query(
            SensorReading.bus_id,
            func.max(SensorReading.timestamp).label("last_ts"),
        )
        .group_by(SensorReading.bus_id)
        .subquery()
    )
    active_buses = (
        db.session.query(func.count(last_per_bus.c.bus_id))
        .filter(last_per_bus.c.last_ts >= cutoff)
        .filter(last_per_bus.c.last_ts <= now_utc)
        .scalar()
    )

    # ── all trips today (how many scheduled)
    today_trips = Trip.query.filter(Trip.service_date == today).count()

    # ── commuter’s tickets & revenue today
    today_tickets = (
        TicketSale.query.filter(
            TicketSale.user_id == g.user.id,
            func.date(TicketSale.created_at) == _date.today(),
        ).count()
    )
    today_revenue = (
        db.session.query(func.coalesce(func.sum(TicketSale.price), 0.0))
        .filter(
            TicketSale.user_id == g.user.id,
            func.date(TicketSale.created_at) == _date.today(),
            TicketSale.paid.is_(True),
        )
        .scalar()
        or 0.0
    )

    # ── last ticket for commuter
    lt = (
        TicketSale.query.filter_by(user_id=g.user.id)
        .order_by(TicketSale.created_at.desc())
        .first()
    )
    last_ticket = None
    if lt:
        last_ticket = {
            "referenceNo": lt.reference_no,
            "fare": f"{float(lt.price):.2f}",
            "paid": bool(lt.paid),
            "date": lt.created_at.strftime("%B %d, %Y"),
            "time": lt.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        }

    # ── latest announcement (any)
    last_ann_row = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)
        .order_by(Announcement.timestamp.desc())
        .first()
    )
    last_announcement = None
    if last_ann_row:
        ann, fn, ln, bid = last_ann_row
        last_announcement = {
            "message": ann.message,
            "timestamp": ann.timestamp.isoformat(),
            "author_name": f"{fn} {ln}",
            "bus_identifier": bid or "unassigned",
        }

    # ── upcoming trips (next 2 from now, across all buses)
    upcoming_rows = (
        db.session.query(Trip, Bus.identifier.label("bus_identifier"))
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(
            Trip.service_date == today,
            Trip.start_time >= now.time(),
        )
        .order_by(Trip.start_time.asc())
        .limit(2)
        .all()
    )
    upcoming = [
        {
            "bus": bid.replace("bus-", "Bus "),
            "start": t.start_time.strftime("%H:%M"),
            "end": t.end_time.strftime("%H:%M"),
        }
        for t, bid in upcoming_rows
    ]

    # ── pretty next trip
    if next_trip_row:
        trip, identifier = next_trip_row
        next_trip = {
            "bus": identifier.replace("bus-", "Bus "),
            "start": trip.start_time.strftime("%H:%M"),
            "end": trip.end_time.strftime("%H:%M"),
        }
    else:
        next_trip = None

    # ───────────────────────────────────────────────────────────────────
    # Mini schedules (per bus): at most 2 nearest trips per bus for today
    # Upcoming-first, then earliest; includes endpoints when available
    # ───────────────────────────────────────────────────────────────────
    limit_per_bus = 2
    now_time = now.time()

    # First/last stop names per trip (endpoints)
    first_stop_sq = (
        db.session.query(StopTime.trip_id, func.min(StopTime.seq).label("min_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    first_name_sq = (
        db.session.query(StopTime.trip_id, StopTime.stop_name.label("origin"))
        .join(
            first_stop_sq,
            (StopTime.trip_id == first_stop_sq.c.trip_id)
            & (StopTime.seq == first_stop_sq.c.min_seq),
        )
        .subquery()
    )
    last_stop_sq = (
        db.session.query(StopTime.trip_id, func.max(StopTime.seq).label("max_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    last_name_sq = (
        db.session.query(StopTime.trip_id, StopTime.stop_name.label("destination"))
        .join(
            last_stop_sq,
            (StopTime.trip_id == last_stop_sq.c.trip_id)
            & (StopTime.seq == last_stop_sq.c.max_seq),
        )
        .subquery()
    )

    mini_schedules = []

    try:
        # Prefer single SQL with ROW_NUMBER per bus (requires MySQL 8+/Postgres)
        past_flag = case((Trip.start_time < now_time, 1), else_=0)
        rn = func.row_number().over(
            partition_by=Trip.bus_id,
            order_by=(past_flag.asc(), Trip.start_time.asc()),
        ).label("rn")

        base = (
            db.session.query(
                Bus.identifier.label("bus_identifier"),
                Trip.start_time.label("start_time"),
                Trip.end_time.label("end_time"),
                first_name_sq.c.origin,
                last_name_sq.c.destination,
                rn,
            )
            .join(Trip, Trip.bus_id == Bus.id)
            .outerjoin(first_name_sq, Trip.id == first_name_sq.c.trip_id)
            .outerjoin(last_name_sq, Trip.id == last_name_sq.c.trip_id)
            .filter(Trip.service_date == today)
        ).subquery()

        rows = (
            db.session.query(
                base.c.bus_identifier,
                base.c.start_time,
                base.c.end_time,
                base.c.origin,
                base.c.destination,
            )
            .filter(base.c.rn <= limit_per_bus)
            .order_by(base.c.bus_identifier.asc(), base.c.start_time.asc())
            .all()
        )

        grouped: dict[str, list[dict]] = {}
        for r in rows:
            grouped.setdefault(r.bus_identifier, []).append(
                {
                    "start": r.start_time.strftime("%H:%M"),
                    "end": r.end_time.strftime("%H:%M"),
                    "origin": (r.origin or ""),
                    "destination": (r.destination or ""),
                }
            )

        for bid, items in grouped.items():
            mini_schedules.append(
                {"bus": bid.replace("bus-", "Bus "), "items": items[:limit_per_bus]}
            )

    except Exception:
        # Fallback (no window functions): do it in Python
        from collections import defaultdict

        trips = (
            db.session.query(Trip, Bus.identifier.label("bus_identifier"))
            .join(Bus, Trip.bus_id == Bus.id)
            .filter(Trip.service_date == today)
            .order_by(Trip.start_time.asc())
            .all()
        )

        # Cache endpoints for each trip to avoid N+1 queries
        endpoints: dict[int, tuple[str, str]] = {}
        for t, _bid in trips:
            fst = (
                db.session.query(StopTime)
                .filter_by(trip_id=t.id)
                .order_by(StopTime.seq.asc(), StopTime.id.asc())
                .first()
            )
            lst = (
                db.session.query(StopTime)
                .filter_by(trip_id=t.id)
                .order_by(StopTime.seq.desc(), StopTime.id.desc())
                .first()
            )
            endpoints[t.id] = (
                (fst.stop_name if fst else ""),
                (lst.stop_name if lst else ""),
            )

        per_bus: dict[str, list[Trip]] = defaultdict(list)
        for t, bid in trips:
            per_bus[bid].append(t)

        def sort_key(t: Trip):
            # Upcoming first (start_time >= now), then earliest start_time
            return (0 if t.start_time >= now_time else 1, t.start_time)

        for bid, ts in per_bus.items():
            ts_sorted = sorted(ts, key=sort_key)[:limit_per_bus]
            mini_schedules.append(
                {
                    "bus": bid.replace("bus-", "Bus "),
                    "items": [
                        {
                            "start": t.start_time.strftime("%H:%M"),
                            "end": t.end_time.strftime("%H:%M"),
                            "origin": endpoints[t.id][0],
                            "destination": endpoints[t.id][1],
                        }
                        for t in ts_sorted
                    ],
                }
            )

    return jsonify(
        {
            "greeting": _choose_greeting(),
            "user_name": f"{g.user.first_name} {g.user.last_name}",
            "next_trip": next_trip,
            "recent_tickets": recent_tix,
            "unread_messages": unread_msgs,
            "active_buses": int(active_buses or 0),
            "today_trips": int(today_trips or 0),
            "today_tickets": int(today_tickets or 0),
            "today_revenue": round(float(today_revenue), 2),
            "last_ticket": last_ticket,
            "last_announcement": last_announcement,
            "upcoming": upcoming,
            "mini_schedules": mini_schedules,
        }
    ), 200


def _choose_greeting() -> str:
    hr = datetime.utcnow().hour
    if hr < 12:
        return "Good morning"
    elif hr < 18:
        return "Good afternoon"
    return "Good evening"


@commuter_bp.route("/trips", methods=["GET"])
def list_all_trips():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify(error="A 'date' parameter is required."), 400
    try:
        svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="Invalid date format. Use YYYY-MM-DD."), 400

    first_stop_sq = (
        db.session.query(StopTime.trip_id, func.min(StopTime.seq).label("min_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    first_stop_name_sq = (
        db.session.query(
            StopTime.trip_id, StopTime.stop_name.label("origin")
        )
        .join(
            first_stop_sq,
            (StopTime.trip_id == first_stop_sq.c.trip_id)
            & (StopTime.seq == first_stop_sq.c.min_seq),
        )
        .subquery()
    )
    last_stop_sq = (
        db.session.query(StopTime.trip_id, func.max(StopTime.seq).label("max_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    last_stop_name_sq = (
        db.session.query(
            StopTime.trip_id, StopTime.stop_name.label("destination")
        )
        .join(
            last_stop_sq,
            (StopTime.trip_id == last_stop_sq.c.trip_id)
            & (StopTime.seq == last_stop_sq.c.max_seq),
        )
        .subquery()
    )

    trips = (
        db.session.query(
            Trip,
            Bus.identifier,
            first_stop_name_sq.c.origin,
            last_stop_name_sq.c.destination,
        )
        .join(Bus, Trip.bus_id == Bus.id)
        .outerjoin(first_stop_name_sq, Trip.id == first_stop_name_sq.c.trip_id)
        .outerjoin(last_stop_name_sq, Trip.id == last_stop_name_sq.c.trip_id)
        .filter(Trip.service_date == svc_date)
        .order_by(Trip.start_time.asc())
        .all()
    )

    result = [
        {
            "id": trip.id,
            "bus_identifier": identifier,
            "start_time": trip.start_time.strftime("%H:%M"),
            "end_time": trip.end_time.strftime("%H:%M"),
            "origin": origin or "N/A",
            "destination": destination or "N/A",
        }
        for trip, identifier, origin, destination in trips
    ]
    return jsonify(result), 200


@commuter_bp.route("/buses", methods=["GET"])
def list_buses():
    date_str = request.args.get("date")
    q = Bus.query
    if date_str:
        try:
            svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        q = q.join(Trip, Bus.id == Trip.bus_id).filter(Trip.service_date == svc_date)
    buses = q.order_by(Bus.identifier.asc()).all()
    return jsonify([{"id": b.id, "identifier": b.identifier} for b in buses]), 200


@commuter_bp.route("/bus-trips", methods=["GET"])
@require_role("commuter")
def commuter_bus_trips():
    bus_id = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")
    if not (bus_id and date_str):
        return jsonify(error="bus_id and date are required"), 400
    try:
        svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="date must be YYYY-MM-DD"), 400

    trips = (
        Trip.query.filter_by(bus_id=bus_id, service_date=svc_date)
        .order_by(Trip.start_time.asc())
        .all()
    )
    return (
        jsonify(
            [
                {
                    "id": t.id,
                    "number": t.number,
                    "start_time": t.start_time.strftime("%H:%M"),
                    "end_time": t.end_time.strftime("%H:%M"),
                }
                for t in trips
            ]
        ),
        200,
    )


@commuter_bp.route("/stop-times", methods=["GET"])
@require_role("commuter")
def commuter_stop_times():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = StopTime.query.filter_by(trip_id=trip_id).order_by(StopTime.seq.asc()).all()
    return (
        jsonify(
            [
                {
                    "stop_name": st.stop_name,
                    "arrive_time": st.arrive_time.strftime("%H:%M"),
                    "depart_time": st.depart_time.strftime("%H:%M"),
                }
                for st in sts
            ]
        ),
        200,
    )


@commuter_bp.route("/location", methods=["GET"])
@require_role("commuter")
def vehicle_location():
    sr = SensorReading.query.order_by(SensorReading.timestamp.desc()).first()
    if not sr:
        return jsonify(error="no sensor data"), 404
    return (
        jsonify(
            lat=sr.lat,
            lng=sr.lng,
            occupied=sr.occupied,
            timestamp=sr.timestamp.isoformat(),
        ),
        200,
    )

@commuter_bp.route("/tickets/mine", methods=["GET"])
@require_role("commuter")
def my_receipts():
    """
    GET /commuter/tickets/mine
      Query params:
        page=1
        page_size=5
        date=YYYY-MM-DD       # exact calendar day
        days=7|30             # fallback range if 'date' not provided
        bus_id=<int>          # filter tickets by bus (via Trip.bus_id)
        light=1               # keep for compatibility
    """
    from datetime import date as _date

    # ── params
    page      = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, request.args.get("page_size", type=int, default=5))
    date_str  = request.args.get("date")
    days      = request.args.get("days")
    bus_id    = request.args.get("bus_id", type=int)
    light     = (request.args.get("light") or "").lower() in {"1", "true", "yes"}

    # ── base query: eager-load to avoid N+1s
    qs = (
        db.session.query(TicketSale)
        .options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.user_id == g.user.id)
    )

    # ── date filter (exact day)
    if date_str:
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        # Use DB's DATE() to avoid timezone mismatch headaches
        qs = qs.filter(func.date(TicketSale.created_at) == day)
    elif days in {"7", "30"}:
        cutoff = datetime.utcnow() - timedelta(days=int(days))
        qs = qs.filter(TicketSale.created_at >= cutoff)

    # ── bus filter (via Trip.bus_id). If your TicketSale has bus_id, use that; else join Trip.
    if bus_id:
        if hasattr(TicketSale, "bus_id"):
            qs = qs.filter(TicketSale.bus_id == bus_id)
        else:
            qs = qs.join(Trip, TicketSale.trip_id == Trip.id).filter(Trip.bus_id == bus_id)

    total = qs.count()

    rows = (
        qs.order_by(TicketSale.created_at.desc())
          .offset((page - 1) * page_size)
          .limit(page_size)
          .all()
    )

    items = []
    for t in rows:
        # choose QR asset
        if t.passenger_type == "discount":
            base = round(float(t.price) / 0.8) if t.price else 0
            prefix = "discount"
        else:
            base = int(t.price)
            prefix = "regular"
        filename = f"{prefix}_{base}.jpg"
        qr_url = url_for("static", filename=f"qr/{filename}", _external=True)

        # resolve stop names
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

        payload = build_qr_payload(
            t,
            origin_name=origin_name,
            destination_name=destination_name,
        )
        qr_link = url_for("commuter.qr_image_for_ticket", ticket_id=t.id, _external=True)

        items.append({
            "id": t.id,
            "referenceNo": t.reference_no,
            "date": t.created_at.strftime("%B %d, %Y"),
            "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "origin": origin_name,
            "destination": destination_name,
            "passengerType": t.passenger_type.title(),
            "commuter": f"{t.user.first_name} {t.user.last_name}",
            "fare": f"{float(t.price):.2f}",
            "paid": bool(t.paid),
            "qr_url": qr_url,
            "qr": payload if not light else payload,  # keep field for app compatibility
            "qr_link": qr_link,
        })

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200


@commuter_bp.route("/trips/<int:trip_id>", methods=["GET"])
@require_role("commuter")
def get_trip(trip_id: int):
    trip = Trip.query.get_or_404(trip_id)

    first_stop = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .first()
    )
    last_stop = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.desc(), StopTime.id.desc())
        .first()
    )

    return (
        jsonify(
            id=trip.id,
            number=trip.number,
            origin=first_stop.stop_name if first_stop else "",
            destination=last_stop.stop_name if last_stop else "",
            start_time=trip.start_time.strftime("%H:%M"),
            end_time=trip.end_time.strftime("%H:%M"),
        ),
        200,
    )


@commuter_bp.route("/timetable", methods=["GET"])
@require_role("commuter")
def timetable():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )
    return (
        jsonify(
            [
                {
                    "stop": st.stop_name,
                    "arrive": st.arrive_time.strftime("%H:%M") if st.arrive_time else "",
                    "depart": st.depart_time.strftime("%H:%M") if st.depart_time else "",
                }
                for st in sts
            ]
        ),
        200,
    )


@commuter_bp.route("/schedule", methods=["GET"])
@require_role("commuter")
def schedule():
    trip_id = request.args.get("trip_id", type=int)
    date_str = request.args.get("date")
    if not trip_id or not date_str:
        return jsonify(error="trip_id and date are required"), 400
    try:
        datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="date must be YYYY-MM-DD"), 400

    stops = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )
    events = []
    for idx, st in enumerate(stops):
        events.append(
            {
                "id": idx * 2 + 1,
                "type": "stop",
                "label": "At Stop",
                "start_time": st.arrive_time.strftime("%H:%M") if st.arrive_time else "",
                "end_time": st.depart_time.strftime("%H:%M") if st.depart_time else "",
                "description": st.stop_name,
            }
        )
        if idx < len(stops) - 1:
            nxt = stops[idx + 1]
            events.append(
                {
                    "id": idx * 2 + 2,
                    "type": "trip",
                    "label": "In Transit",
                    "start_time": st.depart_time.strftime("%H:%M") if st.depart_time else "",
                    "end_time": nxt.arrive_time.strftime("%H:%M") if nxt.arrive_time else "",
                    "description": f"{st.stop_name} → {nxt.stop_name}",
                }
            )
    return jsonify(events=events), 200

@commuter_bp.route("/announcements", methods=["GET"])
def announcements():
    bus_id   = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")

    query = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)
    )
    if bus_id:
        query = query.filter(User.assigned_bus_id == bus_id)

    # ⬇️ default to *today* when client doesn't pass ?date=
    if not date_str:
        day = datetime.utcnow().date()
        query = query.filter(func.date(Announcement.timestamp) == day)
    else:
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        query = query.filter(func.date(Announcement.timestamp) == day)

    results = query.order_by(Announcement.timestamp.desc()).all()
    anns = [
        {
            "id": ann.id,
            "message": ann.message,
            "timestamp": ann.timestamp.isoformat(),
            "author_name": f"{first} {last}",
            "bus_identifier": bus_identifier or "unassigned",
        }
        for ann, first, last, bus_identifier in results
    ]
    return jsonify(anns), 200