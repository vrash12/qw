#backend/routes/manager.py
from flask import Blueprint, request, jsonify, send_from_directory, current_app
import os
import uuid
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from sqlalchemy import func
from sqlalchemy.orm import aliased

from db import db
from routes.auth import require_role
from models.bus import Bus
from models.schedule import Trip, StopTime
from models.qr_template import QRTemplate
from models.fare_segment import FareSegment
from models.sensor_reading import SensorReading
from models.ticket_sale import TicketSale
from models.user import User
from models.ticket_stop import TicketStop
from datetime import timezone
from models.trip_metric import TripMetric  

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

manager_bp = Blueprint("manager", __name__)


@manager_bp.route("/revenue-breakdown", methods=["GET"])
@require_role("manager")
def revenue_breakdown():
    """
    Return revenue & ticket percentages by passenger_type (regular vs discount)
    Window selection mirrors /manager/route-insights:
      - Preferred: ?trip_id=##
      - Or: ?date=YYYY-MM-DD&bus_id=##&from=HH:MM&to=HH:MM
    Optional: &paid_only=(true|false)  (default true)
    Optional: &bus_id when using trip_id to filter a specific bus (usually implicit).
    """
    from datetime import datetime, timedelta

    paid_only = (request.args.get("paid_only", "true").lower() != "false")

    # Resolve time window
    trip_id = request.args.get("trip_id", type=int)
    if trip_id:
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404
        bus_id = trip.bus_id
        day = trip.service_date
        window_from = datetime.combine(day, trip.start_time)
        window_to   = datetime.combine(day, trip.end_time)
        if trip.end_time <= trip.start_time:
            window_to = window_to + timedelta(days=1)
    else:
        date_str = request.args.get("date")
        bus_id   = request.args.get("bus_id", type=int)
        if not (date_str and bus_id):
            return jsonify(error="trip_id OR (date, bus_id, from, to) required"), 400
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="invalid date format"), 400
        try:
            hhmm_from = request.args["from"]
            hhmm_to   = request.args["to"]
        except KeyError:
            return jsonify(error="from and to are required"), 400
        window_from = datetime.combine(day, datetime.strptime(hhmm_from, "%H:%M").time())
        window_to   = datetime.combine(day, datetime.strptime(hhmm_to, "%H:%M").time())
        if window_to <= window_from:
            window_to = window_to + timedelta(days=1)

    # Aggregate by passenger_type within window
    qs = db.session.query(
        TicketSale.passenger_type,
        func.count(TicketSale.id).label("tickets"),
        func.coalesce(func.sum(TicketSale.price), 0.0).label("revenue"),
    ).filter(
        TicketSale.bus_id == bus_id,
        TicketSale.created_at >= window_from,
        TicketSale.created_at <= window_to,
    )
    if paid_only:
        qs = qs.filter(TicketSale.paid.is_(True))
    rows = qs.group_by(TicketSale.passenger_type).all()

    # Normalize + compute percentages
    totals_tickets = 0
    totals_revenue = 0.0
    by_type = []
    for r in rows:
        ttype = (r.passenger_type or "regular").lower()
        tickets = int(r.tickets or 0)
        revenue = float(r.revenue or 0.0)
        by_type.append({"type": ttype, "tickets": tickets, "revenue": revenue})
        totals_tickets += tickets
        totals_revenue += revenue

    # Ensure both buckets exist
    types = {g["type"] for g in by_type}
    if "regular" not in types:
        by_type.append({"type": "regular", "tickets": 0, "revenue": 0.0})
    if "discount" not in types:
        by_type.append({"type": "discount", "tickets": 0, "revenue": 0.0})

    # Percentages
    out = []
    for g in by_type:
        pct_t = (g["tickets"] / totals_tickets * 100.0) if totals_tickets else 0.0
        pct_r = (g["revenue"] / totals_revenue * 100.0) if totals_revenue else 0.0
        out.append({
            "type": g["type"],
            "tickets": g["tickets"],
            "revenue": round(g["revenue"], 2),
            "pct_tickets": round(pct_t, 1),
            "pct_revenue": round(pct_r, 1),
        })

    return jsonify({
        "from": window_from.date().isoformat(),
        "to": window_to.date().isoformat(),
        "paid_only": bool(paid_only),
        "totals": {"tickets": int(totals_tickets), "revenue": round(totals_revenue, 2)},
        "by_type": sorted(out, key=lambda x: 0 if x["type"] == "regular" else 1),
    }), 200



def _active_trip_for(bus_id: int, ts: datetime):
    """Find the trip whose time window contains ts (handles past-midnight windows)."""
    # Check day-of-ts and previous day (for trips that spill after midnight)
    day = ts.date()
    prev = (ts - timedelta(days=1)).date()
    candidates = (
        Trip.query.filter(Trip.bus_id == bus_id, Trip.service_date.in_([day, prev]))
        .order_by(Trip.start_time.asc())
        .all()
    )
    for t in candidates:
        start = datetime.combine(t.service_date, t.start_time)
        end   = datetime.combine(t.service_date, t.end_time)
        if t.end_time <= t.start_time:  # past midnight
            end = end + timedelta(days=1)
        if start <= ts < end:
            return t
    return None

@manager_bp.route("/trips/<int:trip_id>", methods=["PATCH"])
@require_role("manager")
def update_trip(trip_id: int):
    data = request.get_json() or {}
    try:
        number = data.get("number", "").strip()
        start_time = datetime.strptime(data["start_time"], "%H:%M").time()
        end_time = datetime.strptime(data["end_time"], "%H:%M").time()

        trip = Trip.query.get_or_404(trip_id)
        trip.number = number
        trip.start_time = start_time
        trip.end_time = end_time

        db.session.commit()

        return jsonify(
            id=trip.id,
            number=trip.number,
            start_time=trip.start_time.strftime("%H:%M"),
            end_time=trip.end_time.strftime("%H:%M"),
        ), 200
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload or missing required fields"), 400


@manager_bp.route("/trips/<int:trip_id>", methods=["DELETE"])
@require_role("manager")
def delete_trip(trip_id: int):
    try:
        trip = Trip.query.get_or_404(trip_id)
        StopTime.query.filter_by(trip_id=trip_id).delete()
        db.session.delete(trip)
        db.session.commit()
        return jsonify(message="Trip successfully deleted"), 200
    except Exception as e:
        db.session.rollback()
        return jsonify(error="Error deleting trip: " + str(e)), 500
# --- NEW (or replace prior attempt) /manager/tickets/composition ------
@manager_bp.route("/tickets/composition", methods=["GET"])
@require_role("manager")
def tickets_composition():
    """
    Returns counts of regular vs discount for the given day.
    - No payment fields are used.
    - Voided tickets are excluded.
    - Null passenger_type is treated as 'regular' for legacy rows.
    """
    try:
        day = datetime.strptime(
            request.args.get("date") or datetime.utcnow().date().isoformat(),
            "%Y-%m-%d",
        ).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    # COALESCE(NULL, 'regular') so old rows still count
    ptype = func.coalesce(TicketSale.passenger_type, 'regular')

    rows = (
        db.session.query(ptype.label("ptype"), func.count(TicketSale.id))
        .filter(func.date(TicketSale.created_at) == day)
        .filter(TicketSale.voided.is_(False))
        .group_by("ptype")
        .all()
    )

    regular = 0
    discount = 0
    for t, cnt in rows:
        t = (t or "").lower()
        if t == "regular":
            regular = int(cnt or 0)
        elif t == "discount":
            discount = int(cnt or 0)

    return jsonify(regular=regular, discount=discount, total=regular + discount), 200



@manager_bp.route("/tickets", methods=["GET"])
@require_role("manager")
def tickets_for_day():
    try:
        day = datetime.strptime(
            request.args.get("date") or datetime.utcnow().date().isoformat(),
            "%Y-%m-%d",
        ).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    O = aliased(TicketStop)
    D = aliased(TicketStop)

    rows = (
        db.session.query(
            TicketSale.id,
            TicketSale.price,
            TicketSale.passenger_type,                      # include type
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus"),
            O.stop_name.label("origin"),
            D.stop_name.label("destination"),
        )
        .join(User, TicketSale.user_id == User.id)
        .join(Bus,  TicketSale.bus_id  == Bus.id)
        .outerjoin(O, TicketSale.origin_stop_time_id      == O.id)
        .outerjoin(D, TicketSale.destination_stop_time_id == D.id)
        .filter(func.date(TicketSale.created_at) == day)   # ← robust day filter
        .filter(TicketSale.voided.is_(False))              # ignore voided
        .order_by(TicketSale.id.asc())
        .all()
    )

    tickets = [
        {
            "id": r.id,
            "bus": r.bus,
            "commuter": f"{r.first_name} {r.last_name}",
            "origin": r.origin or "",
            "destination": r.destination or "",
            "fare": f"{float(r.price):.2f}",
            # expose both keys so any client naming works
            "passenger_type": (r.passenger_type or "regular"),
            "passengerType":  (r.passenger_type or "regular"),
        }
        for r in rows
    ]

    total = sum(float(r.price) for r in rows)
    return jsonify(tickets=tickets, total=f"{total:.2f}"), 200


@manager_bp.route("/buses", methods=["GET"])
@require_role("manager")
def list_buses():
    try:
        out = []
        buses = Bus.query.order_by(Bus.identifier).all()
        for b in buses:
            latest = (
                SensorReading.query.filter_by(bus_id=b.id)
                .order_by(SensorReading.timestamp.desc())
                .first()
            )
            out.append(
                {
                    "id": b.id,
                    "identifier": b.identifier,
                    "capacity": b.capacity,
                    "description": b.description,
                    "last_seen": latest.timestamp.isoformat() if latest else None,
                    "occupancy": latest.total_count if latest else None,
                }
            )
        return jsonify(out), 200
    except Exception as e:
        current_app.logger.exception("ERROR in /manager/buses")
        return jsonify(error="Could not process the request to list buses."), 500


@manager_bp.route("/route-insights", methods=["GET"])
@require_role("manager")
def route_data_insights():
    trip_id = request.args.get("trip_id", type=int)
    use_snapshot = False

    if trip_id:
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404

        bus_id = trip.bus_id
        day = trip.service_date
        window_from = datetime.combine(day, trip.start_time)
        window_to = datetime.combine(day, trip.end_time)
        if trip.end_time <= trip.start_time:
            window_to = window_to + timedelta(days=1)

        window_end_excl = window_to + timedelta(minutes=1)

        # If trip is over (give a 2-min grace) and we have a snapshot, use it
        if datetime.utcnow() > window_to + timedelta(minutes=2):
            snap = TripMetric.query.filter_by(trip_id=trip_id).first()
            if snap:
                use_snapshot = True
                metrics = dict(
                    avg_pax=snap.avg_pax, peak_pax=snap.peak_pax,
                    boarded=snap.boarded, alighted=snap.alighted,
                    start_pax=snap.start_pax, end_pax=snap.end_pax,
                    net_change=snap.end_pax - snap.start_pax,
                )
            else:
                metrics = None
        else:
            metrics = None

        meta = {
            "trip_id": trip_id,
            "trip_number": trip.number,
            "window_from": window_from.isoformat(),
            "window_to": window_to.isoformat(),
        }
    else:
        # ad-hoc window
        date_str = request.args.get("date")
        bus_id = request.args.get("bus_id", type=int)
        if not date_str or not bus_id:
            return jsonify(error="date and bus_id are required when trip_id is omitted"), 400

        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="invalid date format"), 400

        try:
            start = request.args["from"]; end = request.args["to"]
        except KeyError:
            return jsonify(error="from and to are required when trip_id is omitted"), 400

        window_from = datetime.combine(day, datetime.strptime(start, "%H:%M").time())
        window_to   = datetime.combine(day, datetime.strptime(end, "%H:%M").time())
        if window_to <= window_from:
            window_to = window_to + timedelta(days=1)
        window_end_excl = window_to + timedelta(minutes=1)

        meta = {
            "trip_id": None, "trip_number": None,
            "window_from": window_from.isoformat(),
            "window_to": window_to.isoformat(),
        }
        metrics = None

    # Build the time series (1-minute buckets, max pax per minute + in/out sums)
    occ_rows = (
        db.session.query(
            func.date_format(SensorReading.timestamp, "%H:%i").label("hhmm"),
            func.max(SensorReading.total_count).label("pax"),
            func.sum(SensorReading.in_count).label("ins"),
            func.sum(SensorReading.out_count).label("outs"),
        )
        .filter(
            SensorReading.bus_id == bus_id,
            SensorReading.timestamp >= window_from,
            SensorReading.timestamp < window_end_excl,
        )
        .group_by("hhmm")
        .order_by("hhmm")
        .all()
    )
    series = [
        {
            "time": r.hhmm,
            "passengers": int(r.pax or 0),
            "in": int(r.ins or 0),
            "out": int(r.outs or 0),
        }
        for r in occ_rows
    ]

    # Compute metrics if no snapshot (or ad-hoc window)
    if not metrics:
        pax_values = [p["passengers"] for p in series]
        avg_pax  = round(sum(pax_values) / len(pax_values)) if pax_values else 0
        peak_pax = max(pax_values) if pax_values else 0
        boarded  = sum(p["in"] for p in series)
        alighted = sum(p["out"] for p in series)
        start_pax = pax_values[0] if pax_values else 0
        end_pax   = pax_values[-1] if pax_values else 0
        metrics = {
            "avg_pax":   avg_pax,
            "peak_pax":  peak_pax,
            "boarded":   boarded,
            "alighted":  alighted,
            "start_pax": start_pax,
            "end_pax":   end_pax,
            "net_change": end_pax - start_pax,
        }

    return jsonify(occupancy=series, meta=meta, metrics=metrics, snapshot=use_snapshot), 200


@manager_bp.route("/metrics/tickets", methods=["GET"])
@require_role("manager")
def ticket_metrics():
    today = datetime.utcnow().date()
    date_to = datetime.strptime(request.args.get("to", today.isoformat()), "%Y-%m-%d").date()
    date_from = datetime.strptime(
        request.args.get("from", (date_to - timedelta(days=6)).isoformat()),
        "%Y-%m-%d",
    ).date()

    window_start = datetime.combine(date_from, datetime.min.time())
    window_end = datetime.combine(date_to + timedelta(days=1), datetime.min.time())

    bus_id = request.args.get("bus_id", type=int)

    qs = db.session.query(
        func.date_format(TicketSale.created_at, "%Y-%m-%d").label("d"),
        func.count(TicketSale.id).label("tickets"),
        func.sum(TicketSale.price).label("revenue"),
    ).filter(TicketSale.created_at.between(window_start, window_end))
    if bus_id:
        qs = qs.filter(TicketSale.bus_id == bus_id)

    rows = qs.group_by("d").order_by("d").all()

    daily = []
    total_tickets = 0
    total_revenue = 0.0
    for r in rows:
        daily.append(
            {"date": r.d, "tickets": int(r.tickets), "revenue": float(r.revenue or 0)}
        )
        total_tickets += int(r.tickets)
        total_revenue += float(r.revenue or 0)

    return jsonify(daily=daily, total_tickets=total_tickets, total_revenue=round(total_revenue, 2)), 200


@manager_bp.route("/routes", methods=["GET"])
@require_role("manager")
def list_routes():
    current_app.logger.info("[list_routes] fetching all routes")
    routes = Route.query.order_by(Route.name.asc()).all()
    out = [{"id": r.id, "name": r.name} for r in routes]
    current_app.logger.info(f"[list_routes] returning {len(out)} routes")
    return jsonify(out), 200


@manager_bp.route("/buses/<int:bus_id>", methods=["PATCH"])
@require_role("manager")
def update_bus(bus_id):
    bus = Bus.query.get_or_404(bus_id)
    data = request.get_json() or {}

    if "identifier" in data:
        bus.identifier = data["identifier"].strip()
    if "capacity" in data:
        bus.capacity = data["capacity"]
    if "description" in data:
        bus.description = data["description"].strip()

    db.session.commit()
    return jsonify(success=True), 200


@manager_bp.route("/routes", methods=["POST"])
@require_role("manager")
def create_route():
    name = (request.get_json() or {}).get("name", "").strip()
    if not name:
        return jsonify(error="name is required"), 400

    r = Route(name=name)
    db.session.add(r)
    db.session.commit()
    return jsonify(id=r.id, name=r.name), 201


@manager_bp.route("/stop-times", methods=["GET"])
@require_role("manager")
def list_stop_times():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    # Use DISTINCT to avoid duplicates and proper ordering
    sts = (
        db.session.query(StopTime)
        .filter_by(trip_id=trip_id)
        .distinct(StopTime.stop_name, StopTime.arrive_time, StopTime.depart_time)
        .order_by(StopTime.seq.asc(), StopTime.arrive_time.asc(), StopTime.id.asc())
        .all()
    )

    return (
        jsonify(
            [
                {
                    "id": st.id,
                    "stop_name": st.stop_name,
                    "arrive_time": st.arrive_time.strftime("%H:%M"),
                    "depart_time": st.depart_time.strftime("%H:%M"),
                    "seq": st.seq,  # Include sequence for better sorting
                }
                for st in sts
            ]
        ),
        200,
    )

@manager_bp.route("/trips", methods=["POST"])
@require_role("manager")
def create_trip():
    data = request.get_json() or {}
    try:
        svc_date = datetime.strptime(data["service_date"], "%Y-%m-%d").date()
        number = data["number"].strip()
        start_t = datetime.strptime(data["start_time"], "%H:%M").time()
        end_t = datetime.strptime(data["end_time"], "%H:%M").time()
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload"), 400

    bus_id = data.get("bus_id")
    if bus_id is not None and not Bus.query.get(bus_id):
        return jsonify(error="invalid bus_id"), 400

    trip = Trip(
        service_date=svc_date,
        bus_id=bus_id,
        number=number,
        start_time=start_t,
        end_time=end_t,
    )
    db.session.add(trip)
    db.session.commit()
    return jsonify(id=trip.id), 201


@manager_bp.route("/bus-trips", methods=["GET"])
@require_role("manager")
def list_bus_trips():
    bus_id = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")

    current_app.logger.debug(f"Received bus_id: {bus_id}, date: {date_str}")

    if not (bus_id and date_str):
        return jsonify(error="bus_id and date are required"), 400

    try:
        query_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        current_app.logger.debug(f"Parsed query date: {query_date}")
    except ValueError:
        return jsonify(error="Invalid date format. Expected YYYY-MM-DD."), 400

    trips = (
        Trip.query.filter_by(bus_id=bus_id, service_date=query_date)
        .order_by(Trip.start_time.asc())
        .all()
    )

    current_app.logger.debug(f"Found {len(trips)} trips")

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


@manager_bp.route("/trips/<int:trip_id>", methods=["GET"])
@require_role("manager")
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

    origin = first_stop.stop_name if first_stop else ""
    destination = last_stop.stop_name if last_stop else ""

    return jsonify(
        id=trip.id,
        bus_id=trip.bus_id,
        service_date=trip.service_date.isoformat(),
        number=trip.number,
        origin=origin,
        destination=destination,
        start_time=trip.start_time.strftime("%H:%M"),
        end_time=trip.end_time.strftime("%H:%M"),
    ), 200


@manager_bp.route("/stop-times", methods=["POST"])
@require_role("manager")
def create_stop_time():
    data = request.get_json() or {}
    try:
        st = StopTime(
            trip_id=int(data["trip_id"]),
            stop_name=data["stop_name"].strip(),
            arrive_time=datetime.strptime(data["arrive_time"], "%H:%M").time(),
            depart_time=datetime.strptime(data["depart_time"], "%H:%M").time(),
            seq=int(data.get("seq", 0)),
        )
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload"), 400

    db.session.add(st)
    db.session.commit()
    return jsonify(id=st.id), 201


@manager_bp.route("/qr-templates", methods=["POST"])
@require_role("manager")
def upload_qr():
    if "file" not in request.files or "fare_segment_id" not in request.form:
        return jsonify(error="file & fare_segment_id required"), 400

    seg = FareSegment.query.get(request.form["fare_segment_id"])
    if not seg:
        return jsonify(error="invalid fare_segment_id"), 400

    file = request.files["file"]
    fname = secure_filename(f"{uuid.uuid4().hex}{os.path.splitext(file.filename)[1]}")
    file.save(os.path.join(UPLOAD_DIR, fname))

    tpl = QRTemplate(file_path=fname, price=seg.price, fare_segment_id=seg.id)
    db.session.add(tpl)
    db.session.commit()

    return jsonify(id=tpl.id, url=f"/manager/qr-templates/{tpl.id}/file", price=f"{seg.price:.2f}"), 201


@manager_bp.route("/qr-templates", methods=["GET"])
@require_role("manager")
def list_qr():
    return jsonify(
        [
            {
                "id": t.id,
                "url": f"/manager/qr-templates/{t.id}/file",
                "price": f"{t.price:.2f}",
            }
            for t in QRTemplate.query.order_by(QRTemplate.created_at.desc())
        ]
    ), 200


@manager_bp.route("/qr-templates/<int:tpl_id>/file", methods=["GET"])
def serve_qr_file(tpl_id):
    tpl = QRTemplate.query.get_or_404(tpl_id)
    return send_from_directory(UPLOAD_DIR, tpl.file_path)


@manager_bp.route("/fare-segments", methods=["GET"])
@require_role("manager")
def list_fare_segments():
    rows = FareSegment.query.order_by(FareSegment.id).all()
    return (
        jsonify(
            [
                {
                    "id": s.id,
                    "label": f"{s.origin.stop_name} → {s.destination.stop_name}",
                    "price": f"{s.price:.2f}",
                }
                for s in rows
            ]
        ),
        200,
    )

@manager_bp.route("/sensor-readings", methods=["POST"])
@require_role("manager")
def create_sensor_reading():
    data = request.get_json() or {}
    missing = [k for k in ("deviceId", "in", "out", "total") if k not in data]
    if missing:
        return jsonify(error=f"Missing field(s): {', '.join(missing)}"), 400

    device_id = str(data["deviceId"]).strip()
    bus = (
        Bus.query.filter(func.lower(Bus.identifier) == device_id.lower()).first()
        or (Bus.query.get(int(device_id)) if device_id.isdigit() else None)
    )
    if not bus:
        return jsonify(error="Invalid deviceId: Bus not found"), 404

    try:
        now = datetime.utcnow()  # make timestamp explicit
        reading = SensorReading(
            in_count=int(data["in"]),
            out_count=int(data["out"]),
            total_count=int(data["total"]),
            bus_id=bus.id,
            timestamp=now,
        )

        # Tag the reading to the currently active trip, if any
        active = _active_trip_for(bus.id, now)
        if active:
            reading.trip_id = active.id

        db.session.add(reading)
        db.session.commit()
        return jsonify(id=reading.id, timestamp=reading.timestamp.isoformat()), 201
    except (ValueError, TypeError):
        db.session.rollback()
        return jsonify(error="in/out/total must be integers"), 400
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("Unexpected error inserting sensor reading")
        return jsonify(error=str(e)), 500


@manager_bp.route("/buses/<string:device_id>/sensor-readings", methods=["GET"])
@require_role("manager")
def list_bus_readings(device_id: str):
    bus = Bus.query.filter_by(identifier=device_id).first_or_404()

    readings = (
        SensorReading.query.filter_by(bus_id=bus.id)
        .order_by(SensorReading.timestamp.desc())
        .all()
    )

    return jsonify(
        [
            {
                "id": r.id,
                "timestamp": r.timestamp.isoformat(),
                "in_count": r.in_count,
                "out_count": r.out_count,
                "total_count": r.total_count,
            }
            for r in readings
        ]
    ), 200
