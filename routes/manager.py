# routes/manager.py
from flask import Blueprint, request, jsonify, send_from_directory, current_app
import os
import uuid
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from db import db
from routes.auth import require_role
from models.bus import Bus
from models.schedule import Trip, StopTime
from models.qr_template import QRTemplate
from models.fare_segment import FareSegment
from models.sensor_reading import SensorReading
from models.ticket_sale import TicketSale
from sqlalchemy import func
from sqlalchemy.orm import aliased
from models.user import User 
from models.schedule import StopTime  
from models.ticket_stop import TicketStop 

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

manager_bp = Blueprint("manager", __name__)
# Update Schedule and Trip
@manager_bp.route("/trips/<int:trip_id>", methods=["PATCH"])
@require_role("manager")
def update_trip(trip_id: int):
    data = request.get_json() or {}
    try:
        # Validate and extract new data from request
        number = data.get("number", "").strip()
        start_time = datetime.strptime(data["start_time"], "%H:%M").time()
        end_time = datetime.strptime(data["end_time"], "%H:%M").time()

        trip = Trip.query.get_or_404(trip_id)

        # Update trip details
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

# Delete Trip and Schedule
@manager_bp.route("/trips/<int:trip_id>", methods=["DELETE"])
@require_role("manager")
def delete_trip(trip_id: int):
    try:
        trip = Trip.query.get_or_404(trip_id)

        # Delete related stop times
        StopTime.query.filter_by(trip_id=trip_id).delete()

        # Delete trip
        db.session.delete(trip)
        db.session.commit()

        return jsonify(message="Trip successfully deleted"), 200
    except Exception as e:
        db.session.rollback()
        return jsonify(error="Error deleting trip: " + str(e)), 500

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

    Start = datetime.combine(day, datetime.min.time())
    End   = datetime.combine(day, datetime.max.time())

    O = aliased(TicketStop)                        # ➋  CHANGE
    D = aliased(TicketStop)

    rows = (
        db.session.query(
            TicketSale.id,
            TicketSale.price,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus"),
            O.stop_name.label("origin"),
            D.stop_name.label("destination"),
        )
        .join(User, TicketSale.user_id == User.id)
        .join(Bus,  TicketSale.bus_id == Bus.id)
        .outerjoin(O, TicketSale.origin_stop_time_id      == O.id)
        .outerjoin(D, TicketSale.destination_stop_time_id == D.id)
        .filter(TicketSale.created_at.between(Start, End))
        .order_by(TicketSale.id.asc())
        .all()
    )

    tickets = [{
        "id": r.id,
        "bus":         r.bus,
        "commuter": f"{r.first_name} {r.last_name}",
        "origin": r.origin or "",
        "destination": r.destination or "",
        "fare": f"{float(r.price):.2f}",
    } for r in rows]

    return jsonify(
        tickets=tickets,
        total=f"{sum(float(r.price) for r in rows):.2f}"
    ), 200

# ──────────────────────────────  BUS CRUD  ──────────────────────────────
@manager_bp.route("/buses", methods=["GET"])
@require_role("manager")
def list_buses():
    """List all buses."""
    try:
        out = []
        buses = Bus.query.order_by(Bus.identifier).all()
        for b in buses:
            latest = (
                SensorReading.query
                .filter_by(bus_id=b.id)
                .order_by(SensorReading.timestamp.desc())
                .first()
            )
            out.append({
                "id":          b.id,
                "identifier":  b.identifier,
                "capacity":    b.capacity,
                "description": b.description,
                "last_seen":   latest.timestamp.isoformat() if latest else None,
                "occupancy":   latest.total_count if latest else None,
            })
        return jsonify(out), 200
    except Exception as e:
        # This safely logs the error to your console and returns a JSON error
        print(f"ERROR in /manager/buses: {e}")
        return jsonify(error="Could not process the request to list buses."), 500



@manager_bp.route("/route-insights", methods=["GET"])
@require_role("manager")
def route_data_insights():
    """
    Query params
      date=YYYY-MM-DD   # optional if trip_id provided
      bus_id=1          # optional if trip_id provided
      trip_id=3         # optional
      from=HH:MM        # required if trip_id omitted
      to=HH:MM          # required if trip_id omitted
    """
    # ── parse & validate ────────────────────────────────────────────────
    trip_id = request.args.get("trip_id", type=int)
    if trip_id:
        # derive everything from the trip record
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404

        bus_id = trip.bus_id
        day    = trip.service_date
        window_from = datetime.combine(day, trip.start_time)
        window_to   = datetime.combine(day, trip.end_time)
        meta = {
            "trip_id":     trip_id,
            "trip_number": trip.number,
            "window_from": window_from.isoformat(),
            "window_to":   window_to.isoformat(),
        }

    else:
        # need explicit date, bus_id, from/to
        date_str = request.args.get("date")
        bus_id   = request.args.get("bus_id", type=int)
        if not date_str or not bus_id:
            return jsonify(error="date and bus_id are required when trip_id is omitted"), 400

        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="invalid date format"), 400

        try:
            start = request.args["from"]
            end   = request.args["to"]
        except KeyError:
            return jsonify(error="from and to are required when trip_id is omitted"), 400

        window_from = datetime.combine(day, datetime.strptime(start, "%H:%M").time())
        window_to   = datetime.combine(day, datetime.strptime(end,   "%H:%M").time())
        meta = {
            "trip_id":     None,
            "trip_number": None,
            "window_from": window_from.isoformat(),
            "window_to":   window_to.isoformat(),
        }

    # ── SENSOR READINGS (one row per minute) ─────────────────────────────
    occ_rows = (
        db.session.query(
            func.date_format(SensorReading.timestamp, '%H:%i').label("hhmm"),
            func.max(SensorReading.total_count).label("pax")
        )
        .filter(
            SensorReading.bus_id == bus_id,
            SensorReading.timestamp.between(window_from, window_to)
        )
        .group_by("hhmm")
        .order_by("hhmm")
        .all()
    )
    occupancy = [{"time": r.hhmm, "passengers": int(r.pax)} for r in occ_rows]

    # ── TICKET SALES (grouped per minute) ───────────────────────────────
    tix_rows = (
        db.session.query(
            func.date_format(TicketSale.created_at, '%H:%i').label("hhmm"),
            func.count(TicketSale.id).label("tickets"),
            func.sum(TicketSale.price).label("revenue")
        )
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at.between(window_from, window_to)
        )
        .group_by("hhmm")
        .order_by("hhmm")
        .all()
    )
    tickets = [
        {"time":    r.hhmm,
         "tickets": int(r.tickets),
         "revenue": float(r.revenue or 0)}
        for r in tix_rows
    ]

    return jsonify(occupancy=occupancy, tickets=tickets, meta=meta), 200

@manager_bp.route("/metrics/tickets", methods=["GET"])
@require_role("manager")
def ticket_metrics():
    """
    GET /manager/metrics/tickets
        ?from=2025-07-25&to=2025-07-31   # optional –- defaults to last 7 days
        &bus_id=2                        # optional –- limit to one bus
    Response:
      {
        "daily": [
          {"date":"2025-07-25","tickets":42,"revenue":630.00},
          …
        ],
        "total_tickets": 234,
        "total_revenue": 3510.00
      }
    """
    # ── time window ──────────────────────────────────────────────
    today     = datetime.utcnow().date()
    date_to   = datetime.strptime(request.args.get("to",   today.isoformat()), "%Y-%m-%d").date()
    date_from = datetime.strptime(
        request.args.get("from", (date_to - timedelta(days=6)).isoformat()),
        "%Y-%m-%d"
    ).date()

    # add 1 day so the BETWEEN is inclusive of the end-date’s 23:59
    window_start = datetime.combine(date_from, datetime.min.time())
    window_end   = datetime.combine(date_to + timedelta(days=1), datetime.min.time())

    bus_id = request.args.get("bus_id", type=int)

    # ── aggregate ────────────────────────────────────────────────
    qs = (
        db.session.query(
            func.date_format(TicketSale.created_at, '%Y-%m-%d').label("d"),
            func.count(TicketSale.id).label("tickets"),
            func.sum(TicketSale.price).label("revenue")
        )
        .filter(TicketSale.created_at.between(window_start, window_end))
    )
    if bus_id:
        qs = qs.filter(TicketSale.bus_id == bus_id)

    rows = qs.group_by("d").order_by("d").all()

    daily          = []
    total_tickets  = 0
    total_revenue  = 0.0
    for r in rows:
        daily.append({
            "date":    r.d,
            "tickets": int(r.tickets),
            "revenue": float(r.revenue or 0)
        })
        total_tickets += int(r.tickets)
        total_revenue += float(r.revenue or 0)

    return jsonify(
        daily          = daily,
        total_tickets  = total_tickets,
        total_revenue  = round(total_revenue, 2)
    ), 200

@manager_bp.route("/routes", methods=["GET"])
@require_role("manager")
def list_routes():
    """
    GET /manager/routes
    Returns all routes.
    """
    current_app.logger.info("[list_routes] fetching all routes")
    routes = Route.query.order_by(Route.name.asc()).all()
    out = [{"id": r.id, "name": r.name} for r in routes]
    current_app.logger.info(f"[list_routes] returning {len(out)} routes")
    return jsonify(out), 200

@manager_bp.route("/buses/<int:bus_id>", methods=["PATCH"])
@require_role("manager")
def update_bus(bus_id):
    """
    Body may include any of: identifier, capacity, description
    """
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


# ──────────────────────────────  ROUTES & TRIPS  ────────────────────────
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
    """
    Query params: ?trip_id=<int>
    Returns the list of stops for that trip, ordered by seq.
    """
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = (
      StopTime.query
        .filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )

    return jsonify([
      {
        "id":          st.id,
        "stop_name":   st.stop_name,
        "arrive_time": st.arrive_time.strftime("%H:%M"),
        "depart_time": st.depart_time.strftime("%H:%M")
      }
      for st in sts
    ]), 200

@manager_bp.route("/trips", methods=["POST"])
@require_role("manager")
def create_trip():
    """
      Body:
      {
        "bus_id":   1,                  # optional
        "service_date": "2025-07-29",   # <── NEW, required
        "number":  "Morning-1",
        "start_time": "07:00",
        "end_time":   "08:25"
      }
    """
    data = request.get_json() or {}
    try:
        svc_date = datetime.strptime(data["service_date"], "%Y-%m-%d").date()
        number   = data["number"].strip()
        start_t  = datetime.strptime(data["start_time"], "%H:%M").time()
        end_t    = datetime.strptime(data["end_time"],   "%H:%M").time()
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload"), 400

    bus_id = data.get("bus_id")
    if bus_id is not None and not Bus.query.get(bus_id):
        return jsonify(error="invalid bus_id"), 400

    trip = Trip(
        service_date = svc_date,
        bus_id       = bus_id,
        number       = number,
        start_time   = start_t,
        end_time     = end_t,
    )
    db.session.add(trip)
    db.session.commit()
    return jsonify(id=trip.id), 201
@manager_bp.route("/bus-trips", methods=["GET"])
@require_role("manager")
def list_bus_trips():
    """
    GET /manager/bus-trips?bus_id=1&date=2025-07-29
    """
    bus_id = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")
    
    # Log the incoming request parameters for debugging
    print(f"Received bus_id: {bus_id}, date: {date_str}")

    if not (bus_id and date_str):
        return jsonify(error="bus_id and date are required"), 400

    try:
        query_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        print(f"Parsed query date: {query_date}")
    except ValueError:
        return jsonify(error="Invalid date format. Expected YYYY-MM-DD."), 400

    trips = (
        Trip.query
        .filter_by(bus_id=bus_id, service_date=query_date)
        .order_by(Trip.start_time.asc())
        .all()
    )

    # Log the number of trips found for debugging
    print(f"Found {len(trips)} trips")

    return jsonify([{
        "id": t.id,
        "number": t.number,
        "start_time": t.start_time.strftime("%H:%M"),
        "end_time": t.end_time.strftime("%H:%M"),
    } for t in trips]), 200



# ────────────────────  TRIPS  (single)  ─────────────────────
@manager_bp.route("/trips/<int:trip_id>", methods=["GET"])
@require_role("manager")
def get_trip(trip_id: int):
    """
    Returns one trip plus a handy origin / destination string so the
    front-end doesn’t need to recalculate it.
    """
    trip = Trip.query.get_or_404(trip_id)

    # try to derive origin/destination from first / last StopTime rows
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

    origin      = first_stop.stop_name if first_stop else ""
    destination = last_stop.stop_name  if last_stop  else ""

    return jsonify(
        id          = trip.id,
        bus_id      = trip.bus_id,                      # ✅ ADD THIS
        service_date= trip.service_date.isoformat(),    # ✅ ADD THIS
        number      = trip.number,
        origin      = origin,
        destination = destination,
        start_time  = trip.start_time.strftime("%H:%M"),
        end_time    = trip.end_time.strftime("%H:%M"),
    ), 200


@manager_bp.route("/stop-times", methods=["POST"])
@require_role("manager")
def create_stop_time():
    data = request.get_json() or {}
    try:
        st = StopTime(
            trip_id     = int(data["trip_id"]),
            stop_name   = data["stop_name"].strip(),
            arrive_time = datetime.strptime(data["arrive_time"], "%H:%M").time(),
            depart_time = datetime.strptime(data["depart_time"], "%H:%M").time(),
            seq         = int(data.get("seq", 0)),
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

    # save upload
    file = request.files["file"]
    fname = secure_filename(f"{uuid.uuid4().hex}{os.path.splitext(file.filename)[1]}")
    file.save(os.path.join(UPLOAD_DIR, fname))

    tpl = QRTemplate(
        file_path       = fname,
        price           = seg.price,
        fare_segment_id = seg.id,
    )
    db.session.add(tpl)
    db.session.commit()

    return (
        jsonify(id=tpl.id, url=f"/manager/qr-templates/{tpl.id}/file",
                price=f"{seg.price:.2f}"),
        201,
    )


@manager_bp.route("/qr-templates", methods=["GET"])
@require_role("manager")
def list_qr():
    return jsonify(
        [
            {
                "id":    t.id,
                "url":   f"/manager/qr-templates/{t.id}/file",
                "price": f"{t.price:.2f}",
            }
            for t in QRTemplate.query.order_by(QRTemplate.created_at.desc())
        ]
    ), 200


@manager_bp.route("/qr-templates/<int:tpl_id>/file", methods=["GET"])
def serve_qr_file(tpl_id):
    tpl = QRTemplate.query.get_or_404(tpl_id)
    return send_from_directory(UPLOAD_DIR, tpl.file_path)


# ──────────────────────────────  FARE SEGMENTS  ─────────────────────────
@manager_bp.route("/fare-segments", methods=["GET"])
@require_role("manager")
def list_fare_segments():
    rows = FareSegment.query.order_by(FareSegment.id).all()
    return (
        jsonify(
            [
                {
                    "id":    s.id,
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
    """
    Body:
      {
        "deviceId": "PGT-001",   # or numeric "1"
        "in":       5,
        "out":      3,
        "total":    8
      }
    """
    data = request.get_json() or {}
    missing = [k for k in ("deviceId", "in", "out", "total") if k not in data]
    if missing:
        return jsonify(error=f"Missing field(s): {', '.join(missing)}"), 400

    # ── locate the bus ──────────────────────────────────────────────
    device_id = str(data["deviceId"]).strip()           # trim stray spaces/new-lines
    bus = (
        # ▸ case-insensitive match on identifier (PGT-001 etc.)
        Bus.query.filter(func.lower(Bus.identifier) == device_id.lower()).first()
        or
        # ▸ OR allow numeric id ("1", 1) in the payload
        (Bus.query.get(int(device_id)) if device_id.isdigit() else None)
    )
    if not bus:
        return jsonify(error="Invalid deviceId: Bus not found"), 404

    # ── create reading ──────────────────────────────────────────────
    try:
        reading = SensorReading(
            in_count    = int(data["in"]),
            out_count   = int(data["out"]),
            total_count = int(data["total"]),
            bus_id      = bus.id,
        )
        db.session.add(reading)
        db.session.commit()
        return (
            jsonify(id=reading.id, timestamp=reading.timestamp.isoformat()),
            201,
        )
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
    """
    GET /buses/bus-02/sensor-readings
    Returns all SensorReading rows for bus-02, newest first.
    """
    bus = Bus.query.filter_by(identifier=device_id).first_or_404()

    readings = (
        SensorReading.query
            .filter_by(bus_id=bus.id)
            .order_by(SensorReading.timestamp.desc())
            .all()
    )

    return jsonify([
        {
            "id":         r.id,
            "timestamp":  r.timestamp.isoformat(),
            "in_count":   r.in_count,
            "out_count":  r.out_count,
            "total_count":r.total_count,
        }
        for r in readings
    ]), 200

