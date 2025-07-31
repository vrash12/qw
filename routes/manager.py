# routes/manager.py
import os, uuid
from datetime import datetime

from flask import Blueprint, request, jsonify, send_from_directory
from werkzeug.utils import secure_filename
from flask import current_app
from db               import db
from routes.auth      import require_role
from models.bus       import Bus
from models.schedule  import  Trip, StopTime
from models.qr_template  import QRTemplate
from models.fare_segment import FareSegment
from models.sensor_reading import SensorReading
from sqlalchemy import func   
from models.ticket_sale import TicketSale


UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

manager_bp = Blueprint("manager", __name__)



# ───────────────────────── Route + Bus + Trip insights ─────────────────────────
@manager_bp.route("/route-insights", methods=["GET"])
@require_role("manager")
def route_data_insights():
    """
    Query params
      date=YYYY-MM-DD
      bus_id=1
      trip_id=3             # optional – you may use it later for stop-time filtering
      from=HH:MM            # window start (bus’s local time)
      to=HH:MM              # window end
    Returns
      {
        "occupancy": [ {"time":"13:05","passengers":20}, … ],
        "tickets":   [ {"time":"13:05","tickets":2,"revenue":30.00}, … ]
      }
    """
    # ── parse & validate ─────────────────────────────────────────────────
    try:
        date_str  = request.args["date"]
        bus_id    = int(request.args["bus_id"])
        start     = request.args["from"]
        end       = request.args["to"]
    except (KeyError, ValueError):
        return jsonify(error="date, bus_id, from, to are required"), 400

    day        = datetime.strptime(date_str, "%Y-%m-%d").date()
    window_from= datetime.combine(day,  datetime.strptime(start, "%H:%M").time())
    window_to  = datetime.combine(day,  datetime.strptime(end,   "%H:%M").time())

    # ── SENSOR READINGS (one row per minute) ─────────────────────────────
    occ_rows = (
        db.session.query(
            func.strftime('%H:%M', SensorReading.timestamp).label("hhmm"),
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
    occupancy = [
        {"time": r.hhmm, "passengers": int(r.pax)}
        for r in occ_rows
    ]

    # ── TICKET SALES (grouped per minute) ───────────────────────────────
    tix_rows = (
        db.session.query(
            func.strftime('%H:%M', TicketSale.created_at).label("hhmm"),
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
        {
          "time":    r.hhmm,
          "tickets": int(r.tickets),
          "revenue": float(r.revenue or 0)
        }
        for r in tix_rows
    ]

    return jsonify(occupancy=occupancy, tickets=tickets), 200


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

# ──────────────────────────────  BUS CRUD  ──────────────────────────────
@manager_bp.route("/buses", methods=["POST"])
@require_role("manager")
def create_bus():
    """
    Body: { "identifier": "bus-01", "capacity": 40, "description": "Hino RK1J" }
    """
    data = request.get_json() or {}
    ident = data.get("identifier", "").strip()
    if not ident:
        return jsonify(error="identifier is required"), 400

    bus = Bus(
        identifier  = ident,
        capacity    = data.get("capacity"),
        description = data.get("description", "").strip() or None,
    )
    db.session.add(bus)
    db.session.commit()
    return jsonify(id=bus.id, identifier=bus.identifier), 201


# routes/manager.py  – inside list_buses()
@manager_bp.route("/buses", methods=["GET"])
@require_role("manager")
def list_buses():
    route_id = request.args.get("route_id", type=int)

    q = Bus.query
    if route_id is not None:
        q = q.join(Trip, Bus.id == Trip.bus_id).filter(Trip.route_id == route_id)

    out = []
    for b in q.order_by(Bus.identifier).all():
        latest = (
            SensorReading.query
                .filter_by(bus_id=b.id)
                .order_by(SensorReading.timestamp.desc())
                .first()
        )
        out.append({
            "id":         b.id,           # ← numeric PK, not the string identifier
            "identifier": b.identifier,   # ← what the picker will show
            # keep extra fields if you still need them elsewhere
            "capacity":   b.capacity,
            "description":b.description,
            "last_seen":  latest.timestamp.isoformat() if latest else None,
            "occupancy":  latest.total_count if latest else None,
        })
    return jsonify(out), 200


# ──────────────────────────────  QR TEMPLATES  ──────────────────────────
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
