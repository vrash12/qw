# routes/commuter.py  (add at the very bottom)

import os
from datetime import datetime

from flask import Blueprint, request, jsonify, g, current_app, url_for
from routes.auth import require_role
from db import db
from models.schedule       import  Trip, StopTime
from models.sensor_reading import SensorReading
from models.announcement   import Announcement
from models.ticket_sale import TicketSale
from datetime import datetime, timedelta
from sqlalchemy import func
from models.bus import Bus   
from models.user import User

commuter_bp = Blueprint("commuter", __name__, url_prefix="/commuter")

@commuter_bp.route("/dashboard", methods=["GET"])
@require_role("commuter")
def dashboard():
    """
    Everything the mobile dashboard needs in one round-trip.
    """
    today = datetime.utcnow().date()

    # -- next trip ------------------------------------------------------
    next_trip = (
        db.session.query(Trip, Bus.identifier)
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(Trip.service_date == today)
        .order_by(Trip.start_time.asc())
        .first()
    )

    # -- ticket & announcement counters --------------------------------
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    recent_tix  = TicketSale.query.filter(
                    TicketSale.user_id == g.user.id,
                    TicketSale.created_at >= seven_days_ago).count()
    unread_msgs = Announcement.query.count()  # refine later ↩︎

    # -- build payload --------------------------------------------------
    return jsonify({
        "greeting":      _choose_greeting(),
        "user_name":     f"{g.user.first_name} {g.user.last_name}",
        "next_trip":     None if not next_trip else {
            "bus":   next_trip.identifier.replace("bus-", "Bus "),
            "start": next_trip.Trip.start_time.strftime("%H:%M"),
            "end":   next_trip.Trip.end_time.strftime("%H:%M"),
        },
        "recent_tickets":   recent_tix,
        "unread_messages":  unread_msgs,
    }), 200


def _choose_greeting() -> str:
    hr = datetime.utcnow().hour
    if hr < 12:      return "Good morning"
    elif hr < 18:    return "Good afternoon"
    return "Good evening"

@commuter_bp.route('/trips', methods=['GET'])
def list_all_trips():
    """
    GET /commuter/trips?date=YYYY-MM-DD
    Returns a list of all scheduled trips for a given day.
    This is a public endpoint.
    """
    date_str = request.args.get('date')
    if not date_str:
        return jsonify(error="A 'date' parameter is required."), 400

    try:
        svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="Invalid date format. Use YYYY-MM-DD."), 400

    # Subquery to find the first stop for each trip
    first_stop_sq = (
        db.session.query(
            StopTime.trip_id,
            func.min(StopTime.seq).label('min_seq')
        )
        .group_by(StopTime.trip_id)
        .subquery()
    )
    first_stop_name_sq = (
        db.session.query(
            StopTime.trip_id,
            StopTime.stop_name.label('origin')
        )
        .join(first_stop_sq, (StopTime.trip_id == first_stop_sq.c.trip_id) & (StopTime.seq == first_stop_sq.c.min_seq))
        .subquery()
    )

    # Subquery to find the last stop for each trip
    last_stop_sq = (
        db.session.query(
            StopTime.trip_id,
            func.max(StopTime.seq).label('max_seq')
        )
        .group_by(StopTime.trip_id)
        .subquery()
    )
    last_stop_name_sq = (
        db.session.query(
            StopTime.trip_id,
            StopTime.stop_name.label('destination')
        )
        .join(last_stop_sq, (StopTime.trip_id == last_stop_sq.c.trip_id) & (StopTime.seq == last_stop_sq.c.max_seq))
        .subquery()
    )
    
    # Main query to get trips and join with all the info
    trips = (
        db.session.query(
            Trip,
            Bus.identifier,
            first_stop_name_sq.c.origin,
            last_stop_name_sq.c.destination
        )
        .join(Bus, Trip.bus_id == Bus.id)
        .outerjoin(first_stop_name_sq, Trip.id == first_stop_name_sq.c.trip_id)
        .outerjoin(last_stop_name_sq, Trip.id == last_stop_name_sq.c.trip_id)
        .filter(Trip.service_date == svc_date)
        .order_by(Trip.start_time.asc())
        .all()
    )

    result = [{
        "id": trip.id,
        "bus_identifier": identifier,
        "start_time": trip.start_time.strftime("%H:%M"),
        "end_time": trip.end_time.strftime("%H:%M"),
        "origin": origin or "N/A",
        "destination": destination or "N/A"
    } for trip, identifier, origin, destination in trips]
    
    return jsonify(result), 200

# ─────────────────────────── BUS LIST ────────────────────────────
@commuter_bp.route("/buses", methods=["GET"])
#@require_role("commuter")
def list_buses():
    """
    Optional: ?date=YYYY-MM-DD  → only buses that have trips on that day
    """
    date_str = request.args.get("date")
    q = Bus.query
    if date_str:
        try:
            svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        q = (q.join(Trip, Bus.id == Trip.bus_id)
               .filter(Trip.service_date == svc_date))
    buses = q.order_by(Bus.identifier.asc()).all()
    return jsonify([{"id": b.id, "identifier": b.identifier} for b in buses]), 200

# ──────────────────────── BUS-TRIPS (per day) ────────────────────────
@commuter_bp.route("/bus-trips", methods=["GET"])
@require_role("commuter")
def commuter_bus_trips():
    """
    GET /commuter/bus-trips?bus_id=<int>&date=YYYY-MM-DD
    """
    bus_id  = request.args.get("bus_id", type=int)
    date_str= request.args.get("date")
    if not (bus_id and date_str):
        return jsonify(error="bus_id and date are required"), 400
    try:
        svc_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="date must be YYYY-MM-DD"), 400

    trips = (Trip.query
               .filter_by(bus_id=bus_id, service_date=svc_date)
               .order_by(Trip.start_time.asc())
               .all())
    return jsonify([
        {
            "id":         t.id,
            "number":     t.number,
            "start_time": t.start_time.strftime("%H:%M"),
            "end_time":   t.end_time.strftime("%H:%M"),
        } for t in trips
    ]), 200

# ───────────────────────── STOP-TIMES (read-only) ─────────────────────
@commuter_bp.route("/stop-times", methods=["GET"])
@require_role("commuter")
def commuter_stop_times():
    """
    GET /commuter/stop-times?trip_id=<int>
    (Manager has the same shape; commuters just need read access.)
    """
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = (StopTime.query
             .filter_by(trip_id=trip_id)
             .order_by(StopTime.seq.asc())
             .all())
    return jsonify([
        {
            "stop_name":   st.stop_name,
            "arrive_time": st.arrive_time.strftime("%H:%M"),
            "depart_time": st.depart_time.strftime("%H:%M"),
        } for st in sts
    ]), 200

# ───────────────────────── VEHICLE LOCATION ────────────────────────────
@commuter_bp.route("/location", methods=["GET"])
@require_role("commuter")
def vehicle_location():
    """Latest telemetry pushed by the bus head-unit sensors."""
    sr = (
        SensorReading.query
        .order_by(SensorReading.timestamp.desc())
        .first()
    )
    if not sr:
        return jsonify(error="no sensor data"), 404
    return jsonify(
        lat       = sr.lat,
        lng       = sr.lng,
        occupied  = sr.occupied,
        timestamp = sr.timestamp.isoformat(),
    ), 200

# ───────────────────────── MY RECEIPTS (commuter) ──────────────────────
@commuter_bp.route("/tickets/mine", methods=["GET"])
@require_role("commuter")
def my_receipts():
    # start debugging
    days = request.args.get("days")
    current_app.logger.debug(f"[Commuter:tickets/mine] ENTER user={g.user.id!r} days={days!r}")

    try:
        qs = TicketSale.query.filter_by(user_id=g.user.id)
        current_app.logger.debug("  → Base query constructed")

        if days in ("7", "30"):
            cutoff = datetime.utcnow() - timedelta(days=int(days))
            qs = qs.filter(TicketSale.created_at >= cutoff)
            current_app.logger.debug(f"  → Applied date filter: created_at >= {cutoff.isoformat()}")

        tickets = qs.order_by(TicketSale.created_at.desc()).all()
        current_app.logger.debug(f"  → Retrieved {len(tickets)} tickets from DB")

        out = []
        for t in tickets:
            # detect base & discount to reconstruct QR
            if t.passenger_type == "discount":
                # avoid division by zero if price is zero
                base = round(float(t.price) / 0.8) if t.price else 0
                disc = True
            else:
                base = int(t.price)
                disc = False

            png = f"fare_{base}{'_disc' if disc else ''}.png"
            qr_url = url_for("static", filename=f"qr/{png}", _external=True)
            current_app.logger.debug(f"    • Ticket {t.id}: base={base}, disc={disc}, png={png}")

            out.append({
                "id":          t.id,
                "referenceNo": t.reference_no,
                "date":        t.created_at.strftime("%B %d, %Y"),
                "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
                "fare":        f"{float(t.price):.2f}",
                "paid":        bool(t.paid),
                "qr_url":      qr_url,
            })

        current_app.logger.debug(f"[Commuter:tickets/mine] EXIT returning {len(out)} records")
        return jsonify(out), 200

    except Exception as e:
        current_app.logger.exception("[Commuter:tickets/mine] ERROR generating receipts")
        # include the exception text in the JSON so you can see it in your RN logs
        return jsonify(error=str(e)), 500
    

# ────────────────────────── TRIP DETAILS (read-only) ───────────────────
@commuter_bp.route("/trips/<int:trip_id>", methods=["GET"])
@require_role("commuter")
def get_trip(trip_id: int):
    """Same response shape as the manager’s endpoint, but read-only."""
    trip = Trip.query.get_or_404(trip_id)

    first_stop = (
        StopTime.query
        .filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .first()
    )
    last_stop  = (
        StopTime.query
        .filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.desc(), StopTime.id.desc())
        .first()
    )

    return jsonify(
        id          = trip.id,
        number      = trip.number,
        origin      = first_stop.stop_name if first_stop else "",
        destination = last_stop.stop_name  if last_stop  else "",
        start_time  = trip.start_time.strftime("%H:%M"),
        end_time    = trip.end_time.strftime("%H:%M"),
    ), 200


# ───────────────────────────── TIMETABLE ───────────────────────────────
@commuter_bp.route("/timetable", methods=["GET"])
@require_role("commuter")
def timetable():
    """
    GET /commuter/timetable?trip_id=<int>
      → [ { stop: "...", arrive: "HH:MM", depart: "HH:MM" }, … ]
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
            "stop":   st.stop_name,
            "arrive": st.arrive_time.strftime("%H:%M") if st.arrive_time else "",
            "depart": st.depart_time.strftime("%H:%M") if st.depart_time else "",
        }
        for st in sts
    ]), 200


# ───────────────────────────── SCHEDULE ────────────────────────────────
@commuter_bp.route("/schedule", methods=["GET"])
@require_role("commuter")
def schedule():
    """
    GET /commuter/schedule?trip_id=<int>&date=YYYY-MM-DD
    Returns a flat timeline suitable for your RN component.
    """
    trip_id  = request.args.get("trip_id", type=int)
    date_str = request.args.get("date")

    if not trip_id or not date_str:
        return jsonify(error="trip_id and date are required"), 400

    # Validate date string just to be safe
    try:
        _ = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="date must be YYYY-MM-DD"), 400

    stops = (
        StopTime.query
        .filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )

    events = []
    for idx, st in enumerate(stops):
        # “Stop” event
        events.append({
            "id":          idx * 2 + 1,
            "type":        "stop",
            "label":       "At Stop",
            "start_time":  st.arrive_time.strftime("%H:%M") if st.arrive_time else "",
            "end_time":    st.depart_time.strftime("%H:%M") if st.depart_time else "",
            "description": st.stop_name,
        })
        # “Trip” segment (except after final stop)
        if idx < len(stops) - 1:
            nxt = stops[idx + 1]
            events.append({
                "id":          idx * 2 + 2,
                "type":        "trip",
                "label":       "In Transit",
                "start_time":  st.depart_time.strftime("%H:%M") if st.depart_time else "",
                "end_time":    nxt.arrive_time.strftime("%H:%M") if nxt.arrive_time else "",
                "description": f"{st.stop_name} → {nxt.stop_name}",
            })

    return jsonify(events=events), 200
@commuter_bp.route("/announcements", methods=["GET"])
def announcements():
    bus_id   = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")

    # ── build query ──────────────────────────────────────────────
    query = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)   # ← OUTER join
    )

    if bus_id:
        query = query.filter(User.assigned_bus_id == bus_id)

    if date_str:
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        query = query.filter(func.date(Announcement.timestamp) == day)

    results = query.order_by(Announcement.timestamp.desc()).all()

    anns = [{
        "id":            ann.id,
        "message":       ann.message,
        "timestamp":     ann.timestamp.isoformat(),
        "author_name":   f"{first} {last}",
        "bus_identifier": bus_identifier or "unassigned"   # safe default
    } for ann, first, last, bus_identifier in results]     # ← only **one** “in results”

    return jsonify(anns), 200
