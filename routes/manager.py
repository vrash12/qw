# backend/routes/manager.py
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, date as _Date, time as _Time

from flask import Blueprint, request, jsonify, send_from_directory, current_app
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
from models.trip_metric import TripMetric
from models.route import Route  # ← was missing

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

manager_bp = Blueprint("manager", __name__)


# ────────────────────────────── helpers ──────────────────────────────

def _day_range(d: _Date) -> tuple[datetime, datetime]:
    """Return [start, end) window for a given date (UTC-naive)."""
    start = datetime.combine(d, datetime.min.time())
    return start, start + timedelta(days=1)


def _trip_window(day: _Date, start_t: _Time, end_t: _Time) -> tuple[datetime, datetime]:
    """Return [start, end) for a trip; handles past-midnight spans."""
    start_dt = datetime.combine(day, start_t)
    end_dt = datetime.combine(day, end_t)
    if end_t <= start_t:  # crosses midnight
        end_dt += timedelta(days=1)
    # half-open
    return start_dt, end_dt


def _active_trip_for(bus_id: int, ts: datetime):
    """Find the trip whose time window contains ts (handles past-midnight)."""
    day = ts.date()
    prev = (ts - timedelta(days=1)).date()
    candidates = (
        Trip.query.filter(Trip.bus_id == bus_id, Trip.service_date.in_([day, prev]))
        .order_by(Trip.start_time.asc())
        .all()
    )
    for t in candidates:
        start, end = _trip_window(t.service_date, t.start_time, t.end_time)
        if start <= ts < end:
            return t
    return None


# ────────────────────────────── endpoints ──────────────────────────────

@manager_bp.route("/revenue-breakdown", methods=["GET"])
@require_role("manager")
def revenue_breakdown():
    """
    Revenue & ticket percentages by passenger_type (regular vs discount).
    Window selection mirrors /manager/route-insights. Uses half-open [from, to)
    ranges and avoids func.date() so indexes are used.
    """
    paid_only = (request.args.get("paid_only", "true").lower() != "false")

    trip_id = request.args.get("trip_id", type=int)
    if trip_id:
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404
        bus_id = trip.bus_id
        window_from, window_to = _trip_window(trip.service_date, trip.start_time, trip.end_time)
    else:
        date_str = request.args.get("date")
        bus_id = request.args.get("bus_id", type=int)
        if not (date_str and bus_id):
            return jsonify(error="trip_id OR (date, bus_id, from, to) required"), 400
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
            frm = datetime.strptime(request.args["from"], "%H:%M").time()
            to_ = datetime.strptime(request.args["to"], "%H:%M").time()
        except (KeyError, ValueError):
            return jsonify(error="invalid or missing time range"), 400
        window_from, window_to = _trip_window(day, frm, to_)

    qs = (
        db.session.query(
            TicketSale.passenger_type,
            func.count(TicketSale.id).label("tickets"),
            func.coalesce(func.sum(TicketSale.price), 0.0).label("revenue"),
        )
        .filter(TicketSale.bus_id == bus_id)
        .filter(TicketSale.created_at >= window_from, TicketSale.created_at < window_to)
    )
    if paid_only:
        qs = qs.filter(TicketSale.paid.is_(True))

    rows = qs.group_by(TicketSale.passenger_type).all()

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

    # ensure buckets
    have = {g["type"] for g in by_type}
    if "regular" not in have:
        by_type.append({"type": "regular", "tickets": 0, "revenue": 0.0})
    if "discount" not in have:
        by_type.append({"type": "discount", "tickets": 0, "revenue": 0.0})

    out = []
    for g in by_type:
        pct_t = (g["tickets"] / totals_tickets * 100.0) if totals_tickets else 0.0
        pct_r = (g["revenue"] / totals_revenue * 100.0) if totals_revenue else 0.0
        out.append(
            {
                "type": g["type"],
                "tickets": g["tickets"],
                "revenue": round(g["revenue"], 2),
                "pct_tickets": round(pct_t, 1),
                "pct_revenue": round(pct_r, 1),
            }
        )

    return jsonify(
        {
            "from": window_from.isoformat(),
            "to": window_to.isoformat(),
            "paid_only": bool(paid_only),
            "totals": {"tickets": int(totals_tickets), "revenue": round(totals_revenue, 2)},
            "by_type": sorted(out, key=lambda x: 0 if x["type"] == "regular" else 1),
        }
    ), 200


@manager_bp.route("/trips/<int:trip_id>", methods=["PATCH"])
@require_role("manager")
def update_trip(trip_id: int):
    data = request.get_json() or {}
    try:
        number = (data.get("number") or "").strip()
        start_time = datetime.strptime(data["start_time"], "%H:%M").time()
        end_time = datetime.strptime(data["end_time"], "%H:%M").time()
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload or missing required fields"), 400

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
        return jsonify(error=f"Error deleting trip: {e}"), 500


@manager_bp.route("/tickets/composition", methods=["GET"])
@require_role("manager")
def tickets_composition():
    """
    Counts of regular vs discount for a day.
    - Voided tickets excluded.
    - NULL passenger_type treated as 'regular'.
    Uses [start, end) range (index-friendly).
    """
    try:
        day = datetime.strptime(
            request.args.get("date") or datetime.utcnow().date().isoformat(),
            "%Y-%m-%d",
        ).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    start, end = _day_range(day)
    ptype = func.coalesce(TicketSale.passenger_type, "regular")

    rows = (
        db.session.query(ptype.label("ptype"), func.count(TicketSale.id))
        .filter(TicketSale.created_at >= start, TicketSale.created_at < end)
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

    start, end = _day_range(day)
    O = aliased(TicketStop)
    D = aliased(TicketStop)

    rows = (
        db.session.query(
            TicketSale.id,
            TicketSale.price,
            TicketSale.passenger_type,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus"),
            O.stop_name.label("origin"),
            D.stop_name.label("destination"),
        )
        .join(User, TicketSale.user_id == User.id)
        .join(Bus, TicketSale.bus_id == Bus.id)
        .outerjoin(O, TicketSale.origin_stop_time_id == O.id)
        .outerjoin(D, TicketSale.destination_stop_time_id == D.id)
        .filter(TicketSale.created_at >= start, TicketSale.created_at < end)  # index-friendly
        .filter(TicketSale.voided.is_(False))
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
            "passenger_type": (r.passenger_type or "regular"),
            "passengerType": (r.passenger_type or "regular"),
        }
        for r in rows
    ]

    total = sum(float(r.price) for r in rows)
    return jsonify(tickets=tickets, total=f"{total:.2f}"), 200


@manager_bp.route("/buses", methods=["GET"])
@require_role("manager")
def list_buses():
    """
    Return buses with latest sensor snapshot without N+1 queries.
    """
    try:
        # latest timestamp per bus
        latest_sub = (
            db.session.query(
                SensorReading.bus_id.label("bus_id"),
                func.max(SensorReading.timestamp).label("ts"),
            )
            .group_by(SensorReading.bus_id)
            .subquery()
        )

        # join back to get occupancy at that ts
        SR = aliased(SensorReading)
        rows = (
            db.session.query(
                Bus.id,
                Bus.identifier,
                Bus.capacity,
                Bus.description,
                SR.timestamp,
                SR.total_count,
            )
            .outerjoin(latest_sub, latest_sub.c.bus_id == Bus.id)
            .outerjoin(
                SR,
                (SR.bus_id == Bus.id) & (SR.timestamp == latest_sub.c.ts),
            )
            .order_by(Bus.identifier)
            .all()
        )

        out = [
            {
                "id": b_id,
                "identifier": ident,
                "capacity": cap,
                "description": desc,
                "last_seen": ts.isoformat() if ts else None,
                "occupancy": total if total is not None else None,
            }
            for (b_id, ident, cap, desc, ts, total) in rows
        ]
        return jsonify(out), 200
    except Exception:
        current_app.logger.exception("ERROR in /manager/buses")
        return jsonify(error="Could not process the request to list buses."), 500


@manager_bp.route("/route-insights", methods=["GET"])
@require_role("manager")
def route_data_insights():
    """
    Per-minute occupancy series (+ optional stop labels) and metrics.
    Uses half-open time windows and avoids redundant Python work.
    """
    trip_id = request.args.get("trip_id", type=int)

    # ---- resolve window ----
    if trip_id:
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404

        bus_id = trip.bus_id
        window_from, window_to = _trip_window(trip.service_date, trip.start_time, trip.end_time)
        use_snapshot = False
        metrics = None

        # snapshot after trip ends (+2 min grace)
        if datetime.utcnow() >= window_to + timedelta(minutes=2):
            snap = TripMetric.query.filter_by(trip_id=trip_id).first()
            if snap:
                use_snapshot = True
                metrics = dict(
                    avg_pax=snap.avg_pax,
                    peak_pax=snap.peak_pax,
                    boarded=snap.boarded,
                    alighted=snap.alighted,
                    start_pax=snap.start_pax,
                    end_pax=snap.end_pax,
                    net_change=snap.end_pax - snap.start_pax,
                )

        meta = {
            "trip_id": trip_id,
            "trip_number": trip.number,
            "window_from": window_from.isoformat(),
            "window_to": window_to.isoformat(),
        }

        stop_rows = (
            StopTime.query.filter_by(trip_id=trip_id)
            .order_by(StopTime.seq.asc())
            .all()
        )
    else:
        date_str = request.args.get("date")
        bus_id = request.args.get("bus_id", type=int)
        if not date_str or not bus_id:
            return jsonify(error="date and bus_id are required when trip_id is omitted"), 400
        try:
            day = datetime.strptime(date_str, "%Y-%m-%d").date()
            frm = datetime.strptime(request.args["from"], "%H:%M").time()
            to_ = datetime.strptime(request.args["to"], "%H:%M").time()
        except (KeyError, ValueError):
            return jsonify(error="invalid or missing time range"), 400

        window_from, window_to = _trip_window(day, frm, to_)
        use_snapshot = False
        metrics = None
        meta = {
            "trip_id": None,
            "trip_number": None,
            "window_from": window_from.isoformat(),
            "window_to": window_to.isoformat(),
        }

        # stops from any overlapping trips that day
        trips_today = Trip.query.filter_by(bus_id=bus_id, service_date=day).all()
        ids = []
        for t in trips_today:
            t0, t1 = _trip_window(day, t.start_time, t.end_time)
            if t0 < window_to and window_from < t1:
                ids.append(t.id)

        stop_rows = []
        if ids:
            stop_rows = (
                StopTime.query.filter(StopTime.trip_id.in_(ids))
                .order_by(StopTime.trip_id.asc(), StopTime.seq.asc())
                .all()
            )

    # minute→stop mapping
    minute_to_stop: dict[str, set[str]] = {}
    for st in stop_rows:
        if st.arrive_time:
            k = st.arrive_time.strftime("%H:%M")
            minute_to_stop.setdefault(k, set()).add(st.stop_name)
        if st.depart_time:
            k = st.depart_time.strftime("%H:%M")
            minute_to_stop.setdefault(k, set()).add(st.stop_name)

    # occupancy series (MySQL/MariaDB date_format)
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
            SensorReading.timestamp < (window_to + timedelta(minutes=1)),
        )
        .group_by("hhmm")
        .order_by("hhmm")
        .all()
    )

    series = []
    pax_values = []
    total_in = total_out = 0
    for r in occ_rows:
        hhmm = r.hhmm
        pax = int(r.pax or 0)
        ins = int(r.ins or 0)
        outs = int(r.outs or 0)
        pax_values.append(pax)
        total_in += ins
        total_out += outs
        series.append(
            {
                "time": hhmm,
                "passengers": pax,
                "in": ins,
                "out": outs,
                "stop": " / ".join(sorted(minute_to_stop.get(hhmm, []))) or None,
            }
        )

    if not metrics:
        start_pax = pax_values[0] if pax_values else 0
        end_pax = pax_values[-1] if pax_values else 0
        metrics = dict(
            avg_pax=round(sum(pax_values) / len(pax_values)) if pax_values else 0,
            peak_pax=max(pax_values) if pax_values else 0,
            boarded=total_in,
            alighted=total_out,
            start_pax=start_pax,
            end_pax=end_pax,
            net_change=end_pax - start_pax,
        )

    return jsonify(occupancy=series, meta=meta, metrics=metrics, snapshot=use_snapshot), 200


@manager_bp.route("/metrics/tickets", methods=["GET"])
@require_role("manager")
def ticket_metrics():
    today = datetime.utcnow().date()
    date_to = datetime.strptime(request.args.get("to", today.isoformat()), "%Y-%m-%d").date()
    date_from = datetime.strptime(
        request.args.get("from", (date_to - timedelta(days=6)).isoformat()), "%Y-%m-%d"
    ).date()

    start = datetime.combine(date_from, datetime.min.time())
    end = datetime.combine(date_to + timedelta(days=1), datetime.min.time())

    bus_id = request.args.get("bus_id", type=int)

    qs = db.session.query(
        func.date_format(TicketSale.created_at, "%Y-%m-%d").label("d"),
        func.count(TicketSale.id).label("tickets"),
        func.sum(TicketSale.price).label("revenue"),
    ).filter(TicketSale.created_at >= start, TicketSale.created_at < end)
    if bus_id:
        qs = qs.filter(TicketSale.bus_id == bus_id)

    rows = qs.group_by("d").order_by("d").all()

    daily = [{"date": r.d, "tickets": int(r.tickets), "revenue": float(r.revenue or 0)} for r in rows]
    total_tickets = sum(d["tickets"] for d in daily)
    total_revenue = round(sum(d["revenue"] for d in daily), 2)

    return jsonify(daily=daily, total_tickets=total_tickets, total_revenue=total_revenue), 200


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

    sts = (
        db.session.query(StopTime)
        .filter_by(trip_id=trip_id)
        .distinct(StopTime.stop_name, StopTime.arrive_time, StopTime.depart_time)
        .order_by(StopTime.seq.asc(), StopTime.arrive_time.asc(), StopTime.id.asc())
        .all()
    )
    return jsonify(
        [
            {
                "id": st.id,
                "stop_name": st.stop_name,
                "arrive_time": st.arrive_time.strftime("%H:%M"),
                "depart_time": st.depart_time.strftime("%H:%M"),
                "seq": st.seq,
            }
            for st in sts
        ]
    ), 200


@manager_bp.route("/trips", methods=["POST"])
@require_role("manager")
def create_trip():
    data = request.get_json() or {}
    try:
        svc_date = datetime.strptime(data["service_date"], "%Y-%m-%d").date()
        number = (data["number"] or "").strip()
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
    except ValueError:
        return jsonify(error="Invalid date format. Expected YYYY-MM-DD."), 400

    trips = (
        Trip.query.filter_by(bus_id=bus_id, service_date=query_date)
        .order_by(Trip.start_time.asc())
        .all()
    )
    current_app.logger.debug(f"Found {len(trips)} trips")

    return jsonify(
        [
            {
                "id": t.id,
                "number": t.number,
                "start_time": t.start_time.strftime("%H:%M"),
                "end_time": t.end_time.strftime("%H:%M"),
            }
            for t in trips
        ]
    ), 200


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
    return jsonify(
        [
            {
                "id": s.id,
                "label": f"{s.origin.stop_name} → {s.destination.stop_name}",
                "price": f"{s.price:.2f}",
            }
            for s in rows
        ]
    ), 200


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
        now = datetime.utcnow()
        reading = SensorReading(
            in_count=int(data["in"]),
            out_count=int(data["out"]),
            total_count=int(data["total"]),
            bus_id=bus.id,
            timestamp=now,
        )
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
