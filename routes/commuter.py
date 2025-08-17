# routes/commuter.py
import datetime as dt
from flask import Blueprint, request, jsonify, g, current_app, url_for, redirect
from sqlalchemy import func, case
from sqlalchemy.orm import joinedload
from typing import Any, Dict, List, Optional

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
from models.device_token import DeviceToken

import traceback
from werkzeug.exceptions import HTTPException
from typing import Any, Optional


# --- timezone setup ---
try:
    from zoneinfo import ZoneInfo
    try:
        LOCAL_TZ = ZoneInfo("Asia/Manila")
    except Exception:
        LOCAL_TZ = dt.timezone(dt.timedelta(hours=8))
except Exception:
    LOCAL_TZ = dt.timezone(dt.timedelta(hours=8))


commuter_bp = Blueprint("commuter", __name__, url_prefix="/commuter")

def _debug_enabled() -> bool:
    return (request.args.get("debug") or request.headers.get("X-Debug") or "").lower() in {"1","true","yes"}

@commuter_bp.app_errorhandler(Exception)
def _commuter_errors(e: Exception):
    """
    If ?debug=1 (or X-Debug: 1) => return JSON with error type + message + traceback.
    Otherwise: let HTTPExceptions pass through unchanged; others return a generic 500.
    """
    # Log full traceback to server logs
    current_app.logger.exception("Unhandled error on %s %s", request.method, request.path)

    if isinstance(e, HTTPException) and not _debug_enabled():
        # Preserve normal HTTP errors (401/403/404/400 etc.) unless debug is on
        return e

    status = getattr(e, "code", 500)
    if _debug_enabled():
        return jsonify({
            "ok": False,
            "type": e.__class__.__name__,
            "error": str(e),
            "endpoint": request.endpoint,
            "path": request.path,
            "traceback": traceback.format_exc(),
        }), status

    return jsonify({"error": "internal server error"}), status

# -------- helpers --------
def _as_time(v: Any) -> Optional[dt.time]:
    """Coerce ORM-returned values to datetime.time."""
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.time().replace(tzinfo=None)
    if isinstance(v, dt.time):
        return v.replace(tzinfo=None)
    if isinstance(v, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return dt.datetime.strptime(v, fmt).time()
            except ValueError:
                pass
    return None


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

    if t.passenger_type == "discount":
        base = round(float(t.price) / 0.8) if t.price else 0
        prefix = "discount"
    else:
        base = int(t.price or 0)
        prefix = "regular"

    filename = f"{prefix}_{base}.jpg"
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

    # Choose QR asset and guard for None prices
    if t.passenger_type == "discount":
        base = round(float(t.price or 0) / 0.8)
        prefix = "discount"
    else:
        base = int(t.price or 0)
        prefix = "regular"

    filename = f"{prefix}_{base}.jpg"
    qr_url = url_for("static", filename=f"qr/{filename}", _external=True)

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
        "fare": f"{float(t.price or 0):.2f}",
        "paid": bool(t.paid),
        "qr": payload,      # JSON payload (schema/ids/names/link)
        "qr_link": qr_link, # dynamic redirect to static QR asset
        "qr_url": qr_url,   # direct static asset URL
    }), 200


@commuter_bp.route("/dashboard", methods=["GET"])
@require_role("commuter")
def dashboard():
    """
    Compact dashboard payload + accurate live_now using local time.

    Debug helpers:
      - ?debug=1           -> include a 'debug' object in the JSON
      - ?date=YYYY-MM-DD   -> pretend we're on this service date
      - ?now=HH:MM         -> pretend the current local time is HH:MM
    """
    debug_on = (request.args.get("debug") or "").lower() in {"1", "true", "yes"}

    # -------- Local "now" and service date (with optional overrides) --------
    now_local = dt.datetime.now(LOCAL_TZ) if LOCAL_TZ else dt.datetime.now()
    date_arg = (request.args.get("date") or "").strip()
    force_now = (request.args.get("now") or request.args.get("force_now") or "").strip()

    if date_arg:
        try:
            today_local = dt.datetime.strptime(date_arg, "%Y-%m-%d").date()
        except ValueError:
            today_local = now_local.date()
    else:
        today_local = now_local.date()

    if force_now:
        try:
            hh, mm = map(int, force_now.split(":")[:2])
            now_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except Exception:
            pass

    # Compare purely on naive time values to match DB 'TIME' columns
    now_time_local = now_local.time().replace(tzinfo=None)

    def _choose_greeting() -> str:
        hr = now_local.hour
        if hr < 12:
            return "Good morning"
        elif hr < 18:
            return "Good afternoon"
        return "Good evening"

    # -------- next trip (the next one from "now") --------
    next_trip_row = (
        db.session.query(Trip, Bus.identifier.label("bus_identifier"))
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(
            Trip.service_date == today_local,
            Trip.start_time >= now_time_local,
        )
        .order_by(Trip.start_time.asc())
        .first()
    )
    if next_trip_row:
        trip, identifier = next_trip_row
        next_trip = {
            "bus": (identifier or "").replace("bus-", "Bus "),
            "start": _as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
            "end": _as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
        }
    else:
        next_trip = None

    # -------- unread messages (for Announcements dashlet) --------
    unread_msgs = Announcement.query.count()

    # -------- last announcement pill --------
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

    # -------- LIVE NOW (tolerant like route timeline) --------
    def _is_live_window(now_t: dt.time, s: Optional[dt.time], e: Optional[dt.time], *, grace_min: int = 3) -> bool:
        """
        True if now_t within [s,e). If s==e (zero-dwell), treat as ±grace_min minutes window.
        All params are naive times (tzinfo=None).
        """
        if not s or not e:
            return False
        if s == e:
            base = dt.datetime.combine(today_local, s)
            nowd = dt.datetime.combine(today_local, now_t)
            return abs((nowd - base).total_seconds()) <= grace_min * 60
        return s <= now_t < e

    live_now: List[Dict[str, Any]] = []
    debug_trips: List[Dict[str, Any]] = []

    trips_today = (
        db.session.query(Trip, Bus.identifier.label("bus_identifier"))
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(Trip.service_date == today_local)
        .order_by(Trip.start_time.asc())
        .all()
    )

    for t, bid in trips_today:
        sts = (
            StopTime.query.filter_by(trip_id=t.id)
            .order_by(StopTime.seq.asc(), StopTime.id.asc())
            .all()
        )

        events: List[Dict[str, Any]] = []
        if len(sts) < 2:
            # No usable stop list — fall back to full trip window
            events.append({
                "type": "trip",
                "label": "In Transit",
                "start": _as_time(t.start_time),
                "end": _as_time(t.end_time),
                "description": "",
            })
        else:
            for idx, st in enumerate(sts):
                # STOP window even if only one of arrive/depart exists
                s = _as_time(st.arrive_time or st.depart_time)
                e = _as_time(st.depart_time or st.arrive_time)
                if s or e:
                    events.append({
                        "type": "stop",
                        "label": "At Stop",
                        "start": s,
                        "end": e,
                        "description": st.stop_name,
                    })

                # TRIP window to next stop — be lenient with missing times
                if idx < len(sts) - 1:
                    nxt = sts[idx + 1]
                    s2 = _as_time(st.depart_time or st.arrive_time)
                    e2 = _as_time(nxt.arrive_time or nxt.depart_time)
                    if s2 and e2 and s2 != e2:
                        events.append({
                            "type": "trip",
                            "label": "In Transit",
                            "start": s2,
                            "end": e2,
                            "description": f"{st.stop_name} → {nxt.stop_name}",
                        })

        # If still nothing, ensure one full window
        if not events:
            events.append({
                "type": "trip",
                "label": "In Transit",
                "start": _as_time(t.start_time),
                "end": _as_time(t.end_time),
                "description": "",
            })

        chosen = None
        for ev in events:
            if _is_live_window(now_time_local, ev["start"], ev["end"], grace_min=3):
                chosen = ev
                live_now.append({
                    "bus_id": t.bus_id,
                    "bus": (bid or "").replace("bus-", "Bus "),
                    "trip_id": t.id,
                    "type": ev["type"],
                    "label": ev["label"],
                    "start": ev["start"].strftime("%H:%M"),
                    "end": ev["end"].strftime("%H:%M"),
                    "description": ev["description"],
                })
                break

        # Final fallback: if the specific segments didn't match but we're within the trip window, show In Transit
        ts = _as_time(t.start_time)
        te = _as_time(t.end_time)
        if not chosen and ts and te and _is_live_window(now_time_local, ts, te, grace_min=0):
            live_now.append({
                "bus_id": t.bus_id,
                "bus": (bid or "").replace("bus-", "Bus "),
                "trip_id": t.id,
                "type": "trip",
                "label": "In Transit",
                "start": ts.strftime("%H:%M"),
                "end": te.strftime("%H:%M"),
                "description": "",
            })

        if debug_on:
            def _fmt(x: Optional[dt.datetime.time]) -> Optional[str]:
                return x.strftime("%H:%M") if x else None
            debug_trips.append({
                "trip_id": t.id,
                "bus": (bid or "").replace("bus-", "Bus "),
                "events": [
                    {
                        "type": ev["type"],
                        "label": ev["label"],
                        "start": _fmt(ev["start"]),
                        "end": _fmt(ev["end"]),
                        "desc": ev["description"],
                        "hit": _is_live_window(now_time_local, ev["start"], ev["end"], grace_min=3),
                    } for ev in events
                ],
                "chosen": None if not chosen else {
                    "type": chosen["type"],
                    "start": _fmt(chosen["start"]),
                    "end": _fmt(chosen["end"]),
                },
            })

    # loud server logs
    current_app.logger.info(
        "dashboard live_now=%d now=%s date=%s trips=%d",
        len(live_now),
        now_time_local,
        today_local,
        len(trips_today),
    )

    payload: Dict[str, Any] = {
        "greeting": _choose_greeting(),
        "user_name": f"{g.user.first_name} {g.user.last_name}",
        "next_trip": next_trip,
        "unread_messages": int(unread_msgs or 0),
        "last_announcement": last_announcement,
        "live_now": live_now,
    }

    if debug_on:
        payload["debug"] = {
            "now_local": now_local.strftime("%Y-%m-%d %H:%M:%S"),
            "today_local": str(today_local),
            "live_now_len": len(live_now),
            "trips_today_len": len(trips_today),
            "first_trip_debug": debug_trips[0] if debug_trips else None,
        }

    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp, 200


@commuter_bp.route("/trips", methods=["GET"])
def list_all_trips():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify(error="A 'date' parameter is required."), 400
    try:
        svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
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
            "start_time": _as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
            "end_time": _as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
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
            svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
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
        svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
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
                    "start_time": _as_time(t.start_time).strftime("%H:%M") if _as_time(t.start_time) else "",
                    "end_time": _as_time(t.end_time).strftime("%H:%M") if _as_time(t.end_time) else "",
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
                    "arrive_time": (_as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else ""),
                    "depart_time": (_as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else ""),
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
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        qs = qs.filter(func.date(TicketSale.created_at) == day)
    elif days in {"7", "30"}:
        cutoff = dt.datetime.utcnow() - timedelta(days=int(days))
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
            base = int(t.price or 0)
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
            "fare": f"{float(t.price or 0):.2f}",
            "paid": bool(t.paid),
            "qr_url": qr_url,
            "qr": payload if not light else payload,
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
            start_time=_as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
            end_time=_as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
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
                    "arrive": (_as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else ""),
                    "depart": (_as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else ""),
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

    trip = Trip.query.get_or_404(trip_id)
    stops = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )

    events: List[Dict[str, Any]] = []
    # If we don't have at least 2 stops, synthesize one in-transit segment
    if len(stops) < 2:
        events.append({
            "id": 1,
            "type": "trip",
            "label": "In Transit",
            "start_time": _as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
            "end_time": _as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
            "description": "",
        })
    else:
        for idx, st in enumerate(stops):
            events.append({
                "id": idx * 2 + 1,
                "type": "stop",
                "label": "At Stop",
                "start_time": _as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else "",
                "end_time": _as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else "",
                "description": st.stop_name,
            })
            if idx < len(stops) - 1:
                nxt = stops[idx + 1]
                events.append({
                    "id": idx * 2 + 2,
                    "type": "trip",
                    "label": "In Transit",
                    "start_time": _as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else "",
                    "end_time": _as_time(nxt.arrive_time).strftime("%H:%M") if _as_time(nxt.arrive_time) else "",
                    "description": f"{st.stop_name} → {nxt.stop_name}",
                })
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

    # default to *local today* when client doesn't pass ?date=
    if not date_str:
        day = (dt.datetime.now(LOCAL_TZ) if LOCAL_TZ else dt.datetime.now()).date()
        query = query.filter(func.date(Announcement.timestamp) == day)
    else:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
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
