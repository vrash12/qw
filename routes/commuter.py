# routes/commuter.py
import datetime as dt
from flask import Blueprint, request, jsonify, g, current_app, url_for, redirect
from sqlalchemy import func, case
from sqlalchemy.orm import joinedload
from typing import Any, Dict, List, Optional
import os, textwrap
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
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import qrcode
import traceback
from werkzeug.exceptions import HTTPException
from typing import Any, Optional
from models.schedule import StopTime
from datetime import timedelta

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


THEMES = {
    "light": {
        "bg": (248, 250, 248),
        "card": (255, 255, 255),
        "text": (28, 32, 28),
        "subtle": (102, 114, 102),
        "brand": (16, 122, 82),
        "muted": (160, 168, 160),
        "line": (226, 232, 226),
        "accent": (20, 164, 108),
        "qr_bg": (245, 247, 245),
        "ribbon": (20, 164, 108),
    },
    "dark": {
        "bg": (18, 18, 18),
        "card": (28, 28, 28),
        "text": (240, 240, 240),
        "subtle": (189, 195, 199),
        "brand": (66, 194, 133),
        "muted": (120, 120, 120),
        "line": (52, 52, 52),
        "accent": (66, 194, 133),
        "qr_bg": (36, 36, 36),
        "ribbon": (50, 160, 110),
    },
}

def _rounded_rect(draw: ImageDraw.ImageDraw, xy, radius, fill):
    # Pillow >= 8.2 has rounded_rectangle; fall back to normal rect if missing
    if hasattr(draw, "rounded_rectangle"):
        draw.rounded_rectangle(xy, radius=radius, fill=fill)
    else:
        draw.rectangle(xy, fill=fill)

def _load_font(name_candidates, size):
    """
    Try fonts in this order:
      - /app static fonts (e.g., static/fonts/Inter.ttf, DejaVuSans.ttf)
      - system fonts (Inter, DejaVuSans, Arial)
      - PIL default
    """
    root = current_app.root_path if current_app else os.getcwd()
    for nm in name_candidates:
        # packaged in app
        p = os.path.join(root, "static", "fonts", nm)
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                pass
        # system lookup
        try:
            return ImageFont.truetype(nm, size)
        except Exception:
            continue
    return ImageFont.load_default()

def _wrap(draw: ImageDraw.ImageDraw, text, font, max_width):
    # simple word wrap using textbbox
    if not text:
        return [""]
    words = text.split()
    lines, line = [], []
    for w in words:
        test = " ".join(line + [w])
        if draw.textlength(test, font=font) <= max_width:
            line.append(w)
        else:
            if line:
                lines.append(" ".join(line))
            line = [w]
    if line:
        lines.append(" ".join(line))
    return lines

def _as_local(dt_obj: dt.datetime) -> dt.datetime:
    """
    Convert naive (assumed UTC) or aware datetime to LOCAL_TZ.
    """
    if dt_obj is None:
        return dt.datetime.now(LOCAL_TZ)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(LOCAL_TZ)


def _debug_enabled() -> bool:
    return (request.args.get("debug") or request.headers.get("X-Debug") or "").lower() in {"1","true","yes"}


@commuter_bp.route("/tickets/<int:ticket_id>/image.jpg", methods=["GET"])
def commuter_ticket_image(ticket_id: int):
    """
    Flat RGB-only JPG receipt renderer (no alpha, no rounded corners).
    Optional: ?download=1 to force download.
    """
    # --- fetch ticket ---
    t = (
        TicketSale.query.options(joinedload(TicketSale.user))
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # --- resolve stop names (StopTime or TicketStop fallback) ---
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

    # --- QR that points back to THIS image URL ---
    img_link = url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True)
    qr = qrcode.QRCode(box_size=10, border=1)
    qr.add_data(img_link)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")  # RGB only

    # --- canvas & palette (RGB only) ---
    W, H = 1080, 1600
    M = 48
    GREEN        = (45, 80, 22)
    LIGHT_GREEN  = (230, 244, 230)
    MID_TEXT     = (85, 95, 85)
    MUTED        = (140, 155, 140)
    BORDER       = (210, 225, 210)
    BG_TINT      = (245, 249, 245)
    CARD_BG      = (255, 255, 255)

    bg = Image.new("RGB", (W, H), BG_TINT)
    draw = ImageDraw.Draw(bg)

    # fonts with safe fallbacks
    def _font(name, size):
        try:
            return ImageFont.truetype(name, size)
        except Exception:
            try:
                return ImageFont.truetype("DejaVuSans.ttf", size)
            except Exception:
                return ImageFont.load_default()

    ft_h1   = _font("arialbd.ttf", 46)
    ft_h2   = _font("arialbd.ttf", 38)
    ft_lbl  = _font("arial.ttf",   26)
    ft_val  = _font("arialbd.ttf", 32)
    ft_big  = _font("arialbd.ttf", 56)
    ft_foot = _font("arial.ttf",   22)

    def tw(s, font): return draw.textlength(s, font=font)
    def ellipsize(s: str, max_chars: int) -> str:
        if len(s) <= max_chars: return s
        keep = max(6, max_chars // 2 - 1)
        return s[:keep] + "…" + s[-(max_chars - keep - 1):]

    # --- white card (square corners, no alpha) ---
    card_box = (M, M - 8, W - M, H - M - 8)
    draw.rectangle(card_box, fill=CARD_BG, outline=BORDER, width=1)

    y = M + 20

    # --- header bar (square) ---
    header_h = 96
    head_box = (M, y, W - M, y + header_h)
    draw.rectangle(head_box, fill=LIGHT_GREEN)
    draw.text((M + 28, y + (header_h - 46) // 2), "PGT Onboard — Official Receipt", fill=GREEN, font=ft_h1)
    y += header_h + 18
    draw.line((M + 28, y, W - M - 28, y), fill=BORDER, width=2)
    y += 24

    # --- two-column fields ---
    L = M + 28
    R = W - M - 28
    COL_GAP = 40
    COL_W = (R - L - COL_GAP) // 2

    def field(x, y, label, value):
        draw.text((x, y), label, fill=MUTED, font=ft_lbl)
        y2 = y + ft_lbl.size + 8
        val = value if tw(value, ft_val) <= COL_W else ellipsize(value, 28)
        draw.text((x, y2), val, fill=(20, 20, 20), font=ft_val)
        return y2 + ft_val.size + 18

    yL = y
    yR = y
    yL = field(L, yL, "Reference No.", t.reference_no or "—")
    yR = field(L + COL_W + COL_GAP, yR, "To", destination_name or "—")
    yL = field(L, yL, "Date / Time",
               f"{t.created_at.strftime('%B %d, %Y')}  ·  {t.created_at.strftime('%I:%M %p').lstrip('0').lower()}")
    yR = field(L + COL_W + COL_GAP, yR, "Passenger Type", (t.passenger_type or "").title() or "—")
    yL = field(L, yL, "From", origin_name or "—")
    yR = field(L + COL_W + COL_GAP, yR, "Commuter",
               f"{t.user.first_name} {t.user.last_name}" if t.user else "—")

    y = max(yL, yR) + 2
    draw.line((L, y, R, y), fill=BORDER, width=2)
    y += 22

    # --- amount + status ---
    draw.text((L, y), "Total Amount", fill=MUTED, font=ft_lbl)
    draw.text((L, y + 30), f"PHP {float(t.price or 0):.2f}", fill=GREEN, font=ft_big)

    state_txt = "PAID" if t.paid else "UNPAID"
    state_bg  = (35, 140, 70) if t.paid else (200, 50, 50)
    pill_w = int(tw(state_txt, ft_h2) + 36)
    pill_x1 = R - pill_w
    pill_y1 = y + 4
    # square "pill" to avoid alpha; still looks fine
    draw.rectangle((pill_x1, pill_y1, R, pill_y1 + 58), fill=state_bg)
    draw.text((pill_x1 + 18, pill_y1 + 10), state_txt, fill=(255, 255, 255), font=ft_h2)

    y += 100

    # --- QR panel (square) ---
    qr_side = 520
    qr_pad  = 20
    panel_w = qr_side + qr_pad * 2
    panel_h = qr_side + qr_pad * 2 + 60
    panel_box = (L, y, L + panel_w, y + panel_h)
    draw.rectangle(panel_box, fill=(247, 251, 247), outline=BORDER, width=1)

    qr_resized = qr_img.resize((qr_side, qr_side))
    bg.paste(qr_resized, (L + qr_pad, y + qr_pad))  # qr is RGB; no mask
    draw.text((L + qr_pad, y + qr_pad + qr_side + 14),
              "Scan to view/download this receipt", fill=MUTED, font=ft_lbl)

    # --- right column next to QR ---
    right_x = L + panel_w + 28
    draw.text((right_x, y), "Payment", fill=MUTED, font=ft_lbl)
    draw.text((right_x, y + 30), state_txt, fill=state_bg, font=ft_h2)

    yy = y + 110
    for k, v in [
        ("Bus ID",  str(getattr(t, "bus_id", "") or "—")),
 
    ]:
        draw.text((right_x, yy), k, fill=MUTED, font=ft_lbl)
        draw.text((right_x, yy + 26), v, fill=MID_TEXT, font=ft_val)
        yy += 64

    # footer
    draw.text((L, H - M - 40), ellipsize(img_link, 60), fill=(120, 130, 120), font=ft_foot)

    # --- encode to JPEG ---
    from flask import send_file, make_response
    bio = BytesIO()
    bg.save(bio, format="JPEG", quality=92)
    bio.seek(0)

    as_download = (request.args.get("download") or "").lower() in {"1", "true", "yes"}
    resp = make_response(send_file(
        bio,
        mimetype="image/jpeg",
        as_attachment=as_download,
        download_name=f"receipt_{t.reference_no}.jpg",
    ))
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp

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

    amount = int(round(float(t.price or 0)))
    prefix = "discount" if t.passenger_type == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"

    return redirect(url_for("static", filename=f"qr/{filename}", _external=True), code=302)


@commuter_bp.route("/tickets/<int:ticket_id>/view", methods=["GET"])
def commuter_ticket_view(ticket_id: int):
    """
    Minimal HTML view with the same image + a download button.
    Keeps the image URL canonical for QR verification flows.
    """
    img_url = url_for("commuter.commuter_ticket_image", ticket_id=ticket_id, _external=True)
    dl_url = img_url + ("&" if "?" in img_url else "?") + "download=1"
    return (
        f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Receipt #{ticket_id}</title>
    <style>
      body {{ margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Inter, sans-serif; background:#0b0b0b; color:#f2f2f2; }}
      .wrap {{ max-width: 720px; margin: 24px auto; padding: 16px; }}
      .card {{ background:#1c1c1c; border-radius:16px; padding:16px; box-shadow:0 10px 30px rgba(0,0,0,.3) }}
      img {{ width:100%; height:auto; border-radius:12px; display:block }}
      .actions {{ margin-top:12px; display:flex; gap:8px }}
      a.btn {{ text-decoration:none; padding:12px 16px; border-radius:12px; background:#42c285; color:#0b0b0b; font-weight:600; display:inline-block }}
      a.link {{ color:#bdbdbd }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <img src="{img_url}" alt="Receipt image"/>
        <div class="actions">
          <a class="btn" href="{dl_url}">Download JPG</a>
          <a class="link" href="{img_url}">Open image</a>
        </div>
      </div>
    </div>
  </body>
</html>""",
        200,
        {"Content-Type": "text/html; charset=utf-8"},
    )

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
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
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
# --- date-window helper (local -> UTC) ---
def _local_day_bounds_utc(day: dt.date):
    """
    Return (start_utc_naive, end_utc_naive) covering the local calendar day.
    We return *naive* UTC datetimes because your DB timestamps appear naive-UTC.
    """
    start_local = dt.datetime.combine(day, dt.time(0, 0, 0), tzinfo=LOCAL_TZ)
    end_local   = start_local + dt.timedelta(days=1)
    start_utc   = start_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    end_utc     = end_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return start_utc, end_utc


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
        amount = int(round(float(t.price or 0)))
        prefix = "discount" if t.passenger_type == "discount" else "regular"
        filename = f"{prefix}_{amount}.jpg"
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
        .order_by(StopTime.seq.asc(), StopTime.id.asc())   # stable ordering
        .all()
    )

    def fmt(t):
        tt = _as_time(t)
        return tt.strftime("%H:%M") if tt else ""

    events = []

    if len(stops) == 0:
        # no stop data at all → whole window is in-transit
        events.append({
            "id": 1,
            "type": "trip",
            "label": "In Transit",
            "start_time": fmt(trip.start_time),
            "end_time":   fmt(trip.end_time),
            "description": "",
        })
    else:
        # always include stop windows (even if only one)
        for idx, st in enumerate(stops):
            s = _as_time(st.arrive_time) or _as_time(st.depart_time)
            e = _as_time(st.depart_time) or _as_time(st.arrive_time)
            if s or e:  # skip truly empty rows
                events.append({
                    "id": idx * 2 + 1,
                    "type": "stop",
                    "label": "At Stop",
                    "start_time": fmt(s),
                    "end_time":   fmt(e),
                    "description": st.stop_name,
                })

            # transit segment to next stop (only when both ends exist)
            if idx < len(stops) - 1:
                nxt = stops[idx + 1]
                s2 = _as_time(st.depart_time) or _as_time(st.arrive_time)
                e2 = _as_time(nxt.arrive_time) or _as_time(nxt.depart_time)
                if s2 and e2 and s2 != e2:
                    events.append({
                        "id": idx * 2 + 2,
                        "type": "trip",
                        "label": "In Transit",
                        "start_time": fmt(s2),
                        "end_time":   fmt(e2),
                        "description": f"{st.stop_name} → {nxt.stop_name}",
                    })

    return jsonify(events=events), 200

@commuter_bp.route("/announcements", methods=["GET"])
def announcements():
    """
    GET /commuter/announcements
      Optional:
        bus_id=<int>        # only announcements authored by PAOs assigned to this bus
        date=YYYY-MM-DD     # local calendar day (defaults to *today* in LOCAL_TZ)
        limit=<int>         # cap the number of rows (newest first)
    """
    bus_id   = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")
    limit    = request.args.get("limit", type=int)

    # Base query: author + that author's assigned bus (outer join so unassigned still show)
    q = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)
    )

    # Bus filter, if provided
    if bus_id:
        q = q.filter(User.assigned_bus_id == bus_id)

    # Day filter: local day → [start_utc, end_utc)
    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
    else:
        day = (dt.datetime.now(LOCAL_TZ) if LOCAL_TZ else dt.datetime.now()).date()

    start_utc, end_utc = _local_day_bounds_utc(day)
    q = q.filter(Announcement.timestamp >= start_utc, Announcement.timestamp < end_utc)

    q = q.order_by(Announcement.timestamp.desc())
    if isinstance(limit, int) and limit > 0:
        q = q.limit(limit)

    rows = q.all()
    anns = [
        {
            "id": ann.id,
            "message": ann.message,
            # Treat DB naive timestamps as UTC when serializing
            "timestamp": (ann.timestamp.replace(tzinfo=dt.timezone.utc)).isoformat(),
            "author_name": f"{first} {last}",
            "bus_identifier": bus_identifier or "unassigned",
        }
        for ann, first, last, bus_identifier in rows
    ]
    return jsonify(anns), 200
