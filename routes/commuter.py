# routes/commuter.py
from __future__ import annotations
import datetime as dt
from typing import Any, Dict, List, Optional

from flask import (
    Blueprint, request, jsonify, g, current_app, url_for,
    redirect, send_file, make_response
)
from sqlalchemy import func, text, or_

from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
from sqlalchemy.orm import joinedload
from models.wallet import WalletAccount, WalletLedger, TopUp
from utils.wallet_qr import build_wallet_token
from db import db
from routes.auth import require_role
from models.schedule import Trip, StopTime
from models.sensor_reading import SensorReading
from models.announcement import Announcement
from models.ticket_sale import TicketSale
from models.bus import Bus
from models.user import User
from models.ticket_stop import TicketStop
from models.device_token import DeviceToken
from utils.qr import build_qr_payload
from utils.push import push_to_user
from io import BytesIO
from itsdangerous import URLSafeSerializer
from PIL import Image, ImageDraw, ImageFont
import qrcode
import time as _time
import traceback
from werkzeug.exceptions import HTTPException
from datetime import timedelta
from sqlalchemy.exc import OperationalError
from sqlalchemy import desc
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import os, uuid, time
from werkzeug.utils import secure_filename
from datetime import date, datetime
from models.wallet import TopUp
try:
    from zoneinfo import ZoneInfo
    try:
        LOCAL_TZ = ZoneInfo("Asia/Manila")
    except Exception:
        LOCAL_TZ = dt.timezone(dt.timedelta(hours=8))
except Exception:
    LOCAL_TZ = dt.timezone(dt.timedelta(hours=8))

commuter_bp = Blueprint("commuter", __name__)

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

RECEIPTS_DIR = "topup_receipts"
ALLOWED_EXTS = {"jpg", "jpeg", "png", "webp"}


def _norm_uid(uid: str) -> str:
    u = (uid or "").strip().replace(" ", "").upper()
    # Keep only hex-ish chars (optional hardening)
    u = "".join([c for c in u if c in "0123456789ABCDEF"])
    return u

@commuter_bp.route("/nfc", methods=["GET"])
@require_role("commuter")
def commuter_list_nfc_cards():
    rows = db.session.execute(
        text("""
            SELECT uid, label, status, created_at, updated_at
            FROM nfc_cards
            WHERE user_id = :uid
            ORDER BY id DESC
        """),
        {"uid": int(g.user.id)},
    ).mappings().all()

    return jsonify([{
        "uid": r["uid"],
        "label": r["label"],
        "status": r["status"],
        "created_at": r["created_at"].isoformat(sep=" ") if r["created_at"] else None,
        "updated_at": r["updated_at"].isoformat(sep=" ") if r["updated_at"] else None,
    } for r in rows]), 200


@commuter_bp.route("/nfc/bind", methods=["POST"])
@require_role("commuter")
def commuter_bind_nfc_card():
    data = request.get_json(silent=True) or {}
    uid = _norm_uid(data.get("uid") or "")
    label = (data.get("label") or "").strip() or None

    if not uid:
        return jsonify(error="uid is required"), 400

    try:
        db.session.execute(
            text("""
                INSERT INTO nfc_cards (uid, user_id, label, status)
                VALUES (:uid, :user_id, :label, 'active')
            """),
            {"uid": uid, "user_id": int(g.user.id), "label": label},
        )
        db.session.commit()
        return jsonify(ok=True, uid=uid, user_id=int(g.user.id), already_bound=False), 201

    except IntegrityError:
        db.session.rollback()

        # If UID already exists, check who owns it
        row = db.session.execute(
            text("SELECT user_id FROM nfc_cards WHERE uid=:uid LIMIT 1"),
            {"uid": uid},
        ).scalar()

        if row and int(row) == int(g.user.id):
            # Already bound to same user -> treat as success (optionally update label)
            db.session.execute(
                text("""
                    UPDATE nfc_cards
                    SET label = COALESCE(:label, label),
                        status = 'active'
                    WHERE uid = :uid
                """),
                {"uid": uid, "label": label},
            )
            db.session.commit()
            return jsonify(ok=True, uid=uid, user_id=int(g.user.id), already_bound=True), 200

        return jsonify(error="card_already_bound_to_another_user"), 409


@commuter_bp.route("/nfc/<string:uid>", methods=["DELETE"])
@require_role("commuter")
def commuter_unbind_nfc_card(uid: str):
    uid = _norm_uid(uid)
    if not uid:
        return jsonify(error="invalid uid"), 400

    res = db.session.execute(
        text("DELETE FROM nfc_cards WHERE uid=:uid AND user_id=:user_id"),
        {"uid": uid, "user_id": int(g.user.id)},
    )
    db.session.commit()
    return jsonify(ok=True, deleted=int(res.rowcount or 0)), 200



def _payment_method_for_ticket(t: TicketSale) -> str:
    """
    Decide 'wallet' vs 'gcash' vs 'cash' for a TicketSale row.
    Also logs what was found in the DB columns so we can debug mismatches.
    """
    # Grab raw fields for logging
    raw_pm   = getattr(t, "payment_method", None)
    raw_meth = getattr(t, "method", None)
    raw_paym = getattr(t, "pay_method", None)
    raw_ext  = getattr(t, "external_ref", None)
    raw_gref = getattr(t, "gcash_ref", None)
    raw_pref = getattr(t, "provider_ref", None)
    raw_psp  = getattr(t, "psp_ref", None)

    result = None

    # 1) explicit field on the ticket row
    for attr in ("payment_method", "method", "pay_method"):
        v = getattr(t, attr, None)
        if isinstance(v, str):
            vv = v.strip().lower()
            if vv in {"wallet", "gcash", "cash"}:
                result = vv
                break

    # 2) GCash hints on ticket (only if nothing decided yet)
    if result is None:
        for attr in ("external_ref", "gcash_ref", "provider_ref", "psp_ref"):
            ref = getattr(t, attr, None)
            if isinstance(ref, str) and ref.strip():
                result = "gcash"
                break

    # 3) wallet ledger that references this ticket (only if still undecided)
    if result is None:
        try:
            rid = int(getattr(t, "id", 0) or 0)
            if rid:
                hit = db.session.execute(
                    text("""
                        SELECT 1
                        FROM wallet_ledger
                        WHERE ref_table IN ('ticket_sale','ticket_sales')
                          AND ref_id = :rid
                          AND direction = 'debit'
                          AND event IN ('ticket_purchase','ride')
                        LIMIT 1
                    """),
                    {"rid": rid},
                ).scalar()
                if hit:
                    result = "wallet"
        except Exception:
            pass

    # 4) final fallback: everything else → CASH, not GCash
    if result is None:
        result = "cash"

    # 🔎 LOG
    try:
        current_app.logger.info(
            "[ticket_payment_method][commuter] ticket_id=%s "
            "payment_method=%r method=%r pay_method=%r "
            "external_ref=%r gcash_ref=%r provider_ref=%r psp_ref=%r "
            "-> resolved=%s",
            getattr(t, "id", None),
            raw_pm, raw_meth, raw_paym,
            raw_ext, raw_gref, raw_pref, raw_psp,
            result,
        )
    except Exception:
        pass

    return result

def _fmt_name(u: Optional[User]) -> str:
    if not u:
        return ""
    fn = (getattr(u, "first_name", "") or "").strip()
    ln = (getattr(u, "last_name", "") or "").strip()
    nm = (fn + " " + ln).strip()
    return nm or (getattr(u, "username", None) or "").strip()

def _bus_staff_for(bus_id: Optional[int]) -> dict:
    """
    Return the latest assigned driver/PAO for a single bus.
    {
      "driver": {"id": int, "name": str} | None,
      "pao":    {"id": int, "name": str} | None
    }
    """
    out = {"driver": None, "pao": None}
    if not bus_id:
        return out
    rows = (
        db.session.query(User)
        .filter(User.assigned_bus_id == bus_id, User.role.in_(["driver", "pao"]))
        .order_by(User.id.desc())  # newest assignment wins
        .all()
    )
    for u in rows:
        role = (u.role or "").lower()
        ent = {"id": int(u.id), "name": _fmt_name(u)}
        if role == "driver" and out["driver"] is None:
            out["driver"] = ent
        elif role == "pao" and out["pao"] is None:
            out["pao"] = ent
    return out

def _bus_staff_map(bus_ids: list[int] | set[int]) -> dict[int, dict]:
    """
    Bulk version for lists—we use this inside /tickets/mine to avoid N+1 queries.
    Returns: { bus_id: {"driver": {...}|None, "pao": {...}|None}, ... }
    """
    ids = [int(b) for b in bus_ids if b]
    if not ids:
        return {}
    rows = (
        db.session.query(User)
        .filter(User.assigned_bus_id.in_(ids), User.role.in_(["driver", "pao"]))
        .order_by(User.id.desc())
        .all()
    )
    out = {b: {"driver": None, "pao": None} for b in ids}
    for u in rows:
        b = int(getattr(u, "assigned_bus_id", 0) or 0)
        if b not in out:
            continue
        role = (u.role or "").lower()
        ent = {"id": int(u.id), "name": _fmt_name(u)}
        if role == "driver" and out[b]["driver"] is None:
            out[b]["driver"] = ent
        elif role == "pao" and out[b]["pao"] is None:
            out[b]["pao"] = ent
    return out


# add after other imports
try:
    # optional realtime publish (best-effort)
    from mqtt_ingest import publish as mqtt_publish  # def publish(topic, payload) -> bool
except Exception:
    mqtt_publish = None

def _publish_user_event(uid: int, payload: dict) -> bool:
    """
    Publish a generic event to the commuter's MQTT stream.
    Mobile app should subscribe to: user/{uid}/events
    """
    if not mqtt_publish:
        current_app.logger.warning("[mqtt] disabled: mqtt_ingest.publish not available (commuter/events)")
        return False
    try:
        payload.setdefault("sentAt", int(_time.time() * 1000))
        return bool(mqtt_publish(f"user/{uid}/events", payload))
    except Exception:
        current_app.logger.exception("[mqtt] commuter _publish_user_event failed uid=%s", uid)
        return False

@commuter_bp.route("/me", methods=["GET"])
@require_role("commuter")
def commuter_me():
    u = g.user

    def _val(x):
        return (x or "").strip() if isinstance(x, str) else x

    discount_until = getattr(u, "discount_valid_until", None)

    return jsonify(
        id=int(u.id),
        username=_val(getattr(u, "username", None)),
        phone_number=_val(getattr(u, "phone_number", None)),
        first_name=_val(getattr(u, "first_name", None)),
        last_name=_val(getattr(u, "last_name", None)),
        email=_val(getattr(u, "email", None)),
        role=_val(getattr(u, "role", None)) or "commuter",
        created_at=(
            getattr(u, "created_at", None).isoformat()
            if getattr(u, "created_at", None) else None
        ),
        # 🔹 NEW: category + expiry for dashboard
        passenger_type=_val(getattr(u, "passenger_type", None)),
        passenger_type_expires_at=(
            discount_until.isoformat() if discount_until else None
        ),
        # (optional: also expose raw DB column if you want it elsewhere)
        discount_valid_until=(
            discount_until.isoformat() if discount_until else None
        ),
        
    ), 200


def _unique_ref(prefix: str) -> str:
    return f"{prefix}-{int(time.time()*1000)}-{uuid.uuid4().hex[:8]}"


@commuter_bp.route("/dashboard", methods=["GET"])
@require_role("commuter")
def dashboard():



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

    now_time_local = now_local.time().replace(tzinfo=None)

    def _choose_greeting() -> str:
        hr = now_local.hour
        if hr < 12:
            return "Good morning"
        elif hr < 18:
            return "Good afternoon"
        return "Good evening"

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

    unread_msgs = Announcement.query.count()

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

    def _is_live_window(now_t: dt.time, s: Optional[dt.time], e: Optional[dt.time], *, grace_min: int = 3) -> bool:
        if not s or not e:
            return False
        if s == e:
            base = dt.datetime.combine(today_local, s)
            nowd = dt.datetime.combine(today_local, now_t)
            return abs((nowd - base).total_seconds()) <= grace_min * 60
        return s <= now_t < e

    live_now: List[Dict[str, Any]] = []


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
            events.append({
                "type": "trip",
                "label": "In Transit",
                "start": _as_time(t.start_time),
                "end": _as_time(t.end_time),
                "description": "",
            })
        else:
            for idx, st in enumerate(sts):
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
  
  
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp, 200






def _save_receipt(file_storage, topup_id: int) -> Optional[str]:
    if not file_storage or not getattr(file_storage, "filename", None):
        return None
    fname = secure_filename(file_storage.filename or "")
    ext = (fname.rsplit(".", 1)[-1].lower() if "." in fname else "jpg")
    if ext not in ALLOWED_EXTS:
        ext = "jpg"

    base_dir = os.path.join(current_app.root_path, "static", RECEIPTS_DIR)
    os.makedirs(base_dir, exist_ok=True)

    path = os.path.join(base_dir, f"{topup_id}.{ext}")
    file_storage.save(path)
    return url_for("static", filename=f"{RECEIPTS_DIR}/{topup_id}.{ext}", _external=True)


def _receipt_abs_path(tid: int, ext: str) -> str:
    safe = f"{tid}{ext.lower()}"
    return os.path.join(current_app.root_path, "static", RECEIPTS_DIR, safe)
def _is_ticket_void(t) -> bool:
    """
    Robustly determine if a ticket is voided, regardless of schema.
    True if:
      - a boolean/integer column 'voided' exists and is truthy, OR
      - a 'status' column exists and is one of: void, voided, refunded, cancelled.
    """
    try:
        if bool(getattr(t, "voided", False)):
            return True
    except Exception:
        pass
    try:
        st = (getattr(t, "status", None) or "").strip().lower()
        if st in {"void", "voided", "refunded", "cancelled"}:
            return True
    except Exception:
        pass
    return False


def _display_ticket_ref_for(
    t: Optional[TicketSale] = None,
    *,
    reference_no: Optional[str] = None,
    ticket_id: Optional[int] = None,
    bus_identifier: Optional[str] = None,
    bus_id: Optional[int] = None,
) -> str:
    """
    Prefer BUS-style reference numbers like BUS2-0016.

    Rules:
      - If a ref already starts with "BUS", keep it.
      - Otherwise, render "<bus_identifier>-<ticket_id:04d>".
      - If bus_identifier is missing, use "BUS{bus_id}".
    """
    raw = (reference_no or (getattr(t, "reference_no", None) or "")).strip()
    if raw.upper().startswith("BUS"):
        return raw

    tid = int(ticket_id or getattr(t, "id", 0) or 0)
    # Try the Bus.identifier first; else synthesize BUS{bus_id}
    bid_str = (
        (bus_identifier or (getattr(getattr(t, "bus", None), "identifier", None) or "")).strip()
        or f"BUS{int(bus_id or getattr(t, 'bus_id', 0) or 0)}"
    )
    return f"{bid_str}_{tid:04d}" if tid else (bid_str or raw or "BUS_0000")



def _receipt_url_if_exists(tid: int) -> str | None:
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        p = _receipt_abs_path(tid, ext)
        if os.path.exists(p):
            return url_for("static", filename=f"{RECEIPTS_DIR}/{os.path.basename(p)}", _external=True)
    return None

def _as_local(dt_obj: dt.datetime) -> dt.datetime:
    """Convert naive (assumed UTC) or aware datetime to LOCAL_TZ."""
    if dt_obj is None:
        return dt.datetime.now(LOCAL_TZ)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(LOCAL_TZ)

SALT_WALLET_QR = "wallet-qr-rot-v1"

def _wallet_qr_token(uid: int, bucket: Optional[int] = None) -> str:
    """
    Issue a stateless, *minute-bucketed* signed token.
    The token changes every minute: bucket = floor(now/60).
    """
    if bucket is None:
        bucket = int(_time.time() // 60)
    s = URLSafeSerializer(current_app.config["SECRET_KEY"], salt=SALT_WALLET_QR)
    return s.dumps({"uid": int(uid), "mb": int(bucket)})

@commuter_bp.route("/topup-requests", methods=["GET"])
@require_role("commuter")
def list_my_topup_requests():
    """
    GET /commuter/topup-requests
      Optional query:
        status=pending|approved|rejected|succeeded|cancelled
        page=<int> default 1
        page_size=<int> default 20
    """
    status = (request.args.get("status") or "").strip().lower()
    page = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, min(100, request.args.get("page_size", type=int, default=20)))

    q = TopUp.query.filter(TopUp.account_id == g.user.id)
    if status:
        q = q.filter(TopUp.status == status)

    total = q.count()
    rows = (
        q.order_by(desc(getattr(TopUp, "created_at", TopUp.id)))
         .offset((page - 1) * page_size)
         .limit(page_size)
         .all()
    )

    items = []
    for t in rows:
        items.append({
            "id": int(t.id),
            "account_id": int(t.account_id),
            "amount_pesos": int(getattr(t, "amount_pesos", 0) or 0),
            "method": getattr(t, "method", "cash"),
            "status": getattr(t, "status", "pending"),
            "created_at": (
                t.created_at.isoformat() if getattr(t, "created_at", None) else None
            ),
            # Optional fields
            "note": getattr(t, "note", None) or getattr(t, "external_ref", None) or getattr(t, "provider_ref", None),
            "receipt_url": getattr(t, "receipt_url", None),
            "reject_reason": getattr(t, "reject_reason", None),  # 👈 NEW
        })


    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200

def _load_font(size: int, *candidates: str) -> ImageFont.FreeTypeFont:
    """
    Try to load a TTF from app's static/fonts first, then common system dirs.
    Falls back to load_default() only if everything fails.
    """
    bases = [
        os.path.join(current_app.root_path, "static", "fonts"),
        "/usr/share/fonts/truetype/dejavu",
        "/usr/share/fonts/truetype/noto",
        "/usr/share/fonts/truetype/liberation",
    ]
    for name in candidates:
        for base in bases:
            path = name if os.path.isabs(name) else os.path.join(base, name)
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    return ImageFont.load_default()


def _has_column(table: str, column: str) -> bool:
    try:
        row = db.session.execute(
            text("""
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c
                LIMIT 1
            """),
            {"t": table, "c": column},
        ).first()
        if row:
            return True
    except Exception:
        pass
    try:
        row = db.session.execute(text(f"SHOW COLUMNS FROM {table} LIKE :c"), {"c": column}).first()
        return bool(row)
    except Exception:
        return False

def _wallet_cols() -> Dict[str, str]:
    """
    Returns the actual column names present in DB:
      - balance: 'balance_pesos' or 'balance_cents'
      - amount:  'amount_pesos'  or 'amount_cents'
      - running: 'running_balance_pesos' or 'running_balance_cents'
      - topup_amount: 'amount_pesos' or 'amount_cents'
    """
    bal = "balance_pesos" 
    amt = "amount_pesos" 
    run = "running_balance_pesos"
    tup = "amount_pesos"
    return {"balance": bal, "amount": amt, "running": run, "topup_amount": tup}

def _to_pesos_from_db(value: Optional[int], came_from: str) -> int:
    """
    Convert DB value to whole pesos. If column is *_cents, convert 100c == 1 peso.
    If it is *_pesos already, just coerce to int.
    """
    if value is None:
        return 0
    cols = _wallet_cols()
    if came_from.endswith("_cents"):
        return int(round(int(value) / 100.0))
    return int(value)

# ──────────────────────────────────────────────────────────────────────────────

SALT_USER_QR = "user-qr-v1"

def _user_qr_sign(uid: int) -> str:
    s = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)
    return s.dumps({"uid": int(uid)})



@commuter_bp.route("/users/me/qr.png", methods=["GET"])
@require_role("commuter")
def commuter_my_wallet_qr_png():
    """
    Returns a PNG QR for the logged-in commuter.
    The QR encodes a URL to /teller/users/scan?token=...
    """
    size = max(240, min(1024, int(request.args.get("size", 360) or 360)))

    # signed short-lived token that represents this user id
    token = _user_qr_sign(g.user.id)

    # 🔁 moved from PAO → Teller
    scan_url = url_for("teller.user_qr_scan", _external=True) + f"?token={token}"

    qr = qrcode.QRCode(box_size=10, border=2)
    qr.add_data(scan_url)
    qr.make(fit=True)
    out = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    out = out.resize((size, size))

    bio = BytesIO()
    out.save(bio, format="PNG", optimize=True)
    bio.seek(0)

    resp = make_response(send_file(bio, mimetype="image/png"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp
def _day_range_filter(q, date_str: Optional[str], days: Optional[str]):
    """Apply date/day window filters (created_at) in LOCAL_TZ semantics."""
    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("date must be YYYY-MM-DD")
        start_utc, end_utc = _local_day_bounds_utc(day)
        q = q.filter(TicketSale.created_at >= start_utc,
                     TicketSale.created_at <  end_utc)
    elif days in {"7", "30"}:
        cutoff = dt.datetime.utcnow() - timedelta(days=int(days))
        q = q.filter(TicketSale.created_at >= cutoff)
    return q


def _payment_method_for_ticket_row(t) -> str:
    """
    Decide 'wallet' vs 'gcash' robustly for a TicketSale row.

    Priority:
      1) explicit columns if present (payment_method/method/pay_method)
      2) GCash-style references stored on the ticket (external_ref / gcash_ref / provider_ref / psp_ref)
      3) wallet ledger evidence referencing this ticket (debit)
      4) safer default: 'gcash' (avoids mislabeling commuter-linked GCash as Wallet)
    """
    # 1) explicit field on the ticket row
    for attr in ("payment_method", "method", "pay_method"):
        v = getattr(t, attr, None)
        if isinstance(v, str):
            vv = v.strip().lower()
            # 🔧 include "cash"
            if vv in {"wallet", "gcash", "cash"}:
                return vv
            
    # 2) GCash hints on ticket
    for attr in ("external_ref", "gcash_ref", "provider_ref", "psp_ref"):
        ref = getattr(t, attr, None)
        if isinstance(ref, str) and ref.strip():
            return "gcash"

    # 3) wallet ledger that references this ticket
    try:
        rid = int(getattr(t, "id", 0) or 0)
        if rid:
            hit = db.session.execute(
                text("""
                    SELECT 1
                    FROM wallet_ledger
                    WHERE ref_table IN ('ticket_sale','ticket_sales')
                      AND ref_id = :rid
                      AND direction = 'debit'
                      AND event IN ('ticket_purchase','ride')
                    LIMIT 1
                """),
                {"rid": rid},
            ).scalar()
            if hit:
                return "wallet"
    except Exception:
        pass

    # 4) default
    return "gcash"

def _resolve_staff(bus_id: int | None, issued_by_user_id: int | None):
    pao_u = None
    if issued_by_user_id:
        u = User.query.get(issued_by_user_id)
        if u and (u.role or "").lower() == "pao":
            pao_u = u

    if (not pao_u) and bus_id:
        pao_u = (
            db.session.query(User)
            .filter(User.assigned_bus_id == bus_id, User.role == "pao")
            .order_by(User.id.desc())
            .first()
        )

    driver_u = None
    if bus_id:
        driver_u = (
            db.session.query(User)
            .filter(User.assigned_bus_id == bus_id, User.role.in_(["driver", "Driver"]))
            .order_by(User.id.desc())
            .first()
        )

    def _name(u: User | None) -> str | None:
        if not u:
            return None
        fn = (u.first_name or "").strip()
        ln = (u.last_name or "").strip()
        nm = f"{fn} {ln}".strip()
        return nm or (u.username or f"User #{u.id}")

    return (
        (pao_u.id if pao_u else None),
        _name(pao_u),
        (driver_u.id if driver_u else None),
        _name(driver_u),
    )

@commuter_bp.route("/tickets/<int:ticket_id>/image.jpg", methods=["GET"])
def commuter_ticket_image(ticket_id: int):
    """
    High-contrast, LARGE-TYPE JPG receipt renderer including PAO & Driver names.
    - Shows "Guest" instead of "None None" when commuter name is absent.
    - Adds a batch/group breakdown panel when the ticket represents a group.
    - Falls back to driver/PAO daily assignments if bus staff aren't resolved
      via assigned_bus_id / issued_by.
    - QR code and "Scan to view" panel removed.
    """
    t = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.bus),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Resolve stop names (with TicketStop fallback)
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

    # Staff (PAO/Driver) — try standard resolver first
    issued_by = getattr(t, "issued_by", None)
    pao_id, pao_name, driver_id, driver_name = _resolve_staff(t.bus_id, issued_by)

    # If still missing, fall back to assignment tables for the ticket's local day
    try:
        ldt = _as_local(getattr(t, "created_at", None))
        svc_day = ldt.date()
        if not driver_name and t.bus_id:
            row = db.session.execute(
                text("""
                    SELECT a.user_id, u.first_name, u.last_name
                    FROM driver_assignments a
                    LEFT JOIN users u ON u.id = a.user_id
                    WHERE a.bus_id = :bid AND a.service_date = :d
                    ORDER BY a.id DESC
                    LIMIT 1
                """),
                {"bid": t.bus_id, "d": svc_day},
            ).mappings().first()
            if row:
                driver_id = int(row["user_id"] or driver_id or 0) or None
                driver_name = (f"{(row.get('first_name') or '').strip()} {(row.get('last_name') or '').strip()}".strip()
                               or driver_name)
        if not pao_name and t.bus_id:
            row = db.session.execute(
                text("""
                    SELECT a.user_id, u.first_name, u.last_name
                    FROM pao_assignments a
                    LEFT JOIN users u ON u.id = a.user_id
                    WHERE a.bus_id = :bid AND a.service_date = :d
                    ORDER BY a.id DESC
                    LIMIT 1
                """),
                {"bid": t.bus_id, "d": svc_day},
            ).mappings().first()
            if row:
                pao_id = int(row["user_id"] or pao_id or 0) or None
                pao_name = (f"{(row.get('first_name') or '').strip()} {(row.get('last_name') or '').strip()}".strip()
                            or pao_name)
    except Exception:
        # soft-fail — keep whatever we already have
        pass

    # Group-aware meta (single-row group tickets supported)
    is_group   = bool(getattr(t, "is_group", False))
    g_reg      = int(getattr(t, "group_regular", 0) or 0)
    g_dis      = int(getattr(t, "group_discount", 0) or 0)
    group_qty  = g_reg + g_dis if is_group else 1
    total_peso = int(round(float(getattr(t, "price", 0) or 0)))

    display_ref = _display_ticket_ref_for(t=t)

    # (We still compute the image URL for the footer, but we no longer render a QR.)
    img_link = url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True)

    # ─── Drawing setup ────────────────────────────────────────────────────────
    W, H = 1440, 2100
    M = 80
    WHITE = (255, 255, 255)
    BORDER = (222, 226, 230)
    TEXT = (33, 37, 41)
    MUTED = (108, 117, 125)
    ACCENT = (34, 139, 58)
    LIGHT = (236, 248, 239)

    img = Image.new("RGB", (W, H), (250, 251, 252))
    draw = ImageDraw.Draw(img)

    ft_title  = _load_font(80, "Inter-ExtraBold.ttf", "NotoSans-Bold.ttf")
    ft_header = _load_font(60, "Inter-Bold.ttf", "NotoSans-Bold.ttf")
    ft_label  = _load_font(40, "Inter-Regular.ttf", "NotoSans.ttf")
    ft_value  = _load_font(52, "Inter-SemiBold.ttf", "NotoSans-Bold.ttf")
    ft_big    = _load_font(96, "Inter-Black.ttf", "NotoSans-Bold.ttf")
    ft_small  = _load_font(32, "Inter-Regular.ttf", "NotoSans.ttf")

    # Card
    draw.rectangle((M, M, W - M, H - M), fill=WHITE, outline=BORDER, width=2)

    y = M + 40
    # Header band
    draw.rectangle((M, y, W - M, y + 160), fill=LIGHT, outline=BORDER, width=2)
    draw.text((M + 48, y + 40), "PGT Onboard — Official Receipt", fill=ACCENT, font=ft_title)
    y += 160 + 34
    draw.rectangle((M + 48, y, W - M - 48, y + 5), fill=ACCENT)
    y += 40

    # Columns
    L = M + 48
    R = W - M - 48
    GAP = 60
    COLW = (R - L - GAP) // 2

    def label_value(x, y_, label, value):
        draw.text((x, y_), (label or "").upper(), fill=MUTED, font=ft_label)
        yy = y_ + 52
        draw.text((x, yy), value or "—", fill=TEXT, font=ft_value)
        return yy + 60

    # Names/times (ensure "Guest" when name not present)
    ldt = _as_local(getattr(t, "created_at", None))
    date_str = ldt.strftime("%B %d, %Y")
    time_str = ldt.strftime("%I:%M %p").lstrip("0").lower()
    fn = (getattr(t.user, "first_name", "") or "").strip() if t.user else ""
    ln = (getattr(t.user, "last_name", "") or "").strip() if t.user else ""
    passenger = (f"{fn} {ln}".strip()) or "Guest"

    yl = label_value(L, y, "Reference No.", display_ref)
    yr = label_value(L + COLW + GAP, y, "Destination", destination_name or "—")
    draw.text((L, yl), "DATE & TIME".upper(), fill=MUTED, font=ft_label)
    draw.text((L, yl + 52), date_str, fill=TEXT, font=ft_value)
    draw.text((L, yl + 104), time_str, fill=TEXT, font=ft_value)
    yl = yl + 104 + 60
    yr = label_value(L + COLW + GAP, yr, "Passenger", passenger)
    yl = label_value(L, yl, "Origin", origin_name or "—")

    y = max(yl, yr) + 24
    draw.rectangle((L, y, R, y + 4), fill=BORDER)
    y += 30

    is_void = _is_ticket_void(t)
    method  = _payment_method_for_ticket(t)

    # 🔎 log what the image renderer sees
    try:
        current_app.logger.info(
            "[ticket_image] ticket_id=%s db_payment_method=%r external_ref=%r gcash_ref=%r "
            "provider_ref=%r psp_ref=%r resolved_method=%s paid=%s",
            t.id,
            getattr(t, "payment_method", None),
            getattr(t, "external_ref", None),
            getattr(t, "gcash_ref", None),
            getattr(t, "provider_ref", None),
            getattr(t, "psp_ref", None),
            method,
            bool(getattr(t, "paid", False)),
        )
    except Exception:
        pass

    if method == "wallet":
        method_display = "Wallet"
    elif method == "cash":
        method_display = "Cash"
    else:
        method_display = "GCash"


    draw.text((L, y), "TOTAL AMOUNT", fill=MUTED, font=ft_label)
    draw.text((L, y + 44), f"₱{total_peso}", fill=ACCENT, font=ft_big)

    # Pill
    state_txt = ("VOIDED" if is_void else f"PAID VIA {method_display.upper()}" if getattr(t, "paid", False) else "UNPAID")
    tw = draw.textlength(state_txt, font=ft_header)
    pill_w, pill_h = int(tw + 64), 76
    px1, py1 = R - pill_w, y + 6
    draw.rectangle(
        (px1, py1, px1 + pill_w, py1 + pill_h),
        fill=(212, 237, 218) if "PAID" in state_txt else ((248, 215, 218) if is_void else (255, 243, 205)),
    )
    draw.text(
        (px1 + (pill_w - tw) / 2, py1 + 10),
        state_txt,
        fill=(21, 87, 36) if "PAID" in state_txt else ((114, 28, 36) if is_void else (133, 100, 4)),
        font=ft_header,
    )

    y += 44 + (getattr(ft_big, "size", 96)) + 24

    # ─── Group breakdown (only when group ticket) ─────────────────────────────
    if is_group and group_qty > 1:
        # Try to compute per-type fares from stop sequence (optional; safe fallback to "—")
        def _fare_each_from_seq(o_seq, d_seq, pt):
            try:
                if o_seq is None or d_seq is None:
                    return None
                hops = abs(int(o_seq) - int(d_seq))
                base = 10 + max(hops - 1, 0) * 2
                return int(round(base * 0.8)) if (pt == "discount") else int(base)
            except Exception:
                return None

        # Resolve sequences, if available (StopTime.seq or TicketStop.seq)
        try:
            o_seq = getattr(getattr(t, "origin_stop_time", None), "seq", None)
            d_seq = getattr(getattr(t, "destination_stop_time", None), "seq", None)
            if o_seq is None or d_seq is None:
                o_ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
                d_ts = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
                o_seq = getattr(o_ts, "seq", o_seq)
                d_seq = getattr(d_ts, "seq", d_seq)
        except Exception:
            o_seq = d_seq = None

        reg_each = _fare_each_from_seq(o_seq, d_seq, "regular")
        dis_each = _fare_each_from_seq(o_seq, d_seq, "discount")

        rows = []
        if g_reg:
            rows.append(("Regular", g_reg, reg_each, (reg_each * g_reg) if reg_each is not None else None))
        if g_dis:
            rows.append(("Discount", g_dis, dis_each, (dis_each * g_dis) if dis_each is not None else None))

        # Panel sizing
        pad = 28
        line_h = 60
        head_h = 68
        extra_h = 2 * line_h  # Passengers + Fare total
        panel_h = pad + head_h + (len(rows) * line_h) + extra_h + pad

        # Panel background
        draw.rectangle((L, y, R, y + panel_h), fill=(245, 249, 246), outline=BORDER, width=2)
        draw.text((L + pad, y + pad), "Group breakdown", fill=ACCENT, font=ft_header)

        # Column x-positions
        col_label_x = L + pad
        col_qty_x   = col_label_x + int((R - L) * 0.52)
        col_each_x  = col_label_x + int((R - L) * 0.68)
        col_sub_x   = R - pad

        yy = y + pad + head_h

        def _right_text(x_right, y_top, text, font, fill=TEXT):
            tw_ = draw.textlength(text, font=font)
            draw.text((x_right - tw_, y_top), text, font=font, fill=fill)

        # Header underline
        draw.rectangle((L + pad, yy - 12, R - pad, yy - 10), fill=BORDER)

        # Row lines (type, qty, each, subtotal)
        for label, qty, each_val, sub_val in rows:
            draw.text((col_label_x, yy), label, fill=TEXT, font=ft_value)
            _right_text(col_qty_x, yy, f"× {qty}", font=ft_value, fill=MUTED)
            each_txt = (f"₱{each_val}" if each_val is not None else "—")
            _right_text(col_each_x, yy, each_txt, font=ft_value, fill=MUTED)
            sub_txt = (f"₱{sub_val}" if sub_val is not None else "—")
            _right_text(col_sub_x, yy, sub_txt, font=ft_value, fill=TEXT)
            yy += line_h

        # Divider
        draw.rectangle((L + pad, yy - 8, R - pad, yy - 6), fill=BORDER)

        # Passengers line
        draw.text((col_label_x, yy + 6), "Passengers", fill=MUTED, font=ft_label)
        # total passengers right-aligned at subtotal column
        tot_txt = f"{group_qty}"
        tw_tot = draw.textlength(tot_txt, font=ft_value)
        draw.text((col_sub_x - tw_tot, yy + 6), tot_txt, font=ft_value, fill=TEXT)
        yy += line_h

        y += panel_h + 24  # move y down before the staff section

    # ─── Staff / meta section (no QR) ─────────────────────────────────────────
    # We render a clean panel listing Payment Method, PAO, Driver, Bus ID.
    panel_pad = 36
    line_h = 88
    rows = [
        ("Payment Method", method_display),
        ("PAO", (f"{pao_name} (ID {pao_id})" if pao_id and pao_name else (pao_name or "—"))),
        ("Driver", (f"{driver_name} (ID {driver_id})" if driver_id and driver_name else (driver_name or "—"))),
        ("Bus ID", str(getattr(t, "bus_id", "") or "—")),
    ]
    panel_h = panel_pad * 2 + len(rows) * line_h

    draw.rectangle((L, y, R, y + panel_h), fill=(247, 251, 247), outline=BORDER, width=2)
    yy = y + panel_pad
    for label, value in rows:
        draw.text((L + panel_pad, yy), label.upper(), fill=MUTED, font=ft_label)
        draw.text((L + panel_pad, yy + 40), value or "—", fill=TEXT, font=ft_value)
        yy += line_h

    y = y + panel_h + 36

    # Footer (kept; remove if you also don't want any reference to the URL)
    draw.text((L, y + 24), img_link if len(img_link) <= 90 else (img_link[:90] + "…"), fill=MUTED, font=ft_small)
    now_local = dt.datetime.now(LOCAL_TZ)
    draw.text((L, y + 60), now_local.strftime("Generated on %B %d, %Y at %I:%M %p"), fill=MUTED, font=ft_small)

    bio = BytesIO()
    img.save(bio, format="JPEG", quality=95, optimize=True)
    bio.seek(0)

    resp = make_response(send_file(bio, mimetype="image/jpeg"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp



def _is_unknown_col(err: Exception) -> bool:
    # MySQL error code for "Unknown column": 1054
    try:
        return isinstance(err, OperationalError) and getattr(err.orig, "args", [None])[0] == 1054
    except Exception:
        return False


@commuter_bp.route("/wallet/qr-token", methods=["GET"])
@require_role("commuter")
def wallet_qr_token_alias():
    # Same payload as /wallet/qrcode
    return wallet_qrcode()

@commuter_bp.route("/wallet/qr-token/rotate", methods=["POST"])
@require_role("commuter")
def wallet_qr_token_rotate_alias():
    # Same behavior as /wallet/qrcode/rotate
    return wallet_qrcode_rotate()


def _get_or_create_wallet_account(user_id: int):
    # PESOS-ONLY: never reference *cents columns
    row = db.session.execute(
        text("""
            SELECT user_id, balance_pesos AS bal, qr_token
            FROM wallet_accounts
            WHERE user_id = :uid
        """),
        {"uid": user_id},
    ).mappings().first()

    if not row:
        # create if missing (idempotent)
        db.session.execute(
            text("""
                INSERT INTO wallet_accounts (user_id, balance_pesos)
                VALUES (:uid, 0)
                ON DUPLICATE KEY UPDATE user_id = user_id
            """),
            {"uid": user_id},
        )
        db.session.commit()
        row = {"user_id": user_id, "bal": 0, "qr_token": None}

    class _Acct: ...
    acct = _Acct()
    acct.user_id = int(row["user_id"])
    acct.balance_pesos = int(row["bal"] or 0)
    acct.qr_token = row.get("qr_token")
    return acct


@commuter_bp.route("/wallet/me", methods=["GET"])
@require_role("commuter")
def wallet_me():
    acct = _get_or_create_wallet_account(g.user.id)
    bal = int(getattr(acct, "balance_pesos", 0) or 0)
    return jsonify(balance_pesos=bal, balance_php=bal), 200

@commuter_bp.route("/wallet/ledger", methods=["GET"])
@require_role("commuter")
def wallet_ledger():
    page = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, request.args.get("page_size", type=int, default=5))
    offset = (page - 1) * page_size
    aid = g.user.id

    total = int(
        db.session.execute(
            text("SELECT COUNT(*) FROM wallet_ledger WHERE account_id = :aid"),
            {"aid": aid},
        ).scalar() or 0
    )

    rows = db.session.execute(
        text("""
            SELECT
                id,
                account_id,
                direction,
                event,
                amount_pesos           AS amount_val,
                running_balance_pesos  AS running_val,
                ref_table,
                ref_id,
                created_at
            FROM wallet_ledger
            WHERE account_id = :aid
            ORDER BY id DESC
            LIMIT :lim OFFSET :off
        """),
        {"aid": aid, "lim": page_size, "off": offset},
    ).mappings().all()

    # ——— Gather TopUp meta for the rows that reference wallet_topups
    topup_ref_ids = [
        int(r["ref_id"]) for r in rows
        if r.get("ref_table") == "wallet_topups" and r.get("ref_id")
           and (r.get("event") or "").startswith("topup")
    ]
    topup_meta: dict[int, dict] = {}
    if topup_ref_ids:
        for tup in (
            db.session.query(TopUp)
            .filter(TopUp.id.in_(topup_ref_ids))
            .all()
        ):
            topup_meta[int(tup.id)] = {
                "method": getattr(tup, "method", "cash") or "cash",
                "status": getattr(tup, "status", "pending"),
                "reject_reason": getattr(tup, "reject_reason", None),
                "receipt_url": getattr(tup, "receipt_url", None),
            }

    items = []
    for r in rows:
        ref_table = r.get("ref_table")
        ref_id = int(r["ref_id"]) if r.get("ref_id") is not None else None

        base = {
            "id": int(r["id"]),
            "direction": r["direction"],
            "event": r["event"],
            "amount_pesos": int(r["amount_val"] or 0),
            "running_balance_pesos": int(r["running_val"] or 0),
            "created_at": (r["created_at"].isoformat() if r.get("created_at") else None),
            # 👇 new, for deep-linking on the client
            "ref_table": ref_table,
            "ref_id": ref_id,
        }

        # Add friendly per-type extras
        if ref_table == "wallet_topups" and ref_id:
            m = topup_meta.get(ref_id, {})
            base.update({
                "method": m.get("method") or "cash",
                "topup_status": m.get("status"),
                "topup_reject_reason": m.get("reject_reason"),
                "topup_receipt_url": m.get("receipt_url"),
            })
        elif ref_table == "ticket_sales" and ref_id:
            base.update({
                "ticket_view_url": url_for("commuter.commuter_ticket_view", ticket_id=ref_id, _external=True),
                "ticket_receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=ref_id, _external=True),
            })

        items.append(base)

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200


@commuter_bp.route("/wallet/qrcode", methods=["GET"])
@require_role("commuter")
def wallet_qrcode():
    now = int(_time.time())
    bucket = now // 60
    token = _wallet_qr_token(g.user.id, bucket=bucket)
    # expires at next minute boundary
    expires_at_ts = (bucket + 1) * 60
    deep_link = f"https://pay.example/charge?wallet_token={token}&autopay=1"
    resp = jsonify(
        wallet_token=token,
        deep_link=deep_link,
        expires_at=dt.datetime.fromtimestamp(expires_at_ts, tz=dt.timezone.utc).isoformat(),
        valid_for_sec=max(1, expires_at_ts - now),
    )
    # don't cache: keeps clients honest about expiry/rotation
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp, 200


@commuter_bp.route("/wallet/qrcode/rotate", methods=["POST"])
@require_role("commuter")
def wallet_qrcode_rotate():
    from secrets import token_urlsafe
    new_tok = token_urlsafe(24)

    db.session.execute(
        text("""
            INSERT INTO wallet_accounts (user_id, balance_pesos, qr_token)
            VALUES (:uid, 0, :tok)
            ON DUPLICATE KEY UPDATE qr_token = :tok
        """),
        {"uid": g.user.id, "tok": new_tok},
    )
    db.session.commit()

    deep_link = f"https://pay.example/charge?wallet_token={new_tok}&autopay=1"
    return jsonify(wallet_token=new_tok, deep_link=deep_link), 200


@commuter_bp.route("/tickets/<int:ticket_id>/receipt-qr.png", methods=["GET"])
def commuter_ticket_receipt_qr(ticket_id: int):
    img_link = url_for("commuter.commuter_ticket_image", ticket_id=ticket_id, _external=True)
    qr = qrcode.QRCode(box_size=10, border=2)
    qr.add_data(img_link)
    qr.make(fit=True)
    out = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    bio = BytesIO()
    out.save(bio, format="PNG", optimize=True)
    bio.seek(0)

    resp = make_response(send_file(bio, mimetype="image/png"))
    resp.headers["Cache-Control"] = "public, max-age=86400"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp


def _as_time(v: Any) -> Optional[dt.time]:
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

@commuter_bp.route("/qr/ticket/<int:ticket_id>.jpg", methods=["GET"])
def qr_image_for_ticket(ticket_id: int):
    t = TicketSale.query.get_or_404(ticket_id)
    amount = int(round(float(t.price or 0)))
    prefix = "discount" if t.passenger_type == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"
    return redirect(url_for("static", filename=f"qr/{filename}", _external=True), code=302)


@commuter_bp.route("/tickets/<int:ticket_id>/view", methods=["GET"])
def commuter_ticket_view(ticket_id: int):
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
    t = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.bus),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Stop names (with TicketStop fallback)
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

    # Staff
    issued_by = getattr(t, "issued_by", None)
    pao_id, pao_name, driver_id, driver_name = _resolve_staff(t.bus_id, issued_by)

    # Ticket meta
    is_group   = bool(getattr(t, "is_group", False))
    g_reg      = int(getattr(t, "group_regular", 0) or 0)
    g_dis      = int(getattr(t, "group_discount", 0) or 0)
    group_qty  = g_reg + g_dis if is_group else 1
    total_peso = int(round(float(t.price or 0)))
    is_void    = _is_ticket_void(t)

    ldt = _as_local(t.created_at)
    date_str = ldt.strftime("%B %d, %Y")
    time_str = ldt.strftime("%I:%M %p").lstrip("0").lower()

    amount = total_peso
    prefix = "discount" if (t.passenger_type or "").lower() == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"
    qr_url  = url_for("static", filename=f"qr/{filename}", _external=True)
    qr_link = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)

    payload = {
        "id": t.id,
        "referenceNo": _display_ticket_ref_for(t=t),
        "date": date_str,
        "time": time_str,
        "created_at": ldt.isoformat(),
        "origin": origin_name,
        "destination": destination_name,
        **({"passengerType": (t.passenger_type or "").title()} if group_qty <= 1 else {}),
        "commuter": f"{t.user.first_name} {t.user.last_name}" if t.user else "Guest",
        "fare": total_peso,
        "paid": bool(t.paid) and not is_void,
        "voided": bool(is_void),
        "state": ("voided" if is_void else ("paid" if bool(t.paid) else "unpaid")),
        "void_reason": getattr(t, "void_reason", None),
        "qr_link": qr_link,
        "qr_url": qr_url,
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
        # Staff
        "paoId": pao_id,
        "pao_name": pao_name,
        "driverId": driver_id,
        "driver_name": driver_name,
    }

    # Group summary if applicable
    if is_group:
        payload["is_group"] = True
        payload["group"] = {"regular": g_reg, "discount": g_dis, "total": g_reg + g_dis}

    return jsonify(payload), 200



@commuter_bp.route("/trips", methods=["GET"])
def list_all_trips():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify(error="A 'date' parameter is required."), 400
    try:
        svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="Invalid date format. Use YYYY-MM-DD."), 400

    # 🚧 If stop_times table (or its columns) doesn't exist, fall back gracefully
    if not _has_column("stop_times", "trip_id"):
        current_app.logger.error(
            "stop_times table missing; falling back to trips-only list for %s",
            svc_date,
        )
        trips_only = (
            db.session.query(Trip, Bus.identifier)
            .join(Bus, Trip.bus_id == Bus.id)
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
                "origin": "N/A",
                "destination": "N/A",
            }
            for trip, identifier in trips_only
        ]
        return jsonify(result), 200

    # ✅ Normal path (when stop_times exists): derive origin/destination via subqueries
    first_stop_sq = (
        db.session.query(StopTime.trip_id, func.min(StopTime.seq).label("min_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    first_stop_name_sq = (
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
    last_stop_name_sq = (
        db.session.query(StopTime.trip_id, StopTime.stop_name.label("destination"))
        .join(
            last_stop_sq,
            (StopTime.trip_id == last_stop_sq.c.trip_id)
            & (StopTime.seq == last_stop_sq.c.max_seq),
        )
        .subquery()
    )

    trips = (
        db.session.query(Trip, Bus.identifier, first_stop_name_sq.c.origin, last_stop_name_sq.c.destination)
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
    return jsonify([
        {
            "id": t.id,
            "number": t.number,
            "start_time": _as_time(t.start_time).strftime("%H:%M") if _as_time(t.start_time) else "",
            "end_time": _as_time(t.end_time).strftime("%H:%M") if _as_time(t.end_time) else "",
        }
        for t in trips
    ]), 200

from sqlalchemy.exc import ProgrammingError, OperationalError

@commuter_bp.route("/stop-times", methods=["GET"])
@require_role("commuter")
def commuter_stop_times():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    try:
        sts = (StopTime.query
               .filter_by(trip_id=trip_id)
               .order_by(StopTime.seq.asc())
               .all())
    except (ProgrammingError, OperationalError) as e:
        # MySQL error code 1146 = table doesn't exist
        code = None
        try:
            code = getattr(getattr(e, "orig", None), "args", [None])[0]
        except Exception:
            pass
        if code == 1146:
            current_app.logger.error("stop_times table missing; returning empty list for trip_id=%s", trip_id)
            return jsonify([]), 200
        raise  # other DB errors: re-raise

    return jsonify([
        {
            "stop_name": st.stop_name,
            "arrive_time": (_as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else ""),
            "depart_time": (_as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else ""),
        }
        for st in sts
    ]), 200


@commuter_bp.route("/location", methods=["GET"])
@require_role("commuter")
def vehicle_location():
    sr = SensorReading.query.order_by(SensorReading.timestamp.desc()).first()
    if not sr:
        return jsonify(error="no sensor data"), 404
    return jsonify(
        lat=sr.lat,
        lng=sr.lng,
        occupied=sr.occupied,
        timestamp=sr.timestamp.isoformat(),
    ), 200

def _local_day_bounds_utc(day: dt.date):
    start_local = dt.datetime.combine(day, dt.time(0, 0, 0), tzinfo=LOCAL_TZ)
    end_local   = start_local + dt.timedelta(days=1)
    start_utc   = start_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    end_utc     = end_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return start_utc, end_utc


@commuter_bp.route("/tickets/mine", methods=["GET"])
@require_role("commuter")
def my_receipts():
    """
    One row per ticket (PAID or VOIDED). Adds PAO/Driver names on each item.
    Query: page, page_size, date=YYYY-MM-DD, days=7|30, bus_id, light=1
    """
    from datetime import timedelta
    page      = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, request.args.get("page_size", type=int, default=5))
    date_str  = request.args.get("date")
    days      = request.args.get("days")
    bus_id    = request.args.get("bus_id", type=int)
    light     = (request.args.get("light") or "").lower() in {"1","true","yes"}

    # Base filter (include PAID or VOIDED)
    void_statuses = {"void", "voided", "refunded", "cancelled", "canceled"}
    conds = [TicketSale.user_id == g.user.id]
    # try to be schema-agnostic: paid OR voided by column/status
    if hasattr(TicketSale, "voided"):
        conds.append(db.or_(TicketSale.paid.is_(True), TicketSale.voided.is_(True)))
    elif hasattr(TicketSale, "status"):
        conds.append(db.or_(TicketSale.paid.is_(True), TicketSale.status.in_(list(void_statuses))))
    else:
        conds.append(TicketSale.paid.is_(True))

    q = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(*conds)
    )

    # Date/day window (LOCAL_TZ semantics)
    def _local_day_bounds_utc(day: dt.date):
        start_local = dt.datetime.combine(day, dt.time(0, 0, 0), tzinfo=LOCAL_TZ)
        end_local   = start_local + dt.timedelta(days=1)
        start_utc   = start_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
        end_utc     = end_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return start_utc, end_utc

    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        s, e = _local_day_bounds_utc(day)
        q = q.filter(TicketSale.created_at >= s, TicketSale.created_at < e)
    elif days in {"7", "30"}:
        cutoff = dt.datetime.utcnow() - timedelta(days=int(days))
        q = q.filter(TicketSale.created_at >= cutoff)

    if bus_id:
        q = q.filter(TicketSale.bus_id == bus_id)

    total = q.count()
    rows = (
        q.order_by(TicketSale.id.desc())
         .offset((page - 1) * page_size)
         .limit(page_size)
         .all()
    )

    items = []
    for t in rows:
        # Stops (fallback to TicketStop)
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

        is_void = _is_ticket_void(t)
        fare = int(round(float(t.price or 0)))

        # Staff (per-bus)
        pao_id, pao_name, driver_id, driver_name = _resolve_staff(t.bus_id, getattr(t, "issued_by", None))

        _ldt = _as_local(t.created_at)
        base = {
            "id": t.id,
            "referenceNo": _display_ticket_ref_for(t=t),
            "date": _ldt.strftime("%B %d, %Y"),
            "time": _ldt.strftime("%I:%M %p").lstrip("0").lower(),
            "created_at": _ldt.isoformat(),
            "origin": origin_name,
            "destination": destination_name,
            "fare": fare,
            "paid": bool(t.paid) and not is_void,
            "voided": bool(is_void),
            "state": "voided" if is_void else ("paid" if bool(t.paid) else "unpaid"),
            "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
            "view_url": url_for("commuter.commuter_ticket_view", ticket_id=t.id, _external=True),
            # Staff
            "paoId": pao_id,
            "pao_name": pao_name,
            "driverId": driver_id,
            "driver_name": driver_name,
        }
        if not light:
            base.update({
                "passengerType": (t.passenger_type or "").title(),
                "commuter": f"{t.user.first_name} {t.user.last_name}" if t.user else "Guest",
                "bus_id": t.bus_id,
                "batch_id": int(getattr(t, "batch_id", None) or t.id),
                "status": getattr(t, "status", None),
            })
        items.append(base)

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200

# Alias: /commuter/my/receipts  → same payload as /commuter/tickets/mine
@commuter_bp.route("/my/receipts", methods=["GET"])
@require_role("commuter")
def my_receipts_alias():
    return my_receipts()

# Alias: /commuter/tickets?mine=1  → same payload as /commuter/tickets/mine
@commuter_bp.route("/tickets", methods=["GET"])
@require_role("commuter")
def tickets_index():
    mine = (request.args.get("mine") or "").lower() in {"1", "true", "yes"}
    # If client asks for mine=1, return the same as /tickets/mine (supports all same query params)
    if mine or True:
        return my_receipts()
    # (If you really want a 404 when mine is missing, replace the line above with:)
    # return jsonify(error="not found"), 404


@commuter_bp.route("/tickets/<int:ticket_id>/batch", methods=["GET"])
@require_role("commuter")
def commuter_get_ticket_batch(ticket_id: int):
    """
    Returns a "batch" or "group" summary for a ticket. Includes PAO/Driver names.
    """
    t = TicketSale.query.filter_by(id=ticket_id).first()
    if not t or (t.user_id != g.user.id):
        return jsonify(error="not found"), 404

    # Stops
    if t.origin_stop_time:
        origin = t.origin_stop_time.stop_name
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin = ts.stop_name if ts else ""

    if t.destination_stop_time:
        destination = t.destination_stop_time.stop_name
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination = tsd.stop_name if tsd else ""

    # Staff
    pao_id, pao_name, driver_id, driver_name = _resolve_staff(t.bus_id, getattr(t, "issued_by", None))

    # Prefer group (single-row) first
    is_group = bool(getattr(t, "is_group", False))
    if is_group and (int(getattr(t, "group_regular", 0) or 0) + int(getattr(t, "group_discount", 0) or 0) > 1):
        g_reg = int(getattr(t, "group_regular", 0) or 0)
        g_dis = int(getattr(t, "group_discount", 0) or 0)
        passengers = g_reg + g_dis
        total = int(round(float(t.price or 0)))
        _ldt = _as_local(t.created_at)

        return jsonify({
            "batch_id": int(getattr(t, "batch_id", None) or t.id),
            "head_ticket_id": int(t.id),
            "referenceNo": _display_ticket_ref_for(t=t),
            "date": _ldt.strftime("%B %d, %Y"),
            "time": _ldt.strftime("%I:%M %p").lstrip("0").lower(),
            "origin": origin,
            "destination": destination,
            "passengers": passengers,
            "fare_total": total,
            "breakdown": [
                *([{"passenger_type": "regular", "quantity": g_reg}] if g_reg else []),
                *([{"passenger_type": "discount", "quantity": g_dis}] if g_dis else []),
            ],
            "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
            # Staff
            "paoId": pao_id,
            "pao_name": pao_name,
            "driverId": driver_id,
            "driver_name": driver_name,
        }), 200

    # legacy multi-row batch (paid items under same batch_id)
    bid = getattr(t, "batch_id", None) or t.id
    rows = (
        TicketSale.query
        .filter(func.coalesce(TicketSale.batch_id, TicketSale.id) == bid, TicketSale.paid.is_(True))
        .order_by(TicketSale.id.asc())
        .all()
    )
    if not rows:
        return jsonify(error="not found"), 404

    total = sum(int(round(float(r.price or 0))) for r in rows)
    types = {}
    for r in rows:
        types[r.passenger_type or "regular"] = types.get(r.passenger_type or "regular", 0) + 1

    head = rows[0]
    _ldt = _as_local(head.created_at)

    return jsonify({
        "batch_id": int(bid),
        "head_ticket_id": int(head.id),
        "referenceNo": head.reference_no,
        "date": _ldt.strftime("%B %d, %Y"),
        "time": _ldt.strftime("%I:%M %p").lstrip("0").lower(),
        "origin": origin,
        "destination": destination,
        "passengers": len(rows),
        "fare_total": total,
        "breakdown": [{"passenger_type": k, "quantity": v} for k, v in types.items()],
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=head.id, _external=True),
        # Staff
        "paoId": pao_id,
        "pao_name": pao_name,
        "driverId": driver_id,
        "driver_name": driver_name,
    }), 200


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

    return jsonify(
        id=trip.id,
        number=trip.number,
        origin=first_stop.stop_name if first_stop else "",
        destination=last_stop.stop_name if last_stop else "",
        start_time=_as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
        end_time=_as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
    ), 200

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
    return jsonify([
        {
            "stop": st.stop_name,
            "arrive": (_as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else ""),
            "depart": (_as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else ""),
        }
        for st in sts
    ]), 200

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

    def fmt(t):
        tt = _as_time(t)
        return tt.strftime("%H:%M") if tt else ""

    events = []
    if len(stops) == 0:
        events.append({
            "id": 1,
            "type": "trip",
            "label": "In Transit",
            "start_time": fmt(trip.start_time),
            "end_time": fmt(trip.end_time),
            "description": "",
        })
    else:
        for idx, st in enumerate(stops):
            s = _as_time(st.arrive_time) or _as_time(st.depart_time)
            e = _as_time(st.depart_time) or _as_time(st.arrive_time)
            if s or e:
                events.append({
                    "id": idx * 2 + 1,
                    "type": "stop",
                    "label": "At Stop",
                    "start_time": fmt(s),
                    "end_time": fmt(e),
                    "description": st.stop_name,
                })
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
                        "end_time": fmt(e2),
                        "description": f"{st.stop_name} → {nxt.stop_name}",
                    })

    return jsonify(events=events), 200

@commuter_bp.route("/announcements", methods=["GET"])
def announcements():
    from sqlalchemy.orm import aliased
    from sqlalchemy import func
    import sqlalchemy as sa

    bus_id   = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")
    limit    = request.args.get("limit", type=int)

    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
    else:
        day = (dt.datetime.now(LOCAL_TZ) if LOCAL_TZ else dt.datetime.now()).date()

    start_utc, end_utc = _local_day_bounds_utc(day)

    BusDaily    = aliased(Bus)
    BusAssigned = aliased(Bus)

    use_daily_assignment = (
        _has_column("pao_assignments", "user_id")
        and _has_column("pao_assignments", "bus_id")
        and _has_column("pao_assignments", "service_date")
    )

    if use_daily_assignment:
        # 👇 build a lightweight selectable for pao_assignments
        pa = sa.table(
            "pao_assignments",
            sa.column("user_id"),
            sa.column("bus_id"),
            sa.column("service_date"),
        )

        q = (
            db.session.query(
                Announcement,
                User.first_name,
                User.last_name,
                func.coalesce(BusDaily.identifier, BusAssigned.identifier).label("bus_identifier"),
            )
            .join(User, Announcement.created_by == User.id)
            .outerjoin(
                pa,
                sa.and_(pa.c.user_id == User.id, pa.c.service_date == day),
            )
            .outerjoin(BusDaily, BusDaily.id == pa.c.bus_id)
            .outerjoin(BusAssigned, BusAssigned.id == User.assigned_bus_id)
            .filter(
                Announcement.timestamp >= start_utc,
                Announcement.timestamp <  end_utc,
            )
            .order_by(Announcement.timestamp.desc())
        )
        if bus_id:
            q = q.filter(func.coalesce(BusDaily.id, BusAssigned.id) == bus_id)
    else:
        q = (
            db.session.query(
                Announcement,
                User.first_name,
                User.last_name,
                BusAssigned.identifier.label("bus_identifier"),
            )
            .join(User, Announcement.created_by == User.id)
            .outerjoin(BusAssigned, BusAssigned.id == User.assigned_bus_id)
            .filter(
                Announcement.timestamp >= start_utc,
                Announcement.timestamp <  end_utc,
            )
            .order_by(Announcement.timestamp.desc())
        )
        if bus_id:
            q = q.filter(BusAssigned.id == bus_id)

    if isinstance(limit, int) and limit > 0:
        q = q.limit(limit)

    rows = q.all()
    anns = [
        {
            "id": ann.id,
            "message": ann.message,
            "timestamp": ann.timestamp.replace(tzinfo=dt.timezone.utc).isoformat(),
            "author_name": f"{first} {last}",
            "bus_identifier": (bus_identifier or "unassigned"),
        }
        for ann, first, last, bus_identifier in rows
    ]
    return jsonify(anns), 200
