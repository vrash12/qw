# backend/routes/pao.py
from __future__ import annotations
from datetime import datetime, timezone, timedelta
from typing import Optional

from dateutil import parser as dtparse
from flask import Blueprint, request, jsonify, g, current_app, url_for, redirect
from sqlalchemy import func
from sqlalchemy.orm import joinedload

try:
    from app.realtime import emit_announcement  # when app is a package
except ImportError:
    from realtime import emit_announcement      # when files are top-level
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
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
from utils.push import send_push_async  # ✅ keep this
from decimal import Decimal
import datetime as dt
from models.wallet import TopUp                     # for daily-cap query
from services.wallet import topup_cash, topup_gcash


def _as_utc(x):
    if x is None:
        return None
    # If naive → assume it's UTC; else convert to UTC
    return (x.replace(tzinfo=_tz.utc) if x.tzinfo is None else x.astimezone(_tz.utc))

def _as_mnl(x):
    return _as_utc(x).astimezone(_MNL)

def _iso_utc(x):
    u = _as_utc(x)
    return u.strftime('%Y-%m-%dT%H:%M:%SZ')  # explicit 'Z'

# ---- Time helpers (UTC canonical; Manila convenience) ----
from datetime import timezone as _tz, timedelta as _td
_MNL = _tz(_td(hours=8))

def _as_utc(x):
    if x is None:
        return None
    # If naive → assume it's UTC; else convert to UTC
    return (x.replace(tzinfo=_tz.utc) if x.tzinfo is None else x.astimezone(_tz.utc))

def _as_mnl(x):
    return _as_utc(x).astimezone(_MNL)

def _iso_utc(x):
    u = _as_utc(x)
    return u.strftime('%Y-%m-%dT%H:%M:%SZ')  # explicit 'Z'

pao_bp = Blueprint("pao", __name__, url_prefix="/pao")

SALT_USER_QR = "user-qr-v1"

def _user_qr_serializer():
    # uses your Flask SECRET_KEY
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)

def verify_user_qr_token(token: str, max_age: int = 60*60*24*30) -> dict:
    # 30 days default validity; tweak as you like
    return _user_qr_serializer().loads(token, max_age=max_age)

# …

@pao_bp.route("/users/scan", methods=["GET"])   # <-- was @commuter_bp.route(...)
@require_role("pao")
def user_qr_scan():
    token = (request.args.get("token") or "").strip()
    if not token:
        return jsonify(error="wallet_token required"), 400

    if verify_wallet_token is None:
        return jsonify(error="wallet token not supported"), 400
    try:
        user_id = int(verify_wallet_token(token))
    except Exception:
        return jsonify(error="invalid wallet token"), 400

    u = User.query.get(user_id)
    if not u:
        return jsonify(error="user not found"), 404

    return jsonify(user_id=user_id, name=f"{u.first_name} {u.last_name}"), 200


@pao_bp.route("/wallet/charge", methods=["POST"])
@require_role("pao")
def pao_wallet_charge():
    """
    Body:
      {
        "amount_php": 25.00,
        "wallet_token": "opaque-string"  # OR "user_id": 123,
        "ticket_id": 456                 # optional; will be marked paid on success
      }
    """
    data = request.get_json(silent=True) or {}

    # --- amount (PHP) ---
    try:
        amount_php = float(Decimal(str(data.get("amount_php"))))
    except Exception:
        return jsonify(error="invalid amount_php"), 400
    if amount_php <= 0:
        return jsonify(error="amount must be > 0"), 400
    amount_cents = int(round(amount_php * 100))

    # --- resolve user ---
    token = (data.get("wallet_token") or "").strip()
    user_id = data.get("user_id", None)
    if token:
        if verify_wallet_token is None:
            return jsonify(error="wallet token not supported"), 400
        try:
            user_id = int(verify_wallet_token(token))
        except ValueError:
            return jsonify(error="invalid wallet token"), 400
    if not user_id:
        return jsonify(error="missing wallet_token or user_id"), 400

    # --- get/create account & debit atomically ---
    acct = WalletAccount.query.filter_by(user_id=user_id).with_for_update().first()
    if not acct:
        return jsonify(error="wallet not found"), 404

    ticket_id = data.get("ticket_id")

    # 🔒 If charging a ticket, lock the ticket row and reject repeats
    t = None
    if ticket_id:
        t = TicketSale.query.filter_by(id=int(ticket_id)).with_for_update().first()
        if not t:
            return jsonify(error="ticket not found"), 404
        if t.paid:
            return jsonify(error="already paid"), 409

        dup = (
            WalletLedger.query
            .filter_by(
                account_id=acct.id,
                direction="debit",
                event="ride",
                ref_table="ticket_sale",
                ref_id=int(ticket_id),
            )
            .with_for_update()
            .first()
        )
        if dup:
            return jsonify(error="already charged"), 409

    # funds check
    if int(acct.balance_cents) < amount_cents:
        return jsonify(error="insufficient balance"), 402

    # debit + ledger
    new_balance = int(acct.balance_cents) - amount_cents
    acct.balance_cents = new_balance

    led = WalletLedger(
        account_id=acct.id,
        direction="debit",
        event="ride",
        amount_cents=amount_cents,
        running_balance_cents=new_balance,
        ref_table="ticket_sale" if ticket_id else None,
        ref_id=int(ticket_id) if ticket_id else None,
    )
    db.session.add(led)

    if t:
        t.paid = True

    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return jsonify(error="duplicate charge"), 409
    except Exception:
        db.session.rollback()
        return jsonify(error="internal error"), 500

    return jsonify(ok=True, user_id=int(user_id), new_balance_php=round(new_balance/100.0, 2)), 200


@pao_bp.route("/wallet/<int:user_id>/overview", methods=["GET"])
@require_role("pao")
def wallet_overview(user_id: int):
    """
    Response:
    {
      "user_id": 123,
      "balance_php": 123.45,
      "recent_topups": [{ id, amount_php, created_at, pao_name, method }],
      "recent_ledger": [{ id, direction, event, amount_php, running_balance_php, created_at, ref }],
      "pao_today": { "count": 3, "sum_php": 500.0, "cap_php": 50000.0 }
    }
    """
    limit_topups = request.args.get("limit_topups", type=int) or 10
    limit_ledger = request.args.get("limit_ledger", type=int) or 15

    # resolve account
    acct = WalletAccount.query.filter_by(user_id=user_id).first()
    balance_cents = int(getattr(acct, "balance_cents", 0))

    # queries need account_id; if no account yet, just return zeros
    account_id = getattr(acct, "id", None)

    # recent top-ups (desc)
    topups = []
    if account_id:
        rows = (
            TopUp.query
            .filter_by(account_id=account_id, status="succeeded")
            .order_by(TopUp.id.desc())
            .limit(limit_topups)
            .all()
        )
        # map PAO name (optional)
        pao_map = {}
        def _pao_name(pao_id):
            if pao_id in pao_map: return pao_map[pao_id]
            u = User.query.get(pao_id)
            pao_map[pao_id] = (f"{u.first_name} {u.last_name}" if u else "")
            return pao_map[pao_id]

        topups = [
            {
                "id": t.id,
                "amount_php": round(int(t.amount_cents) / 100.0, 2),
                "created_at": _iso_utc(t.created_at),
                "pao_name": _pao_name(t.pao_id),
                "method": t.method,
            }
            for t in rows
        ]

    # recent ledger (desc)
    ledger = []
    if account_id:
        rows = (
            WalletLedger.query
            .filter_by(account_id=account_id)
            .order_by(WalletLedger.id.desc())
            .limit(limit_ledger)
            .all()
        )
        ledger = [
            {
                "id": l.id,
                "direction": l.direction,   # "credit" / "debit"
                "event": l.event,           # "topup" / "ride" / ...
                "amount_php": round(int(l.amount_cents) / 100.0, 2),
                "running_balance_php": round(int(l.running_balance_cents) / 100.0, 2),
                "created_at": _iso_utc(l.created_at),
                "ref": {"table": l.ref_table, "id": l.ref_id},
            }
            for l in rows
        ]

    # PAO daily usage (PHP only in response)
    day = dt.datetime.utcnow().date()
    start_dt = dt.datetime.combine(day, dt.time.min)
    end_dt   = dt.datetime.combine(day, dt.time.max)

    cap_php = float(current_app.config.get("PAO_DAILY_TOPUP_LIMIT_PHP", 50000))
    used_cents_raw = (
        db.session.query(func.coalesce(func.sum(TopUp.amount_cents), 0))
        .filter(
            TopUp.pao_id == g.user.id,
            TopUp.created_at.between(start_dt, end_dt),
            TopUp.status == "succeeded",
        )
        .scalar() or 0
    )
    used_cents = int(used_cents_raw)  # normalize (SUM may return Decimal)

    used_count = (
        db.session.query(func.count(TopUp.id))
        .filter(
            TopUp.pao_id == g.user.id,
            TopUp.created_at.between(start_dt, end_dt),
            TopUp.status == "succeeded",
        )
        .scalar() or 0
    )

    return jsonify(
        user_id=user_id,
        balance_php=round(balance_cents / 100.0, 2),
        recent_topups=topups,
        recent_ledger=ledger,
        pao_today={
            "count": int(used_count),
            "sum_php": round(used_cents / 100.0, 2),
            "cap_php": cap_php,
        },
    ), 200

    uid = int(data.get("uid"))
    user = User.query.get(uid)
    if not user:
        return jsonify(valid=False, error="user not found"), 404

    return jsonify(valid=True, user={
        "id": user.id,
        "name": f"{user.first_name} {user.last_name}",
        "role": user.role,
    }), 200
def _serialize_ticket_json(t: TicketSale, origin_name: str, destination_name: str) -> dict:
    amount = int(round(float(t.price or 0)))
    prefix = "discount" if t.passenger_type == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"
    qr_url   = url_for("static", filename=f"qr/{filename}", _external=True)
    qr_link  = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)
    img      = jpg_name(amount, t.passenger_type)
    qr_bg_url= f"{request.url_root.rstrip('/')}/{QR_PATH}/{img}"
    payload  = build_qr_payload(t, origin_name=origin_name, destination_name=destination_name)

    return {
        "id": t.id,
        "referenceNo": t.reference_no,
        "qr": payload,
        "qr_link": qr_link,
        "qr_bg_url": qr_bg_url,
        "qr_url": qr_url,
        "origin": origin_name,
        "destination": destination_name,
        "passengerType": (t.passenger_type or "").lower(),
        "fare": f"{float(t.price or 0):.2f}",
        "paid": bool(t.paid),
        "commuter": _commuter_label(t),
        "paoId": getattr(t, "issued_by", None),
    }


@pao_bp.route("/tickets/<int:ticket_id>/receipt.png", methods=["GET"])
def pao_ticket_receipt_image(ticket_id: int):
    # Public shim that just redirects to the canonical commuter image
    return redirect(url_for("commuter.commuter_ticket_image", ticket_id=ticket_id), code=302)
    
def _commuter_label(ticket: TicketSale) -> str:
    if getattr(ticket, "guest", False):
        return "Guest"
    u = getattr(ticket, "user", None)
    if u:
        return f"{u.first_name} {u.last_name}"
    return "Guest"


@pao_bp.route("/device-token", methods=["POST"])
@require_role("pao")
def save_pao_device_token():
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    platform = (data.get("platform") or "").strip() or None
    if not token:
        return jsonify(error="token required"), 400

    created = False
    row = DeviceToken.query.filter_by(token=token).first()
    if not row:
        row = DeviceToken(user_id=g.user.id, token=token, platform=platform)
        db.session.add(row)
        created = True
    else:
        row.user_id = g.user.id
        row.platform = platform or row.platform

    db.session.commit()
    current_app.logger.info(f"[push] saved PAO token token={token[:12]}… uid={g.user.id} created={created} platform={row.platform}")
    return jsonify(ok=True, created=created), (201 if created else 200)


# --- helper (place near other helpers) ---
def _ann_json(ann: Announcement) -> dict:
    u = User.query.get(ann.created_by)
    bus_row = Bus.query.get(getattr(u, "assigned_bus_id", None)) if u else None
    bus_identifier = (bus_row.identifier or f"bus-{bus_row.id:02d}") if bus_row else "—"
    return {
        "id": ann.id,
        "message": ann.message,
        "timestamp": _iso_utc(ann.timestamp),
        "created_by": ann.created_by,
        "author_name": f"{u.first_name} {u.last_name}" if u else "",
        "bus": bus_identifier,
    }


@pao_bp.route("/broadcast/<int:ann_id>", methods=["PATCH"])
@require_role("pao")
def update_broadcast(ann_id: int):
    ann = Announcement.query.get(ann_id)
    if not ann:
        return jsonify(error="announcement not found"), 404
    if ann.created_by != g.user.id:
        return jsonify(error="not allowed to modify this announcement"), 403

    data = request.get_json(silent=True) or {}
    msg = (data.get("message") or "").strip()
    if not msg:
        return jsonify(error="message is required"), 400

    ann.message = msg
    try:
        db.session.commit()
        return jsonify(_ann_json(ann)), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("update_broadcast failed")
        return jsonify(error=str(e)), 500


# --- DELETE an announcement (author-only) ---
@pao_bp.route("/broadcast/<int:ann_id>", methods=["DELETE"])
@require_role("pao")
def delete_broadcast(ann_id: int):
    ann = Announcement.query.get(ann_id)
    if not ann:
        return jsonify(error="announcement not found"), 404
    if ann.created_by != g.user.id:
        return jsonify(error="not allowed to delete this announcement"), 403

    try:
        db.session.delete(ann)
        db.session.commit()
        return jsonify(id=ann_id, deleted=True), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("delete_broadcast failed")
        return jsonify(error=str(e)), 500


@pao_bp.route("/reset-live-stats", methods=["POST"])
@require_role("pao")
def reset_live_stats():
    """
    Ask the device to zero its live passenger counters.
    This does NOT modify database rows; it only triggers the sensor to publish fresh totals.
    Device must subscribe to: device/<bus-identifier>/control
    Payload: {"cmd": "reset_people"}
    """
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    bus_row = Bus.query.get(bus_id)
    bus_identifier = (bus_row.identifier or f"bus-{bus_id:02d}") if bus_row else f"bus-{bus_id:02d}"

    topic = f"device/{bus_identifier}/cmd/reset"
    try:
        publish(topic, {"reset": True})
        current_app.logger.info(f"[PAO] reset request → {topic}")
        # 202 to indicate it's async (device will apply and then publish /people)
        return jsonify(ok=True), 202
    except Exception as e:
        current_app.logger.exception("reset-live-stats publish failed")
        return jsonify(error=str(e)), 500


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
            "timestamp": _iso_utc(ann.timestamp),
            "author_name": f"{first} {last}",
        }

    return jsonify(
        tickets_total = int(total),
        paid_count    = int(paid_count),
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
        .filter(TicketSale.bus_id == bus_id)
        .order_by(TicketSale.id.desc())
        .limit(limit)
        .all()
    )

    out = []
    for t in rows:
        out.append({
            "id": t.id,
            "referenceNo": t.reference_no,
            "commuter": _commuter_label(t),
            "fare": f"{float(t.price):.2f}",
            "paid": bool(t.paid),
            # Canonical ISO (UTC) for clients to format
            "created_at": _iso_utc(t.created_at),
            # Keep human strings, but make them Manila-correct (UI-friendly)
            "time": _as_mnl(t.created_at).strftime("%I:%M %p").lstrip("0").lower(),
        })

    return jsonify(out), 200


def _current_bus_id() -> Optional[int]:
    return getattr(g.user, "assigned_bus_id", None)



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
    send_push_async(
        tokens,
        "🚍 New Pickup Request",
        f"Commuter #{commuter_id} is waiting.",
        {"commuterId": commuter_id},
    )

    return jsonify(success=True), 201




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


@pao_bp.route("/wallet/topups", methods=["POST"])
@require_role("pao")
def pao_cash_topup():
    """
    POST /pao/wallet/topups
    Body: { amount, method?, wallet_token? | user_id?, gcash_ref? }
    """
    import time as _time

    def _mask_token(tok: str) -> str:
        tok = (tok or "").strip()
        if len(tok) <= 8:
            return "…" if tok else ""
        return f"{tok[:4]}…{tok[-4:]}"

    rid = request.headers.get("X-Request-ID") or f"topup-{int(_time.time()*1000)}"
    pao_id = getattr(getattr(g, "user", None), "id", None)

    data = request.get_json(silent=True) or {}
    method = (data.get("method") or "cash").strip().lower()
    token  = (data.get("wallet_token") or "").strip()
    user_id = data.get("user_id")

    current_app.logger.info(
        "[PAO:topup][%s] incoming: pao_id=%s method=%s amount_raw=%r user_id=%r token=%s",
        rid, pao_id, method, data.get("amount"), user_id, _mask_token(token)
    )

    if method not in ("cash", "gcash"):
        current_app.logger.warning("[PAO:topup][%s] reject: invalid method %r", rid, method)
        return jsonify(error="invalid method (must be 'cash' or 'gcash')"), 400

    # amount validation
    try:
        from decimal import Decimal as _D
        amount_php = float(_D(str(data.get("amount"))))
    except Exception:
        current_app.logger.warning("[PAO:topup][%s] reject: invalid amount payload=%r", rid, data.get("amount"))
        return jsonify(error="invalid amount"), 400

    min_php = float(current_app.config.get("MIN_TOPUP_PHP", 20))
    max_php = float(current_app.config.get("MAX_TOPUP_PHP", 2000))
    if amount_php < min_php or amount_php > max_php:
        current_app.logger.warning(
            "[PAO:topup][%s] reject: amount out of bounds amount_php=%.2f min=%.2f max=%.2f",
            rid, amount_php, min_php, max_php
        )
        return jsonify(error=f"amount must be between ₱{min_php:.0f} and ₱{max_php:.0f}"), 400

    # resolve user via token or provided user_id
    if token:
        if verify_wallet_token is None:
            current_app.logger.warning("[PAO:topup][%s] reject: wallet token not supported", rid)
            return jsonify(error="wallet token not supported"), 400
        try:
            user_id = verify_wallet_token(token)
        except ValueError:
            current_app.logger.warning(
                "[PAO:topup][%s] reject: invalid wallet token token=%s", rid, _mask_token(token)
            )
            return jsonify(error="invalid wallet token"), 400

    if not user_id:
        current_app.logger.warning("[PAO:topup][%s] reject: missing wallet_token or user_id", rid)
        return jsonify(error="missing wallet_token or user_id"), 400

    # PAO daily cap
    day = dt.datetime.utcnow().date()
    start_dt = dt.datetime.combine(day, dt.time.min)
    end_dt   = dt.datetime.combine(day, dt.time.max)

    used_cents_raw = (
        db.session.query(func.coalesce(func.sum(TopUp.amount_cents), 0))
        .filter(
            TopUp.pao_id == g.user.id,
            TopUp.created_at.between(start_dt, end_dt),
            TopUp.status == "succeeded",
        )
        .scalar() or 0
    )
    used_cents = int(used_cents_raw)
    cap_php  = float(current_app.config.get("PAO_DAILY_TOPUP_LIMIT_PHP", 50000))
    used_php = used_cents / 100.0

    current_app.logger.debug(
        "[PAO:topup][%s] cap-check: used_php=%.2f + amount_php=%.2f <= cap_php=%.2f ? %s",
        rid, used_php, amount_php, cap_php, (used_php + amount_php) <= cap_php
    )

    if used_php + amount_php > cap_php:
        current_app.logger.warning(
            "[PAO:topup][%s] reject: PAO daily cap exceeded used=%.2f add=%.2f cap=%.2f",
            rid, used_php, amount_php, cap_php
        )
        return jsonify(error="pao daily limit exceeded"), 403

    # ---- perform top-up via services ----
    try:
        amount_cents = int(round(amount_php * 100))
        account_id = int(user_id)

        if method == "gcash":
            external_ref = (data.get("gcash_ref") or "").strip() or None
            topup_id, ledger_id, new_balance_php = topup_gcash(
                account_id=account_id,
                pao_id=g.user.id,
                amount_cents=amount_cents,
                external_ref=external_ref,
            )
        else:
            topup_id, ledger_id, new_balance_php = topup_cash(
                account_id=account_id,
                pao_id=g.user.id,
                amount_cents=amount_cents,
            )

        # services commit internally; extra commit is harmless
        db.session.commit()

        current_app.logger.info(
            "[PAO:topup][%s] success: topup_id=%s ledger_id=%s user_id=%s amount=%.2f new_balance=%.2f",
            rid, topup_id, ledger_id, user_id, amount_php, new_balance_php
        )
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("[PAO:topup][%s] DB failure", rid)
        return jsonify(error=str(e)), 500

    # Push notify commuter (best-effort)
    try:
        tokens = [t.token for t in DeviceToken.query.filter_by(user_id=int(user_id)).all()]
        if tokens:
            sent_at = int(_time.time() * 1000)
            kind = "GCash" if method == "gcash" else "Cash"
            send_push_async(
                tokens,
                "💸 Wallet top-up",
                f"{kind} +₱{amount_php:.2f} • New balance ₱{new_balance_php:.2f}",
                {
                    "deeplink": "/commuter/wallet",
                    "amount": float(round(amount_php, 2)),
                    "newBalance": float(round(new_balance_php, 2)),
                    "method": method,
                    "sentAt": sent_at,
                },
                channelId="payments",
                priority="high",
            )
            current_app.logger.info("[PAO:topup][%s] push: sent tokens=%d user_id=%s", rid, len(tokens), user_id)
        else:
            current_app.logger.debug("[PAO:topup][%s] push: no device tokens user_id=%s", rid, user_id)
    except Exception:
        current_app.logger.exception("[PAO:topup][%s] push notify failed", rid)

    return jsonify(
        topup_id=int(topup_id),
        user_id=int(user_id),
        new_balance_php=float(round(new_balance_php, 2)),
        request_id=rid,
    ), 201


def _fare_for(o, d, passenger_type: str) -> int:
    hops = abs(o.seq - d.seq)
    base = 10 + max(hops - 1, 0) * 2
    return round(base * 0.8) if passenger_type == "discount" else base


@pao_bp.route("/tickets", methods=["POST"])
@require_role("pao")
def create_ticket():
    """
    PAID-ONLY CREATION:
    - No pending tickets are ever written.
    - Every created row is saved with paid=True.
    - Immediately updates MQTT paid count and (if linked) pushes a 'Payment confirmed' notification.
    """
    data = request.get_json(silent=True) or {}
    current_app.logger.debug(f"[PAO:/tickets] raw payload: {data}")

    try:
        # ---- Inputs ----------------------------------------------------------
        o_id = data.get("origin_stop_id") or data.get("origin_stop_time_id")
        d_id = data.get("destination_stop_id") or data.get("destination_stop_time_id")
        uid  = data.get("commuter_id")
        as_guest = bool(data.get("guest"))
        primary_type = data.get("primary_type")          # 'regular' | 'discount' (optional)
        assign_all   = bool(data.get("assign_all_to_commuter") or False)

        client_ts = data.get("created_at")
        try:
            ticket_dt = dtparse.parse(client_ts) if client_ts else datetime.now()
        except Exception:
            ticket_dt = datetime.now()

        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)
        if not o or not d:
            return jsonify(error="origin or destination not found"), 400

        items_spec = data.get("items")
        blocks: list[tuple[str, int]] = []
        if isinstance(items_spec, list):
            for b in items_spec:
                pt = (b or {}).get("passenger_type")
                qty = int((b or {}).get("quantity") or 0)
                if pt not in ("regular", "discount"):
                    return jsonify(error="invalid passenger_type in items"), 400
                qty = max(0, min(qty, 20))
                if qty > 0:
                    blocks.append((pt, qty))
        else:
            pt = data.get("passenger_type")
            if pt not in ("regular", "discount"):
                return jsonify(error="invalid passenger_type"), 400
            qty = int(data.get("quantity") or 1)
            qty = max(1, min(qty, 20))
            blocks = [(pt, qty)]

        if not blocks:
            return jsonify(error="no passengers"), 400

        user = None
        if not as_guest:
            if not uid:
                return jsonify(error="either commuter_id or guest=true is required"), 400
            user = User.query.get(uid)
            if not user:
                return jsonify(error="invalid commuter_id"), 400

        # PAO must be assigned to a bus
        bus_id = _current_bus_id()
        if not bus_id:
            return jsonify(error="PAO has no assigned bus"), 400

        # Cap total quantity
        total_qty = sum(q for _, q in blocks)
        if total_qty > 20:
            return jsonify(error="quantity exceeds limit (20)"), 400

        items: list[TicketSale] = []
        assigned_primary = False

        if primary_type in ("regular", "discount"):
            blocks.sort(key=lambda x: 0 if x[0] == primary_type else 1)

        # ---- Create rows (PAID-ONLY) ----------------------------------------
        items: list[TicketSale] = []
        assigned_primary = False

        for pt, qty in blocks:
            fare = _fare_for(o, d, pt)
            for _ in range(qty):
                this_user_id = None
                if user and not assigned_primary:
                    this_user_id = user.id
                    assigned_primary = True

                t = TicketSale(
                    bus_id=bus_id,
                    user_id=this_user_id,
                    guest=bool(not this_user_id),
                    price=fare,
                    passenger_type=pt,
                    reference_no="TEMP",
                    paid=False,
                    created_at=ticket_dt,
                    origin_stop_time_id=o.id,
                    destination_stop_time_id=d.id,
                    issued_by=g.user.id,
                )
                db.session.add(t)
                db.session.flush()
                t.reference_no = _gen_reference(bus_id, t.id)
                items.append(t)

        db.session.commit()

        current_app.logger.info(
            "[PAO:create_ticket] created %s paid ticket(s) bus_id=%s issued_by=%s mixed=%s",
            len(items), bus_id, getattr(g.user, "id", None),
            any(t.passenger_type != items[0].passenger_type for t in items)
        )

        # ---- MQTT: publish latest paid count for today -----------------------
        from datetime import datetime as _dt
        start = _dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end   = _dt.utcnow().replace(hour=23, minute=59, second=59, microsecond=999999)
        cnt = (
            TicketSale.query.filter_by(bus_id=bus_id, paid=True)
            .filter(TicketSale.created_at.between(start, end))
            .count()
        )
        bus_row = Bus.query.get(bus_id)
        if bus_row and bus_row.identifier:
            topic = f"device/{bus_row.identifier}/fare"
            publish(topic, {"paid": cnt})
            current_app.logger.info(f"MQTT fare update → {topic}: {cnt}")

        # ---- Push: payment confirmed (only if ticket is linked to a user) ----
        if user and items:
            try:
                # If assign_all produced multiple tickets, notify summary; else one head ticket.
                if assign_all and len(items) > 1:
                    total = sum(float(t.price or 0) for t in items)
                    push_to_user(
                        db, DeviceToken, user.id,
                        "✅ Payment confirmed",
                        f"{len(items)} tickets • ₱{total:.2f} • {o.stop_name} → {d.stop_name}",
                        {"deeplink": "/commuter/notifications", "type": "payment", "autonav": True},
                        channelId="payments", priority="high", ttl=600,
                    )
                else:
                    head = next((t for t in items if t.user_id == user.id), items[0])
                    push_to_user(
                        db, DeviceToken, user.id,
                        "✅ Payment confirmed",
                        f"Ref {head.reference_no} • ₱{float(head.price or 0):.2f} • {o.stop_name} → {d.stop_name}",
                        {
                        "deeplink": "/commuter/notifications",
                        "ticketId": head.id,
                        "type": "payment",
                        "autonav": True
                        },
                        channelId="payments", priority="high", ttl=600,
                    )
            except Exception:
                current_app.logger.exception("[push] paid-confirmation failed")

        serialized = [_serialize_ticket_json(t, o.stop_name, d.stop_name) for t in items]
        if len(items) == 1:
            return jsonify(serialized[0]), 201
        else:
            total = sum(float(t.price or 0.0) for t in items)
            return jsonify({
                "count": len(items),
                "total_fare": f"{float(total):.2f}",
                "items": serialized
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
        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)
        if not o or not d:
            return jsonify(error="origin or destination not found"), 400

        def fare_for(pt: str) -> int:
            hops = abs(o.seq - d.seq)
            base = 10 + max(hops - 1, 0) * 2
            return round(base * 0.8) if pt == "discount" else base

        items_spec = data.get("items")
        if isinstance(items_spec, list):
            breakdown = []
            total = 0.0
            for b in items_spec:
                pt = (b or {}).get("passenger_type")
                qty = int((b or {}).get("quantity") or 0)
                if pt not in ("regular", "discount") or qty < 0:
                    return jsonify(error="invalid preview items"), 400
                if qty == 0:
                    continue
                each = float(fare_for(pt))
                sub = each * qty
                total += sub
                breakdown.append({
                    "passenger_type": pt,
                    "quantity": qty,
                    "fare_each": f"{each:.2f}",
                    "subtotal": f"{sub:.2f}",
                })
            return jsonify(total_fare=f"{total:.2f}", items=breakdown), 200

        p = data.get("passenger_type")
        if p not in ("regular", "discount"):
            return jsonify(error="invalid passenger_type"), 400
        fare = float(fare_for(p))
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
                "commuter": _commuter_label(t),
                "date": t.created_at.strftime("%B %d, %Y"),
                "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
                "origin": origin_name,
                "destination": destination_name,
                "fare": f"{float(t.price):.2f}",
                "paid": bool(t.paid),
        
            }
        )
    return jsonify(out), 200


@pao_bp.route("/tickets/<int:ticket_id>", methods=["GET"])
@require_role("pao")
def get_ticket(ticket_id):
    t = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

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

    amount = int(round(float(t.price or 0)))
    prefix = "discount" if t.passenger_type == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"
    qr_url  = url_for("static", filename=f"qr/{filename}", _external=True)
    qr_link = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)

    img = jpg_name(amount, t.passenger_type)
    qr_bg_url = f"{request.url_root.rstrip('/')}/{QR_PATH}/{img}"

    payload = build_qr_payload(t, origin_name=origin_name, destination_name=destination_name)

    current_app.logger.info(
        "[PAO:get_ticket] ticket_id=%s ref=%s issued_by=%s caller_pao=%s",
        t.id, t.reference_no,
        getattr(t, "issued_by", None),
        getattr(getattr(g, "user", None), "id", None),
    )
    return jsonify({
        "id": t.id,
        "referenceNo": t.reference_no,
        "date": t.created_at.strftime("%B %d, %Y"),
        "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "origin": origin_name,
        "destination": destination_name,
        "commuter": ("Guest" if getattr(t, "guest", False)
                    else (f"{t.user.first_name} {t.user.last_name}" if t.user else "Guest")),
        "passengerType": (t.passenger_type or "").lower(),
        "fare": f"{float(t.price or 0):.2f}",
        "paid": bool(t.paid),
        "qr": payload,
        "qr_link": qr_link,
        "qr_url": qr_url,
        "qr_bg_url": qr_bg_url,
        "receipt_image": url_for("pao.pao_ticket_receipt_image", ticket_id=t.id, _external=True),
        "paoId": getattr(t, "issued_by", None) or getattr(g, "user", None).id,
    }), 200


@pao_bp.route("/tickets/<int:ticket_id>", methods=["PATCH"])
@require_role("pao")
def mark_ticket_paid(ticket_id: int):
    data = request.get_json(silent=True) or {}
    paid = bool(data.get("paid"))

    # we need bus (for MQTT topic) and user (for push)
    ticket = (
        TicketSale.query.options(joinedload(TicketSale.bus), joinedload(TicketSale.user))
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not ticket:
        return jsonify(error="ticket not found"), 404

    was_paid = bool(ticket.paid)
    ticket.paid = 1 if paid else 0

    try:
        db.session.commit()

        # MQTT fare count update (UTC “today”)
        from datetime import datetime as _dt
        start = _dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end   = _dt.utcnow().replace(hour=23, minute=59, second=59, microsecond=999999)
        cnt = (
            TicketSale.query.filter_by(bus_id=ticket.bus_id, paid=True)
            .filter(TicketSale.created_at.between(start, end))
            .count()
        )
        if ticket.bus and ticket.bus.identifier:
            topic = f"device/{ticket.bus.identifier}/fare"
            publish(topic, {"paid": cnt})
            current_app.logger.info(f"MQTT fare update → {topic}: {cnt}")

        # 🔔 Only push on the first transition to paid, and only for non-guest tickets
        if (not was_paid) and bool(ticket.paid) and ticket.user_id:
            try:
                import time
                sent_at = int(time.time() * 1000)
                push_to_user(
                    db, DeviceToken, ticket.user_id,
                    "✅ Payment confirmed",
                    f"Ref {ticket.reference_no} • ₱{float(ticket.price or 0):.2f}",
                    {
                    "deeplink": "/commuter/notifications",
                    "ticketId": ticket.id,
                    "type": "payment",
                    "autonav": True,
                    "sentAt": sent_at,
                    },
                    channelId="payments", priority="high", ttl=600,
                )

            except Exception:
                current_app.logger.exception("[push] paid-confirmation failed")

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
            "timestamp": _iso_utc(ann.timestamp),
            "bus_identifier": bus_identifier,
        }
        emit_announcement(payload, bus_id=bus_id)

        tokens = [
            t.token
            for t in DeviceToken.query
            .join(User, User.id == DeviceToken.user_id)
            .filter(User.role == "commuter")
            .all()
        ]
        if tokens:
            send_push_async(
                tokens,
                "🗞️ Announcement",
                f"{bus_identifier}: {message}",
                {"deeplink": "/commuter/notifications"},
                channelId="announcements",
            )

        return jsonify({
            "id": ann.id,
            "message": ann.message,
            "timestamp": payload["timestamp"],
            "created_by": ann.created_by,
            "author_name": f"{g.user.first_name} {g.user.last_name}",
            "bus": bus_identifier,
        }), 201

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

    if scope != "all":
        q = q.filter(User.assigned_bus_id == bus_id)

    rows = q.all()

    anns = [
        {
            "id": ann.id,
            "message": ann.message,
            "timestamp": _iso_utc(ann.timestamp),
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


def _gen_reference(bus_id: int, row_id: int) -> str:
    # Row-safe, no race: use the id we just flushed
    return f"BUS{bus_id}-{row_id:04d}"
