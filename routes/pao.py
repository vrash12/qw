# backend/routes/pao.py
from __future__ import annotations

# === stdlib ===
import time
import datetime as dt
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from functools import lru_cache
from secrets import token_hex as _tokhex
from typing import Any, Dict, List, Optional, Tuple

# === 3p ===
from dateutil import parser as dtparse
from flask import Blueprint, request, jsonify, g, current_app, url_for, redirect
from itsdangerous import URLSafeTimedSerializer, URLSafeSerializer, BadSignature, SignatureExpired
from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import joinedload

# === app ===
from db import db
from mqtt_ingest import publish
from routes.auth import require_role
from models.announcement import Announcement
from models.bus import Bus
from models.schedule import Trip, StopTime
from models.ticket_sale import TicketSale
from models.ticket_stop import TicketStop
from models.user import User
from models.device_token import DeviceToken
from models.wallet import WalletAccount, WalletLedger, TopUp
from services.wallet import topup_cash, topup_gcash  # (kept available if used elsewhere)
from routes.tickets_static import jpg_name, QR_PATH
from utils.qr import build_qr_payload
from utils.push import send_push_async, push_to_user

# Optional (some deployments provide a stronger opaque wallet token)
try:
    from utils.wallet_qr import verify_wallet_token
except Exception:
    verify_wallet_token = None

# ------------------------------------------------------------------------------
# Blueprint
# ------------------------------------------------------------------------------
pao_bp = Blueprint("pao", __name__)

# ------------------------------------------------------------------------------
# Timezone & time helpers
# ------------------------------------------------------------------------------
_MNL = timezone(timedelta(hours=8))

def now_utc_naive() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)

def _as_utc(x: Optional[dt.datetime]) -> Optional[dt.datetime]:
    if x is None:
        return None
    return x.replace(tzinfo=timezone.utc) if x.tzinfo is None else x.astimezone(timezone.utc)

def _as_mnl(x: Optional[dt.datetime]) -> Optional[dt.datetime]:
    u = _as_utc(x)
    return u.astimezone(_MNL) if u else None

def _iso_utc(x: Optional[dt.datetime]) -> Optional[str]:
    u = _as_utc(x)
    return u.strftime("%Y-%m-%dT%H:%M:%SZ") if u else None

def _local_day_bounds_utc(day: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    start_local = dt.datetime.combine(day, dt.time(0, 0, 0), tzinfo=_MNL)
    end_local   = start_local + dt.timedelta(days=1)
    return (
        start_local.astimezone(timezone.utc).replace(tzinfo=None),
        end_local.astimezone(timezone.utc).replace(tzinfo=None),
    )

def _ann_json_fast(
    ann: Announcement,
    *,
    author_first: str,
    author_last: str,
    bus_identifier: Optional[str],
) -> dict:
    return {
        "id": ann.id,
        "message": ann.message,
        "timestamp": _iso_utc(ann.timestamp),
        "created_by": ann.created_by,
        "author_name": f"{(author_first or '').strip()} {(author_last or '').strip()}".strip(),
        "bus": (bus_identifier or "—"),
    }

def _utc_from_local_date(day: dt.date, *, at_time: Optional[dt.time] = None) -> dt.datetime:
    """
    Build a naive UTC datetime representing `day` at Asia/Manila local clock time.
    If at_time is omitted, we reuse the current local time-of-day to preserve ordering.
    """
    local_time = at_time or dt.datetime.now(_MNL).time()
    local_dt = dt.datetime.combine(day, local_time, tzinfo=_MNL)
    return local_dt.astimezone(timezone.utc).replace(tzinfo=None)


def _debug_enabled() -> bool:
    return (request.args.get("debug") or request.headers.get("X-Debug") or "").lower() in {"1","true","yes"}

def _today_bus_for_pao(user_id: int) -> Optional[int]:
    bus_id = db.session.execute(
        text("""
            SELECT bus_id
            FROM pao_assignments
            WHERE user_id = :uid
              AND service_date = DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '+08:00'))
            LIMIT 1
        """),
        {"uid": int(user_id)},
    ).scalar()

    # DEBUG: print computed Manila date and result
    mnl = db.session.execute(
        text("SELECT DATE(CONVERT_TZ(UTC_TIMESTAMP(), '+00:00', '+08:00'))")
    ).scalar()
    current_app.logger.info("[today_bus] uid=%s mnl=%s bus_id=%s", user_id, mnl, bus_id)

    return int(bus_id) if bus_id is not None else None



def _bus_for_pao_on(day: dt.date, user_id: int) -> Optional[int]:
    bus_id = db.session.execute(
        text("""
            SELECT bus_id
            FROM pao_assignments
            WHERE user_id = :uid AND service_date = :d
            LIMIT 1
        """),
        {"uid": int(user_id), "d": day},
    ).scalar()
    return int(bus_id) if bus_id is not None else None


def _current_bus_id() -> Optional[int]:
    """
    Resolve the PAO's bus for *today* (Asia/Manila). If not scheduled, fall back to
    legacy static field for older flows.
    """
    uid = int(getattr(getattr(g, "user", None), "id", 0) or 0)
    if uid:
        bid = _today_bus_for_pao(uid)
        if bid:
            return bid
    return getattr(g.user, "assigned_bus_id", None)

def _who_is_pao_user_id() -> Optional[int]:
    try:
        return int(getattr(g, "user", None).id)
    except Exception:
        return None

def _bus_for_pao() -> Optional[int]:
    try:
        bus_id = getattr(g.user, "assigned_bus_id", None)
        return int(bus_id) if bus_id else None
    except Exception:
        return None

# ------------------------------------------------------------------------------
# DB helpers
# ------------------------------------------------------------------------------
@lru_cache(maxsize=None)
def _has_column(table: str, column: str) -> bool:
    try:
        row = db.session.execute(
            text("""
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE()
                  AND TABLE_NAME = :t AND COLUMN_NAME = :c
                LIMIT 1
            """),
            {"t": table, "c": column},
        ).first()
        return bool(row)
    except Exception:
        return False

# ------------------------------------------------------------------------------
# User QR (itsdangerous-signed) + rotating wallet token helpers
# ------------------------------------------------------------------------------
SALT_USER_QR   = "user-qr-v1"
SALT_WALLET_QR = "wallet-qr-rot-v1"

def _user_qr_serializer() -> URLSafeTimedSerializer:
    return URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)

def verify_user_qr_token(token: str, max_age: int = 60*60*24*30) -> dict:
    return _user_qr_serializer().loads(token, max_age=max_age)

def _wallet_rot_serializer() -> URLSafeSerializer:
    return URLSafeSerializer(current_app.config["SECRET_KEY"], salt=SALT_WALLET_QR)

def _try_user_qr_soft(token: str) -> Tuple[Optional[int], Optional[bool], Optional[str]]:
    """
    Try to decode a signed commuter-QR with a small post-expiry grace.
    Returns: (user_id | None, grace_used | None, error_kind | None)
    """
    s = _user_qr_serializer()
    max_age = int(current_app.config.get("WALLET_QR_MAX_AGE_S", 60))
    grace   = int(current_app.config.get("WALLET_QR_GRACE_S", 8))

    try:
        payload = s.loads(token, max_age=max_age)
        uid = int(payload.get("uid") or 0)
        return (uid if uid > 0 else None, False, None)
    except SignatureExpired as e:
        try:
            valid, payload = s.loads_unsafe(token)  # (bool, payload or None)
        except Exception:
            valid, payload = False, None

        if valid and payload and getattr(e, "date_signed", None):
            signed_at = e.date_signed
            if signed_at.tzinfo is None:
                signed_at = signed_at.replace(tzinfo=timezone.utc)
            age_sec = (datetime.now(timezone.utc) - signed_at).total_seconds()
            if age_sec <= (max_age + grace):
                uid = int(payload.get("uid") or 0)
                return (uid if uid > 0 else None, True, None)
        return (None, None, "expired")
    except BadSignature:
        return (None, None, "invalid")

def _try_wallet_rot_soft(tok: str) -> Tuple[Optional[int], Optional[bool], Optional[str]]:
    """
    Accept rotating wallet tokens: payload {"uid": <int>, "mb": <minuteBucket>}
    Valid for the current minute; allow small post-rollover grace.
    Returns: (user_id|None, grace_used|None, "expired"/"invalid"|None)
    """
    try:
        payload = _wallet_rot_serializer().loads(tok)
        uid = int(payload.get("uid") or 0)
        mb  = int(payload.get("mb")  or -1)
        if uid <= 0 or mb < 0:
            return (None, None, "invalid")
    except BadSignature:
        return (None, None, "invalid")
    except Exception:
        return (None, None, "invalid")

    now = time.time()
    now_bucket = int(now // 60)
    grace = int(current_app.config.get("WALLET_QR_GRACE_S", 8))
    secs_into_min = int(now % 60)

    if mb == now_bucket:
        return (uid, False, None)
    if mb == (now_bucket - 1) and secs_into_min <= grace:
        return (uid, True, None)
    return (None, None, "expired" if mb < (now_bucket - 1) else "invalid")

# ------------------------------------------------------------------------------
# MQTT + Socket.IO helpers
# ------------------------------------------------------------------------------
def _publish_user_wallet(uid: int, *, new_balance_pesos: int, event: str, **extra):
    try:
        payload = {
            "type": "wallet_update",
            "event": event,  # "payment" | "wallet_debit" | "wallet_topup" | "refund"
            "new_balance_php": float(new_balance_pesos),
            "sentAt": int(time.time() * 1000),
            **extra,
        }
        publish(f"user/{uid}/wallet", payload)
    except Exception:
        current_app.logger.exception("[mqtt] user-wallet publish failed")

def _socketio():
    return current_app.extensions.get("socketio")

def _emit_announcement(evt: str, payload: dict):
    sio = _socketio()
    if not sio:
        return
    try:
        sio.emit(evt, payload, namespace="/rt")
    except Exception:
        current_app.logger.exception("[socketio] emit failed: %s", evt)

# ------------------------------------------------------------------------------
# Fare helpers
# ------------------------------------------------------------------------------
def _resolve_stop(stop_id: Optional[int]) -> Tuple[str, Optional[int]]:
    if not stop_id:
        return ("", None)
    ts = TicketStop.query.get(stop_id)
    if not ts:
        return ("", None)
    name = getattr(ts, "stop_name", "") or getattr(ts, "name", "") or ""
    try:
        seq = int(getattr(ts, "seq", None) or 0)
    except Exception:
        seq = None
    return (name, seq)

def _fare_each_from_seq(seq_o: Optional[int], seq_d: Optional[int], ptype: str) -> int:
    """
    base = 10 + max(hops-1, 0)*2; discount = round(base * 0.8)
    If sequence unknown, assume minimum base=10.
    """
    try:
        if seq_o is not None and seq_d is not None:
            hops = abs(int(seq_o) - int(seq_d))
            base = 10 + max(hops - 1, 0) * 2
        else:
            base = 10
    except Exception:
        base = 10
    return int(round(base * 0.8)) if (ptype or "").lower() == "discount" else int(base)

def _compute_totals(origin_stop_id: int, destination_stop_id: int, items: List[Dict[str, Any]]) -> Dict[str, Any]:
    rows = (
        db.session.query(TicketStop.id, TicketStop.stop_name, TicketStop.seq)
        .filter(TicketStop.id.in_([origin_stop_id, destination_stop_id]))
        .all()
    )
    m = {int(r.id): (r.stop_name or "", (int(r.seq) if r.seq is not None else None)) for r in rows}
    o_name, o_seq = m.get(int(origin_stop_id), ("", None))
    d_name, d_seq = m.get(int(destination_stop_id), ("", None))

    reg_qty = sum(int(i.get("quantity") or 0) for i in items if (i.get("passenger_type") or "regular") == "regular")
    dis_qty = sum(int(i.get("quantity") or 0) for i in items if (i.get("passenger_type") or "regular") == "discount")
    total_qty = reg_qty + dis_qty if (reg_qty + dis_qty) > 0 else 1

    reg_each = _fare_each_from_seq(o_seq, d_seq, "regular")
    dis_each = _fare_each_from_seq(o_seq, d_seq, "discount")
    total_fare = (reg_qty * reg_each) + (dis_qty * dis_each)
    if total_qty == 1 and total_fare == 0:
        total_fare = reg_each
        reg_qty = 1
        total_qty = 1

    return {
        "origin_name": o_name,
        "destination_name": d_name,
        "o_seq": o_seq,
        "d_seq": d_seq,
        "reg_qty": reg_qty,
        "dis_qty": dis_qty,
        "total_qty": total_qty,
        "reg_each": reg_each,
        "dis_each": dis_each,
        "total_fare": int(total_fare),
    }

def _primary_type_from_items(items: List[Dict[str, Any]], fallback: str = "regular") -> str:
    if not items:
        return fallback
    reg = sum(int(i.get("quantity") or 0) for i in items if (i.get("passenger_type") or "regular") == "regular")
    dis = sum(int(i.get("quantity") or 0) for i in items if (i.get("passenger_type") or "regular") == "discount")
    if reg == 0 and dis == 0:
        return fallback
    return "regular" if reg >= dis else "discount"

# ------------------------------------------------------------------------------
# Ticket helpers
# ------------------------------------------------------------------------------
def _commuter_label(ticket: TicketSale) -> str:
    if getattr(ticket, "guest", False):
        return "Guest"
    u = getattr(ticket, "user", None)
    if u:
        return f"{u.first_name} {u.last_name}"
    return "Guest"

def _payment_method_for_ticket_row(t: TicketSale) -> str:
    """
    Canonicalize a ticket's payment method as 'wallet' or 'gcash'.
    """
    try:
        if _has_column("ticket_sales", "payment_method"):
            m = (getattr(t, "payment_method", None) or "").strip().lower()
            if m in {"wallet", "gcash"}:
                return m

        if _has_column("ticket_sales", "external_ref") and getattr(t, "external_ref", None):
            return "gcash"
        if _has_column("ticket_sales", "gcash_ref") and getattr(t, "gcash_ref", None):
            return "gcash"

        if getattr(t, "user_id", None) and bool(getattr(t, "paid", False)):
            return "wallet"

        if not getattr(t, "user_id", None):
            return "gcash"

        return "wallet"
    except Exception:
        return "wallet"

def _serialize_ticket_json(t: TicketSale, origin_name: str, destination_name: str) -> dict:
    amount = int(round(float(t.price or 0)))
    img = jpg_name(amount, t.passenger_type)
    qr_url    = url_for("static", filename=f"qr/{img}", _external=True)
    qr_bg_url = f"{request.url_root.rstrip('/')}/{QR_PATH}/{img}"
    qr_link   = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)
    payload   = build_qr_payload(t, origin_name=origin_name, destination_name=destination_name)

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

def _bus_identifier_str(bus_id: Optional[int]) -> str:
    if not bus_id:
        return "BUS"
    try:
        bus_row = Bus.query.get(bus_id)
        ident = (getattr(bus_row, "identifier", None) or "").strip()
        return ident or f"BUS{int(bus_id)}"
    except Exception:
        return f"BUS{int(bus_id)}"

def _temp_reference(bus_id: Optional[int]) -> str:
    return f"{_bus_identifier_str(bus_id)}_TMP_{_tokhex(4)}"

def _gen_reference(bus_id: Optional[int], row_id: int) -> str:
    prefix = _bus_identifier_str(bus_id)
    return f"{prefix}_{int(row_id):04d}"

# ------------------------------------------------------------------------------
# Wallet helpers
# ------------------------------------------------------------------------------
def _charge_wallet_pesos(user_id: int, pesos: int, ref_ticket_id: Optional[int] = None) -> bool:
    """
    Atomic wallet deduction + ledger insert (pesos-only).
    Returns True on success (sufficient balance), False on insufficient.
    """
    if pesos <= 0:
        return True

    upd = db.session.execute(
        text("""
            UPDATE wallet_accounts
            SET balance_pesos = balance_pesos - :amt
            WHERE user_id = :uid AND balance_pesos >= :amt
        """),
        {"uid": user_id, "amt": pesos},
    )
    if upd.rowcount == 0:
        db.session.rollback()
        return False

    bal = db.session.execute(
        text("SELECT balance_pesos FROM wallet_accounts WHERE user_id = :uid"),
        {"uid": user_id},
    ).scalar()
    new_bal = int(bal or 0)

    db.session.execute(
        text("""
            INSERT INTO wallet_ledger
                (account_id, direction, event, amount_pesos, running_balance_pesos, ref_table, ref_id, created_at)
            VALUES
                (:uid, 'debit', 'ticket_purchase', :amt, :run, 'ticket_sales', :ref, NOW())
        """),
        {"uid": user_id, "amt": pesos, "run": new_bal, "ref": ref_ticket_id or 0},
    )
    return True

@pao_bp.route("/me", methods=["GET"])
@require_role("pao")
def pao_me():
    u = g.user
    bus_id = _today_bus_for_pao(u.id)
    source = "assignment"
    if not bus_id:
        bus_id = getattr(u, "assigned_bus_id", None)
        source = "legacy" if bus_id else "none"

    bus = Bus.query.get(bus_id) if bus_id else None
    name = f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or u.username

    return jsonify({
        "id": u.id,
        "name": name,
        "role": "pao",
        "bus": ({"id": bus.id, "identifier": bus.identifier} if bus else None),
        "bus_source": source,          # <— add this for easy debugging
    }), 200


@pao_bp.route("/users/scan", methods=["GET"])
@require_role("pao")
def user_qr_scan():
    token = (request.args.get("token") or "").strip()
    if not token:
        return jsonify(error="token required"), 400
    try:
        payload = verify_user_qr_token(token)
        user_id = int(payload.get("uid"))
    except (BadSignature, SignatureExpired):
        return jsonify(error="invalid or expired token"), 400

    u = User.query.get(user_id)
    if not u:
        return jsonify(error="user not found"), 404

    return jsonify(user_id=user_id, name=f"{u.first_name} {u.last_name}"), 200

@pao_bp.route("/wallet/charge", methods=["POST"])
@require_role("pao")
def pao_wallet_charge():
    """
    Debit a commuter's wallet (whole pesos) and optionally mark a ticket as paid.
    """
    data = request.get_json(silent=True) or {}

    # amount
    try:
        amount_php = float(Decimal(str(data.get("amount_php"))))
    except Exception:
        return jsonify(error="invalid amount_php"), 400
    if amount_php <= 0:
        return jsonify(error="amount must be > 0"), 400
    amount_pesos = int(round(amount_php))

    # resolve user
    token = (data.get("wallet_token") or "").strip()
    user_id = data.get("user_id")
    if token:
        if verify_wallet_token is None:
            return jsonify(error="wallet token not supported"), 400
        try:
            user_id = int(verify_wallet_token(token))
        except Exception:
            return jsonify(error="invalid wallet token"), 400
    if not user_id:
        return jsonify(error="missing wallet_token or user_id"), 400
    user_id = int(user_id)

    ticket_id = data.get("ticket_id")
    t = None

    try:
        # lock wallet row
        row = db.session.execute(
            text(
                "SELECT user_id, COALESCE(balance_pesos,0) AS balance_pesos "
                "FROM wallet_accounts WHERE user_id=:uid FOR UPDATE"
            ),
            {"uid": user_id},
        ).mappings().first()
        if not row:
            db.session.rollback()
            return jsonify(error="wallet not found"), 404
        balance_pesos = int(row["balance_pesos"])

        # optional: lock ticket & duplicate guard
        if ticket_id is not None:
            tid = int(ticket_id)
            t = (
                TicketSale.query
                .filter_by(id=tid)
                .with_for_update()
                .first()
            )
            if not t:
                db.session.rollback()
                return jsonify(error="ticket not found"), 404
            if t.paid:
                db.session.rollback()
                return jsonify(error="already paid"), 409

            if _has_column("ticket_sales", "payment_method"):
                if (getattr(t, "payment_method", None) or "wallet") != "wallet":
                    db.session.rollback()
                    return jsonify(error="cannot wallet-charge a non-wallet ticket"), 409

            dup_id = db.session.execute(
                text("""
                    SELECT id
                    FROM wallet_ledger
                    WHERE account_id = :aid
                      AND direction = 'debit'
                      AND event = 'ride'
                      AND ref_table = 'ticket_sale'
                      AND ref_id = :rid
                    LIMIT 1
                    FOR UPDATE
                """),
                {"aid": user_id, "rid": tid},
            ).scalar()
            if dup_id:
                db.session.rollback()
                return jsonify(error="already charged"), 409

        # funds check
        if balance_pesos < amount_pesos:
            if t is not None and not t.paid:
                try:
                    db.session.delete(t)
                    db.session.commit()
                except Exception:
                    db.session.rollback()
            return jsonify(
                error="insufficient balance",
                balance_php=float(balance_pesos),
                required_php=float(amount_pesos),
            ), 402

        # apply debit
        new_balance = balance_pesos - amount_pesos
        db.session.execute(
            text("UPDATE wallet_accounts SET balance_pesos=:bal WHERE user_id=:uid"),
            {"bal": new_balance, "uid": user_id},
        )

        db.session.execute(
            text("""
                INSERT INTO wallet_ledger
                    (account_id, direction, event, amount_pesos, running_balance_pesos,
                     ref_table, ref_id, created_at)
                VALUES
                    (:aid, 'debit', 'ride', :amt, :run, :rt, :rid, NOW())
            """),
            {
                "aid": user_id,
                "amt": amount_pesos,
                "run": new_balance,
                "rt": ("ticket_sale" if ticket_id is not None else None),
                "rid": (int(ticket_id) if ticket_id is not None else None),
            },
        )

        if t is not None:
            t.paid = True

        db.session.commit()

        # push notify (best-effort)
        try:
            sent_at = int(time.time() * 1000)
            payload = {
                "type": "wallet_debit",
                "user_id": int(user_id),
                "amount_php": float(amount_pesos),
                "new_balance_php": float(new_balance),
                "deeplink": "/commuter/wallet",
                "sentAt": sent_at,
            }
            if ticket_id is not None:
                payload["ticketId"] = int(ticket_id)

            title = "✅ Payment confirmed" if ticket_id is not None else "💳 Wallet charged"
            body = (
                f"Ref #{int(ticket_id)} • ₱{amount_pesos:.2f}"
                if ticket_id is not None
                else f"₱{amount_pesos:.2f} deducted • New ₱{new_balance:.2f}"
            )

            push_to_user(
                db, DeviceToken, user_id,
                title, body, payload,
                channelId="payments", priority="high", ttl=600,
            )
        except Exception:
            current_app.logger.exception("[push] wallet-debit notify failed")

        return jsonify(ok=True, user_id=user_id, new_balance_php=float(new_balance)), 200

    except IntegrityError:
        db.session.rollback()
        return jsonify(error="duplicate charge"), 409
    except Exception:
        current_app.logger.exception("[PAO:wallet_charge] unexpected failure")
        db.session.rollback()
        return jsonify(error="internal error"), 500

@pao_bp.route("/wallet/resolve", methods=["POST", "GET"])
@require_role("pao")
def wallet_resolve():
    """
    Resolve a scanned QR into a commuter. Accepts:
    { "token": "...", "wallet_token": "...", "raw": "full URL or token", "autopay": true|false }
    """
    rid = request.headers.get("X-Request-ID") or f"resolve-{int(time.time()*1000)}"
    data = request.get_json(silent=True) or {}

    raw = (
        data.get("raw")
        or data.get("token")
        or data.get("wallet_token")
        or request.args.get("token")
        or request.args.get("wallet_token")
        or ""
    ).strip()
    qp_token = None
    autopay = bool(data.get("autopay"))

    # parse URL if needed
    try:
        from urllib.parse import urlparse, parse_qs
        u = urlparse(raw)
        if u.scheme and u.netloc:
            qs = parse_qs(u.query)
            qp_token = (
                qs.get("wallet_token", [None])[0]
                or qs.get("token", [None])[0]
                or qs.get("wlt", [None])[0]
            )
            autopay = autopay or (qs.get("autopay", ["0"])[0] == "1")
    except Exception:
        pass

    token = qp_token or raw or (data.get("token") or data.get("wallet_token") or "").strip()
    if not token:
        return jsonify(error="missing token"), 400

    dbg = {"raw": raw, "parsed_token": token} if _debug_enabled() else {}
    user_id = None
    token_type = None
    expired_hint = False

    uid1, grace1, err1 = _try_user_qr_soft(token)
    if uid1:
        user_id = uid1
        token_type = "user_qr"
        if _debug_enabled():
            dbg["user_qr_grace"] = bool(grace1)
    elif err1 == "expired":
        expired_hint = True
    elif err1 == "invalid":
        if _debug_enabled():
            dbg["user_qr_error"] = "invalid"

    if user_id is None:
        uid2, grace2, err2 = _try_wallet_rot_soft(token)
        if uid2:
            user_id = uid2
            token_type = "wallet_token_rot"
            if _debug_enabled():
                dbg["rot_grace"] = bool(grace2)
        elif err2 == "expired":
            expired_hint = True

    if user_id is None and verify_wallet_token:
        try:
            uid = int(verify_wallet_token(token))
            if uid > 0:
                user_id = uid
                token_type = "wallet_token"
        except Exception as e:
            if _debug_enabled():
                dbg["wallet_token_error"] = f"{type(e).__name__}: {e}"
    elif user_id is None and verify_wallet_token is None:
        if _debug_enabled():
            dbg["wallet_token_error"] = "verify_wallet_token unavailable"

    if not user_id:
        status = 410 if expired_hint else 422
        current_app.logger.warning("[PAO:resolve][%s] invalid/expired token", rid)
        out = {"valid": False, "error": ("expired" if expired_hint else "invalid")}
        if _debug_enabled():
            out["debug"] = dbg
        return jsonify(out), status

    user = User.query.get(user_id)
    if not user:
        return jsonify(valid=False, error="user not found"), 404

    balance_pesos = int(
        db.session.execute(
            text("SELECT COALESCE(balance_pesos,0) FROM wallet_accounts WHERE user_id=:uid"),
            {"uid": user_id},
        ).scalar() or 0
    )

    payload = {
        "valid": True,
        "token_type": token_type,
        "autopay": bool(autopay),
        "user": {"id": user.id, "name": f"{user.first_name} {user.last_name}"},
        "user_id": int(user.id),
        "name": f"{user.first_name} {user.last_name}",
        "balance_php": float(balance_pesos),
    }
    resp = jsonify(payload)

    if _debug_enabled():
        resp.headers["X-Resolve-Raw"] = raw[:256]
        resp.headers["X-Resolve-Token"] = (token or "")[:128]
        resp.headers["X-Resolve-Type"] = token_type or ""
        resp.headers["X-Resolve-Autopay"] = "1" if autopay else "0"
        resp.headers["X-Resolve-UserId"] = str(user_id)
        if dbg.get("user_qr_grace"):
            resp.headers["X-Resolve-Grace"] = "1"
        current_app.logger.info(
            "[PAO:resolve][%s] ok user=%s type=%s autopay=%s grace=%s",
            rid, user_id, token_type, autopay, bool(dbg.get("user_qr_grace")),
        )

    return resp, 200



@pao_bp.route("/wallet/<int:user_id>/overview", methods=["GET"])
@require_role("pao")
def wallet_overview(user_id: int):
    """
    Lightweight wallet overview for PAO. Never 500s: on any internal error,
    return a safe fallback payload with balance 0 and empty arrays.
    Supports a fast path when the client only needs the balance.
    """
    try:
        limit_topups = request.args.get("limit_topups", type=int) or 10
        limit_ledger = request.args.get("limit_ledger", type=int) or 15
        include_today = (request.args.get("include_today", "1") != "0")

        row = db.session.execute(
            text(
                "SELECT user_id, COALESCE(balance_pesos,0) AS balance_pesos "
                "FROM wallet_accounts WHERE user_id=:uid"
            ),
            {"uid": user_id},
        ).mappings().first()

        balance_pesos = int((row or {}).get("balance_pesos", 0))
        account_id = int((row or {}).get("user_id", 0) or 0) or None

        # Fast-path: only balance requested
        if (limit_topups <= 0 and limit_ledger <= 0 and not include_today):
            return jsonify(
                user_id=int(user_id),
                balance_php=float(balance_pesos),
                recent_topups=[],
                recent_ledger=[],
                pao_today=None,
            ), 200

        # Recent topups
        topups: List[Dict[str, Any]] = []
        if account_id and limit_topups > 0:
            trs = db.session.execute(
                text("""
                    SELECT id, amount_pesos, status, COALESCE(pao_id,0) AS pao_id, created_at
                    FROM wallet_topups
                    WHERE user_id = :uid
                    ORDER BY id DESC
                    LIMIT :lim
                """),
                {"uid": user_id, "lim": limit_topups},
            ).mappings().all()

            for r in trs:
                topups.append({
                    "id": int(r["id"]),
                    "amount_php": float(int(r["amount_pesos"] or 0)),
                    "status": (r["status"] or ""),
                    "pao_id": int(r["pao_id"] or 0) or None,
                    "created_at": _iso_utc(r["created_at"]),
                })

        # Recent ledger
        ledger: List[Dict[str, Any]] = []
        if account_id and limit_ledger > 0:
            lrs = db.session.execute(
                text("""
                    SELECT id, direction, event, amount_pesos, running_balance_pesos,
                           ref_table, ref_id, created_at
                    FROM wallet_ledger
                    WHERE account_id = :uid
                    ORDER BY id DESC
                    LIMIT :lim
                """),
                {"uid": user_id, "lim": limit_ledger},
            ).mappings().all()

            for r in lrs:
                ledger.append({
                    "id": int(r["id"]),
                    "direction": r["direction"],
                    "event": r["event"],
                    "amount_php": float(int(r["amount_pesos"] or 0)),
                    "running_balance_php": float(int(r["running_balance_pesos"] or 0)),
                    "ref": {"table": r["ref_table"], "id": int(r["ref_id"] or 0) or None},
                    "created_at": _iso_utc(r["created_at"]),
                })

        # PAO "today" usage stats (optional)
        pao_today = None
        if include_today:
            day_local = dt.datetime.now(_MNL).date()
            start_dt, end_dt = _local_day_bounds_utc(day_local)

            used_sum = db.session.execute(
                text("""
                    SELECT COALESCE(SUM(amount_pesos), 0)
                    FROM wallet_topups
                    WHERE pao_id = :pid
                      AND status = 'succeeded'
                      AND created_at >= :s AND created_at < :e
                """),
                {"pid": g.user.id, "s": start_dt, "e": end_dt},
            ).scalar() or 0

            used_count = db.session.execute(
                text("""
                    SELECT COUNT(*)
                    FROM wallet_topups
                    WHERE pao_id = :pid
                      AND status = 'succeeded'
                      AND created_at >= :s AND created_at < :e
                """),
                {"pid": g.user.id, "s": start_dt, "e": end_dt},
            ).scalar() or 0

            pao_today = {"count": int(used_count), "sum_php": float(used_sum)}

        return jsonify(
            user_id=int(user_id),
            balance_php=float(balance_pesos),
            recent_topups=topups,
            recent_ledger=ledger,
            pao_today=pao_today,
        ), 200

    except Exception:
        current_app.logger.exception("[pao:wallet_overview] failed")
        # Never break the client flow; return a safe fallback
        return jsonify(
            user_id=int(user_id),
            balance_php=0.0,
            recent_topups=[],
            recent_ledger=[],
            pao_today=None,
            error="temporary_unavailable",
        ), 200


@pao_bp.route("/wallet/<int:user_id>/balance", methods=["GET"])
@require_role("pao")
def wallet_balance(user_id: int):
    """
    Ultra-light balance fetch used by the PAO checkout UI.
    Always 200; never 500. Adds no-store to avoid stale caches.
    """
    try:
        bal = db.session.execute(
            text("SELECT COALESCE(balance_pesos,0) FROM wallet_accounts WHERE user_id=:uid"),
            {"uid": user_id},
        ).scalar() or 0
        resp = jsonify(user_id=int(user_id), balance_php=float(bal))
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        return resp, 200
    except Exception:
        # graceful fallback so UI can choose the "proceed anyway" path
        resp = jsonify(user_id=int(user_id), balance_php=0.0, stale=True)
        resp.headers["Cache-Control"] = "no-store, max-age=0"
        return resp, 200


@pao_bp.route("/tickets/<int:ticket_id>/receipt.png", methods=["GET"])
def pao_ticket_receipt_image(ticket_id: int):
    return redirect(url_for("commuter.commuter_ticket_image", ticket_id=ticket_id), code=302)

@pao_bp.route("/reset-live-stats", methods=["POST"])
@require_role("pao")
def reset_live_stats():
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    bus_row = Bus.query.get(bus_id)
    bus_identifier = (bus_row.identifier or f"bus-{bus_id:02d}") if bus_row else f"bus-{bus_id:02d}"

    topic = f"device/{bus_identifier}/cmd/reset"
    try:
        publish(topic, {"reset": True})
        current_app.logger.info(f"[PAO] reset request → {topic}")
        return jsonify(ok=True), 202
    except Exception as e:
        current_app.logger.exception("reset-live-stats publish failed")
        return jsonify(error=str(e)), 500

@pao_bp.route("/summary", methods=["GET"])
@require_role("pao")
def pao_summary():
    date_str = request.args.get("date")
    try:
        day_local = dt.datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else dt.datetime.now(_MNL).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    start_dt, end_dt = _local_day_bounds_utc(day_local)
    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    total = (
        db.session.query(func.count(TicketSale.id))
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at >= start_dt,
            TicketSale.created_at <  end_dt,
        )
        .scalar() or 0
    )

    paid_count = (
        db.session.query(func.count(TicketSale.id))
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at >= start_dt,
            TicketSale.created_at <  end_dt,
            TicketSale.paid.is_(True),
        )
        .scalar() or 0
    )

    revenue_total = (
        db.session.query(func.coalesce(func.sum(TicketSale.price), 0.0))
        .filter(
            TicketSale.bus_id == bus_id,
            TicketSale.created_at >= start_dt,
            TicketSale.created_at <  end_dt,
            TicketSale.paid.is_(True),
        )
        .scalar() or 0.0
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

@pao_bp.route("/stops", methods=["GET"])
@require_role("pao")
def list_stops():
    bus_id = request.args.get("bus_id", type=int) or _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus today"), 400

    rows = (
        TicketStop.query
        .filter(TicketStop.bus_id == bus_id)
        .order_by(TicketStop.seq.asc(), TicketStop.id.asc())
        .all()
    )
    return jsonify([
        {"id": r.id, "name": r.stop_name, "seq": int(getattr(r, "seq", 0) or 0)}
        for r in rows
    ]), 200

@pao_bp.route("/recent-tickets", methods=["GET"])
@require_role("pao")
def recent_tickets():
    limit = request.args.get("limit", type=int) or 5
    scope = (request.args.get("scope") or "mine").lower()
    only_mine = scope not in ("bus", "all")

    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    q = (
        TicketSale.query.options(joinedload(TicketSale.user))
        .filter(TicketSale.bus_id == bus_id, TicketSale.paid.is_(True))
    )
    if only_mine:
        q = q.filter(TicketSale.issued_by == g.user.id)

    rows = q.order_by(TicketSale.id.desc()).limit(limit).all()

    out = []
    for t in rows:
        out.append({
            "id": t.id,
            "referenceNo": t.reference_no,
            "commuter": _commuter_label(t),
            "fare": f"{float(t.price):.2f}",
            "paid": bool(t.paid),
            "payment_method": _payment_method_for_ticket_row(t),
            "created_at": _iso_utc(t.created_at),
            "time": _as_mnl(t.created_at).strftime("%I:%M %p").lstrip("0").lower(),
            "voided": bool(getattr(t, "voided", False)),
        })
    return jsonify(out), 200

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
    bus_id = _current_bus_id()
    date_str = request.args.get("date")
    if not bus_id or not date_str:
        return jsonify(error="PAO is not scheduled today or date is missing"), 400

    svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
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
            hops = abs((o.seq or 0) - (d.seq or 0))
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

    except Exception:
        current_app.logger.exception("preview_ticket failed")
        return jsonify(error="internal error"), 500

@pao_bp.route("/tickets", methods=["POST"])
@require_role("pao")
def pao_create_ticket():
    """
    Create a wallet or GCash ticket (supports group via items[]).
    Adds idempotency:
      - Accepts X-Idempotency-Key header or 'idempotency_key' in JSON.
      - Reuses 'external_ref' column to store the key for BOTH wallet & gcash.
      - Returns existing ticket (200) on duplicates.
    """
    data = request.get_json(silent=True) or {}

    # Required stops
    try:
        origin_stop_id = int(data.get("origin_stop_id") or data.get("origin_stop_time_id") or 0)
        destination_stop_id = int(data.get("destination_stop_id") or data.get("destination_stop_time_id") or 0)
    except Exception:
        return jsonify(error="invalid origin/destination id"), 400

    # Normalize items (group)
    raw_items = data.get("items") or []
    if not isinstance(raw_items, list):
        raw_items = []
    items: List[Dict[str, Any]] = []
    for raw in raw_items:
        try:
            pt = (raw.get("passenger_type") or raw.get("type") or "regular").strip().lower()
            if pt not in {"regular", "discount"}:
                pt = "regular"
            qty = int(raw.get("quantity") or raw.get("qty") or 0)
        except Exception:
            continue
        if qty > 0:
            items.append({"passenger_type": pt, "quantity": qty})
    if not items:
        items = [{"passenger_type": (data.get("primary_type") or "regular"), "quantity": 1}]

    # Payment method + fields
    payment_method = (data.get("payment_method") or "").strip().lower()
    if payment_method not in {"wallet", "gcash"}:
        return jsonify(error="unsupported payment_method"), 400

    commuter_id = data.get("commuter_id")
    try:
        commuter_id = int(commuter_id) if commuter_id is not None else None
    except Exception:
        commuter_id = None

    primary_type = (data.get("primary_type") or "").strip().lower() or _primary_type_from_items(items)
    is_gcash = (payment_method == "gcash")
    gcash_paid = bool(data.get("gcash_paid")) if "gcash_paid" in data else is_gcash
    gcash_ref = (data.get("gcash_ref") or data.get("external_ref") or "").strip() or None

    # Idempotency key (works for both methods)
    idem = (
        (request.headers.get("X-Idempotency-Key") or "").strip()
        or (data.get("idempotency_key") or "").strip()
        or gcash_ref  # reuse PSP reference when available
    ) or None

    # Validate requireds per method
    if payment_method == "wallet":
        if not (commuter_id and commuter_id > 0):
            return jsonify(error="commuter_id required for wallet payment"), 400
    if payment_method == "gcash":
        if not gcash_ref:
            return jsonify(error="gcash_ref (or external_ref) required for GCash tickets"), 400
        if "gcash_paid" in data and not bool(gcash_paid):
            return jsonify(error="gcash tickets must be created as paid once PSP ref is present"), 400

    if not (origin_stop_id and destination_stop_id):
        return jsonify(error="origin_stop_id and destination_stop_id are required"), 400

    # Compute fare totals
    totals = _compute_totals(origin_stop_id, destination_stop_id, items)
    total_fare = int(totals["total_fare"] or 0)
    reg_qty    = int(totals["reg_qty"] or 0)
    dis_qty    = int(totals["dis_qty"] or 0)
    total_qty  = int(totals["total_qty"] or 1)
    if total_fare <= 0:
        return jsonify(error="calculated fare is zero"), 400

    # Caller PAO + bus assignment
    pao_id  = _who_is_pao_user_id()
    bus_id  = _bus_for_pao() or data.get("bus_id")
    try:
        bus_id = int(bus_id) if bus_id else None
    except Exception:
        bus_id = None

    # Idempotency pre-check (return existing 200)
    if _has_column("ticket_sales", "external_ref") and (idem is not None):
        existing = (
            TicketSale.query
            .filter_by(bus_id=bus_id, external_ref=idem)
            .order_by(TicketSale.id.desc())
            .first()
        )
        if existing:
            origin_name, _ = _resolve_stop(getattr(existing, "origin_stop_time_id", None))
            destination_name, _ = _resolve_stop(getattr(existing, "destination_stop_time_id", None))
            return jsonify(_serialize_ticket_json(existing, origin_name, destination_name)), 200

    can_group = _has_column("ticket_sales", "is_group")

    try:
        # Create ticket row
        t = TicketSale()
        t.user_id = int(commuter_id) if (commuter_id and commuter_id > 0) else None
        t.bus_id  = int(bus_id) if bus_id else None

        setattr(t, "origin_stop_time_id", int(origin_stop_id))
        setattr(t, "destination_stop_time_id", int(destination_stop_id))

        t.passenger_type = (primary_type or "regular")
        t.price = int(total_fare)
        t.paid  = bool(gcash_paid or (payment_method == "wallet"))

        if _has_column("ticket_sales", "payment_method"):
            t.payment_method = payment_method

        # Persist idempotency/external refs (for both wallet & gcash)
        for col, val in (("external_ref", (idem or gcash_ref)), ("gcash_ref", gcash_ref)):
            if val and _has_column("ticket_sales", col):
                setattr(t, col, val)

        if _has_column("ticket_sales", "issued_by"):
            t.issued_by = int(pao_id) if pao_id else None

        # Group flags (if table supports it)
        if can_group and total_qty > 1:
            setattr(t, "is_group", True)
            setattr(t, "group_regular", int(reg_qty))
            setattr(t, "group_discount", int(dis_qty))
        elif can_group:
            setattr(t, "is_group", False)
            setattr(t, "group_regular", None)
            setattr(t, "group_discount", None)

        # Temporary reference, then final after flush
        if _has_column("ticket_sales", "reference_no") and not getattr(t, "reference_no", None):
            t.reference_no = _temp_reference(bus_id)

        db.session.add(t)
        db.session.flush()  # get t.id

        if _has_column("ticket_sales", "reference_no"):
            try:
                t.reference_no = _gen_reference(bus_id, t.id)
            except Exception:
                t.reference_no = f"BUS{int(bus_id or 0)}_{int(t.id):04d}"

        # Wallet charge (atomic)
        if payment_method == "wallet":
            ok = _charge_wallet_pesos(int(commuter_id), int(total_fare), ref_ticket_id=int(t.id))
            if not ok:
                # Read current balance to report shortfall in one go
                bal = db.session.execute(
                    text("SELECT COALESCE(balance_pesos,0) FROM wallet_accounts WHERE user_id=:uid"),
                    {"uid": int(commuter_id)},
                ).scalar() or 0
                db.session.rollback()
                return jsonify(
                    error="insufficient_funds",
                    required_php=int(total_fare),
                    balance_php=int(bal),
                ), 402
            t.paid = True

        db.session.commit()

        # Build response
        origin_name = totals["origin_name"]
        destination_name = totals["destination_name"]

        # Group response (head only)
        if total_qty > 1:
            head_item: Dict[str, Any] = {
                "id": int(t.id),
                "referenceNo": getattr(t, "reference_no", None) or f"{t.id}",
                "origin": origin_name,
                "destination": destination_name,
                "passengerType": (primary_type or "regular"),
                "fare": int(total_fare),
                "paid": bool(t.paid),
                "commuter": None,
                "payment_method": payment_method,
            }
            try:
                if t.user_id:
                    u = User.query.get(int(t.user_id))
                    if u:
                        head_item["commuter"] = f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or (u.username or f"User #{u.id}")
            except Exception:
                pass

            return jsonify({
                "count": int(total_qty),
                "total_fare": int(total_fare),
                "items": [head_item],
            }), 201

        # Solo ticket response
        commuter_name = None
        if t.user_id:
            try:
                u = User.query.get(int(t.user_id))
                if u:
                    commuter_name = f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or (u.username or f"User #{u.id}")
            except Exception:
                pass

        return jsonify({
            "id": int(t.id),
            "referenceNo": getattr(t, "reference_no", None) or f"{t.id}",
            "origin": origin_name,
            "destination": destination_name,
            "passengerType": (primary_type or "regular").lower(),
            "fare": int(total_fare),
            "paid": bool(t.paid),
            "commuter": commuter_name or ("Guest" if not t.user_id else None),
            "payment_method": payment_method,
            # QR fields omitted for PAO JSON (client can fetch full receipt later)
            "qr": None,
            "qr_link": None,
            "qr_bg_url": None,
        }), 201

    except IntegrityError:
        # If you made (bus_id, external_ref) UNIQUE, concurrent duplicates land here
        db.session.rollback()
        if _has_column("ticket_sales", "external_ref") and (idem or gcash_ref):
            dup = (
                TicketSale.query
                .filter_by(bus_id=bus_id, external_ref=(idem or gcash_ref))
                .order_by(TicketSale.id.desc())
                .first()
            )
            if dup:
                origin_name, _ = _resolve_stop(getattr(dup, "origin_stop_time_id", None))
                destination_name, _ = _resolve_stop(getattr(dup, "destination_stop_time_id", None))
                return jsonify(_serialize_ticket_json(dup, origin_name, destination_name)), 200
        return jsonify(error="duplicate"), 409

    except Exception as e:
        current_app.logger.exception("Failed to create ticket: %s", e)
        db.session.rollback()
        return jsonify(error="failed to create ticket"), 500


@pao_bp.route("/tickets/<int:ticket_id>/void", methods=["PATCH"])
@require_role("pao")
def void_ticket(ticket_id: int):
    """
    Void a ticket and (if paid by wallet) refund the full amount to the commuter wallet.
    """
    data = request.get_json(silent=True) or {}
    want_void = bool(data.get("voided"))
    reason = (data.get("reason") or "").strip()
    if not want_void:
        return jsonify(error="set voided=true to proceed"), 400
    if not reason:
        return jsonify(error="void reason is required"), 400
    reason = reason[:200]

    t = (
        TicketSale.query.options(joinedload(TicketSale.user), joinedload(TicketSale.bus))
        .filter(TicketSale.id == ticket_id)
        .with_for_update()
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    caller_bus_id = getattr(getattr(g, "user", None), "assigned_bus_id", None)
    if caller_bus_id and t.bus_id and int(caller_bus_id) != int(t.bus_id):
        return jsonify(error="not allowed to void tickets from another bus"), 403

    if bool(getattr(t, "voided", False)):
        return jsonify(error="already voided", id=t.id, voided=True), 409

    amount_pesos = int(round(float(t.price or 0)))
    refunded = 0

    meth = (getattr(t, "payment_method", None) or "wallet") if _has_column("ticket_sales", "payment_method") else "wallet"
    refund_to_wallet = (bool(t.paid) and t.user_id and (meth == "wallet"))

    if refund_to_wallet:
        dup = db.session.execute(
            text("""
                SELECT id FROM wallet_ledger
                WHERE account_id = :aid
                  AND direction = 'credit'
                  AND event = 'refund'
                  AND ref_table = 'ticket_sale'
                  AND ref_id = :rid
                LIMIT 1
                FOR UPDATE
            """),
            {"aid": int(t.user_id), "rid": int(t.id)},
        ).scalar()

        if not dup:
            db.session.execute(
                text("UPDATE wallet_accounts SET balance_pesos = balance_pesos + :amt WHERE user_id = :uid"),
                {"amt": amount_pesos, "uid": int(t.user_id)},
            )

            new_balance = int(
                db.session.execute(
                    text("SELECT COALESCE(balance_pesos,0) FROM wallet_accounts WHERE user_id=:uid"),
                    {"uid": int(t.user_id)},
                ).scalar() or 0
            )

            db.session.execute(
                text("""
                    INSERT INTO wallet_ledger
                        (account_id, direction, event, amount_pesos, running_balance_pesos,
                         ref_table, ref_id, created_at)
                    VALUES
                        (:aid, 'credit', 'refund', :amt, :run, 'ticket_sale', :rid, NOW())
                """),
                {"aid": int(t.user_id), "amt": amount_pesos, "run": new_balance, "rid": int(t.id)},
            )

            refunded = amount_pesos

            try:
                _publish_user_wallet(
                    int(t.user_id),
                    new_balance_pesos=int(new_balance),
                    event="refund",
                    ticket_id=int(t.id),
                )
            except Exception:
                current_app.logger.exception("[void] mqtt publish failed")

            try:
                sent_at = int(time.time() * 1000)
                push_to_user(
                    db, DeviceToken, int(t.user_id),
                    "❌ Ticket voided",
                    f"Ref {t.reference_no} • Refund ₱{amount_pesos:.2f}",
                    {
                        "type": "refund",
                        "ticketId": int(t.id),
                        "amount_php": float(amount_pesos),
                        "new_balance_php": float(new_balance),
                        "deeplink": "/commuter/wallet",
                        "sentAt": sent_at,
                    },
                    channelId="payments", priority="high", ttl=600,
                )
            except Exception:
                current_app.logger.exception("[void] push failed")

    t.paid = False
    setattr(t, "voided", True)
    setattr(t, "void_reason", reason)
    setattr(t, "voided_at", dt.datetime.utcnow())
    setattr(t, "voided_by", getattr(g, "user", None).id if getattr(g, "user", None) else None)
    try:
        if hasattr(TicketSale, "status"):
            setattr(t, "status", "voided")
    except Exception:
        pass

    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[void] commit failed")
        return jsonify(error="internal error"), 500

    # Publish updated "paid count" to the device for *today*
    try:
        start = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end   = dt.datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=999999)
        cnt = (
            TicketSale.query.filter_by(bus_id=t.bus_id, paid=True)
            .filter(TicketSale.created_at.between(start, end))
            .count()
        )
        if t.bus and t.bus.identifier:
            topic = f"device/{t.bus.identifier}/fare"
            publish(topic, {"paid": cnt})
            current_app.logger.info(f"[void] MQTT fare update → {topic}: {cnt}")
    except Exception:
        current_app.logger.exception("[void] failed publishing fare update")

    return jsonify(id=t.id, voided=True, refunded_php=float(refunded), reason=reason), 200

@pao_bp.route("/tickets/<int:ticket_id>", methods=["PATCH"])
@require_role("pao")
def mark_ticket_paid(ticket_id: int):
    """
    Toggle GCash ticket payment. Wallet tickets must be paid via /pao/wallet/charge.
    """
    data = request.get_json(silent=True) or {}
    paid = bool(data.get("paid"))

    ticket = (
        TicketSale.query.options(joinedload(TicketSale.bus), joinedload(TicketSale.user))
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not ticket:
        return jsonify(error="ticket not found"), 404

    if bool(getattr(ticket, "voided", False)):
        return jsonify(error="cannot mark a voided ticket paid"), 409

    if _has_column("ticket_sales", "payment_method"):
        meth = (getattr(ticket, "payment_method", None) or "wallet")
        if meth == "wallet" and paid and not bool(ticket.paid):
            return jsonify(error="wallet tickets can only be paid via wallet charge"), 409

    was_paid = bool(ticket.paid)
    ticket.paid = 1 if paid else 0

    try:
        db.session.commit()

        start = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end   = dt.datetime.utcnow().replace(hour=23, minute=59, second=59, microsecond=999999)
        cnt = (
            TicketSale.query.filter_by(bus_id=ticket.bus_id, paid=True)
            .filter(TicketSale.created_at.between(start, end))
            .count()
        )
        if ticket.bus and ticket.bus.identifier:
            topic = f"device/{ticket.bus.identifier}/fare"
            publish(topic, {"paid": cnt})
            current_app.logger.info(f"MQTT fare update → {topic}: {cnt}")

        if (not was_paid) and bool(ticket.paid) and ticket.user_id:
            try:
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
def update_ticket(ticket_id: int):
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

@pao_bp.route("/tickets", methods=["GET"])
@require_role("pao")
def list_tickets():
    scope = (request.args.get("scope") or "mine").lower()
    only_mine = scope not in ("bus", "all")

    date_str = request.args.get("date")
    try:
        day_local = (
            dt.datetime.strptime(date_str, "%Y-%m-%d").date()
            if date_str else
            dt.datetime.now(_MNL).date()
        )
    except ValueError:
        return jsonify(error="invalid date"), 400
    start_dt, end_dt = _local_day_bounds_utc(day_local)

    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    where_extra = " AND issued_by = :pao " if only_mine else " "

    groups = db.session.execute(
        text(f"""
            SELECT
              MIN(id)  AS head_id,
              COUNT(*) AS qty,
              SUM(CAST(price AS SIGNED)) AS total_pesos
            FROM ticket_sales
            WHERE bus_id = :bus
              AND created_at >= :s AND created_at < :e
              AND (paid = 1 OR COALESCE(voided, 0) = 1)
              {where_extra}
            GROUP BY
              COALESCE(
                CAST(batch_id AS CHAR),
                CONCAT(
                  issued_by,'|',
                  IFNULL(origin_stop_time_id,0),'|',
                  IFNULL(destination_stop_time_id,0),'|',
                  DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s')
                )
              )
            ORDER BY head_id DESC
        """),
        {"bus": bus_id, "s": start_dt, "e": end_dt, "pao": int(g.user.id)}
    ).mappings().all()

    if not groups:
        return jsonify([]), 200

    head_ids = [int(r["head_id"]) for r in groups]
    heads = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id.in_(head_ids))
        .all()
    )
    head_map = {t.id: t for t in heads}

    out = []
    for r in groups:
        head = head_map.get(int(r["head_id"]))
        if not head:
            continue

        # Origin/Destination names
        if head.origin_stop_time:
            origin_name = head.origin_stop_time.stop_name
        else:
            ts = TicketStop.query.get(getattr(head, "origin_stop_time_id", None))
            origin_name = ts.stop_name if ts else ""

        if head.destination_stop_time:
            destination_name = head.destination_stop_time.stop_name
        else:
            tsd = TicketStop.query.get(getattr(head, "destination_stop_time_id", None))
            destination_name = tsd.stop_name if tsd else ""

        is_void = bool(getattr(head, "voided", False))
        out.append({
            "id": head.id,
            "referenceNo": head.reference_no,
            "commuter": _commuter_label(head),
            "time": _as_mnl(head.created_at).strftime("%I:%M %p").lstrip("0").lower(),
            "date": _as_mnl(head.created_at).strftime("%B %d, %Y"),
            "origin": origin_name,
            "destination": destination_name,
            "fare": f"{float(r['total_pesos'] or 0):.2f}",
            "paid": (bool(getattr(head, "paid", True)) and not is_void),
            "passengers": int(r["qty"] or 0),
            "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=head.id, _external=True),
            "voided": is_void,
        })

    return jsonify(out), 200

@pao_bp.route("/commuters", methods=["GET"])
@require_role("pao")
def list_commuters():
    users = User.query.filter_by(role="commuter").order_by(User.first_name, User.last_name).all()
    return jsonify([{"id": u.id, "name": f"{u.first_name} {u.last_name}"} for u in users]), 200

# ------------------------------------------------------------------------------
# Announcements (broadcast) CRUD
# ------------------------------------------------------------------------------
def _ann_json(ann: Announcement) -> dict:
    u = User.query.get(ann.created_by)

    bus_id = _today_bus_for_pao(ann.created_by)
    if not bus_id and u:
        try:
            bus_id = int(getattr(u, "assigned_bus_id", None) or 0) or None
        except Exception:
            bus_id = None

    bus_row = Bus.query.get(bus_id) if bus_id else None
    bus_identifier = ((bus_row.identifier or f"bus-{int(bus_row.id):02d}") if bus_row else "—")

    return {
        "id": ann.id,
        "message": ann.message,
        "timestamp": _iso_utc(ann.timestamp),
        "created_by": ann.created_by,
        "author_name": f"{(u.first_name or '')} {(u.last_name or '')}".strip() if u else "",
        "bus": bus_identifier,
    }


@pao_bp.route("/broadcast", methods=["POST"])
@require_role("pao")
def broadcast():
    # payload
    data = request.get_json(silent=True) or {}
    message = (data.get("message") or "").strip()
    if not message:
        return jsonify(error="message is required"), 400

    # figure out service day (Manila)
    svc_day = dt.datetime.now(_MNL).date()
    raw_date = (data.get("date") or "").strip()
    when     = (data.get("when") or data.get("for") or "").strip().lower()
    try:
        if raw_date:
            svc_day = dt.datetime.strptime(raw_date, "%Y-%m-%d").date()
    
    except ValueError:
        return jsonify(error="invalid date (use YYYY-MM-DD)"), 400

    # resolve bus **for that day**, fall back to user's static assignment
    bus_id = _bus_for_pao_on(svc_day, int(g.user.id)) or getattr(g.user, "assigned_bus_id", None)
    if not bus_id:
        return jsonify(error=f"PAO has no assigned bus for {svc_day.isoformat()}"), 400

    # put the announcement timestamp on the chosen Manila-local day
    target_ts = _utc_from_local_date(svc_day)

    try:
        ann = Announcement(message=message, created_by=g.user.id)
        ann.timestamp = target_ts
        ann.bus_id = int(bus_id)  # persist the bus used

        ann.timestamp = target_ts
        db.session.add(ann)
        db.session.commit()

        author_name = f"{(g.user.first_name or '').strip()} {(g.user.last_name or '').strip()}".strip() or (getattr(g.user, "username", "") or "")
        out = {
            "id": ann.id,
            "message": ann.message,
            "timestamp": _iso_utc(ann.timestamp),
            "created_by": g.user.id,
            "author_name": author_name,
            "bus": _bus_identifier_str(bus_id),
        }

        _emit_announcement("announcement:new", out)
        _emit_announcement("announcement:created", out)
        return jsonify(out), 201

    except Exception:
        db.session.rollback()
        current_app.logger.exception("broadcast failed")
        return jsonify(error="internal server error"), 500


@pao_bp.route("/broadcast", methods=["GET"])
@require_role("pao")
def list_broadcasts():
    """
    Return today's announcements for the caller's bus (Asia/Manila),
    strictly by the ANNOUNCEMENT'S bus_id. This ensures you see messages
    from other PAOs on the same bus, regardless of their user assignment.
    Query params:
      - limit=<int>    (default: 200, max 500)
      - since_id=<int> (optional; incremental fetch)
    """
    limit_req = request.args.get("limit", type=int) or 200
    limit = max(1, min(limit_req, 500))
    since_id = request.args.get("since_id", type=int)

    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    # Manila "today"
    day_local = dt.datetime.now(_MNL).date()
    start_dt, end_dt = _local_day_bounds_utc(day_local)

    # Pull today's rows for *this bus* only
    q = (
        db.session.query(
            Announcement,
            User.first_name.label("first"),
            User.last_name.label("last"),
        )
        .join(User, Announcement.created_by == User.id)
        .filter(
            Announcement.bus_id == bus_id,
            Announcement.timestamp >= start_dt,
            Announcement.timestamp <  end_dt,
        )
    )
    if since_id:
        q = q.filter(Announcement.id > since_id)

    rows = q.order_by(Announcement.id.desc()).limit(limit).all()

    # Use the announcement's stored bus_id for the label
    anns = []
    for (ann, first, last) in rows:
        anns.append(
            _ann_json_fast(
                ann,
                author_first=first,
                author_last=last,
                bus_identifier=_bus_identifier_str(getattr(ann, "bus_id", None) or bus_id),
            )
        )

    return jsonify(anns), 200

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

@pao_bp.route("/broadcast/<int:ann_id>", methods=["DELETE"])
@require_role("pao")
def delete_broadcast(ann_id: int):
    """
    Delete an announcement (author-only).
    Emits a realtime event on Socket.IO namespace "/rt":
      - "announcement:deleted" with payload {"id": <ann_id>}
    """
    ann = Announcement.query.get(ann_id)
    if not ann:
        return jsonify(error="announcement not found"), 404
    if ann.created_by != g.user.id:
        return jsonify(error="not allowed to delete this announcement"), 403

    try:
        db.session.delete(ann)
        db.session.commit()

        try:
            _emit_announcement("announcement:deleted", {"id": ann_id})
        except Exception:
            current_app.logger.exception("[socketio] failed to emit announcement:deleted")

        return jsonify(id=ann_id, deleted=True), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("delete_broadcast failed")
        return jsonify(error=str(e)), 500

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

    img = jpg_name(int(round(float(t.price or 0))), t.passenger_type)
    qr_url  = url_for("static", filename=f"qr/{img}", _external=True)
    qr_link = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)
    qr_bg_url = f"{request.url_root.rstrip('/')}/{QR_PATH}/{img}"
    payload = build_qr_payload(t, origin_name=origin_name, destination_name=destination_name)

    current_app.logger.info(
        "[PAO:get_ticket] ticket_id=%s ref=%s issued_by=%s caller_pao=%s",
        t.id, t.reference_no,
        getattr(t, "issued_by", None),
        getattr(getattr(g, "user", None), "id", None),
    )

    gr = int(getattr(t, "group_regular", 0) or 0)
    gd = int(getattr(t, "group_discount", 0) or 0)
    total = gr + gd
    is_group = bool(getattr(t, "is_group", False) or total > 1)
    group_block = ({"regular": gr, "discount": gd, "total": total} if is_group else None)

    resp = {
        "id": t.id,
        "referenceNo": t.reference_no,
        "time": _as_mnl(t.created_at).strftime("%I:%M %p").lstrip("0").lower(),
        "date": _as_mnl(t.created_at).strftime("%B %d, %Y"),
        "origin": origin_name,
        "destination": destination_name,
        "commuter": ("Guest" if getattr(t, "guest", False)
                    else (f"{t.user.first_name} {t.user.last_name}" if t.user else "Guest")),
        "passengerType": (t.passenger_type or "").lower(),
        "fare": f"{float(t.price or 0):.2f}",
        "paid": bool(t.paid),
        "voided": bool(getattr(t, "voided", False)),
        "void_reason": getattr(t, "void_reason", None),
        "qr": payload,
        "qr_link": qr_link,
        "qr_url": qr_url,
        "qr_bg_url": qr_bg_url,
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
        "paoId": getattr(t, "issued_by", None) or getattr(g, "user", None).id,
        "payment_method": _payment_method_for_ticket_row(t),
    }

    if is_group:
        resp["isGroup"] = True
        resp["group"] = group_block
        resp["count"] = total

    return jsonify(resp), 200
