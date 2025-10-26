# backend/routes/manager.py
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from werkzeug.utils import secure_filename
from flask import Blueprint, request, jsonify, send_from_directory, current_app, g

# ✅ add or_ (and and_ if you ever need it)
from sqlalchemy import func, text, literal, or_
from sqlalchemy.orm import aliased

from db import db
# ❌ remove this if you’re moving the guard into auth_guard
# from routes.auth import require_role
# ✅ use the standalone guard (as shown in my last message)
from auth_guard import require_role
import secrets, string

from utils.push import push_to_user
from sqlalchemy.exc import IntegrityError
from models.bus import Bus
from models.schedule import Trip
from models.qr_template import QRTemplate
from models.fare_segment import FareSegment
from models.sensor_reading import SensorReading
from models.ticket_sale import TicketSale
from models.user import User
from models.ticket_stop import TicketStop
from models.trip_metric import TripMetric
from models.wallet import WalletAccount, WalletLedger, TopUp

# --- Staff creation (PAO / Driver) ---
from werkzeug.security import generate_password_hash


import re, uuid, secrets

def _slugify(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def _debug_dump_pao_for_day(day):
    try:
        rows = db.session.execute(
            text("""
                SELECT a.id, a.user_id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE DATE(a.service_date)=:d
                ORDER BY a.bus_id
            """),
            {"d": day}
        ).mappings().all()
        current_app.logger.info("[pao-assignments][DEBUG] %s -> %s", day, [dict(r) for r in rows])
    except Exception:
        current_app.logger.exception("[pao-assignments][DEBUG] dump failed day=%s", day)


def _gen_unique_username(seed: str) -> str:
    """
    Make a unique username from a seed. Falls back to a short uuid.
    """
    base = _slugify(seed) or "user"
    # Try a few random numeric suffixes first
    for _ in range(25):
        candidate = f"{base}-{secrets.randbelow(9000)+1000}"
        if not User.query.filter(User.username == candidate).first():
            return candidate
    # Fallback
    candidate = f"{base}-{uuid.uuid4().hex[:6]}"
    return candidate


MNL_TZ = timezone(timedelta(hours=8))
VOID_WINDOW_HOURS = 24

def _to_utc_z(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=MNL_TZ)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

manager_bp = Blueprint("manager", __name__, url_prefix="/manager")

# Optional realtime publish (best-effort / no-op if module missing)
try:
    from mqtt_ingest import publish as mqtt_publish
except Exception:
    mqtt_publish = None  # type: ignore[assignment]

def _active_trip_for(bus_id: int, ts: datetime):
    """Find the trip whose time window contains ts (handles past-midnight windows)."""
    day = ts.date()
    prev = (ts - timedelta(days=1)).date()
    candidates = (
        Trip.query.filter(Trip.bus_id == bus_id, Trip.service_date.in_([day, prev]))
        .order_by(Trip.start_time.asc())
        .all()
    )
    for t in candidates:
        start = datetime.combine(t.service_date, t.start_time)
        end = datetime.combine(t.service_date, t.end_time)
        if t.end_time <= t.start_time:  # past midnight
            end = end + timedelta(days=1)
        if start <= ts < end:
            return t
    return None

def _as_php(x) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0

def _topup_reason_path(tid: int) -> str:
    # mirror teller: static/topup_receipts/{id}.reject.txt
    return os.path.join(current_app.root_path, "static", "topup_receipts", f"{tid}.reject.txt")

def _save_topup_void_reason(tid: int, text_: str | None) -> None:
    if not (text_ and text_.strip()):
        return
    try:
        p = _topup_reason_path(tid)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write(text_.strip())
    except Exception:
        current_app.logger.exception("[manager] write topup void reason failed tid=%s", tid)

def _publish_user_wallet(uid: int, *, new_balance_pesos: int, event: str, **extra) -> bool:
    """Best-effort realtime wallet update for commuter device(s)."""
    if not mqtt_publish:
        current_app.logger.warning("[mqtt] disabled: mqtt_ingest.publish not available (wallet)")
        return False
    payload = {
        "type": "wallet_update",
        "event": event,
        "new_balance_php": int(new_balance_pesos),
        "sentAt": int(datetime.utcnow().timestamp() * 1000),
        **extra,
    }
    ok = True
    for root in ("user", "users"):
        topic = f"{root}/{int(uid)}/wallet"
        ok = mqtt_publish(topic, payload) and ok
        current_app.logger.info("[mqtt] wallet → %s ok=%s", topic, ok)
    return ok

@manager_bp.route("/staff", methods=["GET"])
@require_role("manager")
def list_staff():
    """
    List staff. Optional query:
      - role=pao|driver
      - q=<search>
    Returns: [ { id, name, username, role } ]
    """
    role = (request.args.get("role") or "").strip().lower()
    q = (request.args.get("q") or "").strip()
    valid = {"pao", "driver"}

    base = User.query
    if role:
        if role not in valid:
            return jsonify(error="invalid role"), 400
        base = base.filter(User.role == role)
    else:
        base = base.filter(User.role.in_(valid))

    if q:
        like = f"%{q}%"
        base = base.filter(
            or_(
                User.first_name.ilike(like),
                User.last_name.ilike(like),
                User.username.ilike(like),
                User.phone_number.ilike(like),
            )
        )

    rows = base.order_by(User.last_name.asc(), User.first_name.asc()).all()
    out = [
        {
            "id": u.id,
            "name": f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or u.username,
            "username": u.username,
            "role": u.role,
        }
        for u in rows
    ]
    return jsonify(out), 200

def _set_password_for_user(u: User, raw: str | None):
    """Set a user's password using model's set_password or a werkzeug hash field."""
    if not raw:
        return
    if hasattr(u, "set_password") and callable(getattr(u, "set_password")):
        u.set_password(raw)  # type: ignore[attr-defined]
    elif hasattr(u, "password_hash"):
        setattr(u, "password_hash", generate_password_hash(raw))

def _ensure_unique_username_phone(un: str | None, ph: str | None, *, exclude_id: int | None = None) -> str | None:
    if un:
        q = User.query.filter(User.username == un)
        if exclude_id:
            q = q.filter(User.id != exclude_id)
        if q.first():
            return "username already in use"
    if ph:
        q = User.query.filter(User.phone_number == ph)
        if exclude_id:
            q = q.filter(User.id != exclude_id)
        if q.first():
            return "phone number already in use"
    return None

def _random_password(n: int = 10) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(n))

def table_has_column(table: str, column: str) -> bool:
    row = db.session.execute(
        text(
            """
            SELECT COUNT(*) AS c
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name   = :t
              AND column_name  = :c
        """
        ),
        {"t": table, "c": column},
    ).mappings().first()
    return bool(row and int(row["c"]) > 0)

def _is_ticket_void_row(row) -> bool:
    """Determine if a ticket row is voided, supporting both schemas."""
    try:
        if bool(getattr(row, "voided")):
            return True
    except Exception:
        pass
    try:
        st = (getattr(row, "status", None) or "").strip().lower()
        if st in {"void", "voided", "refunded", "cancelled", "canceled"}:
            return True
    except Exception:
        pass
    return False

def _amount_pesos_from_price(price) -> int:
    try:
        return int(round(float(price or 0)))
    except Exception:
        return 0

def _ensure_wallet_account(user_id: int):
    """Create wallet_accounts row if missing (pesos schema)."""
    db.session.execute(
        text(
            """
            INSERT INTO wallet_accounts (user_id, balance_pesos)
            VALUES (:uid, 0)
            ON DUPLICATE KEY UPDATE user_id = user_id
        """
        ),
        {"uid": user_id},
    )

def _wallet_tables_exist() -> bool:
    return table_has_column("wallet_accounts", "balance_pesos") and table_has_column(
        "wallet_ledger", "amount_pesos"
    )

def _write_void_reason(ticket_table: str, ticket_id: int, reason: str):
    """
    Persist void reason if a suitable column exists. Priority:
    - void_reason
    - reason
    - note / remarks
    No-op if none exist.
    """
    candidates = []
    for col in ("void_reason", "reason", "note", "remarks"):
        if table_has_column(ticket_table, col):
            candidates.append(col)
            break
    if not candidates:
        return
    col = candidates[0]
    db.session.execute(
        text(
            f"UPDATE {ticket_table} SET {col} = :r WHERE id = :tid"
        ),
        {"r": reason, "tid": ticket_id},
    )

def _mark_ticket_void(ticket: TicketSale, reason: str | None):
    """
    Mark a TicketSale row voided across schemas:
      - if column 'voided' exists: set voided=1
      - else if 'status' exists: set status='voided'
      - always set paid=0 (if exists) for clarity on state
      - optionally persist reason into void_reason/reason/note/remarks when present
    """
    if table_has_column("ticket_sales", "paid"):
        ticket.paid = False  # type: ignore[attr-defined]
    if table_has_column("ticket_sales", "voided"):
        ticket.voided = True  # type: ignore[attr-defined]
    elif table_has_column("ticket_sales", "status"):
        setattr(ticket, "status", "voided")
    if reason:
        _write_void_reason("ticket_sales", ticket.id, reason)

def _refund_paid_ticket(ticket: TicketSale, actor_id: int | None) -> dict:
    """
    Refund a paid ticket into the user's wallet if wallet tables exist.
    Returns a dict summary {refunded: bool, balance: int|None}.
    """
    if not _wallet_tables_exist():
        return {"refunded": False, "balance": None, "note": "wallet tables missing"}
    if not ticket.user_id:
        return {"refunded": False, "balance": None, "note": "no user to refund"}

    amount_pesos = _amount_pesos_from_price(ticket.price)
    if amount_pesos <= 0:
        return {"refunded": False, "balance": None, "note": "zero-amount ticket"}

    _ensure_wallet_account(int(ticket.user_id))

    bal_row = db.session.execute(
        text(
            """
            SELECT balance_pesos AS bal
            FROM wallet_accounts
            WHERE user_id = :uid
        """
        ),
        {"uid": ticket.user_id},
    ).mappings().first()
    cur_bal = int(bal_row["bal"] or 0) if bal_row else 0
    new_bal = cur_bal + amount_pesos

    db.session.execute(
        text(
            """
            UPDATE wallet_accounts
            SET balance_pesos = :b
            WHERE user_id = :uid
        """
        ),
        {"b": new_bal, "uid": ticket.user_id},
    )
    db.session.execute(
        text(
            """
            INSERT INTO wallet_ledger
            (account_id, direction, event, amount_pesos, running_balance_pesos, ref_table, ref_id, created_at)
            VALUES (:uid, 'in', 'refund_ticket', :amt, :run, 'ticket_sales', :tid, :ts)
        """
        ),
        {
            "uid": ticket.user_id,
            "amt": amount_pesos,
            "run": new_bal,
            "tid": ticket.id,
            "ts": datetime.utcnow(),
        },
    )
    return {"refunded": True, "balance": new_bal, "note": None}

# ─────────────────────────────────────────────
# Wallet Top-up: Void (manager)
# ─────────────────────────────────────────────
@manager_bp.route("/topups/<int:tid>/void", methods=["POST"])
@require_role("manager")
def manager_void_topup(tid: int):
    """
    Void a cash top-up within 24h (manager action).
    Body: { "reason": "<optional text>" }
    Rules:
      - only status='succeeded' & method='cash'
      - created_at <= 24h ago (Manila time)
      - wallet must still have enough balance to reverse
    """
    data = request.get_json(silent=True) or {}
    reason = (data.get("reason") or "").strip()

    t = TopUp.query.get(tid)
    if not t:
        return jsonify(error="top-up not found"), 404
    if (t.status or "").lower() != "succeeded":
        return jsonify(error="only succeeded top-ups can be voided"), 400
    if (t.method or "").lower() != "cash":
        return jsonify(error="unsupported method for void: cash only"), 400

    created = getattr(t, "created_at", None)
    if created:
        created_mnl = (created.astimezone(MNL_TZ) if created.tzinfo
                       else created.replace(tzinfo=timezone.utc).astimezone(MNL_TZ))
        age_hours = (datetime.now(MNL_TZ) - created_mnl).total_seconds() / 3600.0
        if age_hours > VOID_WINDOW_HOURS:
            return jsonify(error="void window elapsed (over 24 hours)"), 400

    acct = WalletAccount.query.get(t.account_id)
    if not acct:
        acct = WalletAccount(user_id=t.account_id, balance_pesos=0)
        db.session.add(acct)
        db.session.flush()

    amt = _as_php(getattr(t, "amount_pesos", 0))
    cur = _as_php(getattr(acct, "balance_pesos", 0))
    if cur < amt:
        return jsonify(error="insufficient wallet balance to reverse (funds already spent)"), 400

    new_bal = cur - amt
    try:
        acct.balance_pesos = int(new_bal)
        led = WalletLedger(
            account_id=t.account_id,
            direction="debit",
            event="topup_void",
            amount_pesos=int(amt),
            running_balance_pesos=int(new_bal),
            ref_table="wallet_topups",
            ref_id=int(t.id),
        )
        db.session.add(led)
        t.status = "cancelled"
        if reason:
            _save_topup_void_reason(t.id, reason)

        db.session.commit()

        _publish_user_wallet(
            t.account_id,
            new_balance_pesos=int(new_bal),
            event="wallet_topup_void",
            topup_id=int(t.id),
            method="cash",
            amount_php=int(amt),
        )
        try:
            push_to_user(
                t.account_id,
                title="Top-up Reversed",
                body=f"₱{amt} was reversed. New balance: ₱{new_bal}",
                data={"type": "wallet_topup_void", "topup_id": int(t.id)},
            )
        except Exception:
            current_app.logger.info("[push] void notify skipped/failed uid=%s", t.account_id)

        return jsonify(
            ok=True,
            topup_id=int(t.id),
            ledger_id=int(led.id),
            new_balance_php=int(new_bal),
        ), 200

    except Exception as e:
        current_app.logger.exception("[manager] void_topup failed")
        db.session.rollback()
        return jsonify(error=str(e)), 500

# ─────────────────────────────────────────────
# Ticket void / refund
# ─────────────────────────────────────────────
@manager_bp.route("/tickets/<int:ticket_id>/void", methods=["PATCH"])
@require_role("manager")
def manager_void_ticket(ticket_id: int):
    """
    Body: { "voided": true|false, "reason": "<required when voided=true>" }
    Idempotent when already voided (no double refund).
    """
    data = request.get_json(silent=True) or {}
    want_void = bool(data.get("voided", True))
    reason = (data.get("reason") or "").strip()

    if want_void and not reason:
        return jsonify(error="reason is required when voiding a ticket"), 400

    ticket = TicketSale.query.filter(TicketSale.id == ticket_id).first()
    if not ticket:
        return jsonify(error="ticket not found"), 404

    already_void = _is_ticket_void_row(ticket)
    was_paid = bool(getattr(ticket, "paid", False))

    if want_void:
        if already_void:
            return jsonify(
                ok=True,
                ticket_id=ticket.id,
                state="voided",
                paid=False,
                voided=True,
                status=getattr(ticket, "status", "voided"),
                reason=reason or None,
                refund={"refunded": False, "balance": None, "note": "already voided"},
            ), 200

        try:
            with db.session.begin():
                _mark_ticket_void(ticket, reason)
                refund_summary = {"refunded": False, "balance": None, "note": None}
                if was_paid:
                    refund_summary = _refund_paid_ticket(ticket, actor_id=getattr(g, "user", None) and g.user.id)
                db.session.flush()
            return jsonify(
                ok=True,
                ticket_id=ticket.id,
                state="voided",
                paid=False,
                voided=True,
                status=getattr(ticket, "status", "voided"),
                reason=reason or None,
                refund=refund_summary,
            ), 200
        except Exception as e:
            current_app.logger.exception("[manager:void] failed")
            db.session.rollback()
            return jsonify(error=str(e)), 500

    # Un-void (rare)
    try:
        with db.session.begin():
            if table_has_column("ticket_sales", "voided"):
                setattr(ticket, "voided", False)
            if table_has_column("ticket_sales", "status"):
                setattr(ticket, "status", "paid" if was_paid else "unpaid")
            if table_has_column("ticket_sales", "paid"):
                setattr(ticket, "paid", was_paid)
            for col in ("void_reason", "reason", "note", "remarks"):
                if table_has_column("ticket_sales", col):
                    db.session.execute(
                        text(f"UPDATE ticket_sales SET {col} = NULL WHERE id = :tid"),
                        {"tid": ticket.id},
                    )
                    break
            db.session.flush()
        return jsonify(
            ok=True,
            ticket_id=ticket.id,
            state=("paid" if was_paid else "unpaid"),
            paid=was_paid,
            voided=False,
            status=getattr(ticket, "status", None),
            reason=None,
        ), 200
    except Exception as e:
        current_app.logger.exception("[manager:unvoid] failed")
        db.session.rollback()
        return jsonify(error=str(e)), 500

# ─────────────────────────────────────────────
# Commuters (lists & stats)
# ─────────────────────────────────────────────
@manager_bp.route("/commuters", methods=["GET"])
@require_role("manager")
def list_commuters():
    from sqlalchemy import or_

    q = (request.args.get("q") or "").strip()
    page = request.args.get("page", default=1, type=int) or 1
    page_size = request.args.get("page_size", default=25, type=int) or 25
    page_size = min(max(page_size, 1), 100)

    base = User.query.filter(User.role == "commuter")

    if q:
        like = f"%{q}%"
        base = base.filter(
            or_(
                User.first_name.ilike(like),
                User.last_name.ilike(like),
                User.username.ilike(like),
                User.phone_number.ilike(like),
            )
        )

    total = base.count()

    users = (
        base.order_by(User.last_name.asc(), User.first_name.asc())
        .offset((page - 1) * page_size)
        .limit(page_size)
        .all()
    )

    if users:
        ids = [u.id for u in users]
        stats_rows = (
            db.session.query(
                TicketSale.user_id.label("uid"),
                func.count(TicketSale.id).label("tickets"),
                func.max(TicketSale.created_at).label("last_ticket_at"),
            )
            .filter(TicketSale.user_id.in_(ids))
            .group_by("uid")
            .all()
        )
        stats = {
            r.uid: {
                "tickets": int(r.tickets or 0),
                "last_ticket_at": (r.last_ticket_at.isoformat() if r.last_ticket_at else None),
            }
            for r in stats_rows
        }
    else:
        stats = {}

    items = []
    for u in users:
        s = stats.get(u.id, {"tickets": 0, "last_ticket_at": None})
        items.append(
            {
                "id": u.id,
                "first_name": u.first_name,
                "last_name": u.last_name,
                "name": f"{u.first_name} {u.last_name}".strip(),
                "username": u.username,
                "phone_number": u.phone_number,
                "tickets": s["tickets"],
                "last_ticket_at": s["last_ticket_at"],
            }
        )

    pages = (total + page_size - 1) // page_size
    return (
        jsonify({"items": items, "page": page, "page_size": page_size, "total": total, "pages": pages}),
        200,
    )

@manager_bp.route("/commuters/<int:user_id>", methods=["GET"])
@require_role("manager")
def commuter_detail(user_id: int):
    from sqlalchemy import func as F

    u = User.query.filter(User.id == user_id, User.role == "commuter").first()
    if not u:
        return jsonify(error="commuter not found"), 404

    t_stats = (
        db.session.query(
            F.count(TicketSale.id),
            F.max(TicketSale.created_at),
            F.coalesce(F.sum(TicketSale.price), 0.0),
        )
        .filter(TicketSale.user_id == user_id, TicketSale.voided.is_(False))
        .first()
    )
    tickets_total = int(t_stats[0] or 0)
    last_ticket_at = t_stats[1].isoformat() if t_stats[1] else None
    tickets_revenue = float(t_stats[2] or 0.0)

    topup_stats = db.session.execute(
        text(
            """
            SELECT 
                COALESCE(SUM(t.amount_pesos), 0) AS sum_pesos,
                MAX(t.created_at)               AS last_at
            FROM wallet_topups t
            WHERE t.account_id = :uid
              AND t.status = 'succeeded'
        """
        ),
        {"uid": user_id},
    ).mappings().first() or {}
    topups_total_pesos = int(topup_stats.get("sum_pesos", 0))
    last_topup_dt = topup_stats.get("last_at")
    last_topup_at = last_topup_dt.isoformat() if last_topup_dt else None

    return (
        jsonify(
            {
                "id": u.id,
                "first_name": u.first_name,
                "last_name": u.last_name,
                "name": f"{u.first_name} {u.last_name}".strip(),
                "username": u.username,
                "phone_number": u.phone_number,
                "tickets": {
                    "count": tickets_total,
                    "revenue_php": round(tickets_revenue, 2),
                    "last_at": last_ticket_at,
                },
                "topups": {
                    "count": None,
                    "total_php": float(topups_total_pesos),
                    "last_at": last_topup_at,
                },
            }
        ),
        200,
    )

@manager_bp.route("/commuters/<int:user_id>/tickets", methods=["GET"])
@require_role("manager")
def commuter_tickets(user_id: int):
    try:
        to_str = request.args.get("to")
        from_str = request.args.get("from")
        page = request.args.get("page", type=int, default=1)
        size = min(max(request.args.get("page_size", type=int, default=25), 1), 100)

        to_dt = datetime.strptime(to_str, "%Y-%m-%d") if to_str else datetime.utcnow()
        fr_dt = datetime.strptime(from_str, "%Y-%m-%d") if from_str else (to_dt - timedelta(days=30))

        O = aliased(TicketStop)
        D = aliased(TicketStop)

        has_status = table_has_column("ticket_sales", "status")
        has_voided = table_has_column("ticket_sales", "voided")
        has_paid   = table_has_column("ticket_sales", "paid")

        fields = [
            TicketSale.id,
            TicketSale.reference_no,
            TicketSale.created_at,
            TicketSale.price,
            TicketSale.passenger_type,
            (TicketSale.paid if has_paid else literal(False).label("paid")),
            (TicketSale.status if has_status else literal(None).label("status")),
            (TicketSale.voided if has_voided else literal(False).label("voided")),
            Bus.identifier.label("bus"),
            O.stop_name.label("origin"),
            D.stop_name.label("destination"),
        ]

        base = (
            db.session.query(*fields)
            .join(Bus, TicketSale.bus_id == Bus.id)
            .outerjoin(O, TicketSale.origin_stop_time_id == O.id)
            .outerjoin(D, TicketSale.destination_stop_time_id == D.id)
            .filter(TicketSale.user_id == user_id)
            .filter(TicketSale.created_at.between(fr_dt, to_dt + timedelta(days=1)))
            .order_by(TicketSale.created_at.desc())
        )

        total = base.count()
        rows = base.offset((page - 1) * size).limit(size).all()

        items = []
        for r in rows:
            st = (str(getattr(r, "status", "") or "")).lower()
            is_void = bool(getattr(r, "voided", False)) or st in {"void", "voided", "refunded", "cancelled", "canceled"}
            items.append(
                {
                    "id": r.id,
                    "referenceNo": getattr(r, "reference_no", None),
                    "created_at": r.created_at.isoformat(),
                    "time": r.created_at.strftime("%Y-%m-%d %H:%M"),
                    "fare": f"{float(r.price or 0):.2f}",
                    "paid": bool(getattr(r, "paid", False)) and not is_void,
                    "status": (
                        getattr(r, "status", None)
                        or ("voided" if is_void else ("paid" if getattr(r, "paid", False) else "unpaid"))
                    ),
                    "voided": is_void,
                    "passenger_type": (r.passenger_type or "regular"),
                    "bus": r.bus,
                    "origin": r.origin or "",
                    "destination": r.destination or "",
                }
            )

        return jsonify(
            {
                "items": items,
                "page": page,
                "page_size": size,
                "total": total,
                "pages": (total + size - 1) // size,
            }
        ), 200
    except Exception:
        current_app.logger.exception("ERROR in commuter_tickets")
        return jsonify(error="Failed to load tickets"), 500

@manager_bp.route("/commuters/<int:user_id>/topups", methods=["GET"])
@require_role("manager")
def commuter_topups(user_id: int):
    """
    List a commuter's wallet top-ups (paginated), optionally including voided/cancelled).
    """
    try:
        to_str = request.args.get("to")
        from_str = request.args.get("from")
        page = request.args.get("page", type=int, default=1) or 1
        size = min(max(request.args.get("page_size", type=int, default=25), 1), 100)

        include_voided = (request.args.get("include_voided", "false").strip().lower() in {"1", "true", "yes"})

        to_dt = datetime.strptime(to_str, "%Y-%m-%d") if to_str else datetime.utcnow()
        fr_dt = datetime.strptime(from_str, "%Y-%m-%d") if from_str else (to_dt - timedelta(days=30))

        offset = (page - 1) * size

        status_cond = "AND t.status IN ('succeeded','cancelled')" if include_voided else "AND t.status = 'succeeded'"

        total_row = db.session.execute(
            text(f"""
                SELECT COUNT(*) AS c
                FROM wallet_topups t
                WHERE t.account_id = :uid
                  {status_cond}
                  AND t.created_at >= :fr
                  AND t.created_at <  :to_plus
            """),
            {"uid": user_id, "fr": fr_dt, "to_plus": to_dt + timedelta(days=1)},
        ).mappings().first() or {}
        total = int(total_row.get("c", 0))

        has_teller = table_has_column("wallet_topups", "teller_id")

        if has_teller:
            sql = f"""
                SELECT 
                    t.id,
                    t.created_at,
                    t.amount_pesos,
                    COALESCE(t.method, 'cash') AS method,
                    t.status,
                    COALESCE(t.teller_id, t.pao_id) AS teller_id,
                    COALESCE(tu.first_name, pu.first_name) AS teller_first,
                    COALESCE(tu.last_name,  pu.last_name)  AS teller_last
                FROM wallet_topups t
                LEFT JOIN users tu ON tu.id = t.teller_id
                LEFT JOIN users pu ON pu.id = t.pao_id
                WHERE t.account_id = :uid
                  {status_cond}
                  AND t.created_at >= :fr
                  AND t.created_at <  :to_plus
                ORDER BY t.created_at DESC
                LIMIT :lim OFFSET :off
            """
        else:
            sql = f"""
                SELECT 
                    t.id,
                    t.created_at,
                    t.amount_pesos,
                    COALESCE(t.method, 'cash') AS method,
                    t.status,
                    t.pao_id AS teller_id,
                    pu.first_name AS teller_first,
                    pu.last_name  AS teller_last
                FROM wallet_topups t
                LEFT JOIN users pu ON pu.id = t.pao_id
                WHERE t.account_id = :uid
                  {status_cond}
                  AND t.created_at >= :fr
                  AND t.created_at <  :to_plus
                ORDER BY t.created_at DESC
                LIMIT :lim OFFSET :off
            """

        rows = db.session.execute(
            text(sql),
            {
                "uid": user_id,
                "fr": fr_dt,
                "to_plus": to_dt + timedelta(days=1),
                "lim": size,
                "off": offset,
            },
        ).mappings().all()

        items = [
            {
                "id": r["id"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "amount_php": float(r["amount_pesos"] or 0.0),
                "method": r["method"],
                "status": r["status"],
                "teller_id": r["teller_id"],
                "teller_name": ("{} {}".format(r.get("teller_first") or "", r.get("teller_last") or "").strip() or None),
            }
            for r in rows
        ]

        return (
            jsonify(
                {"items": items, "page": page, "page_size": size, "total": total, "pages": (total + size - 1) // size}
            ),
            200,
        )

    except Exception:
        current_app.logger.exception("ERROR in commuter_topups")
        return jsonify(error="Failed to load top-ups"), 500

# ─────────────────────────────────────────────
# Manager Topups Summary
# ─────────────────────────────────────────────
@manager_bp.route("/topups", methods=["GET"])
@require_role("manager")
def manager_topups():
    from datetime import datetime as _dt

    date_str  = (request.args.get("date")  or "").strip()
    start_str = (request.args.get("start") or "").strip()
    end_str   = (request.args.get("end")   or "").strip()

    try:
        if date_str:
            day = _dt.strptime(date_str, "%Y-%m-%d").date()
            start_dt = _dt.combine(day, _dt.min.time())
            end_dt   = _dt.combine(day, _dt.max.time())
        elif start_str and end_str:
            sd = _dt.strptime(start_str, "%Y-%m-%d").date()
            ed = _dt.strptime(end_str,   "%Y-%m-%d").date()
            if ed < sd:
                return jsonify(error="end must be >= start"), 400
            start_dt = _dt.combine(sd, _dt.min.time())
            end_dt   = _dt.combine(ed, _dt.max.time())
        else:
            day = _dt.utcnow().date()
            start_dt = _dt.combine(day, _dt.min.time())
            end_dt   = _dt.combine(day, _dt.max.time())
    except ValueError:
        return jsonify(error="invalid date format (use YYYY-MM-DD)"), 400

    method    = (request.args.get("method") or "").strip().lower() or None
    teller_id = request.args.get("teller_id", type=int) or request.args.get("pao_id", type=int)
    include_voided = (request.args.get("include_voided", "false").strip().lower() in {"1","true","yes"})
    limit     = request.args.get("limit", type=int)
    if limit is not None:
        limit = max(1, min(limit, 100))

    has_teller = table_has_column("wallet_topups", "teller_id")
    status_cond = "t.status IN ('succeeded','cancelled')" if include_voided else "t.status = 'succeeded'"
    params = {"s": start_dt, "e": end_dt}

    where_extra = []
    if method in ("cash", "gcash"):
        where_extra.append("t.method = :m")
        params["m"] = method
    if teller_id:
        if has_teller:
            where_extra.append("COALESCE(t.teller_id, t.pao_id) = :tid")
        else:
            where_extra.append("t.pao_id = :tid")
        params["tid"] = teller_id

    extra_sql = (" AND " + " AND ".join(where_extra)) if where_extra else ""

    agg_sql = f"""
        SELECT COUNT(*) AS cnt,
               COALESCE(SUM(t.amount_pesos), 0) AS sum_php
        FROM wallet_topups t
        WHERE t.created_at BETWEEN :s AND :e
          AND {status_cond}
          {extra_sql}
    """
    agg = db.session.execute(text(agg_sql), params).mappings().first() or {"cnt": 0, "sum_php": 0}
    count_all = int(agg["cnt"] or 0)
    total_all = float(agg["sum_php"] or 0.0)

    if has_teller:
        base_sql = f"""
            SELECT
                t.id, t.account_id,
                COALESCE(t.teller_id, t.pao_id) AS teller_id,
                COALESCE(t.method, 'cash')      AS method,
                t.amount_pesos, t.status, t.created_at,
                cu.first_name AS commuter_first, cu.last_name AS commuter_last,
                au.first_name AS teller_first,   au.last_name AS teller_last
            FROM wallet_topups t
            LEFT JOIN users cu ON cu.id = t.account_id
            LEFT JOIN users au ON au.id = COALESCE(t.teller_id, t.pao_id)
            WHERE t.created_at BETWEEN :s AND :e
              AND {status_cond}
              {extra_sql}
            ORDER BY t.id DESC
        """
    else:
        base_sql = f"""
            SELECT
                t.id, t.account_id,
                t.pao_id AS teller_id,
                COALESCE(t.method, 'cash') AS method,
                t.amount_pesos, t.status, t.created_at,
                cu.first_name AS commuter_first, cu.last_name AS commuter_last,
                au.first_name AS teller_first,   au.last_name AS teller_last
            FROM wallet_topups t
            LEFT JOIN users cu ON cu.id = t.account_id
            LEFT JOIN users au ON au.id = t.pao_id
            WHERE t.created_at BETWEEN :s AND :e
              AND {status_cond}
              {extra_sql}
            ORDER BY t.id DESC
        """

    if limit is not None:
        base_sql += " LIMIT :lim"
        params["lim"] = int(limit)

    rows = db.session.execute(text(base_sql), params).mappings().all()

    items = []
    for r in rows:
        commuter_name = f"{(r['commuter_first'] or '').strip()} {(r['commuter_last'] or '').strip()}".strip() or None
        teller_name   = f"{(r['teller_first']   or '').strip()} {(r['teller_last']   or '').strip()}".strip() or None
        items.append(
            {
                "id": int(r["id"]),
                "created_at": _to_utc_z(r["created_at"]),
                "amount_php": float(r["amount_pesos"] or 0.0),
                "method": r["method"] or "cash",
                "status": r["status"],
                "commuter_id": int(r["account_id"]) if r["account_id"] is not None else None,
                "commuter_name": commuter_name,
                "teller_id": int(r["teller_id"]) if r["teller_id"] is not None else None,
                "teller_name": teller_name,
            }
        )

    return jsonify(items=items, count=count_all, total_php=total_all), 200

@manager_bp.route("/paos", methods=["GET"])
@require_role("manager", "pao")  # allow PAO & Manager to call
def list_paos():
    """Basic list of PAO users to populate a dropdown."""
    rows = (
        User.query.filter(User.role == "pao")
        .order_by(User.last_name.asc(), User.first_name.asc())
        .all()
    )
    return jsonify(
        [
            {
                "id": u.id,
                "name": f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or u.username,
            }
            for u in rows
        ]
    ), 200


@manager_bp.route("/revenue-breakdown", methods=["GET"])
@require_role("manager")
def revenue_breakdown():
    from datetime import datetime as _dt, timedelta as _td

    paid_only = request.args.get("paid_only", "true").lower() != "false"

    trip_id = request.args.get("trip_id", type=int)
    if trip_id:
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404
        bus_id = trip.bus_id
        day = trip.service_date
        window_from = _dt.combine(day, trip.start_time)
        window_to = _dt.combine(day, trip.end_time)
        if trip.end_time <= trip.start_time:
            window_to = window_to + _td(days=1)
    else:
        date_str = request.args.get("date")
        bus_id = request.args.get("bus_id", type=int)
        if not (date_str and bus_id):
            return jsonify(error="trip_id OR (date, bus_id, from, to) required"), 400
        try:
            day = _dt.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="invalid date format"), 400
        try:
            hhmm_from = request.args["from"]
            hhmm_to = request.args["to"]
        except KeyError:
            return jsonify(error="from and to are required"), 400
        window_from = _dt.combine(day, _dt.strptime(hhmm_from, "%H:%M").time())
        window_to = _dt.combine(day, _dt.strptime(hhmm_to, "%H:%M").time())
        if window_to <= window_from:
            window_to = window_to + _td(days=1)

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

    types = {g["type"] for g in by_type}
    if "regular" not in types:
        by_type.append({"type": "regular", "tickets": 0, "revenue": 0.0})
    if "discount" not in types:
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

    return (
        jsonify(
            {
                "from": window_from.date().isoformat(),
                "to": window_to.date().isoformat(),
                "paid_only": bool(paid_only),
                "totals": {"tickets": int(totals_tickets), "revenue": round(totals_revenue, 2)},
                "by_type": sorted(out, key=lambda x: 0 if x["type"] == "regular" else 1),
            }
        ),
        200,
    )

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
        daily.append({"date": r.d, "tickets": int(r.tickets), "revenue": float(r.revenue or 0)})
        total_tickets += int(r.tickets)
        total_revenue += float(r.revenue or 0)

    return jsonify(daily=daily, total_tickets=total_tickets, total_revenue=round(total_revenue, 2)), 200

# ─────────────────────────────────────────────
# Buses (list + patch)
# ─────────────────────────────────────────────
@manager_bp.route("/buses", methods=["GET"])
@require_role("manager", "pao")
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
    except Exception:
        current_app.logger.exception("ERROR in /manager/buses")
        return jsonify(error="Could not process the request to list buses."), 500

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


@manager_bp.route("/bus-trips", methods=["GET"])
@require_role("manager")
def list_bus_trips():
    """
    GET /manager/bus-trips
      Query:
        - date=YYYY-MM-DD   (required)
        - bus_id=<int>      (required)
        - include_assignments=1|true|yes  (optional)
    
    Response:
      - When include_assignments is falsy: plain array of trips (back-compat)
      - When truthy:
          {
            "trips": [...],
            "assignments": {
              "pao_id": <int|null>,
              "pao_name": <str|null>,
              "driver_id": <int|null>,
              "driver_name": <str|null>,
              "pao_assigned": <bool>,
              "driver_assigned": <bool>
            }
          }
    """
    date_str = request.args.get("date")
    bus_id = request.args.get("bus_id", type=int)
    include_assignments = (request.args.get("include_assignments", "0").strip().lower() in {"1", "true", "yes"})

    if not (date_str and bus_id):
        return jsonify(error="date and bus_id are required"), 400

    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="invalid date format"), 400

    trips = (
        Trip.query.filter(Trip.bus_id == bus_id, Trip.service_date == day)
        .order_by(Trip.start_time.asc())
        .all()
    )

    trips_payload = [
        {
            "id": t.id,
            "number": t.number,
            "start_time": t.start_time.strftime("%H:%M"),
            "end_time": t.end_time.strftime("%H:%M"),
        }
        for t in trips
    ]

    # Back-compat: older clients expect a plain array unless assignments explicitly requested
    if not include_assignments:
        return jsonify(trips_payload), 200

    # Include PAO & Driver assignment for this bus/date
    # Add username to the select and fall back to it when first/last are blank.
    pao_row = db.session.execute(
        text("""
            SELECT a.user_id, u.first_name AS uf, u.last_name AS ul, u.username AS un
            FROM pao_assignments a
            LEFT JOIN users u ON u.id = a.user_id
            WHERE a.service_date = :d AND a.bus_id = :b
            LIMIT 1
        """),
        {"d": day, "b": bus_id},
    ).mappings().first()

    drv_row = db.session.execute(
        text("""
            SELECT a.user_id, u.first_name AS uf, u.last_name AS ul, u.username AS un
            FROM driver_assignments a
            LEFT JOIN users u ON u.id = a.user_id
            WHERE a.service_date = :d AND a.bus_id = :b
            LIMIT 1
        """),
        {"d": day, "b": bus_id},
    ).mappings().first()

    def _name(row):
        if not row:
            return None
        first = (row.get("uf") or "").strip()
        last  = (row.get("ul") or "").strip()
        user  = (row.get("un") or "").strip()
        full  = f"{first} {last}".strip()
        return full or user or None

    payload = {
        "trips": trips_payload,
        "assignments": {
            "pao_id":     int(pao_row["user_id"]) if pao_row and pao_row.get("user_id") is not None else None,
            "pao_name":   _name(pao_row),
            "driver_id":  int(drv_row["user_id"]) if drv_row and drv_row.get("user_id") is not None else None,
            "driver_name": _name(drv_row),
            # helpful booleans (clients can ignore safely)
            "pao_assigned": bool(pao_row and pao_row.get("user_id") is not None),
            "driver_assigned": bool(drv_row and drv_row.get("user_id") is not None),
        },
    }
    return jsonify(payload), 200




@manager_bp.route("/trips", methods=["POST"])
@require_role("manager")
def create_trip():
    data = request.get_json() or {}
    missing = [k for k in ("service_date", "bus_id", "number", "start_time", "end_time") if k not in data]
    if missing:
        return jsonify(error=f"Missing field(s): {', '.join(missing)}"), 400

    try:
        service_date = datetime.strptime(str(data["service_date"]), "%Y-%m-%d").date()
        start_time = datetime.strptime(str(data["start_time"]), "%H:%M").time()
        end_time = datetime.strptime(str(data["end_time"]), "%H:%M").time()
    except ValueError:
        return jsonify(error="Invalid date/time format"), 400

    number = str(data["number"]).strip()
    bus_id = int(data["bus_id"])

    bus = Bus.query.get(bus_id)
    if not bus:
        return jsonify(error="invalid bus_id"), 400

    if end_time <= start_time:
        return jsonify(error="end_time must be after start_time"), 400

    existing = Trip.query.filter(Trip.bus_id == bus_id, Trip.service_date == service_date).all()

    ns = datetime.combine(service_date, start_time)
    ne = datetime.combine(service_date, end_time)

    for t in existing:
        s = datetime.combine(service_date, t.start_time)
        e = datetime.combine(service_date, t.end_time)
        if max(s, ns) < min(e, ne):  # overlap
            return (
                jsonify(error=f"Overlaps with {t.number} ({t.start_time.strftime('%H:%M')}–{t.end_time.strftime('%H:%M')})"),
                409,
            )

    trip = Trip(bus_id=bus_id, service_date=service_date, number=number, start_time=start_time, end_time=end_time)

    try:
        db.session.add(trip)
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("ERROR creating trip")
        return jsonify(error="Failed to create trip"), 500

    return (
        jsonify(id=trip.id, number=trip.number, start_time=trip.start_time.strftime("%H:%M"), end_time=trip.end_time.strftime("%H:%M")),
        201,
    )

@manager_bp.route("/trips/<int:trip_id>", methods=["PATCH"])
@require_role("manager")
def update_trip(trip_id: int):
    data = request.get_json() or {}
    try:
        number = data.get("number", "").strip()
        start_time = datetime.strptime(data["start_time"], "%H:%M").time()
        end_time = datetime.strptime(data["end_time"], "%H:%M").time()
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload or missing required fields"), 400

    if end_time <= start_time:
        return jsonify(error="end_time must be after start_time"), 400

    trip = Trip.query.get_or_404(trip_id)
    trip.number = number or trip.number
    trip.start_time = start_time
    trip.end_time = end_time

    db.session.commit()

    return jsonify(id=trip.id, number=trip.number, start_time=trip.start_time.strftime("%H:%M"), end_time=trip.end_time.strftime("%H:%M")), 200

@manager_bp.route("/trips/<int:trip_id>", methods=["DELETE"])
@require_role("manager")
def delete_trip(trip_id: int):
    try:
        rows = Trip.query.filter_by(id=trip_id).delete(synchronize_session=False)
        if rows == 0:
            db.session.rollback()
            return jsonify(error="Trip not found"), 404

        db.session.commit()
        return jsonify(message="Trip successfully deleted"), 200
    except Exception as e:
        db.session.rollback()
        return jsonify(error="Error deleting trip: " + str(e)), 500

# ─────────────────────────────────────────────
# Tickets endpoints (day view / composition)
# ─────────────────────────────────────────────
@manager_bp.route("/tickets/composition", methods=["GET"])
@require_role("manager")
def tickets_composition():
    try:
        day = datetime.strptime(
            request.args.get("date") or datetime.utcnow().date().isoformat(),
            "%Y-%m-%d",
        ).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    ptype = func.coalesce(TicketSale.passenger_type, "regular")
    has_voided = table_has_column("ticket_sales", "voided")
    has_status = table_has_column("ticket_sales", "status")

    qs = db.session.query(ptype.label("ptype"), func.count(TicketSale.id)).filter(func.date(TicketSale.created_at) == day)
    if has_voided:
        qs = qs.filter(TicketSale.voided.is_(False))
    elif has_status:
        qs = qs.filter(func.lower(func.coalesce(TicketSale.status, "")) != "voided")

    rows = qs.group_by("ptype").all()

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
    """
    GET /manager/tickets
      Query:
        - date=YYYY-MM-DD
        - bus_id=<int>
        - include_voided=true|false
        - array=1|0
    """
    try:
        day = datetime.strptime(
            request.args.get("date") or datetime.now(MNL_TZ).date().isoformat(),
            "%Y-%m-%d",
        ).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    bus_id_filter = request.args.get("bus_id", type=int)
    include_voided = (request.args.get("include_voided", "false").strip().lower() in {"1", "true", "yes"})
    want_array = (request.args.get("array", "1").strip().lower() in {"1", "true", "yes"})

    has_status = table_has_column("ticket_sales", "status")
    has_voided = table_has_column("ticket_sales", "voided")
    has_paid   = table_has_column("ticket_sales", "paid")

    reason_label = None
    for col in ("void_reason", "reason", "note", "remarks"):
        if table_has_column("ticket_sales", col):
            reason_label = getattr(TicketSale, col).label("void_reason")
            break

    O = aliased(TicketStop)
    D = aliased(TicketStop)
    fields = [
        TicketSale.id,
        TicketSale.reference_no,
        TicketSale.created_at,
        TicketSale.price,
        TicketSale.passenger_type,
        User.first_name,
        User.last_name,
        Bus.id.label("bus_id"),
        Bus.identifier.label("bus"),
        O.stop_name.label("origin"),
        D.stop_name.label("destination"),
        (TicketSale.paid   if has_paid   else literal(False).label("paid")),
        (TicketSale.voided if has_voided else literal(False).label("voided")),
        (TicketSale.status if has_status else literal(None).label("status")),
    ]
    if reason_label is not None:
        fields.append(reason_label)

    start_mnl = datetime.combine(day, datetime.min.time(), tzinfo=MNL_TZ)
    end_mnl   = start_mnl + timedelta(days=1)
    start_utc = start_mnl.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc   = end_mnl.astimezone(timezone.utc).replace(tzinfo=None)

    qs = (
        db.session.query(*fields)
        .outerjoin(User, TicketSale.user_id == User.id)
        .join(Bus, TicketSale.bus_id == Bus.id)
        .outerjoin(O, TicketSale.origin_stop_time_id == O.id)
        .outerjoin(D, TicketSale.destination_stop_time_id == D.id)
        .filter(TicketSale.created_at >= start_utc, TicketSale.created_at < end_utc)
    )
    if bus_id_filter:
        qs = qs.filter(TicketSale.bus_id == bus_id_filter)

    if not include_voided:
        if has_voided:
            qs = qs.filter(TicketSale.voided.is_(False))
        elif has_status:
            qs = qs.filter(func.lower(func.coalesce(TicketSale.status, "")) != "voided")

    rows = qs.order_by(TicketSale.created_at.desc()).all()

    def _fmt_price(p) -> str:
        try:
            return f"{float(p or 0):.2f}"
        except Exception:
            return "0.00"

    def _is_voided(row) -> bool:
        st = (str(getattr(row, "status", "") or "")).lower()
        return bool(getattr(row, "voided", False)) or st in {"void", "voided", "refunded", "cancelled", "canceled"}

    items = []
    revenue_ex_voided_paid = 0.0

    for r in rows:
        full_name = ("{} {}".format((r.first_name or "").strip(), (r.last_name or "").strip()).strip() or None)
        is_guest = not bool(full_name)
        commuter_display = full_name or "Guest"
        is_void = _is_voided(r)
        fare_str = _fmt_price(r.price)

        if (bool(getattr(r, "paid", False)) and not is_void):
            try:
                revenue_ex_voided_paid += float(fare_str)
            except Exception:
                pass

        items.append(
            {
                "id": r.id,
                "referenceNo": getattr(r, "reference_no", None),
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "time": r.created_at.strftime("%Y-%m-%d %H:%M") if r.created_at else None,
                "fare": fare_str,
                "paid": bool(getattr(r, "paid", False)) and not is_void,
                "status": getattr(r, "status", None) or ("voided" if is_void else ("paid" if getattr(r, "paid", False) else "unpaid")),
                "voided": is_void,
                "passenger_type": (r.passenger_type or "regular"),
                "commuter": commuter_display,
                "is_guest": bool(is_guest),
                "bus_id": r.bus_id,
                "bus": r.bus,
                "origin": r.origin or "",
                "destination": r.destination or "",
                "void_reason": getattr(r, "void_reason", None),
            }
        )

    if want_array:
        return jsonify(items), 200
    else:
        return jsonify(
            {
                "tickets": items,
                "count": len(items),
                "total": round(revenue_ex_voided_paid, 2),
                "include_voided": bool(include_voided),
                "date": day.isoformat(),
                "bus_id": bus_id_filter,
            }
        ), 200

from datetime import datetime as _dt, timedelta as _td, timezone as _tz
from sqlalchemy import func, text

@manager_bp.route("/route-insights", methods=["GET"])
@require_role("manager", "pao")  # allow PAO to view
def route_data_insights():
    """
    GET /manager/route-insights
      Query:
        - trip_id=<int>
        OR
        - date=YYYY-MM-DD & bus_id=<int> & from=HH:MM & to=HH:MM

      Returns:
        {
          occupancy: [ { time: "HH:MM", passengers: int, in: int, out: int } ],
          meta: { trip_id, trip_number, window_from, window_to },
          metrics: { avg_pax, peak_pax, boarded, alighted, start_pax, end_pax, net_change },
          snapshot: boolean
        }
    """
    # Keep DB session aligned to Manila for DATETIME comparisons
    try:
        db.session.execute(text("SET time_zone = '+08:00'"))
    except Exception:
        pass

    trip_id = request.args.get("trip_id", type=int)
    use_snapshot = False

    def _trip_window(day_, start_t, end_t):
        start_dt = _dt.combine(day_, start_t)
        end_dt = _dt.combine(day_, end_t)
        if end_t <= start_t:  # crosses midnight
            end_dt = end_dt + _td(days=1)
        return start_dt, end_dt

    if trip_id:
        trip = Trip.query.filter_by(id=trip_id).first()
        if not trip:
            return jsonify(error="trip not found"), 404

        bus_id = trip.bus_id
        day = trip.service_date
        window_from, window_to = _trip_window(day, trip.start_time, trip.end_time)
        window_end_excl = window_to + _td(minutes=1)

        # If the trip already ended, try to use a snapshot
        if _dt.utcnow() > window_to + _td(minutes=2):
            snap = TripMetric.query.filter_by(trip_id=trip_id).first()
            if snap:
                use_snapshot = True
                metrics = dict(
                    avg_pax=snap.avg_pax or 0,
                    peak_pax=snap.peak_pax or 0,
                    boarded=snap.boarded or 0,
                    alighted=snap.alighted or 0,
                    start_pax=snap.start_pax or 0,
                    end_pax=snap.end_pax or 0,
                    net_change=(snap.end_pax or 0) - (snap.start_pax or 0),
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
        date_str = request.args.get("date")
        bus_id = request.args.get("bus_id", type=int)
        if not date_str or not bus_id:
            return jsonify(error="trip_id OR (date & bus_id & from & to) required"), 400

        try:
            day = _dt.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="invalid date format"), 400

        try:
            start = request.args["from"]
            end = request.args["to"]
        except KeyError:
            return jsonify(error="'from' and 'to' are required"), 400

        window_from = _dt.combine(day, _dt.strptime(start, "%H:%M").time())
        window_to = _dt.combine(day, _dt.strptime(end, "%H:%M").time())
        if window_to <= window_from:
            window_to = window_to + _td(days=1)
        window_end_excl = window_to + _td(minutes=1)

        meta = {
            "trip_id": None,
            "trip_number": None,
            "window_from": window_from.isoformat(),
            "window_to": window_to.isoformat(),
        }
        metrics = None

    # Aggregate per minute
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
            # "stop": None,  # include if/when you enrich with stop names
        }
        for r in occ_rows
    ]

    # Compute metrics when we didn't return a stored snapshot
    if not metrics:
        pax_values = [p["passengers"] for p in series]
        avg_pax = round(sum(pax_values) / len(pax_values)) if pax_values else 0
        peak_pax = max(pax_values) if pax_values else 0
        boarded = sum(p["in"] for p in series)
        alighted = sum(p["out"] for p in series)
        start_pax = pax_values[0] if pax_values else 0
        end_pax = pax_values[-1] if pax_values else 0
        metrics = {
            "avg_pax": avg_pax,
            "peak_pax": peak_pax,
            "boarded": boarded,
            "alighted": alighted,
            "start_pax": start_pax,
            "end_pax": end_pax,
            "net_change": end_pax - start_pax,
        }

    return jsonify(occupancy=series, meta=meta, metrics=metrics, snapshot=use_snapshot), 200

# ─────────────────────────────────────────────
# Sensor readings (ingest & view)
# ─────────────────────────────────────────────
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

    return (
        jsonify(
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
        ),
        200,
    )

# ─────────────────────────────────────────────
# QR templates & fare segments
# ─────────────────────────────────────────────
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
    return (
        jsonify([{"id": t.id, "url": f"/manager/qr-templates/{t.id}/file", "price": f"{t.price:.2f}"} for t in QRTemplate.query.order_by(QRTemplate.created_at.desc())]),
        200,
    )

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
            [{"id": s.id, "label": f"{s.origin.stop_name} → {s.destination.stop_name}", "price": f"{s.price:.2f}"} for s in rows]
        ),
        200,
    )

# ──────────────── PAO: upsert ────────────────
@manager_bp.route("/pao-assignments", methods=["POST"], endpoint="pao_assignments_upsert")
@require_role("manager")
def pao_assignments_upsert():
    """
    Upsert a PAO assignment for a given service_date.
    Body: { "user_id": <int>, "bus_id": <int>, "service_date": "YYYY-MM-DD" }
    Rules:
      - a PAO can appear only once per day across all buses
      - a bus can have at most one PAO for the day
    Idempotent when the same (user_id, bus_id, date) is already set.
    Also mirrors the assignment to users.assigned_bus_id (best-effort).
    """
    MNL = _tz(_td(hours=8))

    data = request.get_json(silent=True) or {}
    uid = int(data.get("user_id") or 0)
    bid = int(data.get("bus_id") or 0)
    day_str = (data.get("service_date") or _dt.now(MNL).date().isoformat()).strip()

    current_app.logger.info(
        "[pao-assignments][POST] caller uid=%s role=%s payload={user_id=%s, bus_id=%s, service_date=%s}",
        getattr(g, "user", None) and g.user.id,
        getattr(g, "user", None) and g.user.role,
        uid, bid, day_str
    )

    if not (uid and bid):
        return jsonify(error="user_id and bus_id are required"), 400
    try:
        day = _dt.strptime(day_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="invalid service_date"), 400

    u = User.query.get(uid)
    if not u:
        return jsonify(error="invalid user_id"), 400
    if (u.role or "").lower() != "pao":
        return jsonify(error="user is not a pao"), 400
    if not Bus.query.get(bid):
        return jsonify(error="invalid bus_id"), 400

    try:
        # Find existing rows that would conflict (user-per-day, bus-per-day)
        by_user = db.session.execute(
            text("""
                SELECT a.id, a.bus_id
                FROM pao_assignments a
                WHERE a.service_date = :d AND a.user_id = :u
                LIMIT 1
            """),
            {"d": day, "u": uid},
        ).mappings().first()

        by_bus = db.session.execute(
            text("""
                SELECT a.id, a.user_id
                FROM pao_assignments a
                WHERE a.service_date = :d AND a.bus_id = :b
                LIMIT 1
            """),
            {"d": day, "b": bid},
        ).mappings().first()

        # Idempotent no-op
        if by_user and int(by_user["bus_id"]) == bid:
            return jsonify(
                ok=True,
                id=int(by_user["id"]),
                user_id=uid,
                bus_id=bid,
                service_date=day.isoformat(),
                note="no change",
            ), 200

        # Carefully reorder writes to avoid transient unique collisions:
        # - If both exist and are different rows, delete user-day row first then
        #   update the bus-day row to point to the new user.
        with db.session.begin_nested():
            if by_user and not by_bus:
                db.session.execute(
                    text("UPDATE pao_assignments SET bus_id = :b WHERE id = :id"),
                    {"b": bid, "id": int(by_user["id"])},
                )
                res_id = int(by_user["id"])

            elif by_bus and not by_user:
                db.session.execute(
                    text("UPDATE pao_assignments SET user_id = :u WHERE id = :id"),
                    {"u": uid, "id": int(by_bus["id"])},
                )
                res_id = int(by_bus["id"])

            elif by_user and by_bus:
                if int(by_user["id"]) != int(by_bus["id"]):
                    db.session.execute(
                        text("DELETE FROM pao_assignments WHERE id = :id"),
                        {"id": int(by_user["id"])},
                    )
                db.session.execute(
                    text("UPDATE pao_assignments SET user_id = :u WHERE id = :id"),
                    {"u": uid, "id": int(by_bus["id"])},
                )
                res_id = int(by_bus["id"])

            else:
                ins_res = db.session.execute(
                    text("INSERT INTO pao_assignments (user_id, bus_id, service_date) VALUES (:u, :b, :d)"),
                    {"u": uid, "b": bid, "d": day},
                )
                res_id = getattr(ins_res, "lastrowid", None)

        db.session.commit()

        # Optional: mirror to users.assigned_bus_id so the change is visible on the user row.
        try:
            # If we replaced a different PAO on this bus, clear their mirror
            if by_bus and by_bus.get("user_id") and int(by_bus["user_id"]) != uid:
                db.session.execute(
                    text("UPDATE users SET assigned_bus_id = NULL WHERE id = :old"),
                    {"old": int(by_bus["user_id"])},
                )
            db.session.execute(
                text("UPDATE users SET assigned_bus_id = :b WHERE id = :u"),
                {"b": bid, "u": uid},
            )
            db.session.commit()
        except Exception:
            db.session.rollback()
            current_app.logger.warning("[pao-assignments] mirror to users.assigned_bus_id failed", exc_info=True)

        current_app.logger.info(
            "[pao-assignments][POST] UPSERT ok id=%s user_id=%s bus_id=%s date=%s",
            res_id, uid, bid, day.isoformat()
        )
        return jsonify(ok=True, id=res_id, user_id=uid, bus_id=bid, service_date=day.isoformat()), 200

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("[manager] upsert_pao_assignment failed")
        return jsonify(error=str(e)), 500


@manager_bp.route("/pao-assignments", methods=["GET"], endpoint="pao_assignments_get")
@require_role("manager", "pao")  # allow PAO & Manager to call
def pao_assignments_get():
    """
    GET /manager/pao-assignments?date=YYYY-MM-DD
    Defaults to "today" in Manila if date is omitted.
    Returns: [
      { id, service_date, bus_id, bus, user_id, pao_name }, ...
    ]
    """
    MNL = _tz(_td(hours=8))

    date_str = (request.args.get("date") or _dt.now(MNL).date().isoformat()).strip()
    try:
        day = _dt.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        current_app.logger.info("[pao-assignments][GET] invalid date=%r", date_str)
        return jsonify(error="invalid date format"), 400

    current_app.logger.info(
        "[pao-assignments][GET] caller uid=%s role=%s date=%s",
        getattr(g, "user", None) and g.user.id,
        getattr(g, "user", None) and g.user.role,
        day.isoformat(),
    )

    # include username for robust display fallback
    rows = db.session.execute(
        text("""
            SELECT a.id, a.user_id, a.bus_id, a.service_date,
                   u.first_name AS uf, u.last_name AS ul, u.username AS un,
                   b.identifier AS bus
            FROM pao_assignments a
            LEFT JOIN users u ON u.id = a.user_id
            LEFT JOIN buses b ON b.id = a.bus_id
            WHERE a.service_date = :d
            ORDER BY b.identifier
        """),
        {"d": day},
    ).mappings().all()

    out = []
    for r in rows:
        first = (r.get("uf") or "").strip()
        last  = (r.get("ul") or "").strip()
        user  = (r.get("un") or "").strip()
        name  = (f"{first} {last}".strip() or user or None)
        out.append({
            "id": int(r["id"]),
            "service_date": day.isoformat(),
            "bus_id": int(r["bus_id"]),
            "bus": r["bus"],
            "user_id": int(r["user_id"]),
            "pao_name": name,
        })
    current_app.logger.info("[pao-assignments][GET] rows=%d", len(out))
    return jsonify(out), 200


@manager_bp.route("/driver-assignments", methods=["GET"], endpoint="driver_assignments_get")
@require_role("manager")
def driver_assignments_get():
    MNL = _tz(_td(hours=8))

    date_str = (request.args.get("date") or _dt.now(MNL).date().isoformat()).strip()
    try:
        day = _dt.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        current_app.logger.info("[driver-assignments][GET] invalid date=%r", date_str)
        return jsonify(error="invalid date format"), 400

    current_app.logger.info(
        "[driver-assignments][GET] caller uid=%s role=%s date=%s",
        getattr(g, "user", None) and g.user.id,
        getattr(g, "user", None) and g.user.role,
        day.isoformat(),
    )

    rows = db.session.execute(
        text("""
            SELECT a.id, a.user_id, a.bus_id, a.service_date,
                   u.first_name AS uf, u.last_name AS ul,
                   b.identifier AS bus
            FROM driver_assignments a
            LEFT JOIN users u ON u.id = a.user_id
            LEFT JOIN buses b ON b.id = a.bus_id
            WHERE a.service_date = :d
            ORDER BY b.identifier
        """),
        {"d": day},
    ).mappings().all()

    out = []
    for r in rows:
        name = f"{(r['uf'] or '').strip()} {(r['ul'] or '').strip()}".strip() or None
        out.append({
            "id": int(r["id"]),
            "service_date": day.isoformat(),
            "bus_id": int(r["bus_id"]),
            "bus": r["bus"],
            "user_id": int(r["user_id"]),
            "driver_name": name,
        })
    current_app.logger.info("[driver-assignments][GET] rows=%d", len(out))
    return jsonify(out), 200

# ──────────────── Driver: upsert ────────────────
@manager_bp.route("/driver-assignments", methods=["POST"], endpoint="driver_assignments_upsert")
@require_role("manager")
def driver_assignments_upsert():
    """
    Upsert a Driver assignment for a given service_date.
    Body: { "user_id": <int>, "bus_id": <int>, "service_date": "YYYY-MM-DD" }
    Rules:
      - a driver can appear only once per day across all buses
      - a bus can have at most one driver for the day
    Idempotent when the same (user_id, bus_id, date) is already set.
    Also mirrors the assignment to users.assigned_bus_id (best-effort).
    """
    MNL = _tz(_td(hours=8))

    data = request.get_json(silent=True) or {}
    uid = int(data.get("user_id") or 0)
    bid = int(data.get("bus_id") or 0)
    day_str = (data.get("service_date") or _dt.now(MNL).date().isoformat()).strip()

    current_app.logger.info(
        "[driver-assignments][POST] caller uid=%s role=%s payload={user_id=%s, bus_id=%s, service_date=%s}",
        getattr(g, "user", None) and g.user.id,
        getattr(g, "user", None) and g.user.role,
        uid, bid, day_str
    )

    if not (uid and bid):
        return jsonify(error="user_id and bus_id are required"), 400
    try:
        day = _dt.strptime(day_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="invalid service_date"), 400

    u = User.query.get(uid)
    if not u:
        return jsonify(error="invalid user_id"), 400
    if (u.role or "").lower() != "driver":
        return jsonify(error="user is not a driver"), 400
    if not Bus.query.get(bid):
        return jsonify(error="invalid bus_id"), 400

    try:
        by_user = db.session.execute(
            text("""
                SELECT a.id, a.bus_id
                FROM driver_assignments a
                WHERE a.service_date = :d AND a.user_id = :u
                LIMIT 1
            """),
            {"d": day, "u": uid},
        ).mappings().first()

        by_bus = db.session.execute(
            text("""
                SELECT a.id, a.user_id
                FROM driver_assignments a
                WHERE a.service_date = :d AND a.bus_id = :b
                LIMIT 1
            """),
            {"d": day, "b": bid},
        ).mappings().first()

        if by_user and int(by_user["bus_id"]) == bid:
            return jsonify(
                ok=True,
                id=int(by_user["id"]),
                user_id=uid,
                bus_id=bid,
                service_date=day.isoformat(),
                note="no change",
            ), 200

        with db.session.begin_nested():
            if by_user and not by_bus:
                db.session.execute(
                    text("UPDATE driver_assignments SET bus_id = :b WHERE id = :id"),
                    {"b": bid, "id": int(by_user["id"])},
                )
                res_id = int(by_user["id"])

            elif by_bus and not by_user:
                db.session.execute(
                    text("UPDATE driver_assignments SET user_id = :u WHERE id = :id"),
                    {"u": uid, "id": int(by_bus["id"])},
                )
                res_id = int(by_bus["id"])

            elif by_user and by_bus:
                if int(by_user["id"]) != int(by_bus["id"]):
                    db.session.execute(
                        text("DELETE FROM driver_assignments WHERE id = :id"),
                        {"id": int(by_user["id"])},
                    )
                db.session.execute(
                    text("UPDATE driver_assignments SET user_id = :u WHERE id = :id"),
                    {"u": uid, "id": int(by_bus["id"])},
                )
                res_id = int(by_bus["id"])

            else:
                ins_res = db.session.execute(
                    text("INSERT INTO driver_assignments (user_id, bus_id, service_date) VALUES (:u, :b, :d)"),
                    {"u": uid, "b": bid, "d": day},
                )
                res_id = getattr(ins_res, "lastrowid", None)

        db.session.commit()

        # Optional: mirror to users.assigned_bus_id so the change is visible on the user row.
        try:
            if by_bus and by_bus.get("user_id") and int(by_bus["user_id"]) != uid:
                db.session.execute(
                    text("UPDATE users SET assigned_bus_id = NULL WHERE id = :old"),
                    {"old": int(by_bus["user_id"])},
                )
            db.session.execute(
                text("UPDATE users SET assigned_bus_id = :b WHERE id = :u"),
                {"b": bid, "u": uid},
            )
            db.session.commit()
        except Exception:
            db.session.rollback()
            current_app.logger.warning("[driver-assignments] mirror to users.assigned_bus_id failed", exc_info=True)

        current_app.logger.info(
            "[driver-assignments][POST] UPSERT ok id=%s user_id=%s bus_id=%s date=%s",
            res_id, uid, bid, day.isoformat()
        )
        return jsonify(ok=True, id=res_id, user_id=uid, bus_id=bid, service_date=day.isoformat()), 200

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("[manager] upsert_driver_assignment failed")
        return jsonify(error=str(e)), 500


def _set_password_dirty(u, raw: str | None):
    from werkzeug.security import generate_password_hash
    if not raw:
        return
    if hasattr(u, "set_password") and callable(getattr(u, "set_password")):
        u.set_password(raw)  # type: ignore[attr-defined]
    elif hasattr(u, "password_hash"):
        setattr(u, "password_hash", generate_password_hash(raw))

def _ensure_unique_username_phone(un: str | None, ph: str | None, *, exclude_id: int | None = None) -> str | None:
    if un:
      q = User.query.filter(User.username == un)
      if exclude_id:
          q = q.filter(User.id != exclude_id)
      if q.first():
          return "username already in use"
    if ph:
      q = User.query.filter(User.phone_number == ph)
      if exclude_id:
          q = q.filter(User.id != exclude_id)
      if q.first():
          return "phone number already in use"
    return None

@manager_bp.route("/staff/<int:user_id>", methods=["GET"])
@require_role("manager")
def get_staff(user_id: int):
    u = User.query.get(user_id)
    if not u or (u.role or "").lower() not in {"pao","driver"}:
        return jsonify(error="staff not found"), 404
    return jsonify({
        "id": int(u.id),
        "first_name": u.first_name or "",
        "last_name": u.last_name or "",
        "username": u.username or "",
        "phone_number": u.phone_number,
        "role": (u.role or "").lower() or None,
        "name": f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or u.username,
    }), 200

@manager_bp.route("/staff", methods=["DELETE"])
@require_role("manager")
def bulk_delete_staff():
    """
    Body: { "ids": [1,2,3] }
    """
    data = request.get_json(silent=True) or {}
    ids = [int(i) for i in (data.get("ids") or []) if str(i).isdigit()]
    if not ids:
        return jsonify(error="ids required"), 400
    try:
        # only staff roles
        rows = User.query.filter(User.id.in_(ids), User.role.in_(["pao","driver"])).all()
        for u in rows:
            db.session.delete(u)
        db.session.commit()
        return jsonify(ok=True, deleted=len(rows)), 200
    except IntegrityError:
        db.session.rollback()
        return jsonify(error="one or more records are in use"), 409
    except Exception as e:
        db.session.rollback()
        return jsonify(error=str(e)), 500

# helper: consistent payload
def _staff_payload(u):
    return {
        "id": int(u.id),
        "first_name": u.first_name,
        "last_name": u.last_name,
        "username": u.username,
        "phone_number": getattr(u, "phone_number", None),
        "role": (u.role or "").lower(),
    }

def _set_password_for_user(u, raw: str | None):
    if not raw:
        return
    if hasattr(u, "set_password") and callable(getattr(u, "set_password")):
        u.set_password(raw)  # type: ignore[attr-defined]
    elif hasattr(u, "password_hash"):
        setattr(u, "password_hash", generate_password_hash(raw))



@manager_bp.route("/staff/<int:user_id>", methods=["DELETE"])
@require_role("manager")
def delete_staff(user_id: int):
    u = User.query.filter(User.id == user_id, User.role.in_(("pao","driver"))).first()
    if not u:
        return jsonify(error="staff not found"), 404

    # prefer soft-delete if users.active column exists (or deleted_at)
    soft = request.args.get("soft", "1").strip().lower() in {"1","true","yes"}
    try:
        if soft and table_has_column("users", "active"):
            u.active = False  # type: ignore[attr-defined]
            if table_has_column("users", "deleted_at"):
                setattr(u, "deleted_at", datetime.utcnow())
            db.session.commit()
            return jsonify(ok=True, soft=True), 200

        # hard delete
        db.session.delete(u)
        db.session.commit()
        return jsonify(ok=True, soft=False), 200
    except IntegrityError:
        db.session.rollback()
        # fall back to soft if possible
        if table_has_column("users", "active"):
            try:
                u.active = False  # type: ignore[attr-defined]
                if table_has_column("users", "deleted_at"):
                    setattr(u, "deleted_at", datetime.utcnow())
                db.session.commit()
                return jsonify(ok=True, soft=True, note="references exist; performed soft delete"), 200
            except Exception:
                db.session.rollback()
        return jsonify(error="cannot delete: referenced by other records; try soft=1"), 409
    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("[manager] delete_staff failed")
        return jsonify(error=str(e)), 500
