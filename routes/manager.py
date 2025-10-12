# backend/routes/manager.py
from flask import Blueprint, request, jsonify, send_from_directory, current_app, g
import os
import uuid
from datetime import datetime, timedelta
from werkzeug.utils import secure_filename
from sqlalchemy import func, text, literal
from sqlalchemy.orm import aliased

from db import db
from routes.auth import require_role
from models.bus import Bus
from models.schedule import Trip
from models.qr_template import QRTemplate
from models.fare_segment import FareSegment
from models.sensor_reading import SensorReading
from models.ticket_sale import TicketSale
from models.user import User
from models.ticket_stop import TicketStop
from models.trip_metric import TripMetric

UPLOAD_DIR = os.path.join(os.getcwd(), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

manager_bp = Blueprint("manager", __name__)
from datetime import datetime, timedelta, timezone  # ← add timezone
from utils.push import push_to_user                 # ← push (best-effort)
from models.wallet import WalletAccount, WalletLedger, TopUp  # ← wallet models

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
# Manila time & void window
MNL_TZ = timezone(timedelta(hours=8))
VOID_WINDOW_HOURS = 24

def _as_php(x) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0

def _topup_reason_path(tid: int) -> str:
    # mirror teller: static/topup_receipts/{id}.reject.txt
    return os.path.join(current_app.root_path, "static", "topup_receipts", f"{tid}.reject.txt")

def _save_topup_void_reason(tid: int, text: str | None) -> None:
    if not (text and text.strip()):
        return
    try:
        p = _topup_reason_path(tid)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write(text.strip())
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

    # 24h window check
    created = getattr(t, "created_at", None)
    if created:
        created_mnl = (created.astimezone(MNL_TZ) if created.tzinfo
                       else created.replace(tzinfo=timezone.utc).astimezone(MNL_TZ))
        age_hours = (datetime.now(MNL_TZ) - created_mnl).total_seconds() / 3600.0
        if age_hours > VOID_WINDOW_HOURS:
            return jsonify(error="void window elapsed (over 24 hours)"), 400

    # Ensure wallet & balance
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
        # atomic
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

        # Realtime + push (best-effort)
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

# --- helper: does a column exist? ---
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
    # set paid -> false if column exists
    if table_has_column("ticket_sales", "paid"):
        ticket.paid = False  # type: ignore[attr-defined]

    # set void marker
    if table_has_column("ticket_sales", "voided"):
        ticket.voided = True  # type: ignore[attr-defined]
    elif table_has_column("ticket_sales", "status"):
        setattr(ticket, "status", "voided")

    # persist reason via SQL (handles flexible column name)
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

    # ensure account
    _ensure_wallet_account(int(ticket.user_id))

    # fetch current balance
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

    # update balance
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

    # write ledger
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


@manager_bp.route("/tickets/<int:ticket_id>/void", methods=["PATCH"])
@require_role("manager")
def manager_void_ticket(ticket_id: int):
    """
    Void a ticket (manager action) with a reason and refund wallet if needed.

    Body:
      { "voided": true|false, "reason": "<required when voided=true>" }

    Notes:
      - Idempotent for void=true: if already voided, no duplicate refund occurs.
      - If ticket was paid and becomes voided now, wallet is credited with the full fare.
      - Works on schemas with either `voided` (bool) or `status` (str).
      - Persists reason to `void_reason`/`reason`/`note`/`remarks` if available.
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
            # idempotent: do NOT refund again
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
            # perform state change + optional refund in one transaction
            with db.session.begin():
                _mark_ticket_void(ticket, reason)
                # do refund if it had been paid
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

    # Un-void (rare): support only when schema has boolean 'voided'; otherwise set status='paid' if it existed
    try:
        with db.session.begin():
            if table_has_column("ticket_sales", "voided"):
                setattr(ticket, "voided", False)
            if table_has_column("ticket_sales", "status"):
                # best-effort: restore to 'paid' if it had been paid; else 'unpaid'
                setattr(ticket, "status", "paid" if was_paid else "unpaid")
            if table_has_column("ticket_sales", "paid"):
                setattr(ticket, "paid", was_paid)
            # clear reason if a reason column exists
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


@manager_bp.route("/commuters", methods=["GET"])
@require_role("manager")
def list_commuters():
    """
    List commuters with basic profile info, ticket stats, search and pagination.
    Query params:
      q: optional search across first_name, last_name, username, phone_number
      page: 1-based page index (default 1)
      page_size: items per page (default 25, max 100)
    """
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

    # Tickets stats (exclude voided from revenue)
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

    # Top-ups stats — pesos only
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
            TicketSale.reference_no,  # ← include reference number
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
    try:
        to_str = request.args.get("to")
        from_str = request.args.get("from")
        page = request.args.get("page", type=int, default=1)
        size = min(max(request.args.get("page_size", type=int, default=25), 1), 100)

        to_dt = datetime.strptime(to_str, "%Y-%m-%d") if to_str else datetime.utcnow()
        fr_dt = datetime.strptime(from_str, "%Y-%m-%d") if from_str else (to_dt - timedelta(days=30))
        offset = (page - 1) * size

        total_row = db.session.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM wallet_topups t
                WHERE t.account_id = :uid
                  AND t.status = 'succeeded'
                  AND t.created_at >= :fr
                  AND t.created_at <  :to_plus
            """
            ),
            {"uid": user_id, "fr": fr_dt, "to_plus": to_dt + timedelta(days=1)},
        ).mappings().first() or {}
        total = int(total_row.get("c", 0))

        has_teller = table_has_column("wallet_topups", "teller_id")

        if has_teller:
            sql = """
                SELECT 
                    t.id,
                    t.created_at,
                    t.amount_pesos,
                    COALESCE(t.method, 'cash') AS method,
                    COALESCE(t.teller_id, t.pao_id) AS teller_id,
                    COALESCE(tu.first_name, pu.first_name) AS teller_first,
                    COALESCE(tu.last_name,  pu.last_name)  AS teller_last
                FROM wallet_topups t
                LEFT JOIN users tu ON tu.id = t.teller_id
                LEFT JOIN users pu ON pu.id = t.pao_id
                WHERE t.account_id = :uid
                  AND t.status = 'succeeded'
                  AND t.created_at >= :fr
                  AND t.created_at <  :to_plus
                ORDER BY t.created_at DESC
                LIMIT :lim OFFSET :off
            """
        else:
            sql = """
                SELECT 
                    t.id,
                    t.created_at,
                    t.amount_pesos,
                    COALESCE(t.method, 'cash') AS method,
                    t.pao_id AS teller_id,
                    pu.first_name AS teller_first,
                    pu.last_name  AS teller_last
                FROM wallet_topups t
                LEFT JOIN users pu ON pu.id = t.pao_id
                WHERE t.account_id = :uid
                  AND t.status = 'succeeded'
                  AND t.created_at >= :fr
                  AND t.created_at <  :to_plus
                ORDER BY t.created_at DESC
                LIMIT :lim OFFSET :off
            """

        rows = db.session.execute(
            text(sql),
            {"uid": user_id, "fr": fr_dt, "to_plus": to_dt + timedelta(days=1), "lim": size, "off": offset},
        ).mappings().all()

        items = [
            {
                "id": r["id"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "amount_php": float(r["amount_pesos"] or 0.0),
                "method": r["method"],
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
    
@manager_bp.route("/topups", methods=["GET"])
@require_role("manager")
def manager_topups():
    from datetime import datetime as _dt

    # Window parsing (date or start/end)
    date_str = (request.args.get("date") or "").strip()
    start_str = (request.args.get("start") or "").strip()
    end_str = (request.args.get("end") or "").strip()
    try:
        if date_str:
            day = _dt.strptime(date_str, "%Y-%m-%d").date()
            start_dt = _dt.combine(day, _dt.min.time())
            end_dt = _dt.combine(day, _dt.max.time())
        elif start_str and end_str:
            sd = _dt.strptime(start_str, "%Y-%m-%d").date()
            ed = _dt.strptime(end_str, "%Y-%m-%d").date()
            if ed < sd:
                return jsonify(error="end must be >= start"), 400
            start_dt = _dt.combine(sd, _dt.min.time())
            end_dt = _dt.combine(ed, _dt.max.time())
        else:
            day = _dt.utcnow().date()
            start_dt = _dt.combine(day, _dt.min.time())
            end_dt = _dt.combine(day, _dt.max.time())
    except ValueError:
        return jsonify(error="invalid date format (use YYYY-MM-DD)"), 400

    method = (request.args.get("method") or "").strip().lower() or None
    teller_id = request.args.get("teller_id", type=int) or request.args.get("pao_id", type=int)  # legacy

    # NEW: include voided/cancelled rows when requested
    include_voided = (request.args.get("include_voided", "false").strip().lower() in {"1", "true", "yes"})

    has_teller = table_has_column("wallet_topups", "teller_id")
    params = {"s": start_dt, "e": end_dt}

    # Two schema-safe variants (with or without teller_id column)
    if has_teller:
        sql = """
            SELECT
                t.id,
                t.account_id,
                COALESCE(t.teller_id, t.pao_id) AS teller_id,
                COALESCE(t.method, 'cash')      AS method,
                t.amount_pesos,
                t.status,
                t.created_at,
                cu.first_name  AS commuter_first,
                cu.last_name   AS commuter_last,
                au.first_name  AS teller_first,
                au.last_name   AS teller_last
            FROM wallet_topups t
            LEFT JOIN users cu ON cu.id = t.account_id
            LEFT JOIN users au ON au.id = COALESCE(t.teller_id, t.pao_id)
            WHERE t.created_at BETWEEN :s AND :e
        """
    else:
        sql = """
            SELECT
                t.id,
                t.account_id,
                t.pao_id                         AS teller_id,
                COALESCE(t.method, 'cash')       AS method,
                t.amount_pesos,
                t.status,
                t.created_at,
                cu.first_name  AS commuter_first,
                cu.last_name   AS commuter_last,
                au.first_name  AS teller_first,
                au.last_name   AS teller_last
            FROM wallet_topups t
            LEFT JOIN users cu ON cu.id = t.account_id
            LEFT JOIN users au ON au.id = t.pao_id
            WHERE t.created_at BETWEEN :s AND :e
        """

    # Status filter
    if include_voided:
        sql += " AND t.status IN ('succeeded','cancelled')"
    else:
        sql += " AND t.status = 'succeeded'"

    # Optional filters
    if method in ("cash", "gcash"):
        sql += " AND t.method = :m"
        params["m"] = method
    if teller_id:
        sql += " AND " + ("COALESCE(t.teller_id, t.pao_id)" if has_teller else "t.pao_id") + " = :tid"
        params["tid"] = teller_id

    sql += " ORDER BY t.id DESC"

    rows = db.session.execute(text(sql), params).mappings().all()

    items = []
    total_php = 0.0  # total across returned rows (note: frontend will exclude cancelled for its summary)
    for r in rows:
        commuter_name = f"{(r['commuter_first'] or '').strip()} {(r['commuter_last'] or '').strip()}".strip() or None
        teller_name = f"{(r['teller_first'] or '').strip()} {(r['teller_last'] or '').strip()}".strip() or None
        amt = float(r["amount_pesos"] or 0.0)
        total_php += amt
        items.append(
            {
                "id": int(r["id"]),
                "created_at": r["created_at"].strftime("%Y-%m-%dT%H:%M:%SZ") if r["created_at"] else None,
                "amount_php": amt,
                "method": r["method"] or "cash",
                "status": r["status"],
                "commuter_id": int(r["account_id"]) if r["account_id"] is not None else None,
                "commuter_name": commuter_name,
                "teller_id": int(r["teller_id"]) if r["teller_id"] is not None else None,
                "teller_name": teller_name,
            }
        )

    return jsonify(items=items, count=len(items), total_php=float(total_php)), 200



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


@manager_bp.route("/trips/<int:trip_id>", methods=["PATCH"])
@require_role("manager")
def update_trip(trip_id: int):
    data = request.get_json() or {}
    try:
        number = data.get("number", "").strip()
        start_time = datetime.strptime(data["start_time"], "%H:%M").time()
        end_time = datetime.strptime(data["end_time"], "%H:%M").time()

        trip = Trip.query.get_or_404(trip_id)
        trip.number = number
        trip.start_time = start_time
        trip.end_time = end_time

        db.session.commit()

        return jsonify(id=trip.id, number=trip.number, start_time=trip.start_time.strftime("%H:%M"), end_time=trip.end_time.strftime("%H:%M")), 200
    except (KeyError, ValueError):
        return jsonify(error="Invalid payload or missing required fields"), 400


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
    try:
        day = datetime.strptime(
            request.args.get("date") or datetime.utcnow().date().isoformat(),
            "%Y-%m-%d",
        ).date()
    except ValueError:
        return jsonify(error="invalid date"), 400

    bus_id_filter = request.args.get("bus_id", type=int)

    O = aliased(TicketStop)
    D = aliased(TicketStop)

    # Column existence checks
    has_status = table_has_column("ticket_sales", "status")
    has_voided = table_has_column("ticket_sales", "voided")
    has_paid   = table_has_column("ticket_sales", "paid")

    # Optional "reason" column detection (first match wins)
    reason_label = None
    for col in ("void_reason", "reason", "note", "remarks"):
        if table_has_column("ticket_sales", col):
            reason_label = getattr(TicketSale, col).label("void_reason")
            break

    # Base fields (always present)
    fields = [
        TicketSale.id,
        TicketSale.reference_no,  # ← include reference number
        TicketSale.created_at,
        TicketSale.price,
        TicketSale.passenger_type,
        User.first_name,
        User.last_name,
        Bus.id.label("bus_id"),
        Bus.identifier.label("bus"),
        O.stop_name.label("origin"),
        D.stop_name.label("destination"),
    ]

    # Schema-aware columns with fallbacks
    fields.append(TicketSale.paid if has_paid else literal(False).label("paid"))
    fields.append(TicketSale.voided if has_voided else literal(False).label("voided"))
    fields.append(TicketSale.status if has_status else literal(None).label("status"))
    if reason_label is not None:
        fields.append(reason_label)

    qs = (
        db.session.query(*fields)
        .outerjoin(User, TicketSale.user_id == User.id)
        .join(Bus, TicketSale.bus_id == Bus.id)
        .outerjoin(O, TicketSale.origin_stop_time_id == O.id)
        .outerjoin(D, TicketSale.destination_stop_time_id == D.id)
        .filter(func.date(TicketSale.created_at) == day)
    )
    if bus_id_filter:
        qs = qs.filter(TicketSale.bus_id == bus_id_filter)

    rows = qs.order_by(TicketSale.id.asc()).all()

    # Cache trips per bus for trip mapping
    trips_by_bus: dict[int, list[Trip]] = {}
    prev = day - timedelta(days=1)

    def windows_for(bus_id: int):
        if bus_id not in trips_by_bus:
            trips_by_bus[bus_id] = (
                Trip.query.filter(Trip.bus_id == bus_id, Trip.service_date.in_([day, prev]))
                .order_by(Trip.start_time.asc())
                .all()
            )
        wins = []
        for t in trips_by_bus[bus_id]:
            start = datetime.combine(t.service_date, t.start_time)
            end = datetime.combine(t.service_date, t.end_time)
            if t.end_time <= t.start_time:
                end = end + timedelta(days=1)
            wins.append((t, start, end))
        return wins

    tickets = []
    for r in rows:
        # Map to trip window
        trip_num = None
        trip_window = None
        for t, start, end in windows_for(r.bus_id):
            if start <= r.created_at < end:
                trip_num = t.number
                trip_window = f"{t.start_time.strftime('%H:%M')}–{t.end_time.strftime('%H:%M')}"
                break

        st = (str(getattr(r, "status", "") or "")).lower()
        is_void = bool(getattr(r, "voided", False)) or st in {"void", "voided", "refunded", "cancelled", "canceled"}
        void_reason = getattr(r, "void_reason", None)

        tickets.append(
            {
                "id": r.id,
                "referenceNo": getattr(r, "reference_no", None),
                "bus": r.bus,
                "commuter": f"{(r.first_name or '')} {(r.last_name or '')}".strip() or "Guest",
                "origin": r.origin or "",
                "destination": r.destination or "",
                "fare": f"{float(r.price):.2f}",
                "paid": bool(getattr(r, "paid", False)) and not is_void,
                "status": (
                    getattr(r, "status", None)
                    or ("voided" if is_void else ("paid" if getattr(r, "paid", False) else "unpaid"))
                ),
                "passenger_type": (r.passenger_type or "regular"),
                "passengerType": (r.passenger_type or "regular"),
                "voided": is_void,
                "void_reason": void_reason,
                "created_at": r.created_at.isoformat(),
                "time": r.created_at.strftime("%I:%M %p").lstrip("0").lower(),
                "trip": trip_num,
                "trip_window": trip_window,
            }
        )

    total = sum(
        float(r.price or 0)
        for r in rows
        if not (
            bool(getattr(r, "voided", False))
            or (str(getattr(r, "status", "") or "").lower() in {"void", "voided", "refunded", "cancelled", "canceled"})
        )
    )
    return jsonify(tickets=tickets, total=f"{total:.2f}"), 200

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


@manager_bp.route("/route-insights", methods=["GET"])
@require_role("manager")
def route_data_insights():
    from datetime import datetime as _dt, timedelta as _td

    trip_id = request.args.get("trip_id", type=int)
    use_snapshot = False

    def _trip_window(day_, start_t, end_t):
        start_dt = _dt.combine(day_, start_t)
        end_dt = _dt.combine(day_, end_t)
        if end_t <= start_t:
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

        if _dt.utcnow() > window_to + _td(minutes=2):
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
            return jsonify(error="date and bus_id are required when trip_id is omitted"), 400

        try:
            day = _dt.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="invalid date format"), 400

        try:
            start = request.args["from"]
            end = request.args["to"]
        except KeyError:
            return jsonify(error="from and to are required when trip_id is omitted"), 400

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

    series = [{"time": r.hhmm, "passengers": int(r.pax or 0), "in": int(r.ins or 0), "out": int(r.outs or 0)} for r in occ_rows]

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


@manager_bp.route("/bus-trips", methods=["GET"])
@require_role("manager")
def list_bus_trips():
    date_str = request.args.get("date")
    bus_id = request.args.get("bus_id", type=int)

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

    return (
        jsonify(
            [{"id": t.id, "number": t.number, "start_time": t.start_time.strftime("%H:%M"), "end_time": t.end_time.strftime("%H:%M")} for t in trips]
        ),
        200,
    )


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
