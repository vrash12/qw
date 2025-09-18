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
from utils.push import send_push_async, push_to_user  # ✅ now both are available
from decimal import Decimal
import datetime as dt
from models.wallet import TopUp                     # for daily-cap query
from services.wallet import topup_cash, topup_gcash


from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError

# make sure we actually have these in this file:
from models.wallet import WalletAccount, WalletLedger, TopUp

# wallet token verifier (opaque WLT-* or similar)
try:
    from utils.wallet_qr import verify_wallet_token
except Exception:
    verify_wallet_token = None


def _debug_enabled() -> bool:
    return (request.args.get("debug") or request.headers.get("X-Debug") or "").lower() in {"1","true","yes"}

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


from threading import Thread

# backend/routes/pao.py
from time import time as _epoch_ms

def _try_user_qr_soft(token: str):
    """
    Try to decode a signed commuter-QR with a small post-expiry grace.
    Returns: (user_id | None, grace_used | None, error_kind | None)
      - user_id: int if OK (grace_used may be True/False)
      - error_kind: "expired" or "invalid" when not OK
    """
    s = _user_qr_serializer()  # already defined in your file
    max_age = int(current_app.config.get("WALLET_QR_MAX_AGE_S", 60))
    grace   = int(current_app.config.get("WALLET_QR_GRACE_S", 8))

    try:
        payload = s.loads(token, max_age=max_age)
        uid = int(payload.get("uid") or 0)
        return (uid if uid > 0 else None, False, None)
    except SignatureExpired as e:
        # Signature is valid but too old; allow tiny grace if within max_age+grace
        try:
            valid, payload = s.loads_unsafe(token)  # (bool, payload or None)
        except Exception:
            valid, payload = False, None

        if valid and payload and getattr(e, "date_signed", None):
            # How old is the token?
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


def _try_wallet_rot_soft(tok: str):
    """
    Accept rotating wallet tokens: URLSafeSerializer payload {"uid": <int>, "mb": <minuteBucket>}
    Valid for the current minute; allow small post-rollover grace.
    Returns: (user_id|None, grace_used|None, "expired"/"invalid"|None)
    """
    from itsdangerous import BadSignature
    import time as _t

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

    now = _t.time()
    now_bucket = int(now // 60)
    grace = int(current_app.config.get("WALLET_QR_GRACE_S", 8))
    secs_into_min = int(now % 60)

    if mb == now_bucket:
        return (uid, False, None)
    if mb == (now_bucket - 1) and secs_into_min <= grace:
        return (uid, True, None)
    # older than allowed window → expired; future/garbage → invalid
    return (None, None, "expired" if mb < (now_bucket - 1) else "invalid")

# Accept minute-bucket wallet tokens from /commuter/wallet/qrcode
SALT_WALLET_QR = "wallet-qr-rot-v1"

def _wallet_rot_serializer():
    from itsdangerous import URLSafeSerializer
    return URLSafeSerializer(current_app.config["SECRET_KEY"], salt=SALT_WALLET_QR)


def _publish_user_wallet(uid: int, *, new_balance_pesos: int, event: str, **extra):
    """Send realtime wallet balance to the commuter's device via MQTT."""
    try:
        payload = {
            "type": "wallet_update",
            "event": event,                         # "payment" | "wallet_debit" | "wallet_topup"
            "new_balance_php": float(new_balance_pesos),
            "sentAt": int(_epoch_ms() * 1000),
            **extra,
        }
        publish(f"user/{uid}/wallet", payload)
    except Exception:
        current_app.logger.exception("[mqtt] user-wallet publish failed")

def _post_commit_notify(
    app,
    bus_id: int,
    user_id: int | None,
    items_ids: list[int],
    o_name: str,
    d_name: str,
    *,
    new_balance_pesos: int | None = None,
    wallet_owner_id: int | None = None,
    total_pesos: int | None = None,
    pao_id: int | None = None,
):
    """
    Background notifier to run *after* a successful DB commit.

    Args:
        app: The real Flask app object (pass current_app._get_current_object()).
        bus_id: The bus related to the transaction/operation.
        user_id: The commuter (or actor) id that initiated the action.
        items_ids: Related DB row ids (e.g., tickets) for context/telemetry.
        o_name: Origin name (for user-facing push content).
        d_name: Destination name (for user-facing push content).
        new_balance_pesos: If provided, publish a wallet balance update (PHP).
        wallet_owner_id: The wallet owner to notify/publish balance for (defaults to user_id if omitted).
        total_pesos: Optional total amount for context in payloads and pushes.
        pao_id: PAO user id (kept for parity, unused here).
    """
    import logging
    import time as _time

    # Defensive copy / normalization
    try:
        items_ids = list(items_ids or [])
    except Exception:
        items_ids = []

    try:
        with app.app_context():
            # ------------------------------
            # 1) MQTT: wallet update (if any)
            # ------------------------------
            try:
                # Prefer explicit wallet_owner_id; fall back to user_id
                wallet_uid = wallet_owner_id or user_id
                if wallet_uid is not None and new_balance_pesos is not None:
                    _publish_user_wallet(
                        int(wallet_uid),
                        new_balance_pesos=int(new_balance_pesos),
                        event="wallet_update",
                        bus_id=bus_id,
                        related_items=items_ids,
                    )
            except Exception:
                try:
                    current_app.logger.exception("[mqtt] wallet publish failed")
                except Exception:
                    logging.exception("[mqtt] wallet publish failed (no app logger)")

            # ---------------------------------------------------
            # 2) Optional: ticket/payment push (NOT announcements)
            # ---------------------------------------------------
            # We intentionally DO NOT create Announcement rows or emit_announcement here.
            # Only push a user-facing notification (if we can identify a recipient).
            try:
                push_uid = user_id or wallet_owner_id
                if push_uid:
                    # Build a concise push message; keep it short & localized-friendly.
                    title = "Ticket processed"
                    body = f"{o_name} → {d_name}"
                    if total_pesos is not None:
                        try:
                            body = f"{body} • ₱{float(total_pesos):.2f}"
                        except Exception:
                            pass

                    # Add structured data for client-side handling (optional)
                    data = {
                        "type": "ticket",
                        "bus_id": int(bus_id) if bus_id is not None else None,
                        "items": items_ids,
                        "origin": o_name,
                        "destination": d_name,
                        "total_php": float(total_pesos) if total_pesos is not None else None,
                        "sentAt": int(_epoch_ms() * 1000),
                    }

                    # Fire-and-forget push
                    try:
                        push_to_user(int(push_uid), title=title, body=body, data=data)
                    except Exception:
                        # Fallback: best-effort
                        current_app.logger.exception("[push] push_to_user failed")
            except Exception:
                try:
                    current_app.logger.exception("[push] block crashed")
                except Exception:
                    logging.exception("[push] block crashed (no app logger)")

            # ---------------------------------------------------
            # 3) (Intentionally removed) Ticket Announcements
            # ---------------------------------------------------
            # NOTE:
            # Previously this function persisted an Announcement and emitted it
            # ~2 seconds after commit:
            #   - created an Announcement("Ticket issued • {o_name} → {d_name} …")
            #   - emit_announcement(payload, bus_id=bus_id)
            #
            # Per request, that behavior has been removed so ticket-create events
            # will NOT appear in the Announcements feed anymore.

    except Exception:
        # Top-level guard: never let this background worker crash the caller.
        try:
            current_app.logger.exception("[post-commit] worker crashed")
        except Exception:
            logging.exception("[post-commit] worker crashed (no app logger)")



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

@pao_bp.route("/users/scan", methods=["GET"])
@require_role("pao")
def user_qr_scan():
    token = (request.args.get("token") or "").strip()
    if not token:
        return jsonify(error="token required"), 400

    try:
        payload = verify_user_qr_token(token)  # ✅ use the signed-QR verifier
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
    Body JSON:
      {
        "amount_php": 25,               # required, float or int
        "wallet_token": "WLT-...",      # optional (opaque token → user_id)
        "user_id": 123,                 # optional (alternative to wallet_token)
        "ticket_id": 456                # optional (when charging a specific ticket)
      }
    Returns 200 on success with {"ok": true, "user_id": <id>, "new_balance_php": <int>}.
    Sends a push:
      - type="wallet_debit" (and ticketId if provided)
      - channelId="payments"
    """
    data = request.get_json(silent=True) or {}

    # --- amount (PHP) ---
    try:
        amount_php = float(Decimal(str(data.get("amount_php"))))
    except Exception:
        return jsonify(error="invalid amount_php"), 400
    if amount_php <= 0:
        return jsonify(error="amount must be > 0"), 400
    amount_pesos = int(round(amount_php))  # WHOLE PESOS (no *100)

    # --- resolve user ---
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
    t = None  # TicketSale row, if provided

    try:
        # --- lock the wallet row (table has no 'id') ---
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

        # --- if charging a ticket, lock ticket & guard duplicates ---
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

            # Duplicate guard in ledger (same ticket already charged)
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

        # --- funds check ---
        if balance_pesos < amount_pesos:
            # If a ticket exists and isn't paid, delete it to avoid stray UNPAID rows
            if t is not None and not t.paid:
                try:
                    db.session.delete(t)
                    db.session.commit()  # commit the delete to release locks
                    current_app.logger.info(
                        "[PAO:wallet_charge] insufficient funds → deleted ticket_id=%s", t.id
                    )
                except Exception:
                    db.session.rollback()
                    current_app.logger.exception(
                        "[PAO:wallet_charge] failed deleting unpaid ticket after insufficient funds"
                    )
            return jsonify(
                error="insufficient balance",
                balance_php=float(balance_pesos),
                required_php=float(amount_pesos),
            ), 402

        # --- apply debit ---
        new_balance = balance_pesos - amount_pesos
        db.session.execute(
            text("UPDATE wallet_accounts SET balance_pesos=:bal WHERE user_id=:uid"),
            {"bal": new_balance, "uid": user_id},
        )

        # Ledger insert
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

        # --- Push notify commuter (best-effort) ---
        try:
            import time as _time
            sent_at = int(_time.time() * 1000)

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

        return jsonify(
            ok=True,
            user_id=user_id,
            new_balance_php=float(new_balance)
        ), 200

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
    Resolve a scanned QR into a commuter.

    Accepts JSON body or query string:
      { "token": "...", "wallet_token": "...", "raw": "full URL or token", "autopay": true|false }

    Returns on success (200):
      {
        "valid": true,
        "token_type": "user_qr" | "wallet_token",
        "autopay": false,
        "user": { "id": 123, "name": "First Last" },
        "user_id": 123,                # legacy mirror
        "name": "First Last",          # legacy mirror
        "balance_php": 123.0
      }

    Failure codes:
      410 Gone          -> signed commuter QR expired (grace not applicable / exceeded)
      422 Unprocessable -> invalid token/signature
      404               -> user not found
      400               -> missing token
    """
    import time
    from urllib.parse import urlparse, parse_qs
    from itsdangerous import BadSignature, SignatureExpired
    from datetime import datetime, timezone

    rid = request.headers.get("X-Request-ID") or f"resolve-{int(time.time()*1000)}"
    data = request.get_json(silent=True) or {}

    # --- collect inputs (works for POST or GET) --------------------------------
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

    try:
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
    expired_hint = False  # used to pick 410 vs 422

    # --- helper: soften expiry for signed commuter QR --------------------------
    def _try_user_qr_soft(tok: str):
        """
        Try signed commuter-QR with small post-expiry grace.
        Returns: (user_id | None, grace_used | None, error_kind | None)
        """
        s = _user_qr_serializer()
        max_age = int(current_app.config.get("WALLET_QR_MAX_AGE_S", 60))
        grace = int(current_app.config.get("WALLET_QR_GRACE_S", 8))

        try:
            payload = s.loads(tok, max_age=max_age)
            uid = int(payload.get("uid") or 0)
            return (uid if uid > 0 else None, False, None)
        except SignatureExpired as e:
            # Signature ok but too old; allow small grace if within max_age+grace
            try:
                valid, payload = s.loads_unsafe(tok)  # (bool, payload)
            except Exception:
                valid, payload = False, None

            signed_at = getattr(e, "date_signed", None)
            if valid and payload and signed_at:
                if signed_at.tzinfo is None:
                    signed_at = signed_at.replace(tzinfo=timezone.utc)
                age_sec = (datetime.now(timezone.utc) - signed_at).total_seconds()
                if age_sec <= (max_age + grace):
                    uid = int(payload.get("uid") or 0)
                    return (uid if uid > 0 else None, True, None)
            return (None, None, "expired")
        except BadSignature:
            return (None, None, "invalid")
        except Exception:
            return (None, None, "invalid")

    # --- First: commuter QR signed by itsdangerous -----------------------------
    try_uid, grace_used, err_kind = _try_user_qr_soft(token)
    if try_uid:
        user_id = try_uid
        token_type = "user_qr"
        if _debug_enabled():
            dbg["user_qr_grace"] = bool(grace_used)
    elif err_kind == "expired":
        expired_hint = True
    elif err_kind == "invalid":
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

    # --- no match --------------------------------------------------------------
    if not user_id:
        status = 410 if expired_hint else 422  # Gone vs Unprocessable
        current_app.logger.warning("[PAO:resolve][%s] invalid/expired token", rid)
        out = {"valid": False, "error": ("expired" if expired_hint else "invalid")}
        if _debug_enabled():
            out["debug"] = dbg
        return jsonify(out), status

    # --- look up user + balance -----------------------------------------------
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
        # legacy mirrors for older clients
        "user_id": int(user.id),
        "name": f"{user.first_name} {user.last_name}",
        "balance_php": float(balance_pesos),
    }
    resp = jsonify(payload)

    # handy debug headers
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
    Overview of a commuter wallet (amounts are whole pesos).
    - recent_topups: last N successful topups to this account
    - recent_ledger: last M ledger entries for this account
    - pao_today: aggregate of successful topups performed by the current PAO today
    """
    limit_topups = request.args.get("limit_topups", type=int) or 10
    limit_ledger = request.args.get("limit_ledger", type=int) or 15

    # --- account + balance (wallet_accounts uses user_id as the account id) ---
    row = db.session.execute(
        text(
            "SELECT user_id, COALESCE(balance_pesos,0) AS balance_pesos "
            "FROM wallet_accounts WHERE user_id=:uid"
        ),
        {"uid": user_id},
    ).mappings().first()

    balance_pesos = int((row or {}).get("balance_pesos", 0))
    account_id = int((row or {}).get("user_id", 0)) or None  # we use user_id as account_id

    # --- recent top-ups (desc) ---
    # Use raw SQL + explicit column list so we don't depend on ORM column mapping.
    topups = []
    if account_id and limit_topups > 0:
        rows = db.session.execute(
            text(
                """
                SELECT id, account_id, method, amount_pesos, status, pao_id, created_at
                FROM wallet_topups
                WHERE account_id = :aid AND status = 'succeeded'
                ORDER BY id DESC
                LIMIT :lim
                """
            ),
            {"aid": account_id, "lim": int(limit_topups)},
        ).mappings().all()

        pao_cache: dict[int, str] = {}

        def _pao_name(pao_id: int | None) -> str:
            if not pao_id:
                return ""
            if pao_id in pao_cache:
                return pao_cache[pao_id]
            u = User.query.get(pao_id)
            pao_cache[pao_id] = (f"{u.first_name} {u.last_name}" if u else "")
            return pao_cache[pao_id]

        topups = [
            {
                "id": r["id"],
                "amount_php": float(r["amount_pesos"]),
                "created_at": _iso_utc(r["created_at"]),
                "pao_name": _pao_name(r.get("pao_id")),
                "method": r.get("method"),
            }
            for r in rows
        ]

    # --- recent ledger (desc) ---
    ledger = []
    if account_id and limit_ledger > 0:
        rows = db.session.execute(
            text(
                """
                SELECT id, account_id, direction, event, amount_pesos, running_balance_pesos,
                       ref_table, ref_id, created_at
                FROM wallet_ledger
                WHERE account_id = :aid
                ORDER BY id DESC
                LIMIT :lim
                """
            ),
            {"aid": account_id, "lim": int(limit_ledger)},
        ).mappings().all()

        ledger = [
            {
                "id": r["id"],
                "direction": r["direction"],
                "event": r["event"],
                "amount_php": float(r["amount_pesos"]),
                "running_balance_php": float(r["running_balance_pesos"]),
                "created_at": _iso_utc(r["created_at"]),
                "ref": {"table": r["ref_table"], "id": r["ref_id"]},
            }
            for r in rows
        ]

    # --- PAO "today" usage stats (no cap enforcement) ---
    day = dt.datetime.utcnow().date()
    start_dt = dt.datetime.combine(day, dt.time.min)
    end_dt = dt.datetime.combine(day, dt.time.max)

    used_sum = db.session.execute(
        text(
            """
            SELECT COALESCE(SUM(amount_pesos), 0)
            FROM wallet_topups
            WHERE pao_id = :pid
              AND status = 'succeeded'
              AND created_at BETWEEN :s AND :e
            """
        ),
        {"pid": g.user.id, "s": start_dt, "e": end_dt},
    ).scalar() or 0

    used_count = db.session.execute(
        text(
            """
            SELECT COUNT(*)
            FROM wallet_topups
            WHERE pao_id = :pid
              AND status = 'succeeded'
              AND created_at BETWEEN :s AND :e
            """
        ),
        {"pid": g.user.id, "s": start_dt, "e": end_dt},
    ).scalar() or 0

    return jsonify(
        user_id=user_id,
        balance_php=float(balance_pesos),
        recent_topups=topups,
        recent_ledger=ledger,
        pao_today={
            "count": int(used_count),
            "sum_php": float(used_sum),
            # keep `cap_php` out (or set to None for client compat if needed)
        },
    ), 200


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
        .filter(TicketSale.bus_id == bus_id, TicketSale.paid.is_(True))
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
            "voided": bool(getattr(t, "voided", False)),

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
    Body: {
      "amount": 500,
      "method": "cash" | "gcash",
      "wallet_token": "opaque" |  null,
      "user_id": 123 | null,
      "gcash_ref": "PSP-REF-123" | null
    }
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

    # validate method
    if method not in ("cash", "gcash"):
        current_app.logger.warning("[PAO:topup][%s] reject: invalid method %r", rid, method)
        return jsonify(error="invalid method (must be 'cash' or 'gcash')"), 400

    # validate amount
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

    # resolve user via token or user_id
    if token:
        if verify_wallet_token is None:
            current_app.logger.warning("[PAO:topup][%s] reject: wallet token not supported", rid)
            return jsonify(error="wallet token not supported"), 400
        try:
            user_id = int(verify_wallet_token(token))
        except Exception:
            current_app.logger.warning(
                "[PAO:topup][%s] reject: invalid wallet token token=%s", rid, _mask_token(token)
            )
            return jsonify(error="invalid wallet token"), 400

    if not user_id:
        current_app.logger.warning("[PAO:topup][%s] reject: missing wallet_token or user_id", rid)
        return jsonify(error="missing wallet_token or user_id"), 400



    # ---- perform top-up via services ----
    try:
        amount_pesos = int(round(amount_php))  
        account_id = int(user_id)

        if method == "gcash":
            # accept either "gcash_ref" or generic "external_ref"
            external_ref = (data.get("gcash_ref") or data.get("external_ref") or "").strip() or None
            topup_id, ledger_id, new_balance_php = topup_gcash(
                account_id=account_id,
                pao_id=g.user.id,
                amount_pesos=amount_pesos,
                external_ref=external_ref,
            )
        else:
            topup_id, ledger_id, new_balance_php = topup_cash(
                account_id=account_id,
                pao_id=g.user.id,
                amount_pesos=amount_pesos,
            )

        current_app.logger.info(
            "[PAO:topup][%s] success: topup_id=%s ledger_id=%s user_id=%s amount=%.2f new_balance=%.2f",
            rid, topup_id, ledger_id, user_id, amount_php, new_balance_php
        )
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[PAO:topup][%s] DB failure", rid)
        return jsonify(error="internal error"), 500

    # Push notify commuter (best-effort)
    try:
        tokens = [t.token for t in DeviceToken.query.filter_by(user_id=int(user_id)).all()]
        if tokens:
            sent_at = int(_time.time() * 1000)
            kind = "GCash" if method == "gcash" else "Cash"

            # Push notify commuter (best-effort)
            try:
                import time as _time
                sent_at = int(_time.time() * 1000)
                kind = "GCash" if method == "gcash" else "Cash"

                payload = {
                    "type": "wallet_topup",
                    "topup_id": int(topup_id),
                    "user_id": int(user_id),
                    "method": method,
                    "amount_php": float(round(amount_php, 2)),
                    "new_balance_php": float(round(new_balance_php, 2)),
                    "amount": float(round(amount_php, 2)),
                    "newBalance": float(round(new_balance_php, 2)),
                    "deeplink": "/commuter/wallet",
                    "sentAt": sent_at,
                }

                push_to_user(
                    db, DeviceToken, int(user_id),
                    "💸 Wallet top-up",
                    f"{kind} +₱{amount_php:.2f} • New balance ₱{new_balance_php:.2f}",
                    payload,
                    channelId="payments", priority="high", ttl=600, sound="default",
                )
            except Exception:
                current_app.logger.exception("[push] topup notify failed")

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

    data = request.get_json(silent=True) or {}
    current_app.logger.debug(f"[PAO:/tickets wallet-only SINGLE] raw payload: {data}")

    try:
        # ---- Resolve O/D stops ------------------------------------------------
        o_id = data.get("origin_stop_id") or data.get("origin_stop_time_id")
        d_id = data.get("destination_stop_id") or data.get("destination_stop_time_id")
        o = TicketStop.query.get(o_id)
        d = TicketStop.query.get(d_id)
        if not o or not d:
            return jsonify(error="origin or destination not found"), 400

        # ---- Parse passenger blocks (supports list or legacy single) ----------
        items_spec = data.get("items")
        n_regular = 0
        n_discount = 0
        if isinstance(items_spec, list):
            for b in items_spec:
                pt = (b or {}).get("passenger_type")
                qty = int((b or {}).get("quantity") or 0)
                if pt not in ("regular", "discount") or qty < 0:
                    return jsonify(error="invalid passenger_type/quantity"), 400
                if pt == "regular":
                    n_regular += qty
                else:
                    n_discount += qty
        else:
            # legacy solo
            pt = data.get("passenger_type")
            if pt not in ("regular", "discount"):
                return jsonify(error="invalid passenger_type"), 400
            qty = max(1, min(int(data.get("quantity") or 1), 20))
            if pt == "regular":
                n_regular = qty
            else:
                n_discount = qty

        total_qty = n_regular + n_discount
        if total_qty <= 0:
            return jsonify(error="no passengers"), 400
        if total_qty > 20:
            return jsonify(error="quantity exceeds limit (20)"), 400

        # ---- Ticket timestamp --------------------------------------------------
        client_ts = data.get("created_at")
        try:
            ticket_dt = dtparse.parse(client_ts) if client_ts else datetime.utcnow()

        except Exception:
            ticket_dt = datetime.now()

        # ---- Decide stored passenger_type (for compatibility) -----------------
        primary_type = (data.get("primary_type") or ("regular" if n_regular >= n_discount else "discount")).lower()
        if primary_type not in ("regular", "discount"):
            primary_type = "regular"

        # ---- Wallet credential (REQUIRED) -------------------------------------
        wallet_token = (data.get("wallet_token") or "").strip()
        wallet_user_id = data.get("user_id")
        account_user_id: int | None = None
        if wallet_token:
            if verify_wallet_token is None:
                return jsonify(error="wallet token not supported"), 400
            try:
                account_user_id = int(verify_wallet_token(wallet_token))
            except Exception:
                return jsonify(error="invalid wallet token"), 400
        elif wallet_user_id not in (None, "", 0):
            account_user_id = int(wallet_user_id)
        if not account_user_id:
            return jsonify(error="missing wallet_token or user_id"), 400

        # Ticket "commuter" to assign to (use commuter_id if provided, else the wallet owner)
        requested_commuter_id = data.get("commuter_id")
        linked_user_id = int(requested_commuter_id or account_user_id)
        user = User.query.get(linked_user_id)
        if not user:
            return jsonify(error="invalid commuter_id"), 400

        # ---- Bus context -------------------------------------------------------
        bus_id = _current_bus_id()
        if not bus_id:
            return jsonify(error="PAO has no assigned bus"), 400

        # ---- Fare math (WHOLE PESOS) ------------------------------------------
        def _fare_for_local(pt: str) -> int:
            hops = abs(o.seq - d.seq)
            base = 10 + max(hops - 1, 0) * 2
            return round(base * 0.8) if pt == "discount" else base

        total_pesos = int(round(
            _fare_for_local("regular") * n_regular +
            _fare_for_local("discount") * n_discount
        ))

        # ---- Atomic pre-debit (fails → 402; nothing created) ------------------
        upd = db.session.execute(
            text("""
                UPDATE wallet_accounts
                SET balance_pesos = balance_pesos - :amt
                WHERE user_id = :uid AND balance_pesos >= :amt
            """),
            {"uid": account_user_id, "amt": total_pesos},
        )
        if upd.rowcount != 1:
            return jsonify(
                error="insufficient balance",
                account_user_id=int(account_user_id),
                required_php=float(total_pesos),
            ), 402

        # ---- Create a *single* paid ticket carrying group meta ----------------
        t = TicketSale(
            bus_id=bus_id,
            user_id=user.id,                 # single owner (no guests in single-row model)
            guest=False,
            price=total_pesos,               # whole pesos (group total)
            passenger_type=primary_type,     # keep legacy compatibility
            reference_no="TEMP",
            paid=True,
            created_at=ticket_dt,
            origin_stop_time_id=o.id,
            destination_stop_time_id=d.id,
            issued_by=g.user.id,
            # group meta:
            is_group=bool(total_qty > 1),
            group_regular=int(n_regular),
            group_discount=int(n_discount),
        )
        db.session.add(t)
        db.session.flush()                   # get t.id
        t.reference_no = _gen_reference(bus_id, t.id)

        # ---- Ledger entry (single row, references this ticket) ----------------
        new_balance = db.session.execute(
            text("SELECT COALESCE(balance_pesos,0) FROM wallet_accounts WHERE user_id=:uid"),
            {"uid": account_user_id}
        ).scalar() or 0
        db.session.execute(
            text("""
                INSERT INTO wallet_ledger
                    (account_id, direction, event, amount_pesos, running_balance_pesos,
                     ref_table, ref_id, created_at)
                VALUES
                    (:aid, 'debit', 'ride', :amt, :run, 'ticket_sale', :rid, NOW())
            """),
            {"aid": account_user_id, "amt": total_pesos, "run": int(new_balance), "rid": int(t.id)},
        )

        db.session.commit()

        # ---- Async notifications (MQTT + push best-effort) --------------------
        app_obj = current_app._get_current_object()
        Thread(
            target=_post_commit_notify,
            args=(app_obj, bus_id, int(user.id), [int(t.id)], o.stop_name, d.stop_name),
            kwargs=dict(
                new_balance_pesos=int(new_balance),
                wallet_owner_id=int(account_user_id),
                total_pesos=int(total_pesos),
                pao_id=int(g.user.id),                   # 👈 NEW
            ),
            daemon=True,
        ).start()


        # ---- Response: single ticket ------------------------------------------
        origin_name = o.stop_name
        destination_name = d.stop_name
        payload = _serialize_ticket_json(t, origin_name, destination_name)
        # extra hints (clients may ignore safely)
        payload["count"] = int(total_qty)
        payload["group"] = {
            "regular": int(n_regular),
            "discount": int(n_discount),
            "total": int(total_qty),
        }
        return jsonify(payload), 201

    except Exception as e:
        db.session.rollback()
        current_app.logger.exception("!! create_ticket (wallet-only SINGLE) unexpected error")
        return jsonify(error=str(e)), 500


@pao_bp.route("/tickets/<int:ticket_id>/void", methods=["PATCH"])
@require_role("pao")
def void_ticket(ticket_id: int):
    """
    Void a ticket and (if previously paid to a commuter wallet) refund the full amount.
    Body JSON: { "voided": true, "reason": "..." }
    Returns: { id, voided, refunded_php, reason }

    Notes:
      - Locks the ticket row to prevent concurrent updates.
      - If ticket was paid and has a user_id, credits the wallet back and writes a
        `wallet_ledger` credit (event='refund', ref_table='ticket_sale', ref_id=ticket_id).
      - Publishes a best-effort wallet MQTT update to the commuter channel.
      - Push-notifies the commuter (channelId=payments).
      - After commit, recalculates the device's "paid" count for today and publishes to MQTT.
      - Checks that the calling PAO is assigned to the same bus as the ticket.
    """
    data = request.get_json(silent=True) or {}
    want_void = bool(data.get("voided"))
    reason = (data.get("reason") or "").strip()
    if not want_void:
        return jsonify(error="set voided=true to proceed"), 400
    if not reason:
        return jsonify(error="void reason is required"), 400
    reason = reason[:200]  # keep short; aligns with UI expectations

    # Lock ticket row and fetch related user for push
    t = (
        TicketSale.query.options(joinedload(TicketSale.user), joinedload(TicketSale.bus))
        .filter(TicketSale.id == ticket_id)
        .with_for_update()
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Authorization: PAO can only void tickets from their own assigned bus
    caller_bus_id = getattr(getattr(g, "user", None), "assigned_bus_id", None)
    if caller_bus_id and t.bus_id and int(caller_bus_id) != int(t.bus_id):
        return jsonify(error="not allowed to void tickets from another bus"), 403

    # Already voided?
    if bool(getattr(t, "voided", False)):
        return jsonify(error="already voided", id=t.id, voided=True), 409

    amount_pesos = int(round(float(t.price or 0)))
    refunded = 0

    # If paid and belongs to a commuter, credit their wallet back
    if bool(t.paid) and t.user_id:
        # Duplicate guard: ensure we haven't already written a refund for this ticket
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
            # Update wallet balance
            db.session.execute(
                text("UPDATE wallet_accounts SET balance_pesos = balance_pesos + :amt WHERE user_id = :uid"),
                {"amt": amount_pesos, "uid": int(t.user_id)},
            )

            # Read new balance for ledger + push
            new_balance = int(
                db.session.execute(
                    text("SELECT COALESCE(balance_pesos,0) FROM wallet_accounts WHERE user_id=:uid"),
                    {"uid": int(t.user_id)},
                ).scalar() or 0
            )

            # Ledger: refund credit (event='refund', ref_table='ticket_sale')
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

            # Realtime wallet update (best-effort)
            try:
                _publish_user_wallet(
                    int(t.user_id),
                    new_balance_pesos=int(new_balance),
                    event="refund",
                    ticket_id=int(t.id),
                )
            except Exception:
                current_app.logger.exception("[void] mqtt publish failed")

            # Push notify commuter (best-effort)
            try:
                import time as _time
                sent_at = int(_time.time() * 1000)
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

    # Flip ticket flags and set metadata
    from datetime import datetime as _dt
    t.paid = False
    setattr(t, "voided", True)
    setattr(t, "void_reason", reason)
    setattr(t, "voided_at", _dt.utcnow())
    setattr(t, "voided_by", getattr(g.user, "id", None))

    # If your table has a status column, keep it in sync
    try:
        if hasattr(TicketSale, "status"):
            setattr(t, "status", "voided")
    except Exception:
        pass

    # Commit DB changes first
    try:
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[void] commit failed")
        return jsonify(error="internal error"), 500

    # After-commit: publish updated "paid count" to the device for *today*
    # (mirrors mark_ticket_paid behavior so hardware dashboards stay accurate)
    try:
        from datetime import datetime as _dt
        start = _dt.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        end   = _dt.utcnow().replace(hour=23, minute=59, second=59, microsecond=999999)
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
    end_dt   = datetime.combine(day, datetime.max.time())

    bus_id = _current_bus_id()
    if not bus_id:
        return jsonify(error="PAO has no assigned bus"), 400

    # 1) Find batches for the day
    groups = db.session.execute(
        text("""
            SELECT
            MIN(id)  AS head_id,
            COUNT(*) AS qty,
            SUM(CAST(price AS SIGNED)) AS total_pesos
            FROM ticket_sales
            WHERE bus_id = :bus
            AND created_at BETWEEN :s AND :e
            AND (paid = 1 OR COALESCE(voided, 0) = 1)
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
        {"bus": bus_id, "s": start_dt, "e": end_dt},
    ).mappings().all()


    if not groups:
        return jsonify([]), 200

    head_ids = [int(r["head_id"]) for r in groups]

    # 2) Load head tickets in one go for details (commuter, O/D, timestamps, ref)
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

        # Origin/Destination names (use head ticket)
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
            "id": head.id,                                 # use head-id as the row id
            "referenceNo": head.reference_no,
            "commuter": _commuter_label(head),
            "date": head.created_at.strftime("%B %d, %Y"),
            "time": head.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "origin": origin_name,
            "destination": destination_name,
            "fare": f"{float(r['total_pesos'] or 0):.2f}", # TOTAL for the batch
            "paid": (bool(getattr(head, "paid", True)) and not is_void),
            # Optional extras (UI can ignore safely):
            "passengers": int(r["qty"] or 0),
            "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=head.id, _external=True),
            "voided": is_void,

        })

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
    gr = int(getattr(t, "group_regular", 0) or 0)
    gd = int(getattr(t, "group_discount", 0) or 0)
    total = gr + gd
    is_group = bool(getattr(t, "is_group", False) or total > 1)
    group_block = ({"regular": gr, "discount": gd, "total": total} if is_group else None)
    resp = {
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
        "voided": bool(getattr(t, "voided", False)),
        "void_reason": getattr(t, "void_reason", None),
        "qr": payload,
        "qr_link": qr_link,
        "qr_url": qr_url,
        "qr_bg_url": qr_bg_url,
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
        "paoId": getattr(t, "issued_by", None) or getattr(g, "user", None).id,
    }

    if is_group:
        resp["isGroup"] = True
        resp["group"] = group_block
        resp["count"] = total

    return jsonify(resp), 200


@pao_bp.route("/tickets/<int:ticket_id>", methods=["PATCH"])
@require_role("pao")
def mark_ticket_paid(ticket_id: int):
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

    was_paid = bool(ticket.paid)
    ticket.paid = 1 if paid else 0

    try:
        db.session.commit()

        # Update device’s live paid count via MQTT for *today* (UTC)
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