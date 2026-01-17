from __future__ import annotations

import os
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from flask import Blueprint, request, jsonify, current_app, url_for, g
from sqlalchemy.orm import aliased
from sqlalchemy import func, text
from sqlalchemy.exc import SQLAlchemyError
from db import db
from models.user import User
from models.wallet import WalletAccount, WalletLedger, TopUp
from models.device_token import DeviceToken
from utils.push import push_to_user  # (kept if you still use FCM push elsewhere)

try:
    from routes.auth import require_role
except Exception:
    from auth import require_role

# ──────────────────────────────────────────────────────────────────────────────
# Wallet services (operator-less) — CASH ONLY
from services.wallet import topup_cash, approve_topup_existing

# Optional realtime publish (best-effort / no-op if module missing)
try:
    # def publish(topic: str, payload: dict) -> bool
    from mqtt_ingest import publish as mqtt_publish
except Exception:
    mqtt_publish = None  # type: ignore[assignment]

# For verifying commuter QR tokens (must match routes/commuter.py)
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from itsdangerous import URLSafeSerializer

teller_bp = Blueprint("teller", __name__, url_prefix="/teller")

# ──────────────────────────────────────────────────────────────────────────────
# Config / constants

# Manila time (UTC+8, no DST)
MNL_TZ = timezone(timedelta(hours=8))

# Per-transaction bounds (whole pesos)
MIN_TOPUP = 100
MAX_TOPUP = 1000

# Where commuter receipts are stored by /commuter/topup-requests
RECEIPTS_DIR = "topup_receipts"

# Must match SALT_USER_QR and SALT_WALLET_QR in routes/commuter.py
SALT_USER_QR = "user-qr-v1"
SALT_WALLET_QR = "wallet-qr-rot-v1"


def _norm_uid(uid: str) -> str:
    # keep only hex-ish chars + digits; works with "47631906" too
    u = (uid or "").strip().replace(" ", "").upper()
    u = "".join([c for c in u if c in "0123456789ABCDEF"])
    return u

def _ensure_nfc_cards_table() -> None:
    """
    Dev-friendly: auto-create table if missing.
    If you use migrations, do it there instead and remove this.
    """
    try:
        db.session.execute(text("""
            CREATE TABLE IF NOT EXISTS nfc_cards (
                uid TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """))
        db.session.execute(text("CREATE INDEX IF NOT EXISTS idx_nfc_cards_user_id ON nfc_cards(user_id)"))
        db.session.commit()
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[teller] ensure nfc_cards table failed")

def _lookup_user_by_nfc(uid: str) -> Optional[int]:
    uid = _norm_uid(uid)
    if not uid:
        return None

    _ensure_nfc_cards_table()

    try:
        row = db.session.execute(
            text("SELECT user_id, status FROM nfc_cards WHERE uid=:uid LIMIT 1"),
            {"uid": uid},
        ).mappings().first()
    except Exception:
        current_app.logger.exception("[teller] nfc lookup failed")
        return None

    if not row:
        return None
    if (row.get("status") or "").lower() != "active":
        return None

    try:
        return int(row["user_id"])
    except Exception:
        return None

def _now_mnl() -> datetime:
    return datetime.now(MNL_TZ)

def _today_bounds_mnl() -> Tuple[datetime, datetime]:
    now = _now_mnl()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def _as_php(x: Optional[int]) -> int:
    """All wallet domain amounts are whole pesos already; coerce to int."""
    try:
        return int(x or 0)
    except Exception:
        return 0

def _save_reject_reason(tid: int, text: str) -> None:
    try:
        p = _reject_reason_path(tid)
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write((text or "").strip())
    except Exception:
        current_app.logger.exception("[teller] write reject/void reason failed tid=%s", tid)

VOID_WINDOW_HOURS = 24

def _save_reason(tid: int, text: Optional[str]) -> None:
    if not text:
        return
    try:
        p = _reject_reason_path(tid)  # reuse same file path
        os.makedirs(os.path.dirname(p), exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write((text or "").strip() + "\n")
    except Exception:
        current_app.logger.exception("[teller] save reason failed tid=%s", tid)


def _user_name(u: Optional[User]) -> str:
    if not u:
        return ""
    fn = (u.first_name or "").strip()
    ln = (u.last_name or "").strip()
    name = (fn + " " + ln).strip()
    return name or (u.username or f"User #{u.id}")

# Signed user QR (static per user, time-limited)
def _unsign_user_qr(token: str, *, max_age_sec: int = 7 * 24 * 3600) -> Optional[int]:
    """
    Accepts signed user QR tokens (from /commuter/users/me/qr.png).
    Defaults to 7 days validity unless your commuter.py uses another window.
    """
    try:
        s = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)
        data = s.loads(token, max_age=max_age_sec)
        uid = int(data.get("uid", 0))
        return uid if uid > 0 else None
    except Exception:
        return None

@teller_bp.route("/wallet/topups/<int:tid>/void", methods=["POST"])
@require_role("teller")
def void_topup(tid: int):
    """
    Reverse a succeeded cash top-up:
      - guard: succeeded status, method=cash, <=24h old
      - guard: wallet has sufficient balance (no overdraft)
      - effect: debit wallet, add ledger row, mark TopUp as 'cancelled'
    """
    data = request.get_json(silent=True) or {}
    reason = (data.get("reason") or "").strip()

    # Fetch the top-up
    t = TopUp.query.get(tid)
    if not t:
        return jsonify(error="top-up not found"), 404

    if (t.status or "").lower() != "succeeded":
        return jsonify(error="only succeeded top-ups can be voided"), 400

    if (t.method or "").lower() != "cash":
        # Your UI is cash-only; keep the guard here for legacy records
        return jsonify(error="unsupported method for void: cash only"), 400

    # 24h reversal window
    created = getattr(t, "created_at", None)
    if created:
        # normalize to aware datetime in Manila tz
        created_mnl = (created.astimezone(MNL_TZ) if created.tzinfo
                       else created.replace(tzinfo=timezone.utc).astimezone(MNL_TZ))
        age_seconds = (_now_mnl() - created_mnl).total_seconds()
        if age_seconds > 5 * 60:
            return jsonify(error="void window elapsed (over 5 minutes)"), 400

    # Ensure wallet exists & check balance
    acct = WalletAccount.query.get(t.account_id)
    if not acct:
        # should not happen if create_topup ensured the row, but be safe
        acct = WalletAccount(user_id=t.account_id, balance_pesos=0)
        db.session.add(acct)
        db.session.flush()

    amt = _as_php(getattr(t, "amount_pesos", 0))
    cur = _as_php(getattr(acct, "balance_pesos", 0))
    if cur < amt:
        return jsonify(error="insufficient wallet balance to reverse (funds already spent)"), 400

    new_bal = cur - amt

    try:
        # 1) Update wallet balance
        acct.balance_pesos = int(new_bal)

        # 2) Ledger entry (debit)
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

        # 3) Mark original top-up as cancelled
        t.status = "cancelled"

        # 4) Optional: save reason text alongside other reasons
        if reason:
            _save_reject_reason(t.id, reason)

        db.session.commit()

        # Realtime publish (best-effort)
        _publish_user_wallet(
            t.account_id,
            new_balance_pesos=int(new_bal),
            event="wallet_topup_void",
            topup_id=int(t.id),
            method="cash",
            amount_php=int(amt),
        )

        # Optional push (best-effort)
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

    except SQLAlchemyError as e:
        current_app.logger.exception("[teller] void_topup DB error")
        db.session.rollback()
        return jsonify(error="database error"), 500
    except Exception as e:
        current_app.logger.exception("[teller] void_topup failed")
        db.session.rollback()
        return jsonify(error=str(e)), 400

def _unsign_wallet_qr(token: str, *, leeway_buckets: int = 2) -> Optional[int]:
    """
    Accepts rotating wallet QR tokens from /commuter/wallet/qrcode.
    Valid only if |now_bucket - mb| <= leeway_buckets.
    """
    try:
        s = URLSafeSerializer(current_app.config["SECRET_KEY"], salt=SALT_WALLET_QR)
        data = s.loads(token)
        uid = int(data.get("uid", 0))
        mb = int(data.get("mb", -1))
        now_bucket = int(_time.time() // 60)
        if uid > 0 and mb >= 0 and abs(now_bucket - mb) <= max(0, int(leeway_buckets)):
            return uid
    except Exception:
        pass
    return None

def _reject_reason_path(tid: int) -> str:
    """Local file path for a saved reject reason (no DB migration needed)."""
    return os.path.join(current_app.root_path, "static", RECEIPTS_DIR, f"{tid}.reject.txt")

def _reject_reason_if_exists(tid: int) -> Optional[str]:
    """Return the saved reject reason text, if present."""
    try:
        p = _reject_reason_path(tid)
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                return (f.read() or "").strip() or None
    except Exception:
        current_app.logger.exception("[teller] read reject reason failed tid=%s", tid)
    return None

def _receipt_url_if_exists(tid: int) -> Optional[str]:
    """
    If a commuter uploaded a receipt, return its public static URL.
    Tries jpg/png/jpeg/webp under /static/topup_receipts/{tid}.<ext>.
    """
    static_root = os.path.join(current_app.root_path, "static", RECEIPTS_DIR)
    for ext in ("jpg", "png", "jpeg", "webp"):
        candidate = os.path.join(static_root, f"{tid}.{ext}")
        if os.path.exists(candidate):
            return url_for("static", filename=f"{RECEIPTS_DIR}/{tid}.{ext}", _external=False)
    return None

# ──────────────────────────────────────────────────────────────────────────────
# PUBLISH HELPERS

def _publish_user_wallet(uid: int, *, new_balance_pesos: int, event: str, **extra) -> bool:
    """
    Best-effort realtime wallet update for the commuter device(s).
    Publishes to BOTH topic roots to cover app variants:
      user/{uid}/wallet   and   users/{uid}/wallet
    """
    if not mqtt_publish:
        current_app.logger.warning("[mqtt] disabled: mqtt_ingest.publish not available (wallet)")
        return False
    payload = {
        "type": "wallet_update",
        "event": event,  # "wallet_topup" | "wallet_debit" | …
        "new_balance_php": int(new_balance_pesos),
        "sentAt": int(_time.time() * 1000),
        **extra,
    }
    ok = True
    for root in ("user", "users"):
        topic = f"{root}/{int(uid)}/wallet"
        ok = mqtt_publish(topic, payload) and ok
        current_app.logger.info("[mqtt] wallet → %s ok=%s", topic, ok)
    return ok

def _debug_log_push(uid: int, payload: dict):
    try:
        toks = (
            db.session.query(DeviceToken.platform, DeviceToken.token)
            .filter(DeviceToken.user_id == uid)
            .all()
        )
        sample = [(p or "?", (t or "")[:16] + "…") for (p, t) in toks]
        current_app.logger.info(
            "[push][debug] target uid=%s tokens=%s sample=%s payload_keys=%s",
            uid, len(toks), sample, sorted(list(payload.keys())),
        )
    except Exception:
        current_app.logger.exception("[push][debug] failed to list tokens")

# ──────────────────────────────────────────────────────────────────────────────
# TOKEN REGISTRATION

@teller_bp.route("/device-token", methods=["POST"])
@require_role("teller", "pao")
def register_teller_device_token():
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    platform = (data.get("platform") or "unknown").strip()

    current_app.logger.info(
        "[device-token] HIT uid=%s role=%s platform=%s tok=%s",
        getattr(g, "user", None) and g.user.id,
        getattr(g, "user", None) and g.user.role,
        platform,
        (token[:16] + "…") if token else "(none)",
    )
    if not token:
        return jsonify(error="token required"), 400

    try:
        row = DeviceToken.query.filter_by(user_id=g.user.id, token=token).first()
        if not row:
            row = DeviceToken(user_id=g.user.id, token=token, platform=platform)
            db.session.add(row)
        else:
            row.platform = platform
        db.session.commit()
        current_app.logger.info("[device-token] SAVED uid=%s platform=%s", g.user.id, platform)
        return jsonify(ok=True), 200
    except Exception as e:
        current_app.logger.exception("[teller] device-token upsert failed")
        return jsonify(error=str(e)), 400

# ──────────────────────────────────────────────────────────────────────────────
# WALLET HELPERS

def _ensure_wallet_row(user_id: int) -> int:
    """Ensure wallet_accounts row exists; return current balance (pesos)."""
    acct = WalletAccount.query.get(user_id)
    if not acct:
        acct = WalletAccount(user_id=user_id, balance_pesos=0)
        db.session.add(acct)
        db.session.commit()
        return 0
    return _as_php(getattr(acct, "balance_pesos", 0))

# ──────────────────────────────────────────────────────────────────────────────
# SCANNING / LOOKUPS

@teller_bp.route("/users/scan", methods=["GET"])
@require_role("teller")
def user_qr_scan():
    tok = (request.args.get("token") or "").strip()
    if not tok:
        return jsonify(error="token required"), 400

    uid = None
    try:
        uid = _unsign_user_qr(tok)  # user QR (from /commuter/users/me/qr.png)
    except (BadSignature, SignatureExpired, ValueError):
        uid = _unsign_wallet_qr(tok, leeway_buckets=2)  # graceful fallback

    if not uid:
        return jsonify(error="invalid token"), 400

    user = User.query.get(uid)
    if not user:
        return jsonify(error="not found"), 404

    bal = _ensure_wallet_row(uid)
    return jsonify(
        id=user.id,
        name=_user_name(user),
        balance_pesos=int(bal),
        balance_php=int(bal),
    ), 200

# ──────────────────────────────────────────────────────────────────────────────
# TOP-UP REQUESTS (COMMUTER-SUBMITTED)

@teller_bp.route("/topup-requests", methods=["GET"])
@require_role("teller")
def list_topup_requests():
    status = (request.args.get("status") or "pending").strip().lower()
    limit = max(1, min(200, request.args.get("limit", type=int, default=50)))

    # NEW: parse optional date range
    def _parse_date(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        s = s.strip()
        try:
            if len(s) == 10:  # YYYY-MM-DD
                dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=MNL_TZ)
            else:
                dt = datetime.fromisoformat(s)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=MNL_TZ)
                else:
                    dt = dt.astimezone(MNL_TZ)
            return dt
        except Exception:
            return None

    from_q = request.args.get("from") or request.args.get("start") or request.args.get("start_date")
    to_q = request.args.get("to") or request.args.get("end") or request.args.get("end_date")

    dt_from = _parse_date(from_q)
    dt_to = _parse_date(to_q)

    if dt_to and len((to_q or "").strip()) == 10:
        dt_to = dt_to + timedelta(days=1)

    U = aliased(User)
    q = (
        db.session.query(TopUp, U)
        .outerjoin(U, U.id == TopUp.account_id)  # ← don’t drop rows if user is absent
    )
    # NEW: status=any → no filter; else filter equality
    if status != "any":
        q = q.filter(TopUp.status == status)

    if dt_from:
        q = q.filter(TopUp.created_at >= dt_from)
    if dt_to:
        q = q.filter(TopUp.created_at < dt_to)

    rows = q.order_by(TopUp.created_at.desc(), TopUp.id.desc()).limit(limit).all()

    items = []
    for t, u in rows:
        items.append({
            "id": t.id,
            "account_id": t.account_id,
            "amount_pesos": int(getattr(t, "amount_pesos", 0) or 0),
            "method": getattr(t, "method", "cash"),  # may contain legacy 'gcash'/'maya' from old data
            "status": getattr(t, "status", "pending"),
            "created_at": (t.created_at.isoformat() if getattr(t, "created_at", None) else None),
            "note": None,
            "receipt_url": _receipt_url_if_exists(t.id),
            "receipt_thumb_url": _receipt_url_if_exists(t.id),
            "reject_reason": _reject_reason_if_exists(t.id),
            "commuter": {
                "id": u.id,
                "first_name": u.first_name,
                "last_name": u.last_name,
                "username": u.username,
                "phone_number": getattr(u, "phone_number", None),
            } if u else None,
        })

    return jsonify({"items": items}), 200

@teller_bp.route("/topup-requests/<int:tid>", methods=["GET"])
@require_role("teller")
def get_topup_request(tid: int):
    t = TopUp.query.get_or_404(tid)
    u = User.query.get(t.account_id)
    return jsonify({
        "id": t.id,
        "account_id": t.account_id,
        "commuter_name": _user_name(u),
        "amount_pesos": int(t.amount_pesos or 0),
        "method": t.method,  # may be legacy value in DB
        "note": None,
        "status": t.status,
        "receipt_url": _receipt_url_if_exists(t.id),
        "created_at": t.created_at.isoformat() if getattr(t, "created_at", None) else None,
        "reject_reason": _reject_reason_if_exists(t.id),
    }), 200

# ──────────────────────────────────────────────────────────────────────────────
# WALLET TOKEN RESOLVE / OVERVIEW

@teller_bp.route("/wallet/resolve", methods=["POST"])
@require_role("teller")
def resolve_wallet_token():
    data = request.get_json(silent=True) or {}

    wallet_token = (data.get("wallet_token") or data.get("token") or "").strip()
    wallet_user_id = data.get("user_id") or data.get("wallet_user_id")

    # NEW: NFC UID support
    nfc_uid = (data.get("uid") or data.get("nfc_uid") or "").strip()

    account_user_id: Optional[int] = None
    token_type: Optional[str] = None

    # 0) NFC UID (highest priority if provided)
    if nfc_uid:
        uid = _lookup_user_by_nfc(nfc_uid)
        if not uid:
            return jsonify(error="unknown_or_blocked_card"), 404
        account_user_id = int(uid)
        token_type = "nfc_uid"

    # 1) Existing wallet token flows
    if not account_user_id and wallet_token:
        acct = WalletAccount.query.filter_by(qr_token=wallet_token).first()
        if not acct:
            acct = WalletAccount.query.filter_by(nfc_uid=wallet_token).first()

        if acct:
            account_user_id = int(acct.user_id)
            token_type = "wallet_token"
        else:
            uid = _unsign_wallet_qr(wallet_token, leeway_buckets=2)
            if uid:
                account_user_id = int(uid)
                token_type = "wallet_qr"
            else:
                return jsonify(error="invalid wallet token"), 400

    # 2) user_id direct
    if not account_user_id and wallet_user_id not in (None, "", 0, "0"):
        try:
            account_user_id = int(wallet_user_id)
        except Exception:
            return jsonify(error="invalid user_id"), 400
        token_type = "user_id"

    if not account_user_id:
        return jsonify(error="missing wallet_token or user_id or uid"), 400

    user = User.query.get(account_user_id)
    if not user:
        return jsonify(error="user not found"), 404

    acct = WalletAccount.query.get(account_user_id)
    balance_php = _as_php(getattr(acct, "balance_pesos", 0))

    return jsonify({
        "valid": True,
        "token_type": token_type or "wallet_qr",
        "autopay": False,
        "user": {"id": user.id, "name": _user_name(user)},
        "user_id": user.id,
        "balance_php": int(balance_php),
        "name": _user_name(user),
        "id": user.id,
    }), 200

@teller_bp.route("/wallet/nfc/link", methods=["POST"])
@require_role("teller")
def link_nfc_card():
    data = request.get_json(silent=True) or {}

    raw_uid = str(data.get("nfc_uid") or data.get("uid") or "").strip()
    if not raw_uid:
        return jsonify(error="missing nfc_uid"), 400

    try:
        user_id = int(data.get("user_id"))
    except Exception:
        return jsonify(error="invalid user_id"), 400

    uid = _norm_uid(raw_uid)
    if not uid:
        return jsonify(error="invalid uid format"), 400

    # ensure table exists
    _ensure_nfc_cards_table()

    # ensure user exists
    user = User.query.get(user_id)
    if not user:
        return jsonify(error="user not found"), 404

    # prevent one card being linked to another user
    existing = db.session.execute(
        text("SELECT user_id FROM nfc_cards WHERE uid=:uid LIMIT 1"),
        {"uid": uid},
    ).mappings().first()

    if existing and int(existing["user_id"]) != int(user_id):
        return jsonify(error="card already linked to another user"), 409

    try:
        # upsert (works on SQLite/Postgres)
        dialect = db.engine.dialect.name
        if dialect in ("sqlite", "postgresql"):
            db.session.execute(text("""
                INSERT INTO nfc_cards(uid, user_id, status)
                VALUES(:uid, :user_id, 'active')
                ON CONFLICT(uid) DO UPDATE SET
                  user_id=excluded.user_id,
                  status='active'
            """), {"uid": uid, "user_id": user_id})
        else:
            # fallback for other DBs
            db.session.execute(text("DELETE FROM nfc_cards WHERE uid=:uid"), {"uid": uid})
            db.session.execute(text("""
                INSERT INTO nfc_cards(uid, user_id, status)
                VALUES(:uid, :user_id, 'active')
            """), {"uid": uid, "user_id": user_id})

        # OPTIONAL (but good): mirror into wallet_accounts too (backwards compatible)
        acct = WalletAccount.query.get(user_id)
        if not acct:
            acct = WalletAccount(user_id=user_id, balance_pesos=0)
            db.session.add(acct)
        acct.nfc_uid = uid

        db.session.commit()
        return jsonify(ok=True, user_id=user_id, nfc_uid=uid), 200

    except Exception as e:
        current_app.logger.exception("[teller] link_nfc_card failed uid=%s user_id=%s", uid, user_id)
        db.session.rollback()
        return jsonify(error=str(e)), 400


@teller_bp.route("/wallet/<int:user_id>/bind-card", methods=["POST"])
@require_role("teller", "pao")
def bind_card(user_id: int):
    data = request.get_json(silent=True) or {}
    uid = (data.get("card_uid") or "").strip()
    if not uid:
        return jsonify(error="card_uid required"), 400

    acct = WalletAccount.query.get(user_id)
    if not acct:
        acct = WalletAccount(user_id=user_id, balance_pesos=0)
        db.session.add(acct)
        db.session.flush()

    acct.qr_token = uid
    db.session.commit()
    return jsonify(ok=True, user_id=user_id, card_uid=uid), 200

@teller_bp.route("/wallet/<int:user_id>/overview", methods=["GET"])
@require_role("teller")
def wallet_overview(user_id: int):
    """
    Overview for a wallet:
      - wallet balance (PHP)
      - recent top-ups (method + created_at)
      - recent ledger entries
    """
    user = User.query.get(user_id)
    if not user:
        return jsonify(error="user not found"), 404

    acct = WalletAccount.query.get(user_id)
    balance_php = _as_php(getattr(acct, "balance_pesos", 0))

    # Recent top-ups
    recent_topups_q = (
        TopUp.query
        .filter(TopUp.account_id == user_id)
        .order_by(TopUp.created_at.desc(), TopUp.id.desc())
        .limit(15)
    )
    recent_topups = []
    for tup in recent_topups_q.all():
        ts = None
        if tup.created_at:
            ts = (tup.created_at.astimezone(MNL_TZ).isoformat()
                  if tup.created_at.tzinfo
                  else tup.created_at.replace(tzinfo=timezone.utc).astimezone(MNL_TZ).isoformat())
        recent_topups.append({
            "id": tup.id,
            "amount_php": _as_php(getattr(tup, "amount_pesos", 0)),
            "created_at": ts,
            "method": getattr(tup, "method", None),  # legacy-safe
            "status": getattr(tup, "status", None),
        })

    # Recent ledger
    recent_ledger_q = (
        WalletLedger.query
        .filter(WalletLedger.account_id == user_id)
        .order_by(WalletLedger.created_at.desc(), WalletLedger.id.desc())
        .limit(20)
    )
    recent_ledger = []
    for row in recent_ledger_q.all():
        ts = None
        if row.created_at:
            ts = (row.created_at.astimezone(MNL_TZ).isoformat()
                  if row.created_at.tzinfo
                  else row.created_at.replace(tzinfo=timezone.utc).astimezone(MNL_TZ).isoformat())
        recent_ledger.append({
            "id": row.id,
            "direction": getattr(row, "direction", "credit"),
            "event": getattr(row, "event", ""),
            "amount_php": _as_php(getattr(row, "amount_pesos", 0)),
            "running_balance_php": _as_php(getattr(row, "running_balance_pesos", 0)),
            "created_at": ts,
            "ref": {
                "table": getattr(row, "ref_table", None),
                "id": getattr(row, "ref_id", None),
            },
        })

    return jsonify({
        "user_id": user_id,
        "balance_php": balance_php,
        "teller_today": {  # kept for UI compatibility; not tracked per-operator anymore
            "count": None,
            "sum_php": None,
            "cap_php": None,
        },
        "recent_topups": recent_topups,
        "recent_ledger": recent_ledger,
    }), 200

# ──────────────────────────────────────────────────────────────────────────────
# CREATE TOP-UP (CASH ONLY)
@teller_bp.route("/wallet/topups", methods=["POST"])
@require_role("teller")
def create_topup():
    data = request.get_json(silent=True) or {}

    # Identify target wallet (by user_id OR signed commuter QR token OR NFC UID)
    account_user_id: Optional[int] = None

    if data.get("user_id") is not None:
        try:
            account_user_id = int(data.get("user_id"))
        except Exception:
            return jsonify(error="invalid user_id"), 400

    elif (data.get("token") or "").strip():
        try:
            uid = _unsign_user_qr((data.get("token") or "").strip())
        except (BadSignature, SignatureExpired, ValueError):
            uid = None
        if not uid:
            return jsonify(error="invalid token"), 400
        account_user_id = int(uid)

    else:
        # NEW: NFC UID
        nfc_uid = (data.get("uid") or data.get("nfc_uid") or "").strip()
        if nfc_uid:
            uid = _lookup_user_by_nfc(nfc_uid)
            if not uid:
                return jsonify(error="unknown_or_blocked_card"), 404
            account_user_id = int(uid)
            token_type = "nfc_uid"

        else:
            return jsonify(error="user_id or token or uid is required"), 400

    # Cash-only
    method = str(data.get("method") or "cash").strip().lower()
    if method != "cash":
        return jsonify(error="unsupported method: cash only"), 400

    # Amount validation (whole pesos)
    try:
        amount_pesos = int(data.get("amount_pesos") or data.get("amount_php") or 0)
    except Exception:
        amount_pesos = 0

    if amount_pesos < MIN_TOPUP or amount_pesos > MAX_TOPUP:
        return jsonify(error=f"amount must be between {MIN_TOPUP} and {MAX_TOPUP}"), 400

    _ensure_wallet_row(account_user_id)

    try:
        topup_id, ledger_id, new_bal = topup_cash(
            account_id=account_user_id,
            amount_pesos=amount_pesos,
        )

        out = {
            "ok": True,
            "topup_id": int(topup_id),
            "ledger_id": int(ledger_id),
            "new_balance_php": int(round(float(new_bal))),
        }

        _publish_user_wallet(
            account_user_id,
            new_balance_pesos=int(round(float(new_bal))),
            event="wallet_topup",
            topup_id=int(topup_id),
            method="cash",
            amount_php=int(amount_pesos),
        )

        try:
            push_to_user(
                account_user_id,
                title="Wallet Top-up",
                body=f"₱{amount_pesos} added. New balance: ₱{int(round(float(new_bal)))}",
                data={"type": "wallet_topup", "topup_id": int(topup_id)},
            )
        except Exception:
            current_app.logger.info("[push] skipped or failed (non-fatal) uid=%s", account_user_id)

        return jsonify(out), 200

    except Exception as e:
        current_app.logger.exception("[teller] create_topup failed")
        return jsonify(error=str(e)), 400
