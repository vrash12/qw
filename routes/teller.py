from __future__ import annotations

import os
import time as _time
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from flask import Blueprint, request, jsonify, current_app, url_for, g
from sqlalchemy.orm import aliased

from db import db
from models.user import User
from models.wallet import WalletAccount, WalletLedger, TopUp
from models.device_token import DeviceToken
from utils.push import push_to_user  # (kept if you still use FCM push elsewhere)
from sqlalchemy import func

try:
    from routes.auth import require_role
except Exception:
    from auth import require_role

# Wallet services (operator-less)
from services.wallet import topup_cash, topup_gcash, approve_topup_existing

# Optional realtime publish (best-effort / no-op if module missing)
try:
    # def publish(topic: str, payload: dict) -> bool
    from mqtt_ingest import publish as mqtt_publish
except Exception:
    mqtt_publish = None  # type: ignore[assignment]

# For verifying commuter QR tokens (must match routes/commuter.py)
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

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

# Must match SALT_USER_QR in routes/commuter.py
SALT_USER_QR = "user-qr-v1"

# ──────────────────────────────────────────────────────────────────────────────
# Small helpers

def _now_mnl() -> datetime:
    return datetime.now(MNL_TZ)

def _today_bounds_mnl() -> Tuple[datetime, datetime]:
    now = _now_mnl()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end
# add import at top with the others
from itsdangerous import URLSafeSerializer

# keep existing SALT_USER_QR
SALT_USER_QR = "user-qr-v1"
# 👇 add this to match routes/commuter.py
SALT_WALLET_QR = "wallet-qr-rot-v1"

def _unsign_wallet_qr(token: str, *, leeway_buckets: int = 2) -> Optional[int]:
    """
    Accepts rotating wallet QR tokens from /commuter/wallet/qrcode.
    Valid only if |now_bucket - mb| <= leeway_buckets.
    """
    try:
        s = URLSafeSerializer(current_app.config["SECRET_KEY"], salt=SALT_WALLET_QR)
        data = s.loads(token)
        uid = int(data.get("uid", 0))
        mb  = int(data.get("mb", -1))
        now_bucket = int(_time.time() // 60)
        if uid > 0 and mb >= 0 and abs(now_bucket - mb) <= max(0, int(leeway_buckets)):
            return uid
    except Exception:
        pass
    return None

def _user_name(u: Optional[User]) -> str:
    if not u:
        return ""
    fn = (u.first_name or "").strip()
    ln = (u.last_name or "").strip()
    name = (fn + " " + ln).strip()
    return name or (u.username or f"User #{u.id}")

def _as_php(x: Optional[int]) -> int:
    """All wallet domain amounts are whole pesos already; coerce to int."""
    try:
        return int(x or 0)
    except Exception:
        return 0

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

# … imports unchanged …
try:
    from mqtt_ingest import publish as mqtt_publish  # best-effort; returns bool
except Exception:
    mqtt_publish = None

# ──────────────────────────────────────────────────────────────────────────────
# PUBLISH HELPERS (UPDATE THESE)

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

# routes/teller.py — REPLACE _publish_user_event with this version
def _publish_user_event(uid: int, payload: dict) -> bool:
    """
    Publish a generic event to the commuter stream.
    Publishes to BOTH roots and BOTH channels:
      user/{uid}/events, users/{uid}/events,
      user/{uid}/notify, users/{uid}/notify
    """
    if not mqtt_publish:
        current_app.logger.warning("[mqtt] disabled: mqtt_ingest.publish not available (events)")
        return False
    try:
        payload.setdefault("sentAt", int(_time.time() * 1000))
        ok = True
        for root in ("user", "users"):
            ok = mqtt_publish(f"{root}/{int(uid)}/events", payload) and ok
            ok = mqtt_publish(f"{root}/{int(uid)}/notify", payload) and ok
        return ok
    except Exception:
        current_app.logger.exception("[mqtt] user event publish failed uid=%s", uid)
        return False


def _publish_tellers(event: str, **data) -> bool:
    if not mqtt_publish:
        current_app.logger.warning("[mqtt] disabled: mqtt_ingest.publish not available (tellers)")
        return False
    try:
        mqtt_publish("tellers/topups", {"event": event, **data})
        return True
    except Exception:
        current_app.logger.exception("[mqtt] tellers publish failed")
        return False

def _receipt_url_if_exists(tid: int) -> Optional[str]:
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        p = os.path.join(current_app.root_path, "static", RECEIPTS_DIR, f"{tid}{ext}")
        if os.path.exists(p):
            return url_for("static", filename=f"{RECEIPTS_DIR}/{tid}{ext}", _external=True)
    return None

def _unsign_user_qr(token: str, *, max_age: Optional[int] = None) -> int:
    """Returns the user id encoded in commuter QR token from /commuter/users/me/qr.png."""
    s = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)
    data = s.loads(token, max_age=max_age) if max_age else s.loads(token)
    uid = int(data.get("uid"))
    if uid <= 0:
        raise ValueError("bad uid")
    return uid

@teller_bp.route("/device-token", methods=["POST"])
@require_role("teller","pao")
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

def _ensure_wallet_row(user_id: int) -> int:
    """Ensure wallet_accounts row exists; return current balance (pesos)."""
    acct = WalletAccount.query.get(user_id)
    if not acct:
        acct = WalletAccount(user_id=user_id, balance_pesos=0)
        db.session.add(acct)
        db.session.commit()
        return 0
    return _as_php(getattr(acct, "balance_pesos", 0))

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

@teller_bp.route("/topup-requests", methods=["GET"])
@require_role("teller")
def list_topup_requests():
    status = (request.args.get("status") or "pending").strip().lower()
    limit  = max(1, min(200, request.args.get("limit", type=int, default=50)))

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
    to_q   = request.args.get("to")   or request.args.get("end")   or request.args.get("end_date")

    dt_from = _parse_date(from_q)
    dt_to   = _parse_date(to_q)

    if dt_to and len((to_q or "").strip()) == 10:
        dt_to = dt_to + timedelta(days=1)

    U = aliased(User)
    q = (
        db.session.query(TopUp, U)
        .join(U, U.id == TopUp.account_id)
        .filter(TopUp.status == status)
    )

    if dt_from:
        q = q.filter(TopUp.created_at >= dt_from)
    if dt_to:
        q = q.filter(TopUp.created_at < dt_to)

    rows = (
        q.order_by(TopUp.created_at.desc(), TopUp.id.desc())
         .limit(limit)
         .all()
    )

    items = []
    for t, u in rows:
        items.append({
            "id": t.id,
            "account_id": t.account_id,
            "amount_pesos": int(getattr(t, "amount_pesos", 0) or 0),
            "method": getattr(t, "method", "gcash"),
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
        "method": t.method,
        "note": None,
        "status": t.status,
        "receipt_url": _receipt_url_if_exists(t.id),
        "created_at": t.created_at.isoformat() if getattr(t, "created_at", None) else None,
        "reject_reason": _reject_reason_if_exists(t.id),
    }), 200

@teller_bp.route("/topup-requests/<int:tid>/approve", methods=["POST"])
@require_role("teller")
def approve_topup(tid: int):
    """
    Approve a commuter-submitted top-up request (token-free notifications):
      - optional amount override (validated and persisted)
      - credits the wallet and writes ledger
      - marks topup.status='succeeded'
      - sends realtime MQTT events to the commuter
      - publishes 'wallet_update' for live UI
    """
    from utils.notify_user import notify_user  # token-free

    t = TopUp.query.get_or_404(tid)
    if t.status not in {"pending", "approved"}:
        return jsonify(error=f"invalid state {t.status}"), 400

    data = request.get_json(silent=True) or {}
    override_amount = data.get("amount_pesos") if data else None
    if override_amount is None:
        override_amount = data.get("amount_php") if data else None
    if override_amount not in (None, ""):
        try:
            amt = int(override_amount)
        except Exception:
            return jsonify(error="amount_pesos must be an integer"), 400
        if amt < MIN_TOPUP or amt > MAX_TOPUP:
            return jsonify(error=f"amount must be between {MIN_TOPUP} and {MAX_TOPUP}"), 400
        try:
            db.session.execute(
                db.text("UPDATE wallet_topups SET amount_pesos=:amt WHERE id=:id"),
                {"amt": int(amt), "id": tid},
            )
            db.session.commit()
            t = TopUp.query.get(tid)
        except Exception:
            current_app.logger.exception("[teller] failed to persist override amount tid=%s", tid)
            return jsonify(error="failed to save override amount"), 400

    try:
        ledger_id, new_balance = approve_topup_existing(
            account_id=t.account_id,
            topup_id=t.id,
            method=t.method,
            amount_pesos=int(t.amount_pesos or 0),
            rid=f"approve-{t.id}",
        )

        payload = {
            "ok": True,
            "topup_id": t.id,
            "ledger_id": int(ledger_id),
            "new_balance_php": int(new_balance),
            "status": "succeeded",
        }

        # ── realtime notify (stateless; no device tokens) ────────────────────
        try:
            sent_at = int(_time.time() * 1000)

            notify_user(
                int(t.account_id),
                {
                    "type": "wallet_topup",
                    "method": t.method,
                    "amount_php": int(t.amount_pesos or 0),
                    "topup_id": int(t.id),
                    "ledger_id": int(ledger_id),
                    "new_balance_php": int(new_balance),
                    "deeplink": "/(tabs)/commuter/wallet",
                    "sentAt": sent_at,
                    "ledger_hint": {
                        "direction": "credit",
                        "event": f"topup:{t.method or 'cash'}",
                        "amount_php": int(t.amount_pesos or 0),
                        "running_balance_php": int(new_balance),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "ref": {"table": "wallet_topups", "id": int(t.id)},
                    },
                },
            )

            _publish_user_event(
                int(t.account_id),
                {
                    "type": "wallet_topup",
                    "method": t.method,
                    "amount_php": int(t.amount_pesos or 0),
                    "topup_id": int(t.id),
                    "ledger_id": int(ledger_id),
                    "new_balance_php": int(new_balance),
                    "deeplink": "/(tabs)/commuter/wallet",
                },
            )

            _publish_user_wallet(
                int(t.account_id),
                new_balance_pesos=int(new_balance),
                event="wallet_topup",
                amount_php=int(t.amount_pesos or 0),
                topup_id=int(t.id),
                ledger_id=int(ledger_id),
            )
        except Exception:
            current_app.logger.exception("[mqtt] approve_topup realtime publish failed")

        return jsonify(payload), 200

    except Exception as e:
        current_app.logger.exception("[teller] approve_topup failed")
        return jsonify(error=str(e)), 400

@teller_bp.route("/topup-requests/<int:tid>/reject", methods=["POST"])
@require_role("teller")
def reject_topup(tid: int):
    """
    Mark a commuter-submitted request as rejected (no wallet change).
    Accepts JSON: { reason?: str }
      - Persists status='rejected'
      - Saves reason to static/topup_receipts/{tid}.reject.txt
      - Stateless realtime notify to commuter (no device tokens)
    """
    from utils.notify_user import notify_user  # token-free

    t = TopUp.query.get_or_404(tid)
    if t.status not in {"pending", "approved"}:
        return jsonify(error=f"invalid state {t.status}"), 400

    data = request.get_json(silent=True) or {}
    reason = (data.get("reason") or "").strip()

    try:
        db.session.execute(
            db.text("UPDATE wallet_topups SET status='rejected' WHERE id=:id"),
            {"id": tid},
        )
        db.session.commit()
    except Exception:
        current_app.logger.exception("[teller] reject update failed tid=%s", tid)
        return jsonify(error="failed to reject"), 400

    # Optional: persist reason to a sidecar file (keeps schema untouched)
    try:
        if reason:
            outdir = os.path.join(current_app.root_path, "static", RECEIPTS_DIR)
            os.makedirs(outdir, exist_ok=True)
            with open(_reject_reason_path(tid), "w", encoding="utf-8") as f:
                ts = datetime.now(timezone.utc).isoformat()
                actor = getattr(g, "user", None) and getattr(g.user, "id", None)
                f.write(f"{reason}\n— by {actor or 'teller'} @ {ts}\n")
    except Exception:
        current_app.logger.exception("[teller] failed to write reject reason tid=%s", tid)

    # ── realtime notify (stateless; no device tokens) ────────────────────────
    try:
        preview = reason if len(reason) <= 140 else (reason[:137] + "…")
        notify_user(
            int(t.account_id),
            {
                "type": "wallet_topup_rejected",
                "topup_id": int(tid),
                "amount_php": int(t.amount_pesos or 0),
                "method": t.method,
                "reason": preview,
                "deeplink": "/(tabs)/commuter/wallet",
                "sentAt": int(_time.time() * 1000),
            },
        )
    except Exception:
        current_app.logger.exception("[mqtt] reject_topup realtime publish failed tid=%s", tid)

    return jsonify({"ok": True, "status": "rejected", "topup_id": tid, "reason": reason or None}), 200
@teller_bp.route("/wallet/resolve", methods=["POST"])
@require_role("teller")
def resolve_wallet_token():
    data = request.get_json(silent=True) or {}
    wallet_token = (data.get("wallet_token") or data.get("token") or "").strip()
    wallet_user_id = data.get("user_id") or data.get("wallet_user_id")

    account_user_id: Optional[int] = None
    token_type: Optional[str] = None

    if wallet_token:
        # 1) static DB token (from /wallet/qrcode/rotate)
        acct = WalletAccount.query.filter_by(qr_token=wallet_token).first()
        if acct:
            account_user_id = int(acct.user_id)
            token_type = "wallet_token"
        else:
            # 2) rotating signed token (from /wallet/qrcode)
            uid = _unsign_wallet_qr(wallet_token, leeway_buckets=2)
            if uid:
                account_user_id = int(uid)
                token_type = "wallet_qr"
            else:
                return jsonify(error="invalid wallet token"), 400

    if not account_user_id and wallet_user_id not in (None, "", 0, "0"):
        try:
            account_user_id = int(wallet_user_id)
        except Exception:
            return jsonify(error="invalid user_id"), 400
        token_type = "user_id"

    if not account_user_id:
        return jsonify(error="missing wallet_token or user_id"), 400

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
            "method": getattr(tup, "method", None),
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

@teller_bp.route("/wallet/topups", methods=["POST"])
@require_role("teller")
def create_topup():
    """
    Create an immediate wallet top-up (token-free notifications).
    Body:
      - user_id (int) OR token (signed commuter QR)
      - method: 'cash' | 'gcash'
      - amount_pesos (int)  OR amount_php
      - external_ref (optional, for gcash)
    """
    from utils.notify_user import notify_user  # token-free

    data = request.get_json(silent=True) or {}

    account_user_id: Optional[int] = None
    if data.get("user_id") is not None:
        try:
            account_user_id = int(data.get("user_id"))
        except Exception:
            return jsonify(error="invalid user_id"), 400
    elif (data.get("token") or "").strip():
        try:
            account_user_id = _unsign_user_qr((data.get("token") or "").strip())
        except (BadSignature, SignatureExpired, ValueError):
            return jsonify(error="invalid token"), 400
    else:
        return jsonify(error="user_id or token is required"), 400

    method = str(data.get("method") or "cash").strip().lower()
    try:
        amount_pesos = int(data.get("amount_pesos") or data.get("amount_php") or 0)
    except Exception:
        amount_pesos = 0
    if amount_pesos < MIN_TOPUP or amount_pesos > MAX_TOPUP:
        return jsonify(error=f"amount must be between {MIN_TOPUP} and {MAX_TOPUP}"), 400

    _ensure_wallet_row(account_user_id)

    try:
        if method == "cash":
            topup_id, ledger_id, new_bal = topup_cash(
                account_id=account_user_id,
                amount_pesos=amount_pesos,
            )
        elif method == "gcash":
            ext = (data.get("external_ref") or "").strip() or None
            topup_id, ledger_id, new_bal = topup_gcash(
                account_id=account_user_id,
                amount_pesos=amount_pesos,
                external_ref=ext,
            )
        else:
            return jsonify(error="unsupported method"), 400

        out = {
            "ok": True,
            "topup_id": int(topup_id),
            "ledger_id": int(ledger_id),
            "new_balance_php": int(round(float(new_bal))),
        }

        # ── realtime notify (stateless; no device tokens) ────────────────────
        try:
            sent_at = int(_time.time() * 1000)

            notify_user(
                account_user_id,
                {
                    "type": "wallet_topup",
                    "method": method,
                    "amount_php": int(amount_pesos),
                    "topup_id": int(topup_id),
                    "ledger_id": int(ledger_id),
                    "new_balance_php": int(round(float(new_bal))),
                    "deeplink": "/(tabs)/commuter/wallet",
                    "sentAt": sent_at,
                    "ledger_hint": {
                        "direction": "credit",
                        "event": f"topup:{method}",
                        "amount_php": int(amount_pesos),
                        "running_balance_php": int(round(float(new_bal))),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "ref": {"table": "wallet_topups", "id": int(topup_id)},
                    },
                },
            )

            _publish_user_event(
                account_user_id,
                {
                    "type": "wallet_topup",
                    "method": method,
                    "amount_php": int(amount_pesos),
                    "topup_id": int(topup_id),
                    "ledger_id": int(ledger_id),
                    "new_balance_php": int(round(float(new_bal))),
                    "deeplink": "/(tabs)/commuter/wallet",
                },
            )

            _publish_user_wallet(
                account_user_id,
                new_balance_pesos=int(round(float(new_bal))),
                event="wallet_topup",
                amount_php=int(amount_pesos),
                topup_id=int(topup_id),
                ledger_id=int(ledger_id),
            )
        except Exception:
            current_app.logger.exception("[mqtt] create_topup realtime publish failed")

        return jsonify(out), 201

    except Exception as e:
        current_app.logger.exception("[teller] create_topup failed")
        return jsonify(error=str(e)), 400

@teller_bp.route("/notify-test", methods=["POST"])
@require_role("teller")
def teller_notify_test():
    from utils.notify_user import notify_tellers
    ok = notify_tellers({
        "type": "test",
        "message": "Hello tellers 👋",
        "sentAt": int(_time.time() * 1000),
    })
    return jsonify(ok=bool(ok)), 200
