# backend/routes/teller.py
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
from utils.push import push_to_user
from sqlalchemy import func
try:
    from routes.auth import require_role
except Exception:
    from auth import require_role

# Wallet services (operator-less)
from services.wallet import topup_cash, topup_gcash, approve_topup_existing

# Optional realtime publish (best-effort / no-op if module missing)
try:
    from mqtt_ingest import publish as mqtt_publish  # def publish(topic: str, payload: dict) -> None
except Exception:
    mqtt_publish = None

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


def _receipt_url_if_exists(tid: int) -> Optional[str]:
    """Return absolute URL to the stored receipt image for this top-up id, if any."""
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        p = os.path.join(current_app.root_path, "static", RECEIPTS_DIR, f"{tid}{ext}")
        if os.path.exists(p):
            return url_for("static", filename=f"{RECEIPTS_DIR}/{tid}{ext}", _external=True)
    return None


def _publish_user_wallet(uid: int, *, new_balance_pesos: int, event: str, **extra) -> bool:
    """
    Best-effort realtime wallet update for the commuter's device(s).
    Mirrors the PAO/commuter payload shape so clients can reuse handlers.
    """
    if not mqtt_publish:
        return False
    try:
        payload = {
            "type": "wallet_update",
            "event": event,  # "wallet_topup" | "wallet_debit" | ...
            "new_balance_php": int(new_balance_pesos),
            "sentAt": int(_time.time() * 1000),
            **extra,
        }
        mqtt_publish(f"user/{uid}/wallet", payload)
        return True
    except Exception:
        current_app.logger.exception("[mqtt] teller wallet publish failed")
        return False


def _unsign_user_qr(token: str, *, max_age: Optional[int] = None) -> int:
    """
    Returns the user id encoded in commuter QR token from /commuter/users/me/qr.png.
    """
    s = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)
    data = s.loads(token, max_age=max_age) if max_age else s.loads(token)
    uid = int(data.get("uid"))
    if uid <= 0:
        raise ValueError("bad uid")
    return uid

@teller_bp.route("/device-token", methods=["POST"])
@require_role("teller","pao")  # ← allow both
def register_teller_device_token():
    """Save/refresh this teller's device push token."""
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    platform = (data.get("platform") or "unknown").strip()
    if not token:
        return jsonify(error="token required"), 400

    try:
        # upsert by (user_id, token)
        row = DeviceToken.query.filter_by(user_id=g.user.id, token=token).first()
        if not row:
            row = DeviceToken(user_id=g.user.id, token=token, platform=platform)
            db.session.add(row)
        else:
            row.platform = platform
        db.session.commit()
        return jsonify(ok=True), 200
    except Exception as e:
        current_app.logger.exception("[teller] device-token upsert failed")
        return jsonify(error=str(e)), 400

def _ensure_wallet_row(user_id: int) -> int:
    """
    Ensure wallet_accounts row exists; return current balance (pesos).
    """
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
@require_role("teller")  # adjust if your PAO role is used to scan
def user_qr_scan():
    """
    GET /teller/users/scan?token=...
    Verifies commuter QR token and returns minimal identity for display before charging.
    """
    tok = (request.args.get("token") or "").strip()
    if not tok:
        return jsonify(error="token required"), 400
    try:
        uid = _unsign_user_qr(tok)  # token minted by commuter qr endpoint
    except (BadSignature, SignatureExpired, ValueError):
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
# Requests list/detail + approve/reject

@teller_bp.route("/topup-requests", methods=["GET"])
@require_role("teller")
def list_topup_requests():
    """
    GET /teller/topup-requests?status=pending&limit=50
    Returns: { items: [...] } matching the shape used by the Teller lists.
    """
    status = (request.args.get("status") or "pending").strip().lower()
    limit = max(1, min(200, request.args.get("limit", type=int, default=50)))

    U = aliased(User)
    rows = (
        db.session.query(TopUp, U)
        .join(U, U.id == TopUp.account_id)
        .filter(TopUp.status == status)
        .order_by(TopUp.created_at.desc(), TopUp.id.desc())
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
            # 'note' column doesn't exist; commuter's free-form note is not stored in DB
            "note": None,
            "receipt_url": _receipt_url_if_exists(t.id),
            "receipt_thumb_url": _receipt_url_if_exists(t.id),
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
    }), 200


@teller_bp.route("/topup-requests/<int:tid>/approve", methods=["POST"])
@require_role("teller")
def approve_topup(tid: int):
    """
    Approve a commuter-submitted top-up request:
      - credits the wallet,
      - writes a ledger row (ref_table='wallet_topups', ref_id=tid),
      - marks topup.status='succeeded',
      - sends a push notification to the commuter,
      - best-effort realtime publish for instant UI.

    Returns 200 with { ok, topup_id, ledger_id, new_balance_php, status }.
    """
    t = TopUp.query.get_or_404(tid)
    if t.status not in {"pending", "approved"}:
        return jsonify(error=f"invalid state {t.status}"), 400

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

        # 🔔 Notify commuter immediately (push + realtime publish; best-effort)
        try:
            sent_at = int(_time.time() * 1000)

            _debug_log_push(int(t.account_id), {
                "type": "wallet_topup",
                "method": t.method,
                "amount_php": int(t.amount_pesos or 0),
                "topup_id": int(t.id),
                "ledger_id": int(ledger_id),
                "new_balance_php": int(new_balance),
                "deeplink": "/(tabs)/commuter/wallet",
            })
            # push (consumed by appEvents 'push' listener for instantaneous UI)
            push_to_user(
                db, DeviceToken, int(t.account_id),
                "🪙 Wallet top-up approved",
                f"₱{int(t.amount_pesos or 0):,} via {str(t.method or 'cash').title()}. "
                f"New balance: ₱{int(new_balance):,}.",
                {
                    "type": "wallet_topup",
                    "method": t.method,
                    "amount_php": int(t.amount_pesos or 0),
                    "topup_id": int(t.id),
                    "ledger_id": int(ledger_id),
                    "new_balance_php": int(new_balance),
                    "deeplink": "/(tabs)/commuter/wallet",
                    "sentAt": sent_at,
                    # Let client show an optimistic row in "Recent Activity"
                    "ledger_hint": {
                        "direction": "credit",
                        "event": f"topup:{t.method or 'cash'}",
                        "amount_php": int(t.amount_pesos or 0),
                        "running_balance_php": int(new_balance),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "ref": {"table": "wallet_topups", "id": int(t.id)},
                    },
                    # Optionally include a receipt preview URL if you store it:
                    # "receipt_url": _receipt_url_if_exists(t.id),
                },
                channelId="payments",
                priority="high",
                ttl=600,
            )

            # MQTT realtime (for any connected clients/widgets)
            _publish_user_wallet(
                int(t.account_id),
                new_balance_pesos=int(new_balance),
                event="wallet_topup",
                amount_php=int(t.amount_pesos or 0),
                topup_id=int(t.id),
                ledger_id=int(ledger_id),
            )
        except Exception:
            current_app.logger.exception("[push] approve_topup push/mqtt failed")

        return jsonify(payload), 200

    except Exception as e:
        current_app.logger.exception("[teller] approve_topup failed")
        return jsonify(error=str(e)), 400


@teller_bp.route("/topup-requests/<int:tid>/reject", methods=["POST"])
@require_role("teller")
def reject_topup(tid: int):
    """
    Mark a commuter-submitted request as rejected (no wallet change).
    """
    t = TopUp.query.get_or_404(tid)
    if t.status not in {"pending", "approved"}:
        return jsonify(error=f"invalid state {t.status}"), 400
    db.session.execute(
        db.text("UPDATE wallet_topups SET status='rejected' WHERE id=:id"),
        {"id": tid},
    )
    db.session.commit()
    return jsonify({"ok": True, "status": "rejected", "topup_id": tid}), 200


# ──────────────────────────────────────────────────────────────────────────────
# Wallet utilities for Teller screens

@teller_bp.route("/wallet/resolve", methods=["POST"])
@require_role("teller")
def resolve_wallet_token():
    """
    Resolve a commuter wallet by either:
      - wallet_token (matches WalletAccount.qr_token), or
      - user_id (direct).
    Response aligns with the app's ResolveResp shape.
    """
    data = request.get_json(silent=True) or {}
    wallet_token = (data.get("wallet_token") or data.get("token") or "").strip()
    wallet_user_id = data.get("user_id") or data.get("wallet_user_id")

    account_user_id: Optional[int] = None
    token_type: Optional[str] = None

    if wallet_token:
        acct = WalletAccount.query.filter_by(qr_token=wallet_token).first()
        if not acct:
            return jsonify(error="invalid wallet token"), 400
        account_user_id = int(acct.user_id)
        token_type = "wallet_token"

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
        "token_type": token_type or "wallet_token",
        "autopay": False,  # reserved; set true if you implement autopay semantics
        "user": {"id": user.id, "name": _user_name(user)},
        "user_id": user.id,
        "balance_php": balance_php,
        "name": _user_name(user),  # for clients that read top-level
        "id": user.id,             # compatibility
    }), 200


@teller_bp.route("/wallet/<int:user_id>/overview", methods=["GET"])
@require_role("teller")
def wallet_overview(user_id: int):
    """
    Overview for a wallet:
      - wallet balance (PHP)
      - recent top-ups (method + created_at)
      - recent ledger entries
    (Per-teller daily caps are omitted since the schema no longer tracks an operator.)
    """
    user = User.query.get(user_id)
    if not user:
        return jsonify(error="user not found"), 404

    acct = WalletAccount.query.get(user_id)
    balance_php = _as_php(getattr(acct, "balance_pesos", 0))

    # Recent top-ups for this wallet
    recent_topups_q = (
        TopUp.query
        .filter(TopUp.account_id == user_id)
        .order_by(TopUp.created_at.desc(), TopUp.id.desc())
        .limit(15)
    )
    recent_topups = []
    for tup in recent_topups_q.all():
        # normalize timestamp to Manila in output
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

    # Recent ledger for this wallet
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
    Create an immediate wallet top-up for a commuter (bypasses 'pending').

    Body (JSON):
      - user_id (int) OR token (signed commuter QR token from /commuter/users/me/qr.png)
      - method: 'cash' | 'gcash'
      - amount_pesos (int)  OR amount_php (alias)
      - external_ref (optional, for gcash receipts / reference nos.)

    Returns 201 with { ok, topup_id, ledger_id, new_balance_php }.
    Also sends a push notification to the commuter on success and a realtime publish.
    """
    data = request.get_json(silent=True) or {}

    # Resolve target wallet from user_id or signed QR token
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

    # Validate method + amount
    method = str(data.get("method") or "cash").strip().lower()
    try:
        amount_pesos = int(data.get("amount_pesos") or data.get("amount_php") or 0)
    except Exception:
        amount_pesos = 0

    if amount_pesos < MIN_TOPUP or amount_pesos > MAX_TOPUP:
        return jsonify(error=f"amount must be between {MIN_TOPUP} and {MAX_TOPUP}"), 400

    # Ensure wallet row exists (creates if missing)
    _ensure_wallet_row(account_user_id)

    # Execute the credit
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

        payload = {
            "ok": True,
            "topup_id": int(topup_id),
            "ledger_id": int(ledger_id),
            "new_balance_php": int(round(float(new_bal))),
        }

        # 🔔 Push + realtime publish (best-effort; don't fail the request if this errors)
        try:
            sent_at = int(_time.time() * 1000)

            _debug_log_push(account_user_id, {
                "type": "wallet_topup",
                "method": method,
                "amount_php": int(amount_pesos),
                "topup_id": int(topup_id),
                "ledger_id": int(ledger_id),
                "new_balance_php": int(round(float(new_bal))),
                "deeplink": "/(tabs)/commuter/wallet",
            })
            push_to_user(
                db, DeviceToken, account_user_id,
                "🪙 Wallet top-up received",
                f"₱{amount_pesos:,} via {'GCash' if method == 'gcash' else 'Cash'}. "
                f"New balance: ₱{int(round(float(new_bal))):,}.",
                {
                    "type": "wallet_topup",
                    "method": method,
                    "amount_php": int(amount_pesos),
                    "topup_id": int(topup_id),
                    "ledger_id": int(ledger_id),
                    "new_balance_php": int(round(float(new_bal))),
                    "deeplink": "/(tabs)/commuter/wallet",
                    "sentAt": sent_at,
                    # Optimistic "Recent Activity" hint for the client:
                    "ledger_hint": {
                        "direction": "credit",
                        "event": f"topup:{method}",
                        "amount_php": int(amount_pesos),
                        "running_balance_php": int(round(float(new_bal))),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "ref": {"table": "wallet_topups", "id": int(topup_id)},
                    },
                },
                channelId="payments",
                priority="high",
                ttl=600,
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
            current_app.logger.exception("[push] wallet_topup push/mqtt failed")

        return jsonify(payload), 201

    except Exception as e:
        current_app.logger.exception("[teller] create_topup failed")
        return jsonify(error=str(e)), 400
