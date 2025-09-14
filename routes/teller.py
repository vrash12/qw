# backend/routes/teller.py
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from flask import Blueprint, request, jsonify, g, current_app, url_for
from sqlalchemy import func
from sqlalchemy.orm import joinedload, aliased
from models.device_token import DeviceToken
from utils.push import push_to_user
from db import db
from models.user import User
from models.wallet import WalletAccount, WalletLedger, TopUp

# Auth decorator (support both import paths)
try:
    from routes.auth import require_role
except Exception:
    from auth import require_role

# Wallet services (operator-less)
from services.wallet import topup_cash, topup_gcash, approve_topup_existing

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
    limit  = max(1, min(200, request.args.get("limit", type=int, default=50)))

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
      - sends a push notification to the commuter.

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

        # 🔔 Notify commuter immediately (best-effort)
        try:
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
                    "new_balance_php": int(new_balance),
                    "deeplink": "/commuter/wallet",
                    # Optionally include a receipt preview URL if you store it:
                    # "receipt_url": _receipt_url_if_exists(t.id),
                },
                channelId="payments",
                priority="high",
                ttl=600,
            )
        except Exception:
            current_app.logger.exception("[push] approve_topup push failed")

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
      - user_id (int)
      - method: 'cash' | 'gcash'
      - amount_pesos (int)  OR amount_php (alias)
      - external_ref (optional, for gcash receipts / reference nos.)

    Returns 201 with { ok, topup_id, ledger_id, new_balance_php }.
    Also sends a push notification to the commuter on success.
    """
    data = request.get_json(silent=True) or {}

    # Validate target wallet
    try:
        account_user_id = int(data.get("user_id"))
    except Exception:
        return jsonify(error="invalid user_id"), 400

    # Validate method + amount
    method = str(data.get("method") or "cash").strip().lower()
    try:
        amount_pesos = int(data.get("amount_pesos") or data.get("amount_php") or 0)
    except Exception:
        amount_pesos = 0

    if amount_pesos < MIN_TOPUP or amount_pesos > MAX_TOPUP:
        return jsonify(error=f"amount must be between {MIN_TOPUP} and {MAX_TOPUP}"), 400

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

        # 🔔 Fire a push to the commuter (best-effort; don't fail the request if this errors)
        try:
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
                    "new_balance_php": int(round(float(new_bal))),
                    "deeplink": "/commuter/wallet",
                },
                channelId="payments",
                priority="high",
                ttl=600,
            )
        except Exception:
            current_app.logger.exception("[push] wallet_topup push failed")

        return jsonify(payload), 201

    except Exception as e:
        current_app.logger.exception("[teller] create_topup failed")
        return jsonify(error=str(e)), 400
