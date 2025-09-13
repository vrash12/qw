# backend/routes/teller.py
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, Tuple

from flask import Blueprint, request, jsonify, g, current_app
from sqlalchemy import func, and_
from sqlalchemy.orm import joinedload

from db import db
from models.user import User
from models.wallet import WalletAccount, WalletLedger, TopUp

# Auth decorator (support both import paths)
try:
    from routes.auth import require_role
except Exception:
    from auth import require_role

# Wallet services (top-up + ledger-safe updates)
from services.wallet import topup_cash, topup_gcash


teller_bp = Blueprint("teller", __name__, url_prefix="/teller")

# ──────────────────────────────────────────────────────────────────────────────
# Config / constants

# Manila time (UTC+8, no DST)
MNL_TZ = timezone(timedelta(hours=8))

# Per-transaction bounds (whole pesos)
MIN_TOPUP = 100
MAX_TOPUP = 1000

# Optional per-operator daily cap (peso). Only informational in overview;
# enforce in create_topup() if your policy requires it.
TELLER_DAILY_CAP = 20000


# ──────────────────────────────────────────────────────────────────────────────
# Small helpers

def _now_mnl() -> datetime:
    return datetime.now(MNL_TZ)


def _today_bounds_mnl() -> Tuple[datetime, datetime]:
    now = _now_mnl()
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def _user_name(u: User) -> str:
    try:
        return f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or f"User #{u.id}"
    except Exception:
        return f"User #{getattr(u, 'id', 0) or 0}"


def _as_php(x: Optional[int]) -> int:
    """All wallet domain amounts are whole pesos already; coerce to int."""
    try:
        return int(x or 0)
    except Exception:
        return 0


def _operator_column():
    """
    Return the SQLAlchemy column in TopUp that points to the operator
    (teller in the new schema, PAO in legacy). Also return a label to
    use in payloads.
    """
    if hasattr(TopUp, "teller_id"):
        return TopUp.teller_id, "teller"
    # Legacy fallback
    return TopUp.pao_id, "teller"  # still expose as 'teller' in responses


# ──────────────────────────────────────────────────────────────────────────────
# Routes

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

    # 1) By wallet_token → lookup WalletAccount.qr_token
    if wallet_token:
        acct = WalletAccount.query.filter_by(qr_token=wallet_token).first()
        if not acct:
            return jsonify(error="invalid wallet token"), 400
        account_user_id = int(acct.user_id)
        token_type = "wallet_token"

    # 2) Or direct by user_id
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
    Overview used by the teller top-up screen:
      - wallet balance (PHP)
      - today's teller totals (count/sum/cap)
      - recent top-ups (with operator name + method)
      - recent ledger entries
    """
    user = User.query.get(user_id)
    if not user:
        return jsonify(error="user not found"), 404

    acct = WalletAccount.query.get(user_id)
    balance_php = _as_php(getattr(acct, "balance_pesos", 0))

    # Teller's activity today
    start, end = _today_bounds_mnl()
    op_col, _ = _operator_column()

    today_q = TopUp.query.filter(
        op_col == g.user.id,
        TopUp.status == "succeeded",
        TopUp.created_at >= start,
        TopUp.created_at < end,
    )

    today_count = today_q.count()
    today_sum = _as_php(today_q.with_entities(func.coalesce(func.sum(TopUp.amount_pesos), 0)).scalar())
    cap_php = int(TELLER_DAILY_CAP)

    # Recent top-ups for this wallet (join to operator for display name)
    op_col, _lbl = _operator_column()
    recent_topups_q = (
        db.session.query(TopUp, User)
        .outerjoin(User, op_col == User.id)
        .filter(TopUp.account_id == user_id)
        .order_by(TopUp.created_at.desc(), TopUp.id.desc())
        .limit(15)
    )
    recent_topups = []
    for tup, oper in recent_topups_q.all():
        recent_topups.append({
            "id": tup.id,
            "amount_php": _as_php(getattr(tup, "amount_pesos", 0)),
            "created_at": (tup.created_at.astimezone(MNL_TZ).isoformat() if tup.created_at.tzinfo else (tup.created_at.replace(tzinfo=timezone.utc).astimezone(MNL_TZ).isoformat() if tup.created_at else None)),
            "teller_name": _user_name(oper) if oper else None,
            "method": getattr(tup, "method", None),
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
        recent_ledger.append({
            "id": row.id,
            "direction": getattr(row, "direction", "credit"),
            "event": getattr(row, "event", ""),
            "amount_php": _as_php(getattr(row, "amount_pesos", 0)),
            "running_balance_php": _as_php(getattr(row, "running_balance_pesos", 0)),
            "created_at": (row.created_at.astimezone(MNL_TZ).isoformat() if row.created_at.tzinfo else (row.created_at.replace(tzinfo=timezone.utc).astimezone(MNL_TZ).isoformat() if row.created_at else None)),
            "ref": {
                "table": getattr(row, "ref_table", None),
                "id": getattr(row, "ref_id", None),
            },
        })

    return jsonify({
        "user_id": user_id,
        "balance_php": balance_php,
        "teller_today": {
            "count": int(today_count),
            "sum_php": int(today_sum),
            "cap_php": cap_php,
        },
        "recent_topups": recent_topups,
        "recent_ledger": recent_ledger,
    }), 200


@teller_bp.route("/wallet/topups", methods=["POST"])
@require_role("teller")
def create_topup():
    """
    Create a wallet top-up for a commuter.
    Body:
      - user_id (int)
      - method: 'cash' | 'gcash'
      - amount_pesos (int) or amount_php (alias)
      - external_ref (optional, for gcash)
    """
    data = request.get_json(silent=True) or {}

    try:
        account_user_id = int(data.get("user_id"))
    except Exception:
        return jsonify(error="invalid user_id"), 400

    method = str(data.get("method") or "cash").strip().lower()
    try:
        amount_pesos = int(data.get("amount_pesos") or data.get("amount_php") or 0)
    except Exception:
        amount_pesos = 0

    if amount_pesos < MIN_TOPUP or amount_pesos > MAX_TOPUP:
        return jsonify(error=f"amount must be between {MIN_TOPUP} and {MAX_TOPUP}"), 400

    # Optional: enforce a per-teller daily cap
    # start, end = _today_bounds_mnl()
    # op_col, _ = _operator_column()
    # today_sum = (
    #     TopUp.query
    #     .with_entities(func.coalesce(func.sum(TopUp.amount_pesos), 0))
    #     .filter(op_col == g.user.id, TopUp.status == "succeeded",
    #             TopUp.created_at >= start, TopUp.created_at < end)
    #     .scalar()
    # )
    # if _as_php(today_sum) + amount_pesos > TELLER_DAILY_CAP:
    #     return jsonify(error="teller daily limit exceeded"), 400

    try:
        if method == "cash":
            topup_id, ledger_id, new_bal = topup_cash(
                account_id=account_user_id,
                teller_id=getattr(g, "user", None).id,
                amount_pesos=amount_pesos,
            )
        elif method == "gcash":
            ext = (data.get("external_ref") or "").strip() or None
            topup_id, ledger_id, new_bal = topup_gcash(
                account_id=account_user_id,
                teller_id=getattr(g, "user", None).id,
                amount_pesos=amount_pesos,
                external_ref=ext,
            )
        else:
            return jsonify(error="unsupported method"), 400

        return jsonify({
            "ok": True,
            "topup_id": int(topup_id),
            "ledger_id": int(ledger_id),
            "new_balance_php": int(round(float(new_bal))),
        }), 201

    except Exception as e:
        current_app.logger.exception("[teller] create_topup failed")
        return jsonify(error=str(e)), 400
