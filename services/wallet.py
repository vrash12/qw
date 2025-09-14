# backend/services/wallet.py
"""
Operator-less wallet services.

Amounts are WHOLE PESOS (ints).

Public API:
  - topup_cash(account_id: int, amount_pesos: int, rid: str | None = None)
  - topup_gcash(account_id: int, amount_pesos: int,
                external_ref: str | None = None, rid: str | None = None)
  - credit_wallet(account_id: int, amount_pesos: int,
                  event: str = 'credit:manual', ref_table: str | None = None,
                  ref_id: int | None = None, rid: str | None = None)
  - approve_topup_existing(account_id: int, topup_id: int, method: str,
                           amount_pesos: int, rid: str | None = None)

Return values:
  - topup_* -> (topup_id: int, ledger_id: int, new_balance_php: float)
  - credit_wallet -> (ledger_id: int, new_balance_php: float)
  - approve_topup_existing -> (ledger_id: int, new_balance_php: float)
"""

from __future__ import annotations

import time
import uuid
from typing import Optional, Tuple, Dict, Set

from flask import current_app
from sqlalchemy import text, inspect
from sqlalchemy.exc import IntegrityError, OperationalError

from db import db
from models.wallet import TopUp


# ---------- small utils ----------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _unique_ref(prefix: str) -> str:
    """Compact, unique, non-guessable reference with time + 8-hex suffix."""
    return f"{prefix}-{_now_ms()}-{uuid.uuid4().hex[:8]}"


def _sanitize_external_ref(provider: str, ref: Optional[str]) -> str:
    """Trim external ref; if empty/None, synthesize a unique one."""
    if ref is None:
        return _unique_ref(provider)
    v = str(ref).strip()
    return v if v else _unique_ref(provider)


def _columns_of(table: str) -> Set[str]:
    """Return actual DB columns for a table using SQLAlchemy inspector."""
    try:
        insp = inspect(db.engine)
        return {c["name"] for c in insp.get_columns(table)}
    except Exception:
        current_app.logger.exception("[wallet] failed to inspect columns of %s", table)
        return set()


def _show_create_table(table: str, tag: str) -> None:
    """Best-effort SHOW CREATE TABLE (MySQL/MariaDB), for debugging."""
    try:
        row = db.session.execute(text(f"SHOW CREATE TABLE {table}")).fetchone()
        if row and len(row) > 1:
            current_app.logger.error("[wallet][%s] SHOW CREATE TABLE %s:\n%s", tag, table, row[1])
    except Exception:
        current_app.logger.exception("[wallet][%s] show create %s failed", tag, table)


# ---------- low-level SQL helpers ----------

def _ensure_account_and_lock(account_id: int) -> int:
    """
    Ensure a row exists in wallet_accounts, then SELECT ... FOR UPDATE the balance.
    Returns current balance in whole pesos.
    """
    # Create parent if missing (INSERT IGNORE works on MySQL/MariaDB)
    db.session.execute(
        text("""
            INSERT IGNORE INTO wallet_accounts (user_id, balance_pesos, created_at, updated_at)
            VALUES (:uid, 0, NOW(), NOW())
        """),
        {"uid": account_id},
    )
    # Lock and read current balance
    row = db.session.execute(
        text("""
            SELECT COALESCE(balance_pesos, 0) AS bal
            FROM wallet_accounts
            WHERE user_id = :uid
            FOR UPDATE
        """),
        {"uid": account_id},
    ).mappings().first()
    if not row:
        raise ValueError("wallet not found")
    return int(row["bal"])


def _insert_ledger_row_credit(
    *,
    account_id: int,
    amount_pesos: int,
    running_balance_pesos: int,
    event: str,
    ref_table: str,
    ref_id: int,
    rid: Optional[str] = None,
) -> int:
    """
    Insert a credit row into wallet_ledger using raw SQL (schema-safe).
    Returns new ledger id.
    """
    tag = rid or "no-rid"
    sql = text("""
        INSERT INTO wallet_ledger
            (account_id, direction, event, amount_pesos, running_balance_pesos,
             ref_table, ref_id, created_at)
        VALUES
            (:aid, 'credit', :ev, :amt, :run, :rt, :rid, NOW())
    """)
    try:
        res = db.session.execute(sql, {
            "aid": account_id,
            "ev": event,
            "amt": amount_pesos,
            "run": running_balance_pesos,
            "rt": ref_table,
            "rid": ref_id,
        })
        new_id = getattr(res, "lastrowid", None)
        if not new_id:
            new_id = db.session.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        if not new_id:
            new_id = db.session.execute(text("SELECT MAX(id) FROM wallet_ledger")).scalar()
        return int(new_id)
    except OperationalError:
        db.session.rollback()
        current_app.logger.exception("[wallet][%s] ledger insert failed; schema is:", tag)
        _show_create_table("wallet_ledger", tag)
        raise


def _insert_topup_row(
    *,
    account_id: int,
    method: str,           # 'cash' | 'gcash'
    provider: str,         # nominal provider hint (may be adjusted for enum)
    provider_ref: str,     # non-empty unique-ish string
    amount_pesos: int,
    status: str = "succeeded",
    rid: Optional[str] = None,
) -> int:
    """
    Insert into wallet_topups using only columns that actually exist.
    If provider enum rejects the value, fallback to 'other' once.
    Returns new topup id.
    """
    tag = rid or "no-rid"
    cols = _columns_of("wallet_topups")
    use_provider_fields = {"provider", "provider_ref"}.issubset(cols)

    base_cols = ["account_id", "method", "amount_pesos", "status"]
    params: Dict[str, object] = {
        "account_id": account_id,
        "method": method,
        "amount_pesos": amount_pesos,
        "status": status,
    }

    if use_provider_fields:
        base_cols += ["provider", "provider_ref"]
        params["provider"] = provider
        params["provider_ref"] = provider_ref
    else:
        current_app.logger.info(
            "[wallet][%s] wallet_topups has no provider/provider_ref; inserting minimal row",
            tag
        )

    col_list = ", ".join(base_cols)
    val_list = ", ".join(f":{c}" for c in base_cols)
    sql = text(f"INSERT INTO wallet_topups ({col_list}) VALUES ({val_list})")

    try:
        res = db.session.execute(sql, params)
        new_id = getattr(res, "lastrowid", None)
        if not new_id:
            new_id = db.session.execute(text("SELECT LAST_INSERT_ID()")).scalar()
        if not new_id:
            new_id = db.session.execute(text("SELECT MAX(id) FROM wallet_topups")).scalar()
        return int(new_id)
    except IntegrityError as ie:
        # Handle ENUM mismatch for provider once by falling back to 'other'
        msg = str(ie).lower()
        if use_provider_fields and ("enum" in msg or "incorrect" in msg or "truncated" in msg):
            current_app.logger.warning("[wallet][%s] provider enum mismatch; retrying with provider='other'", tag)
            params["provider"] = "other"
            res = db.session.execute(sql, params)
            new_id = getattr(res, "lastrowid", None)
            if not new_id:
                new_id = db.session.execute(text("SELECT LAST_INSERT_ID()")).scalar()
            if not new_id:
                new_id = db.session.execute(text("SELECT MAX(id) FROM wallet_topups")).scalar()
            return int(new_id)
        raise
    except OperationalError:
        db.session.rollback()
        current_app.logger.exception("[wallet][%s] topup insert failed; schema is:", tag)
        _show_create_table("wallet_topups", tag)
        raise


# ---------- core transactional helpers ----------

def _credit_and_ledger_no_commit(
    *,
    account_id: int,
    amount_pesos: int,
    event: str,
    ref_table: str,
    ref_id: int,
    rid: Optional[str] = None,
) -> Tuple[int, float]:
    """
    Lock wallet, update balance, append ledger. DOES NOT commit.
    Returns (ledger_id, new_balance).
    """
    if amount_pesos <= 0:
        raise ValueError("amount_pesos must be positive")

    current_balance = _ensure_account_and_lock(account_id)
    new_balance = current_balance + amount_pesos

    db.session.execute(
        text("UPDATE wallet_accounts SET balance_pesos=:b, updated_at=NOW() WHERE user_id=:uid"),
        {"b": new_balance, "uid": account_id},
    )

    ledger_id = _insert_ledger_row_credit(
        account_id=account_id,
        amount_pesos=amount_pesos,
        running_balance_pesos=new_balance,
        event=event,
        ref_table=ref_table,
        ref_id=ref_id,
        rid=rid,
    )

    return int(ledger_id), float(new_balance)


# ---------- public API ----------

def topup_cash(
    *,
    account_id: int,
    amount_pesos: int,
    rid: Optional[str] = None,
) -> Tuple[int, int, float]:
    """
    Record a CASH top-up for a wallet account.
    Returns (topup_id, ledger_id, new_balance_php).
    """
    if amount_pesos <= 0:
        raise ValueError("amount_pesos must be positive")

    provider = "cash"
    provider_ref = _unique_ref(provider)

    try:
        # 1) Insert topup row (status 'succeeded' since it's immediate posting)
        topup_id = _insert_topup_row(
            account_id=account_id,
            method="cash",
            provider=provider,
            provider_ref=provider_ref,
            amount_pesos=amount_pesos,
            status="succeeded",
            rid=rid,
        )

        # 2) Credit wallet + ledger (same transaction)
        ledger_id, new_balance = _credit_and_ledger_no_commit(
            account_id=account_id,
            amount_pesos=amount_pesos,
            event="topup:cash",
            ref_table="wallet_topups",
            ref_id=topup_id,
            rid=rid,
        )

        # 3) Commit atomically
        db.session.commit()
        return int(topup_id), int(ledger_id), float(new_balance)
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[wallet][%s] topup_cash failed", rid or "no-rid")
        raise


def topup_gcash(
    *,
    account_id: int,
    amount_pesos: int,
    external_ref: Optional[str] = None,
    rid: Optional[str] = None,
) -> Tuple[int, int, float]:
    """
    Record a GCASH top-up for a wallet account.
    Returns (topup_id, ledger_id, new_balance_php).
    """
    if amount_pesos <= 0:
        raise ValueError("amount_pesos must be positive")

    provider = "gcash"
    provider_ref = _sanitize_external_ref(provider, external_ref)

    try:
        # 1) Insert topup row (status 'succeeded' since we're posting now)
        topup_id = _insert_topup_row(
            account_id=account_id,
            method="gcash",
            provider=provider,
            provider_ref=provider_ref,
            amount_pesos=amount_pesos,
            status="succeeded",
            rid=rid,
        )

        # 2) Credit wallet + ledger
        ledger_id, new_balance = _credit_and_ledger_no_commit(
            account_id=account_id,
            amount_pesos=amount_pesos,
            event="topup:gcash",
            ref_table="wallet_topups",
            ref_id=topup_id,
            rid=rid,
        )

        # 3) Commit
        db.session.commit()
        return int(topup_id), int(ledger_id), float(new_balance)
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[wallet][%s] topup_gcash failed", rid or "no-rid")
        raise


def credit_wallet(
    *,
    account_id: int,
    amount_pesos: int,
    event: str = "credit:manual",
    ref_table: Optional[str] = None,
    ref_id: Optional[int] = None,
    rid: Optional[str] = None,
) -> Tuple[int, float]:
    """
    Generic wallet credit (no wallet_topups row).
    Returns (ledger_id, new_balance_php).
    """
    try:
        ledger_id, new_balance = _credit_and_ledger_no_commit(
            account_id=account_id,
            amount_pesos=amount_pesos,
            event=event,
            ref_table=(ref_table or ""),
            ref_id=(ref_id or 0),
            rid=rid,
        )
        db.session.commit()
        return int(ledger_id), float(new_balance)
    except Exception:
        db.session.rollback()
        current_app.logger.exception("[wallet][%s] credit_wallet failed", rid or "no-rid")
        raise


def approve_topup_existing(
    *,
    account_id: int,
    topup_id: int,
    method: str,
    amount_pesos: int,
    rid: Optional[str] = None,
) -> Tuple[int, float]:
    """
    Approve a previously created wallet_topups row (e.g., commuter request).
    - Credits the wallet
    - Inserts a ledger row with ref_table='wallet_topups' and ref_id=<topup_id>
    - Sets wallet_topups.status = 'succeeded'
    (All in a single commit.)
    Returns (ledger_id, new_balance_php).
    """
    tag = rid or f"approve-{topup_id}"
    if amount_pesos <= 0:
        raise ValueError("amount_pesos must be positive")

    try:
        # Verify row & ownership
        row: TopUp | None = db.session.query(TopUp).filter(TopUp.id == topup_id).with_for_update(read=False, nowait=False).first()
        if not row:
            raise ValueError("topup request not found")
        if int(row.account_id) != int(account_id):
            raise ValueError("account mismatch for this topup")

        # Idempotency: already succeeded? Just return current balance.
        if (row.status or "").lower() == "succeeded":
            bal = _ensure_account_and_lock(account_id)  # locks, ensures row
            db.session.rollback()  # release lock; no changes
            return 0, float(bal)

        # Credit + ledger
        event = f"topup:{(method or 'cash').lower()}"
        ledger_id, new_balance = _credit_and_ledger_no_commit(
            account_id=account_id,
            amount_pesos=amount_pesos,
            event=event,
            ref_table="wallet_topups",
            ref_id=topup_id,
            rid=tag,
        )

        # Mark as succeeded
        db.session.execute(
            text("UPDATE wallet_topups SET status='succeeded' WHERE id=:id"),
            {"id": topup_id},
        )

        # Commit atomically
        db.session.commit()
        return int(ledger_id), float(new_balance)

    except Exception:
        db.session.rollback()
        current_app.logger.exception("[wallet][%s] approve_topup_existing failed", tag)
        raise
