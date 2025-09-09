# services/wallet.py
"""
Wallet services: safe wallet top-ups and credits with unique provider_ref handling.

Fixes IntegrityError 1062 on uq_topup_provider_ref by ensuring we NEVER write an
empty provider_ref (e.g., "gcash-"). For every top-up we assign:
  - provider = 'cash' | 'gcash' (matching the method)
  - provider_ref = non-empty, unique token (or a sanitized external ref)

Public functions expected by routes:
  - topup_cash(db, account_id: int, pao_id: int, amount_cents: int)
  - topup_gcash(db, account_id: int, pao_id: int, amount_cents: int, external_ref: str | None = None)
  - credit_wallet(db, account_id: int, amount_cents: int, *, event: str = 'credit:manual', ref_table: str | None = None, ref_id: int | None = None)

All functions return tuples:
  - topup_* -> (topup_id: int, ledger_id: int, new_balance_php: float)
  - credit_wallet -> (ledger_id: int, new_balance_php: float)
"""

from __future__ import annotations

import time
import uuid
from typing import Optional, Tuple

from sqlalchemy import desc, text
from sqlalchemy.exc import IntegrityError

# Local app imports
from db import db
from models.wallet import TopUp, WalletLedger  # aligns with your models/wallet.py

CENTS_PER_PHP = 100


def _now_ms() -> int:
    return int(time.time() * 1000)


def _unique_ref(prefix: str) -> str:
    """Generate a compact, unique, non-guessable reference that always has a suffix."""
    return f"{prefix}-{_now_ms()}-{uuid.uuid4().hex[:8]}"


def _sanitize_external_ref(provider: str, ref: Optional[str]) -> str:
    """
    Trim an externally-supplied reference (e.g., PSP RRN). If it's empty/whitespace,
    fall back to a synthetic unique reference so we never violate the unique index.
    """
    if ref is None:
        return _unique_ref(provider)
    val = str(ref).strip()
    return val if val else _unique_ref(provider)


def _last_running_balance_cents(account_id: int) -> int:
    """Get the latest running balance for the wallet account from the ledger."""
    last = (
        db.session.query(WalletLedger.running_balance_cents)
        .filter(WalletLedger.account_id == account_id)
        .order_by(desc(WalletLedger.id))
        .first()
    )
    return int(last[0]) if last and last[0] is not None else 0


def _insert_topup_row_orm(
    *,
    account_id: int,
    pao_id: int,
    method: str,
    provider: str,
    provider_ref: str,
    amount_cents: int,
) -> int:
    """
    Insert via ORM when the mapped model exposes provider/provider_ref.
    Returns new topup id.
    """
    kwargs = dict(
        account_id=account_id,
        pao_id=pao_id,
        method=method,
        amount_cents=amount_cents,
        status="succeeded",
    )
    # Only set if attributes exist on the mapped model
    if hasattr(TopUp, "provider"):
        kwargs["provider"] = provider
    if hasattr(TopUp, "provider_ref"):
        kwargs["provider_ref"] = provider_ref

    top = TopUp(**kwargs)  # type: ignore[arg-type]
    db.session.add(top)
    db.session.flush()  # unique constraint evaluated here
    return int(top.id)


def _insert_topup_row_sql(
    *,
    account_id: int,
    pao_id: int,
    method: str,
    provider: str,
    provider_ref: str,
    amount_cents: int,
) -> int:
    """
    Raw SQL insert that explicitly includes provider/provider_ref.
    Use this when the ORM model does NOT declare those columns but the DB table has them.
    """
    sql = text(
        """
        INSERT INTO wallet_topups
            (account_id, pao_id, method, amount_cents, status, provider, provider_ref)
        VALUES
            (:account_id, :pao_id, :method, :amount_cents, 'succeeded', :provider, :provider_ref)
        """
    )
    res = db.session.execute(
        sql,
        dict(
            account_id=account_id,
            pao_id=pao_id,
            method=method,
            amount_cents=amount_cents,
            provider=provider,
            provider_ref=provider_ref,
        ),
    )
    # MySQL/MariaDB: result.lastrowid; for others, fallback fetch
    new_id = getattr(res, "lastrowid", None)
    if not new_id:
        new_id = db.session.execute(text("SELECT LAST_INSERT_ID()")).scalar()  # MySQL
    if not new_id:
        # Generic fallback (may not be portable)
        new_id = db.session.execute(text("SELECT MAX(id) FROM wallet_topups")).scalar()
    return int(new_id)


def _insert_topup_and_ledger(
    *,
    account_id: int,
    pao_id: int,
    method: str,          # 'cash' | 'gcash'
    provider: str,        # keep in sync with method
    provider_ref: str,    # non-empty & unique per provider
    amount_cents: int,
    max_retries: int = 3,
) -> Tuple[int, int, float]:
    """
    Core insert with retry on provider_ref uniqueness collisions.
    Returns (topup_id, ledger_id, new_balance_php).
    """
    if amount_cents <= 0:
        raise ValueError("amount_cents must be positive")

    # Decide strategy: ORM (if model exposes provider fields) or raw SQL
    model_has_provider_cols = hasattr(TopUp, "provider") and hasattr(TopUp, "provider_ref")

    attempts = 0
    while True:
        attempts += 1
        try:
            # 1) Insert topup
            if model_has_provider_cols:
                topup_id = _insert_topup_row_orm(
                    account_id=account_id,
                    pao_id=pao_id,
                    method=method,
                    provider=provider,
                    provider_ref=provider_ref,
                    amount_cents=amount_cents,
                )
            else:
                # Force provider/provider_ref into the row to avoid DB defaults like "gcash-"
                topup_id = _insert_topup_row_sql(
                    account_id=account_id,
                    pao_id=pao_id,
                    method=method,
                    provider=provider,
                    provider_ref=provider_ref,
                    amount_cents=amount_cents,
                )

            # 2) Compute new balance
            prev_cents = _last_running_balance_cents(account_id)
            new_cents = prev_cents + amount_cents

            # 3) Append ledger entry
            led = WalletLedger(
                account_id=account_id,
                direction="credit",
                event=f"topup:{method}",
                amount_cents=amount_cents,
                running_balance_cents=new_cents,
                ref_table="wallet_topups",
                ref_id=topup_id,
            )
            db.session.add(led)

            db.session.commit()
            return int(topup_id), int(led.id), new_cents / CENTS_PER_PHP

        except IntegrityError as ie:
            db.session.rollback()
            # If the unique constraint hit is about provider_ref, regenerate and retry.
            msg = str(ie).lower()
            if (
                "duplicate entry" in msg
                and "uq_topup_provider_ref".lower() in msg
                and attempts <= max_retries
            ):
                provider_ref = _unique_ref(provider)
                continue
            raise  # bubble up

        except Exception:
            db.session.rollback()
            raise


def topup_cash(account_id: int, pao_id: int, amount_cents: int) -> Tuple[int, int, float]:
    """
    Record a CASH top-up for a wallet account.
    :return: (topup_id, ledger_id, new_balance_php)
    """
    provider = "cash"
    provider_ref = _unique_ref(provider)
    return _insert_topup_and_ledger(
        account_id=account_id,
        pao_id=pao_id,
        method="cash",
        provider=provider,
        provider_ref=provider_ref,
        amount_cents=amount_cents,
    )


def topup_gcash(
    account_id: int,
    pao_id: int,
    amount_cents: int,
    external_ref: Optional[str] = None,
) -> Tuple[int, int, float]:
    """
    Record a GCASH top-up for a wallet account.
    :return: (topup_id, ledger_id, new_balance_php)
    """
    provider = "gcash"
    provider_ref = _sanitize_external_ref(provider, external_ref)
    return _insert_topup_and_ledger(
        account_id=account_id,
        pao_id=pao_id,
        method="gcash",
        provider=provider,
        provider_ref=provider_ref,
        amount_cents=amount_cents,
    )


def credit_wallet(
    account_id: int,
    amount_cents: int,
    *,
    event: str = "credit:manual",
    ref_table: Optional[str] = None,
    ref_id: Optional[int] = None,
) -> Tuple[int, float]:
    """
    Generic wallet credit (no topup row). Used by commuter APIs or admin adjustments.
    :return: (ledger_id, new_balance_php)
    """
    if amount_cents <= 0:
        raise ValueError("amount_cents must be positive")

    try:
        prev_cents = _last_running_balance_cents(account_id)
        new_cents = prev_cents + amount_cents

        led = WalletLedger(
            account_id=account_id,
            direction="credit",
            event=event,
            amount_cents=amount_cents,
            running_balance_cents=new_cents,
            ref_table=ref_table,
            ref_id=ref_id,
        )
        db.session.add(led)
        db.session.commit()
        return int(led.id), new_cents / CENTS_PER_PHP
    except Exception:
        db.session.rollback()
        raise
