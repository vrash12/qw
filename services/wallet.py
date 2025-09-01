# services/wallet.py
from typing import Optional, Tuple
from db import db
from models.wallet import WalletAccount, WalletLedger, TopUp

def _get_or_create_account(user_id: int, *, lock: bool = True) -> WalletAccount:
    q = WalletAccount.query.filter_by(user_id=user_id)
    if lock:
        q = q.with_for_update()
    acct = q.first()
    if not acct:
        acct = WalletAccount(user_id=user_id, balance_cents=0)
        db.session.add(acct)
        db.session.flush()
    return acct

def credit_wallet(
    *,
    account: WalletAccount,
    amount_php: float,                         # <-- PHP in, not cents
    event: str = "topup",
    performed_by: Optional[int] = None,
    ref_table: Optional[str] = None,
    ref_id: Optional[int] = None,
) -> Tuple[int, float]:
    """Credits balance and writes a ledger row. Returns (ledger_id, new_balance_php)."""
    delta_cents = int(round(float(amount_php) * 100))
    old_cents = int(account.balance_cents or 0)
    new_cents = old_cents + delta_cents
    account.balance_cents = new_cents

    led = WalletLedger(
        account_id=account.id,
        direction="credit",
        event="topup",                          # enum-safe
        amount_cents=delta_cents,
        running_balance_cents=new_cents,
        performed_by=performed_by,
        ref_table=ref_table,
        ref_id=ref_id,
        # station_id REMOVED (not in model)
        # bus_id     OMITTED (we won't set it anymore)
    )
    db.session.add(led)
    db.session.flush()
    return led.id, new_cents / 100.0

def topup_cash(
    *,
    pao_id: int,
    user_id: int,
    amount_php: float,
    method: str = "cash",
):
    if float(amount_php) <= 0:
        raise ValueError("amount must be > 0")

    acct = _get_or_create_account(user_id, lock=True)

    topup = TopUp(
        account_id=acct.id,
        method=method,  # ← now from caller
        amount_cents=int(round(float(amount_php) * 100)),
        status="succeeded",
        pao_id=pao_id,
    )
    db.session.add(topup)
    db.session.flush()

    ledger_id, new_balance_php = credit_wallet(
        account=acct,
        amount_php=amount_php,
        event="topup",
        performed_by=pao_id,
        ref_table="wallet_topups",
        ref_id=topup.id,
    )

    return int(topup.id), int(ledger_id), float(new_balance_php)
