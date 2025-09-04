# backend/utils/push.py
import os
import time
from threading import Thread
from typing import List, Optional, Dict, Any, Iterable
from flask import current_app

# Expo push endpoint (override via env if needed)
EXPO_URL = os.getenv("EXPO_PUSH_URL", "https://exp.host/--/api/v2/push/send")

# Set to "1" to disable network calls (useful on dev boxes without SSL bundle)
DISABLE_PUSH = os.getenv("DISABLE_PUSH", "0") == "1"

# Keep a little headroom under Expo's 100-message limit
EXPO_CHUNK_SIZE = int(os.getenv("EXPO_CHUNK_SIZE", "90"))

# Short timeouts so requests never hang your request thread
# (connect timeout, read timeout)
CONNECT_TIMEOUT_S = float(os.getenv("EXPO_CONNECT_TIMEOUT_S", "1.5"))
READ_TIMEOUT_S    = float(os.getenv("EXPO_READ_TIMEOUT_S", "4.0"))


def _chunk(xs: Iterable[str], n: int):
    it = iter(xs)
    while True:
        batch = []
        try:
            for _ in range(n):
                batch.append(next(it))
        except StopIteration:
            if batch:
                yield batch
            break
        yield batch


def _valid_token(tok: str) -> bool:
    # Expo tokens typically look like: ExponentPushToken[xxxxxxxx...]
    return isinstance(tok, str) and tok.startswith("ExponentPushToken[")

def push_to_user(db, DeviceToken, user_id: int, title: str, body: str, data=None, **expo_fields):
    """
    Look up all Expo tokens for a single user and send a push (async).
    Returns True if at least one token existed (send is still fire-and-forget).
    """
    try:
        tokens = [t.token for t in DeviceToken.query.filter_by(user_id=user_id).all()]
        if not tokens:
            current_app.logger.info("[push_to_user] no tokens for user_id=%s", user_id)
            return False
        send_push_async(tokens, title, body, data or {}, **expo_fields)
        return True
    except Exception as e:
        current_app.logger.warning(f"[push_to_user] failed: {e}")
        return False

        
def send_push(
    tokens: List[str],
    title: str,
    body: str,
    data: Optional[Dict[str, Any]] = None,
    **expo_fields: Any,  # e.g. channelId="payments"
) -> bool:
    """
    Fire Expo push to given tokens. Returns True if at least one message in any batch was accepted.
    - Respects DISABLE_PUSH.
    - Uses high priority + short TTL to encourage immediate delivery.
    - Accepts extra Expo fields (e.g., channelId) via **expo_fields.
    - Uses short connect/read timeouts so this never stalls your request thread.
    """
    if not tokens:
        return False
    if DISABLE_PUSH:
        current_app.logger.info("[push] disabled by env; skipping send")
        return False

    # Lazy import to avoid startup issues where 'requests' may not be available
    try:
        import requests  # type: ignore
    except Exception as e:
        current_app.logger.warning(f"[push] requests not available: {e}")
        return False

    # Filter obviously invalid tokens early
    valid_tokens = [t for t in tokens if _valid_token(t)]
    dropped = len(tokens) - len(valid_tokens)
    if dropped:
        current_app.logger.warning("[push] dropped %d invalid Expo token(s)", dropped)
    if not valid_tokens:
        return False

    headers = {
        "Accept": "application/json",
        "Accept-encoding": "gzip, deflate",
        "Content-Type": "application/json",
    }

    ok_any = False
    payload_base: Dict[str, Any] = {
        "title": title,
        "body": body,
        "sound": "default",
        # Encourage immediate delivery
        "priority": "high",   # Android
        "ttl": 60,            # seconds
        "data": data or {},
    }
    # Allow passing fields like channelId="payments"
    if expo_fields:
        payload_base.update(expo_fields)

    for batch in _chunk(valid_tokens, EXPO_CHUNK_SIZE):
        msgs = [{**payload_base, "to": t} for t in batch]
        t0 = time.perf_counter()
        try:
            r = requests.post(
                EXPO_URL,
                json=msgs,
                headers=headers,
                timeout=(CONNECT_TIMEOUT_S, READ_TIMEOUT_S),
            )
        except Exception as e:
            current_app.logger.warning(f"[push] send failed: {e}")
            continue

        dt_ms = int((time.perf_counter() - t0) * 1000)
        current_app.logger.info(
            "[push] POST /push/send status=%s in=%dms batch=%d",
            r.status_code, dt_ms, len(batch)
        )

        if r.status_code != 200:
            txt = r.text[:500] if isinstance(r.text, str) else str(r.text)
            current_app.logger.error("Expo push error %s – %s", r.status_code, txt)
            continue

        # Parse response
        try:
            resp = r.json()
        except Exception:
            current_app.logger.warning("[push] non-JSON response from Expo")
            continue

        items = []
        if isinstance(resp, list):  # legacy
            items = resp
        elif isinstance(resp, dict):
            if isinstance(resp.get("data"), list):
                items = resp["data"]
            if resp.get("errors"):
                current_app.logger.warning("[push] top-level errors: %s", resp["errors"])
        else:
            current_app.logger.warning("[push] unexpected response shape: %r", resp)
            continue

        any_ok = False
        for idx, item in enumerate(items):
            status = (item or {}).get("status")
            if status == "ok":
                any_ok = True
                continue
            token = batch[idx] if idx < len(batch) else "<?>"
            current_app.logger.warning(
                "[push] token=%s status=%s message=%s details=%s",
                token, status, item.get("message"), item.get("details"),
            )
        ok_any = ok_any or any_ok

    return ok_any


def send_push_async(
    tokens: List[str],
    title: str,
    body: str,
    data: Optional[Dict[str, Any]] = None,
    **expo_fields: Any,
) -> None:
    """
    Fire-and-forget wrapper. Returns immediately; logs inside the thread.
    Use this from request handlers so HTTP returns instantly.
    """
    app = current_app._get_current_object()

    def _run():
        with app.app_context():
            try:
                send_push(tokens, title, body, data, **expo_fields)
            except Exception:
                app.logger.exception("[push_async] failure")

    Thread(target=_run, daemon=True).start()


def push_to_bus(db, User, DeviceToken, bus_id: int, title: str, body: str,
                extra: Optional[Dict[str, Any]] = None) -> bool:
    """
    Sends push to all PAOs assigned to a bus. Models are passed in to avoid circular imports.
    Uses async send so callers don't block.
    """
    try:
        pao_ids = [u.id for u in User.query.filter_by(role="pao", assigned_bus_id=bus_id)]
        if not pao_ids:
            return False
        tokens = [t.token for t in DeviceToken.query.filter(DeviceToken.user_id.in_(pao_ids))]
        if not tokens:
            return False
        # Non-blocking
        send_push_async(tokens, title, body, extra)
        return True
    except Exception as e:
        current_app.logger.warning(f"[push_to_bus] failed: {e}")
        return False
