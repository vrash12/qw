# auth_guard.py
from __future__ import annotations

import os
import jwt
from functools import wraps
from datetime import datetime, timezone, timedelta

from flask import request, jsonify, g, current_app
from sqlalchemy import text

from db import db
from models.user import User

__all__ = ["require_role"]

SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key-here")
_MNL = timezone(timedelta(hours=8))


def _assigned_bus(uid: int) -> int | None:
    """
    Return the user's statically assigned bus (users.assigned_bus_id).
    This replaces any dependency on per-day pao/driver assignment tables.
    """
    bus_id = db.session.execute(
        text("SELECT assigned_bus_id FROM users WHERE id = :uid"),
        {"uid": int(uid)},
    ).scalar()
    return int(bus_id) if bus_id is not None else None


def require_role(*roles):
    """
    Usage:
      @require_role()                    -> any authenticated user
      @require_role("pao")               -> only PAO (or admin)
      @require_role("pao", "manager")    -> PAO or Manager (or admin)
    """
    # Support passing a single list/tuple as well
    if len(roles) == 1 and isinstance(roles[0], (list, tuple, set)):
        roles = tuple(roles[0])
    allowed = {str(r).lower() for r in roles if r}

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                return jsonify(error="Missing token"), 401

            token = auth.split(" ", 1)[1]
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                uid = payload.get("user_id")
                user = db.session.get(User, uid)
                if not user:
                    return jsonify(error="User not found"), 401

                # Stash user for downstream handlers
                role = (user.role or "").lower()
                g.user = user  # type: ignore[attr-defined]
                g.role = role  # type: ignore[attr-defined]

                # Enriched guard logging (for PAO/Driver, include assigned bus)
                bus_for_log = None
                if role in {"pao", "driver"}:
                    try:
                        bus_for_log = _assigned_bus(user.id)
                    except Exception:
                        bus_for_log = None

                try:
                    current_app.logger.info(
                        "[guard] %s %s uid=%s user=%s role=%s bus=%s ip=%s",
                        request.method,
                        request.path,
                        user.id,
                        (user.username or ""),
                        role,
                        (bus_for_log if bus_for_log is not None else "—"),
                        request.remote_addr,
                    )
                except Exception:
                    # Never fail a request because logging exploded
                    pass

                # Role check (admin bypass)
                if allowed and role not in allowed and role != "admin":
                    return jsonify(error="Insufficient permissions"), 403

            except jwt.ExpiredSignatureError:
                return jsonify(error="Token has expired"), 401
            except jwt.InvalidTokenError:
                return jsonify(error="Invalid token"), 401
            except Exception as e:
                current_app.logger.error(f"[auth_guard] Authentication error: {e}")
                return jsonify(error="Authentication processing error"), 500

            return f(*args, **kwargs)

        return wrapped

    return decorator
