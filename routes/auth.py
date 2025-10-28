# backend/routes/auth.py
from __future__ import annotations

import os
import re
import time
import jwt
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify, g, current_app
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from sqlalchemy import text

from db import db
from models.user import User
from models.device_token import DeviceToken

# 🔐 Import the decorator from the helper (and re-export it for convenience)
from auth_guard import require_role
__all__ = ["auth_bp", "require_role"]

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# -------------------------------------------------------------------
# Config & helpers
# -------------------------------------------------------------------
SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key-here")
MNL_TZ = timezone(timedelta(hours=8))


def _as_bool(x, default=False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _bus_for_pao_on(user_id: int, day) -> int | None:
    """Helper used by some PAO/driver screens; looks up assignment on a given day."""
    bus_id = db.session.execute(
        text("""
            SELECT bus_id
            FROM pao_assignments
            WHERE user_id = :uid AND service_date = :d
            LIMIT 1
        """),
        {"uid": int(user_id), "d": day},
    ).scalar()
    current_app.logger.info("[auth] lookup pao uid=%s day=%s → bus_id=%r", user_id, day, bus_id)
    return int(bus_id) if bus_id is not None else None


def _today_bus_for_pao(user_id: int) -> int | None:
    day = datetime.now(MNL_TZ).date()
    bus_id = db.session.execute(
        text("""
            SELECT bus_id
            FROM pao_assignments
            WHERE user_id = :uid AND DATE(service_date) = :d
            LIMIT 1
        """),
        {"uid": int(user_id), "d": day},
    ).scalar()
    return int(bus_id) if bus_id is not None else None


def _debug_dump_pao_state(user_id: int, day):
    """Optional debug: log what we have for PAO assignments for the given date."""
    try:
        row = db.session.execute(
            text("""
                SELECT a.id, a.user_id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE a.user_id=:uid AND DATE(a.service_date)=:d
                ORDER BY a.id DESC
                LIMIT 1
            """),
            {"uid": int(user_id), "d": day}
        ).mappings().first()

        day_rows = db.session.execute(
            text("""
                SELECT a.id, a.user_id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE DATE(a.service_date)=:d
                ORDER BY a.bus_id
            """),
            {"d": day}
        ).mappings().all()

        near = db.session.execute(
            text("""
                SELECT a.id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE a.user_id=:uid
                ORDER BY ABS(DATEDIFF(DATE(a.service_date), :d)) ASC
                LIMIT 3
            """),
            {"uid": int(user_id), "d": day}
        ).mappings().all()

        current_app.logger.info(
            "[pao:debug] uid=%s check_day=%s user_day_row=%s day_rows=%s near=%s",
            user_id, day, (dict(row) if row else None),
            [dict(r) for r in day_rows],
            [dict(n) for n in near],
        )
    except Exception:
        current_app.logger.exception("[pao:debug] dump failed")


# -------------------------------------------------------------------
# Middleware
# -------------------------------------------------------------------
@auth_bp.after_request
def add_perf_headers(resp):
    resp.headers["Connection"] = "keep-alive"
    resp.headers["Cache-Control"] = "no-store"
    return resp


# -------------------------------------------------------------------
# Health
# -------------------------------------------------------------------
@auth_bp.route("/ping", methods=["GET"])
def ping():
    return jsonify(ok=True, ts=time.time()), 200


# -------------------------------------------------------------------
# Me (token-based)
# -------------------------------------------------------------------
@auth_bp.route("/me", methods=["GET"])
def me():
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return jsonify(error="unauthorized"), 401

    token = auth.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        uid = payload.get("user_id")
        u = db.session.get(User, uid)
        if not u:
            return jsonify(error="unauthorized"), 401
    except jwt.ExpiredSignatureError:
        return jsonify(error="Token has expired"), 401
    except jwt.InvalidTokenError:
        return jsonify(error="Invalid token"), 401
    except Exception as e:
        current_app.logger.error(f"/auth/me token error: {e}")
        return jsonify(error="Authentication processing error"), 500

    return jsonify({
        "id": u.id,
        "email": getattr(u, "email", None),
        "first_name": getattr(u, "first_name", ""),
        "last_name": getattr(u, "last_name", ""),
        "role": getattr(u, "role", None),
        "assigned_bus_id": getattr(u, "assigned_bus_id", None),
    }), 200


# -------------------------------------------------------------------
# Signup (commuter)
# -------------------------------------------------------------------
@auth_bp.route("/signup", methods=["POST"])
def signup():
    data = request.get_json() or {}
    required = ["firstName", "lastName", "username", "phoneNumber", "password"]
    if not all(k in data and str(data[k]).strip() for k in required):
        return jsonify(error="Missing fields"), 400

    # Normalize & validate phone number (PH: 09123456789)
    raw_phone = str(data.get("phoneNumber", ""))
    digits = re.sub(r"\D", "", raw_phone)
    if not re.fullmatch(r"09\d{9}", digits):
        return jsonify(error="phoneNumber must start with 09 and be 11 digits (e.g., 09123456789)"), 400

    existing = User.query.filter(
        (User.username == data["username"].strip()) | (User.phone_number == digits)
    ).first()
    if existing:
        return jsonify(error="Username or phone number already exists"), 409

    user = User(
        first_name=data["firstName"].strip(),
        last_name=data["lastName"].strip(),
        username=data["username"].strip(),
        phone_number=digits,
        role="commuter",
    )
    user.set_password(data["password"])
    db.session.add(user)
    db.session.commit()

    return jsonify(message="User registered successfully"), 201


# -------------------------------------------------------------------
# Login
# -------------------------------------------------------------------
@auth_bp.route("/login", methods=["POST"])
def login():
    data = request.get_json() or {}
    if "username" not in data or "password" not in data:
        return jsonify(error="Missing username or password"), 400

    def _get_user():
        return (
            User.query.options(
                load_only(
                    User.id, User.username, User.role, User.first_name, User.last_name,
                    User.assigned_bus_id, User.password_hash, User.phone_number,
                )
            )
            .filter_by(username=data["username"])
            .first()
        )

    # one-time retry if connection dropped
    try:
        user = _get_user()
    except OperationalError as e:
        current_app.logger.warning("DB connection dropped; retrying once… %s", e)
        db.session.remove()
        db.engine.dispose()
        user = _get_user()

    if not (user and user.check_password(data["password"])):  # noqa: SIM103
        return jsonify(error="Invalid username or password"), 401

    role_lower = (user.role or "").lower()
    legacy_bus = int(getattr(user, "assigned_bus_id", None) or 0) or None

    # issue JWT (24h)
    token = jwt.encode(
        {"user_id": user.id, "username": user.username, "role": user.role,
         "exp": datetime.utcnow() + timedelta(hours=24)},
        SECRET_KEY, algorithm="HS256"
    )

    # optional push token registration
    expo_token = (data.get("expoPushToken") or "").strip()
    platform = (data.get("platform") or "").strip()
    if expo_token:
        rec = DeviceToken.query.filter_by(token=expo_token).first()
        if rec:
            changed = False
            if rec.user_id != user.id:
                rec.user_id = user.id; changed = True
            if platform and rec.platform != platform:
                rec.platform = platform; changed = True
            if changed:
                db.session.commit()
        else:
            db.session.add(DeviceToken(user_id=user.id, token=expo_token, platform=platform))
            db.session.commit()

    # For PAO / Driver apps, keep returning assigned_bus_id for legacy compatibility.
    bus_id = legacy_bus if role_lower in {"pao", "driver"} else None

    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=bus_id,
        busSource=("legacy" if bus_id is not None else "none"),
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "phoneNumber": user.phone_number,
        },
    ), 200


# -------------------------------------------------------------------
# Verify Token
# -------------------------------------------------------------------
@auth_bp.route("/verify-token", methods=["GET"])
def verify_token():
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return jsonify(error="No token provided"), 401

    token = auth_header.split(" ")[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user = db.session.get(User, payload["user_id"])
        if not user:
            return jsonify(error="User not found"), 401
        return jsonify(valid=True, user={"id": user.id, "username": user.username, "role": user.role}), 200
    except jwt.ExpiredSignatureError:
        return jsonify(error="Token has expired"), 401
    except jwt.InvalidTokenError:
        return jsonify(error="Invalid token"), 401

@auth_bp.route("/reset-password", methods=["POST"])
def reset_password_by_username_phone():
    """
    Reset password for a commuter by matching username + phoneNumber.

    Request JSON:
      {
        "username": "jdoe",
        "phoneNumber": "09123456789",
        "newPassword": "newpass123"
      }

    Rules:
      - Only commuters can reset via this endpoint (staff should be reset by admins).
      - Phone must be PH format: starts with 09 and is 11 digits.
      - newPassword must be at least 6 chars.
    """
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    phone    = (data.get("phoneNumber") or "").strip()
    new_pw   = (data.get("newPassword") or "").strip()

    # Basic presence checks
    if not username or not phone or not new_pw:
        return jsonify(error="username, phoneNumber and newPassword are required"), 400
    if len(new_pw) < 6:
        return jsonify(error="newPassword must be at least 6 characters"), 400

    # Normalize + validate PH phone number (e.g., 09123456789)
    digits = re.sub(r"\D", "", phone)
    if not re.fullmatch(r"09\d{9}", digits):
        return jsonify(error="phoneNumber must start with 09 and be 11 digits (e.g., 09123456789)"), 400

    # Only commuters are allowed to reset here
    user = User.query.filter_by(username=username, phone_number=digits, role="commuter").first()
    if not user:
        # Do not reveal which field failed (prevents enumeration)
        return jsonify(error="Username and phone number do not match"), 404

    # Update password
    user.set_password(new_pw)
    db.session.commit()

    return jsonify(message="Password updated successfully"), 200



@auth_bp.route("/check-username-phone", methods=["POST"])
def check_username_phone():
    """
    Lightweight identity check used by the Forgot Password screen.

    Request JSON:
      { "username": "jdoe", "phoneNumber": "09123456789" }

    Response JSON:
      {
        "usernameExists": true/false,
        "phoneExists": true/false,
        "match": true/false,           # username + phone belong to the SAME commuter
        "roleOfUsername": "commuter"|...|None,
        "resetAllowed": true/false     # True only for commuters
      }
    """
    import re as _re

    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    phone    = (data.get("phoneNumber") or "").strip()
    digits   = _re.sub(r"\D", "", phone)

    username_exists = False
    phone_exists = False
    pair_match_commuter = False
    role_of_username = None

    if username:
      row = User.query.filter_by(username=username).first()
      if row:
        username_exists = True
        role_of_username = (row.role or None)

    if digits:
      phone_exists = User.query.filter_by(phone_number=digits).first() is not None

    if username and digits:
      pair_match_commuter = (
        User.query.filter_by(username=username, phone_number=digits, role="commuter").first() is not None
      )

    return jsonify(
        usernameExists=username_exists,
        phoneExists=phone_exists,
        match=pair_match_commuter,
        roleOfUsername=role_of_username,
        resetAllowed=(role_of_username == "commuter")
    ), 200

# ... keep the existing /reset-password and other routes below ...

