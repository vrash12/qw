# backend/routes/auth.py
from __future__ import annotations

import os
import re
import time
import jwt
import hashlib, secrets
from datetime import datetime, timedelta, timezone
from flask import Blueprint, request, jsonify, g, current_app
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from sqlalchemy import text

from db import db
from models.user import User
from models.user_otp import UserOtp
from models.device_token import DeviceToken

from auth_guard import require_role
from utils.mail import send_email

__all__ = ["auth_bp", "require_role"]
auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# -------------------------------------------------------------------
# Config & helpers
# -------------------------------------------------------------------
SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key-here")
MNL_TZ = timezone(timedelta(hours=8))

# OTP config
OTP_TTL_MINUTES        = int(os.environ.get("OTP_TTL_MINUTES", "10"))
OTP_MAX_ATTEMPTS       = int(os.environ.get("OTP_MAX_ATTEMPTS", "5"))
OTP_RESEND_COOLDOWN_SEC= int(os.environ.get("OTP_RESEND_COOLDOWN_SEC", "60"))
OTP_PEPPER             = os.environ.get("OTP_PEPPER", "change-me")  # set long random in prod


def _as_bool(x, default=False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "on"}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _gen_otp_code() -> str:
    return f"{secrets.randbelow(1_000_000):06d}"


def _hash_code(code: str) -> str:
    return hashlib.sha256((OTP_PEPPER + code).encode("utf-8")).hexdigest()


def _otp_expiry() -> datetime:
    return _now_utc() + timedelta(minutes=OTP_TTL_MINUTES)


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

FIRST_USER_ID_NO_OTP = int(os.environ.get("FIRST_USER_ID_NO_OTP", "1"))

def _mask_email(addr: str) -> str:
    try:
        local, domain = addr.split("@", 1)
    except ValueError:
        return addr
    local_mask = local[0] + "***" if len(local) > 1 else "*"
    # keep domain TLD visible
    dot = domain.rfind(".")
    if dot > 0:
        dom_mask = domain[0] + "***" + domain[dot:]
    else:
        dom_mask = domain[0] + "***"
    return f"{local_mask}@{dom_mask}"

def _create_and_email_otp(user: User, *, purpose: str) -> None:
    """Create a new OTP row and email it to the user."""
    code = _gen_otp_code()
    rec = UserOtp(
        user_id=user.id,
        channel="email",
        destination=user.email,
        purpose=purpose,
        code_hash=_hash_code(code),
        expires_at=_otp_expiry(),
    )
    db.session.add(rec)
    db.session.commit()

    html = f"""
      <div style="font-family:system-ui,Segoe UI,Roboto,Arial">
        <h2>Verify your sign-in</h2>
        <p>Your one-time code is:</p>
        <div style="font-size:24px;font-weight:700;letter-spacing:3px">{code}</div>
        <p>This code expires in {OTP_TTL_MINUTES} minutes.</p>
      </div>
    """
    send_email(to=user.email, subject="Your verification code", html=html, text=f"Your code is {code}")


def _require_login_otp(user: User) -> bool:
    # Require OTP only for commuters who have a verified email, except the first user (id==1).
    return (
        (user.id != FIRST_USER_ID_NO_OTP) and
        (user.role or "").lower() == "commuter" and
        bool(getattr(user, "email", None)) and
        bool(getattr(user, "email_verified_at", None))
    )

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
        "emailVerified": bool(getattr(u, "email_verified_at", None)),
    }), 200


# -------------------------------------------------------------------
# Signup (commuter) + send OTP if email provided
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

    email = (data.get("email") or "").strip().lower()
    if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return jsonify(error="Invalid email address"), 400

    # Uniqueness
    cond = (User.username == data["username"].strip()) | (User.phone_number == digits)
    if email:
        cond = cond | (User.email == email)
    existing = User.query.filter(cond).first()

    if existing:
        return jsonify(error="Username, phone or email already exists"), 409

    user = User(
        first_name=data["firstName"].strip(),
        last_name=data["lastName"].strip(),
        username=data["username"].strip(),
        phone_number=digits,
        email=email or None,
        role="commuter",
    )
    user.set_password(data["password"])
    db.session.add(user)
    db.session.commit()

    # Send OTP if email present
    if user.email:
        code = _gen_otp_code()
        rec = UserOtp(
            user_id=user.id,
            channel="email",
            destination=user.email,
            purpose="signup",
            code_hash=_hash_code(code),
            expires_at=_otp_expiry(),
        )
        db.session.add(rec)
        db.session.commit()

        html = f"""
          <div style="font-family:system-ui,Segoe UI,Roboto,Arial">
            <h2>Verify your email</h2>
            <p>Your one-time code is:</p>
            <div style="font-size:24px;font-weight:700;letter-spacing:3px">{code}</div>
            <p>This code expires in {OTP_TTL_MINUTES} minutes.</p>
          </div>
        """
        try:
            send_email(to=user.email, subject="Your verification code",
                       html=html, text=f"Your code is {code}")
        except Exception:
            current_app.logger.exception("Failed to send signup OTP email")

    return jsonify(
        message="User registered successfully. Verification code sent." if user.email else "User registered successfully",
        userId=user.id,
        email=user.email
    ), 201

@auth_bp.route("/login", methods=["POST"])
def login():
    """
    Sign in a user and return a JWT.
    For PAO users, require an assigned bus (either users.assigned_bus_id or a row in pao_assignments for today).
    For commuter users with a verified email, require MFA (email OTP).
    """
    data = request.get_json() or {}
    if "username" not in data or "password" not in data:
        return jsonify(error="Missing username or password"), 400

    def _get_user():
        return (
            User.query.options(
                load_only(
                    User.id,
                    User.username,
                    User.role,
                    User.first_name,
                    User.last_name,
                    User.assigned_bus_id,
                    User.password_hash,
                    User.phone_number,
                    User.email,
                    User.email_verified_at,
                )
            )
            .filter_by(username=data["username"])
            .first()
        )

    # One-time retry if DB connection dropped
    try:
        user = _get_user()
    except OperationalError as e:
        current_app.logger.warning("DB connection dropped; retrying once… %s", e)
        db.session.remove()
        db.engine.dispose()
        user = _get_user()

    if not (user and user.check_password(data["password"])):
        return jsonify(error="Invalid username or password"), 401

    # Gate commuters that have email but not verified
    if (user.role or "").lower() == "commuter" and user.email and not user.email_verified_at:
        return jsonify(error="Please verify your email to sign in."), 403

    role_lower = (user.role or "").lower()

    # Prefer the static column first
    legacy_bus = int(getattr(user, "assigned_bus_id", None) or 0) or None

    # 🔒 Enforce PAO must have a bus today
    bus_id = None
    bus_source = "none"
    if role_lower == "pao":
        if legacy_bus is not None:
            bus_id = legacy_bus
            bus_source = "static"
        else:
            try:
                bus_id = _today_bus_for_pao(user.id)
                if bus_id is not None:
                    bus_source = "pao_assignments"
            except Exception:
                current_app.logger.exception("[auth] _today_bus_for_pao lookup failed")

        if bus_id is None:
            return jsonify(error="You are not assigned to a bus today. Please contact your manager."), 403

    elif role_lower == "driver":
        bus_id = legacy_bus
        bus_source = "static" if bus_id is not None else "none"

    # ✅ MFA branch for commuters with verified email
    if role_lower == "commuter" and user.email and user.email_verified_at:
        try:
            # Cooldown on re-sends for purpose='login'
            last = (
                UserOtp.query.filter_by(user_id=user.id, purpose="login", channel="email")
                .order_by(UserOtp.id.desc())
                .first()
            )
            if last:
                last_ts = last.created_at if last.created_at.tzinfo else last.created_at.replace(tzinfo=timezone.utc)
                since = (_now_utc() - last_ts).total_seconds()
                if since < OTP_RESEND_COOLDOWN_SEC:
                    # Don't create a new row; re-use last (email a fresh code only if you want)
                    pass

            # Create & send login OTP
            _create_and_email_otp(user, purpose="login")
        except Exception:
            current_app.logger.exception("Failed to send login OTP email")
            # If email fails, do not log in silently
            return jsonify(error="Unable to send verification code. Please try again."), 500

        return jsonify(
            mfaRequired=True,
            delivery="email",
            to=_mask_email(user.email),
        ), 200

    # Otherwise: issue JWT (24h)
    token = jwt.encode(
        {
            "user_id": user.id,
            "username": user.username,
            "role": user.role,
            "exp": datetime.utcnow() + timedelta(hours=24),
        },
        SECRET_KEY,
        algorithm="HS256",
    )

    # Optional push-token registration
    expo_token = (data.get("expoPushToken") or "").strip()
    platform = (data.get("platform") or "").strip()
    if expo_token:
        rec = DeviceToken.query.filter_by(token=expo_token).first()
        if rec:
            changed = False
            if rec.user_id != user.id:
                rec.user_id = user.id
                changed = True
            if platform and rec.platform != platform:
                rec.platform = platform
                changed = True
            if changed:
                db.session.commit()
        else:
            db.session.add(DeviceToken(user_id=user.id, token=expo_token, platform=platform))
            db.session.commit()

    include_bus_id = bus_id if role_lower in {"pao", "driver"} else None

    return (
        jsonify(
            message="Login successful",
            token=token,
            role=user.role,
            busId=include_bus_id,
            busSource=bus_source,
            user={
                "id": user.id,
                "username": user.username,
                "firstName": user.first_name,
                "lastName": user.last_name,
                "phoneNumber": user.phone_number,
                "email": user.email,
                "emailVerified": bool(user.email_verified_at),
            },
        ),
        200,
    )



@auth_bp.route("/login/verify-otp", methods=["POST"])
def login_verify_otp():
    """
    Verify a commuter login OTP (purpose='login') and return a JWT on success.
    Body: { username, code, expoPushToken?, platform? }
    """
    data = request.get_json(silent=True) or {}
    ident = (data.get("username") or "").strip() or (data.get("email") or "").strip().lower()
    code  = (data.get("code") or "").strip()

    if not ident or not code:
        return jsonify(error="username/email and code are required"), 400

    user = User.query.filter((User.username == ident) | (User.email == ident)).first()
    if not user:
        return jsonify(error="User not found"), 404

    # Only commuters use this login MFA path
    if (user.role or "").lower() != "commuter":
        return jsonify(error="MFA not required for this account"), 400

    if not (user.email and user.email_verified_at):
        return jsonify(error="Email not verified"), 403

    row = (
        UserOtp.query.filter_by(user_id=user.id, purpose="login", channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if not row:
        return jsonify(error="No pending code. Please request a new one."), 404

    if row.attempts >= OTP_MAX_ATTEMPTS:
        return jsonify(error="Too many attempts. Please request a new code."), 429

    exp_ts = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=timezone.utc)
    if _now_utc() > exp_ts:
        return jsonify(error="Code expired. Please request a new code."), 410

    ok = secrets.compare_digest(row.code_hash, _hash_code(code))
    if not ok:
        row.attempts += 1
        db.session.commit()
        return jsonify(error="Invalid code"), 401

    # Success: consume OTP and issue JWT
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(error="Verification failed. Try again."), 500

    token = jwt.encode(
        {
            "user_id": user.id,
            "username": user.username,
            "role": user.role,
            "exp": datetime.utcnow() + timedelta(hours=24),
        },
        SECRET_KEY,
        algorithm="HS256",
    )

    # Optional push-token registration
    expo_token = (data.get("expoPushToken") or "").strip()
    platform = (data.get("platform") or "").strip()
    if expo_token:
        rec = DeviceToken.query.filter_by(token=expo_token).first()
        if rec:
            changed = False
            if rec.user_id != user.id:
                rec.user_id = user.id
                changed = True
            if platform and rec.platform != platform:
                rec.platform = platform
                changed = True
            if changed:
                db.session.commit()
        else:
            db.session.add(DeviceToken(user_id=user.id, token=expo_token, platform=platform))
            db.session.commit()

    # Return same shape as normal login
    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=None,
        busSource="none",
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "phoneNumber": user.phone_number,
            "email": user.email,
            "emailVerified": bool(user.email_verified_at),
        },
    ), 200



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


# -------------------------------------------------------------------
# Reset password + Check username/phone (unchanged)
# -------------------------------------------------------------------
@auth_bp.route("/reset-password", methods=["POST"])
def reset_password_by_username_phone():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    phone    = (data.get("phoneNumber") or "").strip()
    new_pw   = (data.get("newPassword") or "").strip()

    if not username or not phone or not new_pw:
        return jsonify(error="username, phoneNumber and newPassword are required"), 400
    if len(new_pw) < 6:
        return jsonify(error="newPassword must be at least 6 characters"), 400

    digits = re.sub(r"\D", "", phone)
    if not re.fullmatch(r"09\d{9}", digits):
        return jsonify(error="phoneNumber must start with 09 and be 11 digits (e.g., 09123456789)"), 400

    user = User.query.filter_by(username=username, phone_number=digits, role="commuter").first()
    if not user:
        return jsonify(error="Username and phone number do not match"), 404

    user.set_password(new_pw)
    db.session.commit()
    return jsonify(message="Password updated successfully"), 200


@auth_bp.route("/check-username-phone", methods=["POST"])
def check_username_phone():
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


@auth_bp.route("/otp/send", methods=["POST"])
def otp_send():
    data = request.get_json(silent=True) or {}
    ident = (data.get("username") or "").strip() or (data.get("email") or "").strip().lower()
    purpose = (data.get("purpose") or "signup").strip().lower()  # "signup" | "login"
    if purpose not in {"signup", "login"}:
        return jsonify(error="Invalid purpose"), 400

    if not ident:
        return jsonify(error="Provide username or email"), 400

    user = User.query.filter((User.username == ident) | (User.email == ident)).first()
    if not user or not user.email:
        return jsonify(error="User/email not found"), 404

    # For login purpose, ensure commuter + verified email
    if purpose == "login":
        if (user.role or "").lower() != "commuter":
            return jsonify(error="MFA not required for this account"), 400
        if not user.email_verified_at:
            return jsonify(error="Email not verified"), 403

    # Cooldown per purpose
    last = (
        UserOtp.query.filter_by(user_id=user.id, purpose=purpose, channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if last:
        last_ts = last.created_at if last.created_at.tzinfo else last.created_at.replace(tzinfo=timezone.utc)
        since = (_now_utc() - last_ts).total_seconds()
        if since < OTP_RESEND_COOLDOWN_SEC:
            return jsonify(error=f"Please wait {int(OTP_RESEND_COOLDOWN_SEC - since)}s before requesting a new code"), 429

    try:
        _create_and_email_otp(user, purpose=purpose)
    except Exception:
        current_app.logger.exception("Failed to send OTP email")
        return jsonify(error="Unable to send OTP right now"), 500

    return jsonify(message="OTP sent"), 200


@auth_bp.route("/otp/verify", methods=["POST"])
def otp_verify():
    """
    Verify a one-time code for SIGNUP email verification.
    Body: { "username": "..."} OR { "email": "..." }, plus { "code": "123456" }.
    Notes:
      - Purpose here is fixed to 'signup' (login MFA uses /auth/login/verify-otp).
      - Consumes the latest OTP if valid and marks users.email_verified_at.
    """
    data = request.get_json(silent=True) or {}
    ident = (data.get("username") or "").strip() or (data.get("email") or "").strip().lower()
    code  = (data.get("code") or "").strip()

    if not ident or not code:
        return jsonify(error="username/email and code are required"), 400

    user = User.query.filter((User.username == ident) | (User.email == ident)).first()
    if not user or not user.email:
        return jsonify(error="User/email not found"), 404

    # Get the most recent signup OTP for this user
    row = (
        UserOtp.query
        .filter_by(user_id=user.id, purpose="signup", channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if not row:
        return jsonify(error="No pending code. Please request a new one."), 404

    # Expiry check (treat naive datetimes as UTC)
    exp_ts = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=timezone.utc)
    if _now_utc() > exp_ts:
        return jsonify(error="Code expired. Please request a new code."), 410

    # Attempts & comparison (constant-time)
    if not secrets.compare_digest(row.code_hash, _hash_code(code)):
        row.attempts += 1
        db.session.commit()
        if row.attempts >= OTP_MAX_ATTEMPTS:
            return jsonify(error="Too many attempts. Please request a new code."), 429
        return jsonify(error="Invalid code"), 401

    # Success → mark verified and consume OTP
    try:
        user.email_verified_at = _now_utc()
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(error="Verification failed. Try again."), 500

    return jsonify(message="Email verified"), 200
