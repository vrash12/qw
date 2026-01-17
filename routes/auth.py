#routes/auth.py
from __future__ import annotations

import os
import re
import time
import jwt
import hashlib, secrets
from datetime import datetime, timedelta, timezone, date

from flask import Blueprint, request, jsonify, g, current_app
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from sqlalchemy import text

from db import db
from models.user import User
from models.user_otp import UserOtp
from models.device_token import DeviceToken
from models.pao_assignment import PaoAssignment

from auth_guard import require_role
from utils.mail import send_email

__all__ = ["auth_bp", "require_role"]
auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# -------------------------------------------------------------------
# Config & helpers
# -------------------------------------------------------------------
SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key-here")
MNL_TZ = timezone(timedelta(hours=8))
LOGIN_MFA_ROLES = {"commuter", "pao", "manager", "teller"}

# OTP config
OTP_TTL_MINUTES         = int(os.environ.get("OTP_TTL_MINUTES", "10"))
OTP_MAX_ATTEMPTS        = int(os.environ.get("OTP_MAX_ATTEMPTS", "5"))
OTP_RESEND_COOLDOWN_SEC = int(os.environ.get("OTP_RESEND_COOLDOWN_SEC", "60"))
OTP_PEPPER              = os.environ.get("OTP_PEPPER", "change-me")  # set long random in prod

FIRST_USER_ID_NO_OTP    = int(os.environ.get("FIRST_USER_ID_NO_OTP", "1"))


def _to_utc(dt: datetime) -> datetime:
    """Coerce a possibly-naive DB timestamp to aware UTC for safe math."""
    if dt.tzinfo:
        return dt.astimezone(timezone.utc)
    # Assume DB wrote local server time (Asia/Manila), then convert to UTC
    return dt.replace(tzinfo=MNL_TZ).astimezone(timezone.utc)


def _as_bool(x, default: bool = False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in {"1", "true", "yes", "on"}


OTP_DEV_MODE = (str(os.environ.get("OTP_DEV_MODE", "0")).strip().lower() in {"1", "true", "yes", "on"})


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
        text(
            """
            SELECT bus_id
            FROM pao_assignments
            WHERE user_id = :uid AND service_date = :d
            LIMIT 1
        """
        ),
        {"uid": int(user_id), "d": day},
    ).scalar()
    current_app.logger.info("[auth] lookup pao uid=%s day=%s → bus_id=%r", user_id, day, bus_id)
    return int(bus_id) if bus_id is not None else None


def _today_bus_for_pao(user_id: int) -> int | None:
    day = datetime.now(MNL_TZ).date()
    start = datetime.combine(day, datetime.min.time())
    end   = datetime.combine(day + timedelta(days=1), datetime.min.time())

    bus_id = db.session.execute(
        text(
            """
            SELECT bus_id
            FROM pao_assignments
            WHERE user_id = :uid
              AND service_date >= :start
              AND service_date <  :end
            LIMIT 1
            """
        ),
        {"uid": int(user_id), "start": start, "end": end},
    ).scalar()
    return int(bus_id) if bus_id is not None else None



def _debug_dump_pao_state(user_id: int, day):
    """Optional debug: log what we have for PAO assignments for the given date."""
    try:
        row = db.session.execute(
            text(
                """
                SELECT a.id, a.user_id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE a.user_id=:uid AND DATE(a.service_date)=:d
                ORDER BY a.id DESC
                LIMIT 1
            """
            ),
            {"uid": int(user_id), "d": day},
        ).mappings().first()

        day_rows = db.session.execute(
            text(
                """
                SELECT a.id, a.user_id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE DATE(a.service_date)=:d
                ORDER BY a.bus_id
            """
            ),
            {"d": day},
        ).mappings().all()

        near = db.session.execute(
            text(
                """
                SELECT a.id, a.bus_id,
                       CAST(a.service_date AS CHAR) AS service_date_txt
                FROM pao_assignments a
                WHERE a.user_id=:uid
                ORDER BY ABS(DATEDIFF(DATE(a.service_date), :d)) ASC
                LIMIT 3
            """
            ),
            {"uid": int(user_id), "d": day},
        ).mappings().all()

        current_app.logger.info(
            "[pao:debug] uid=%s check_day=%s user_day_row=%s day_rows=%s near=%s",
            user_id,
            day,
            (dict(row) if row else None),
            [dict(r) for r in day_rows],
            [dict(n) for n in near],
        )
    except Exception:
        current_app.logger.exception("[pao:debug] dump failed")


@auth_bp.after_request
def add_perf_headers(resp):
    resp.headers["Connection"] = "keep-alive"
    resp.headers["Cache-Control"] = "no-store"
    return resp


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


def _create_and_email_otp(user: User, *, purpose: str) -> str:
    """
    Create a new OTP row and email it to the user (transaction-safe).
    Returns the plaintext OTP (useful in DEV mode / logs).
    """
    code = _gen_otp_code()
    rec = UserOtp(
        user_id=user.id,
        channel="email",
        destination=user.email,
        purpose=purpose,
        code_hash=_hash_code(code),
        expires_at=_otp_expiry(),
    )
    # Don't commit yet; if email fails we don't want a cooldown record
    db.session.add(rec)
    db.session.flush()  # allocates rec.id but keeps txn open

    if OTP_DEV_MODE:
        current_app.logger.warning("[DEV_OTP] user_id=%s purpose=%s code=%s", user.id, purpose, code)
        db.session.commit()
        return code

    try:
        # Send first; only commit if delivery succeeded
        subj = {
            "signup": "Verify your email",
            "login": "Your verification code",
            "reset": "Password reset code",
        }.get(purpose, "Your verification code")
        heading = {
            "signup": "Verify your email",
            "login": "Verify your sign-in",
            "reset": "Reset your password",
        }.get(purpose, "Your verification code")
        html = f"""
          <div style="font-family:system-ui,Segoe UI,Roboto,Arial">
            <h2>{heading}</h2>
            <p>Your one-time code is:</p>
            <div style="font-size:24px;font-weight:700;letter-spacing:3px">{code}</div>
            <p>This code expires in {OTP_TTL_MINUTES} minutes.</p>
          </div>
        """
        send_email(to=user.email, subject=subj, html=html, text=f"Your code is {code}")
        db.session.commit()
        return code
    except Exception:
        db.session.rollback()  # removes the OTP row so cooldown doesn't trigger
        raise


def _require_login_otp(user: User) -> bool:
    """
    Legacy helper: require OTP only for commuters who have a verified email,
    except the first user (id==FIRST_USER_ID_NO_OTP). Currently not used
    by the /login route, which enforces OTP for all roles with email.
    """
    return (
        (user.id != FIRST_USER_ID_NO_OTP)
        and (user.role or "").lower() == "commuter"
        and bool(getattr(user, "email", None))
        and bool(getattr(user, "email_verified_at", None))
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

    return jsonify(
        {
            "id": u.id,
            "email": getattr(u, "email", None),
            "first_name": getattr(u, "first_name", ""),
            "last_name": getattr(u, "last_name", ""),
            "role": getattr(u, "role", None),
            "assigned_bus_id": getattr(u, "assigned_bus_id", None),
            "emailVerified": bool(getattr(u, "email_verified_at", None)),
        }
    ), 200


# -------------------------------------------------------------------
# Signup
# -------------------------------------------------------------------
@auth_bp.route("/signup", methods=["POST"])
def signup():
    """
    Create a commuter account, send a signup OTP by email (if email provided),
    and immediately issue a JWT so the app can go straight to the dashboard.
    Body JSON:
      {
        "firstName": str,
        "lastName": str,
        "email": str (optional),
        "username": str,
        "phoneNumber": "09XXXXXXXXX",  # PH format, 11 digits
        "password": str
      }
    """
    data = request.get_json(silent=True) or {}

    # Required fields (email optional)
    required = ["firstName", "lastName", "username", "phoneNumber", "password"]
    if not all(k in data and str(data[k]).strip() for k in required):
        return jsonify(error="Missing fields"), 400

    # Normalize & validate phone number (PH: 09123456789)
    raw_phone = str(data.get("phoneNumber", ""))
    digits = re.sub(r"\D", "", raw_phone)
    if not re.fullmatch(r"09\d{9}", digits):
        return jsonify(
            error="phoneNumber must start with 09 and be 11 digits (e.g., 09123456789)"
        ), 400

    # Normalize & validate email (optional)
    email = (data.get("email") or "").strip().lower()
    if email and not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
        return jsonify(error="Invalid email address"), 400

    # Uniqueness checks (username, phone, and email if given)
    cond = (User.username == data["username"].strip()) | (User.phone_number == digits)
    if email:
        cond = cond | (User.email == email)
    existing = User.query.filter(cond).first()
    if existing:
        return jsonify(error="Username, phone or email already exists"), 409

    # Create user
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

    # If email present, create & send a signup OTP (non-blocking on send failure)
    if user.email:
        try:
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
                send_email(
                    to=user.email,
                    subject="Your verification code",
                    html=html,
                    text=f"Your code is {code}",
                )
            except Exception:
                current_app.logger.exception("Failed to send signup OTP email (delivery error)")
        except Exception:
            current_app.logger.exception("Failed to create/signup OTP row")

    return jsonify(
        message=(
            "User registered successfully. Verification code sent."
            if user.email
            else "User registered successfully"
        ),
        userId=user.id,
        email=user.email,
        role=user.role,
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "phoneNumber": user.phone_number,
            "email": user.email,
            "emailVerified": bool(getattr(user, "email_verified_at", None)),
        },
    ), 201


@auth_bp.route("/login", methods=["POST"])
def login():
    """
    Password check → issue JWT immediately (NO OTP).
    Still enforces:
      - PAO must have a bus today (before issuing token)
    Accepts optional:
      - expoPushToken, platform  (for registering device token)
    """
    data = request.get_json(silent=True) or {}
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

    try:
        user = _get_user()
    except OperationalError as e:
        current_app.logger.warning("DB connection dropped; retrying once… %s", e)
        db.session.remove()
        db.engine.dispose()
        user = _get_user()

    if not (user and user.check_password(data["password"])):
        return jsonify(error="Invalid username or password"), 401

    role_lower = (user.role or "").lower()

    # 🔒 PAO must have a bus today
    legacy_bus = int(getattr(user, "assigned_bus_id", None) or 0) or None
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
            return jsonify(
                error="You are not assigned to a bus today. Please contact your manager."
            ), 403

    elif role_lower == "driver":
        bus_id = legacy_bus
        bus_source = "static" if bus_id is not None else "none"

    # ✅ Issue JWT (24h)
    token = jwt.encode(
        {
            "user_id": user.id,
            "username": user.username,
            "role": role_lower,  # always lower-case in JWT
            "exp": datetime.utcnow() + timedelta(hours=24),
        },
        SECRET_KEY,
        algorithm="HS256",
    )

    # ✅ Optional push-token registration (same logic you had in verify-otp)
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

    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=bus_id if role_lower in {"pao", "driver"} else None,
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
    ), 200

@auth_bp.route("/login/verify-otp", methods=["POST"])
def login_verify_otp():
    """
    Verify a login OTP (purpose='login') for ANY role and return a JWT on success.
    Body: { username | email, code, expoPushToken?, platform? }
    """
    data = request.get_json(silent=True) or {}
    ident = (data.get("username") or "").strip() or (data.get("email") or "").strip().lower()
    code = (data.get("code") or "").strip()

    if not ident or not code:
        return jsonify(error="username/email and code are required"), 400

    user = User.query.filter((User.username == ident) | (User.email == ident)).first()
    if not user:
        return jsonify(error="User not found"), 404

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

    if not secrets.compare_digest(row.code_hash, _hash_code(code)):
        row.attempts += 1
        db.session.commit()
        return jsonify(error="Invalid code"), 401

    # Success → consume OTP
    try:
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(error="Verification failed. Try again."), 500

    # Compute bus info (same rules as /login)
    role_lower = (user.role or "").lower()
    legacy_bus = int(getattr(user, "assigned_bus_id", None) or 0) or None
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
            # Should be very rare since we checked in /login, but keep safety
            return jsonify(
                error="You are not assigned to a bus today. Please contact your manager."
            ), 403
    elif role_lower == "driver":
        bus_id = legacy_bus
        bus_source = "static" if bus_id is not None else "none"

    role_lower = (user.role or "").lower()

    token = jwt.encode(
        {
            "user_id": user.id,
            "username": user.username,
            "role": role_lower,  # ← always lower-case in JWT
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

    current_app.logger.info(
        "[auth] verify-otp uid=%s → bus_id=%r source=%s",
        user.id,
        bus_id,
        bus_source,
    )

    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=bus_id if role_lower in {"pao", "driver"} else None,
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
    ), 200


# -------------------------------------------------------------------
# PAO session check (for live bus reassignment)
# -------------------------------------------------------------------
def resolve_pao_bus_for_today(user_id: int) -> tuple[int | None, str]:
    """
    Returns (bus_id, source) for the PAO's current assignment.

    source is:
      - 'pao_assignments' if coming from daily assignment table
      - 'static' if from users.assigned_bus_id
      - 'none' if no assignment
    """
    today = date.today()

    # 1. Try daily assignment table first
    pa = (
        db.session.query(PaoAssignment)
        .filter(
            PaoAssignment.user_id == user_id,
            PaoAssignment.service_date == today,
        )
        .order_by(PaoAssignment.id.desc())
        .first()
    )
    if pa and pa.bus_id:
        return pa.bus_id, "pao_assignments"

    # 2. Fallback to static assigned_bus_id on users table
    user = db.session.get(User, user_id)
    if user and user.assigned_bus_id:
        return user.assigned_bus_id, "static"

    # 3. Nothing
    return None, "none"

@auth_bp.route("/session/check", methods=["GET"])
def session_check():
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return jsonify(error="UNAUTHENTICATED"), 401

    token = auth.split(" ", 1)[1]

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return jsonify(error="Token has expired"), 401
    except jwt.InvalidTokenError:
        return jsonify(error="Invalid token"), 401
    except Exception:
        current_app.logger.exception("/session/check decode failed")
        return jsonify(error="Authentication error"), 500

    role = (payload.get("role") or "").lower()
    if role != "pao":
        # Token is valid, but not a PAO → forbid, not "expired"
        return jsonify(error="Forbidden"), 403

    user_id = payload.get("user_id")
    if not user_id:
        return jsonify(error="UNAUTHENTICATED"), 401

    # Now resolve today's bus
    bus_id, source = resolve_pao_bus_for_today(int(user_id))
    return jsonify(ok=True, busId=bus_id, busSource=source), 200

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
        return jsonify(
            valid=True,
            user={"id": user.id, "username": user.username, "role": user.role},
        ), 200
    except jwt.ExpiredSignatureError:
        return jsonify(error="Token has expired"), 401
    except jwt.InvalidTokenError:
        return jsonify(error="Invalid token"), 401


# -------------------------------------------------------------------
# Reset password + Check username/phone
# -------------------------------------------------------------------
@auth_bp.route("/reset-password", methods=["POST"])
def reset_password_by_username_phone():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    phone = (data.get("phoneNumber") or "").strip()
    new_pw = (data.get("newPassword") or "").strip()

    if not username or not phone or not new_pw:
        return jsonify(error="username, phoneNumber and newPassword are required"), 400
    if len(new_pw) < 6:
        return jsonify(error="newPassword must be at least 6 characters"), 400

    digits = re.sub(r"\D", "", phone)
    if not re.fullmatch(r"09\d{9}", digits):
        return jsonify(
            error="phoneNumber must start with 09 and be 11 digits (e.g., 09123456789)"
        ), 400

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
    phone = (data.get("phoneNumber") or "").strip()
    digits = _re.sub(r"\D", "", phone)

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
            User.query.filter_by(username=username, phone_number=digits, role="commuter").first()
            is not None
        )

    return jsonify(
        usernameExists=username_exists,
        phoneExists=phone_exists,
        match=pair_match_commuter,
        roleOfUsername=role_of_username,
        resetAllowed=(role_of_username == "commuter"),
    ), 200


# -------------------------------------------------------------------
# Generic OTP send / signup / login / reset
# -------------------------------------------------------------------
@auth_bp.route("/otp/send", methods=["POST"])
def otp_send():
    """
    Send an OTP by email for purpose in {'signup','login','reset'}.
    - 'login': allowed for ANY role; no email_verified_at requirement.
    - 'reset': still commuter-only (keep/change to your policy).
    Cooldown per purpose uses OTP_RESEND_COOLDOWN_SEC. Accepts JSON or form-encoded.
    """
    raw = request.get_json(silent=True) or {}
    if not raw and request.form:
        raw = request.form.to_dict()

    def _coerce_purpose(p):
        p = (p or "signup").strip().lower()
        alias = {
            "forgot": "reset",
            "forgot-password": "reset",
            "password_reset": "reset",
            "password-reset": "reset",
        }
        return alias.get(p, p)

    purpose = _coerce_purpose(raw.get("purpose"))
    ident = (
        (raw.get("username") or "").strip()
        or (raw.get("email") or "").strip().lower()
        or (raw.get("to") or "").strip().lower()
    )

    current_app.logger.info("[otp_send] payload=%r → purpose=%s ident=%s", raw, purpose, ident or "<empty>")

    if purpose not in {"signup", "login", "reset"}:
        return jsonify(error="Invalid purpose; use signup, login, or reset"), 400
    if not ident:
        return jsonify(error="Provide username or email"), 400

    user = User.query.filter((User.username == ident) | (User.email == ident)).first()
    if not user or not user.email:
        return jsonify(error="User/email not found"), 404

    role_lower = (user.role or "").lower()

    # Reset: keep commuter-only (align with your existing reset policy)
    if purpose == "reset" and role_lower != "commuter":
        return jsonify(
            error="Password reset via email is only available for commuter accounts"
        ), 400

    # Cooldown per purpose
    last = (
        UserOtp.query.filter_by(user_id=user.id, purpose=purpose, channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if last:
        last_ts_utc = _to_utc(last.created_at)
        since = (_now_utc() - last_ts_utc).total_seconds()
        remaining = int(max(0, OTP_RESEND_COOLDOWN_SEC - since))
        if remaining > 0:
            return jsonify(error=f"Please wait {remaining}s before requesting a new code"), 429

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
    Optional: { "expoPushToken": str, "platform": "ios"|"android" }
    On success: marks email as verified AND returns a JWT so client can go to dashboard.
    """
    data = request.get_json(silent=True) or {}
    ident = (data.get("username") or "").strip() or (data.get("email") or "").strip().lower()
    code = (data.get("code") or "").strip()
    expo_token = (data.get("expoPushToken") or "").strip()
    platform = (data.get("platform") or "").strip()

    if not ident or not code:
        return jsonify(error="username/email and code are required"), 400

    user = User.query.filter((User.username == ident) | (User.email == ident)).first()
    if not user or not user.email:
        return jsonify(error="User/email not found"), 404

    # Get most recent SIGNUP OTP
    row = (
        UserOtp.query.filter_by(user_id=user.id, purpose="signup", channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if not row:
        return jsonify(error="No pending code. Please request a new one."), 404

    # Expiry (treat naive as UTC)
    exp_ts = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=timezone.utc)
    if _now_utc() > exp_ts:
        return jsonify(error="Code expired. Please request a new one."), 410

    # Attempts & comparison
    if not secrets.compare_digest(row.code_hash, _hash_code(code)):
        row.attempts += 1
        db.session.commit()
        if row.attempts >= OTP_MAX_ATTEMPTS:
            return jsonify(error="Too many attempts. Please request a new code."), 429
        return jsonify(error="Invalid code"), 401

    # Success → mark verified & consume OTP
    try:
        user.email_verified_at = _now_utc()
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(error="Verification failed. Try again."), 500

    # Optional: register push token
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

    # Issue JWT (24h)
    try:
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
    except Exception:
        current_app.logger.exception("JWT encode failed after OTP verify")
        return jsonify(error="Could not create session token"), 500

    return jsonify(
        message="Email verified",
        token=token,
        role=user.role,
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "phoneNumber": user.phone_number,
            "email": user.email,
            "emailVerified": True,
        },
    ), 200


@auth_bp.route("/otp/verify-reset", methods=["POST"])
def otp_verify_reset():
    """
    Verify a password-reset OTP (purpose='reset') by email.
    Body: { email: str, code: str }
    Does NOT consume the OTP; only validates it.
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    code = (data.get("code") or "").strip()

    if not email or not code:
        return jsonify(error="email and code are required"), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify(error="User/email not found"), 404

    # Optional: restrict to commuter accounts (keeps consistent with your older reset flow)
    if (user.role or "").lower() != "commuter":
        return jsonify(
            error="Password reset via email is only available for commuter accounts"
        ), 400

    row = (
        UserOtp.query.filter_by(user_id=user.id, purpose="reset", channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if not row:
        return jsonify(error="No pending code. Please request a new one."), 404

    # Expiry
    exp_ts = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=timezone.utc)
    if _now_utc() > exp_ts:
        return jsonify(error="Code expired. Please request a new one."), 410

    # Attempts & comparison
    if not secrets.compare_digest(row.code_hash, _hash_code(code)):
        row.attempts += 1
        db.session.commit()
        if row.attempts >= OTP_MAX_ATTEMPTS:
            return jsonify(error="Too many attempts. Please request a new code."), 429
        return jsonify(error="Invalid code"), 401

    # Valid (do NOT delete here)
    return jsonify(message="Code valid"), 200


@auth_bp.route("/reset-password-email", methods=["POST"])
def reset_password_email():
    """
    Reset password using email + OTP.
    Body: { email: str, code: str, newPassword: str }
    Validates latest OTP (purpose='reset'), updates password, and CONSUMES the OTP.
    """
    data = request.get_json(silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    code = (data.get("code") or "").strip()
    new_pw = (data.get("newPassword") or "").strip()

    if not email or not code or not new_pw:
        return jsonify(error="email, code and newPassword are required"), 400
    if len(new_pw) < 6:
        return jsonify(error="newPassword must be at least 6 characters"), 400

    user = User.query.filter_by(email=email).first()
    if not user:
        return jsonify(error="User/email not found"), 404

    # Optional: commuter-only, for parity with existing logic
    if (user.role or "").lower() != "commuter":
        return jsonify(
            error="Password reset via email is only available for commuter accounts"
        ), 400

    row = (
        UserOtp.query.filter_by(user_id=user.id, purpose="reset", channel="email")
        .order_by(UserOtp.id.desc())
        .first()
    )
    if not row:
        return jsonify(error="No pending code. Please request a new one."), 404

    # Expiry
    exp_ts = row.expires_at if row.expires_at.tzinfo else row.expires_at.replace(tzinfo=timezone.utc)
    if _now_utc() > exp_ts:
        return jsonify(error="Code expired. Please request a new one."), 410

    # Attempts & comparison
    if not secrets.compare_digest(row.code_hash, _hash_code(code)):
        row.attempts += 1
        db.session.commit()
        if row.attempts >= OTP_MAX_ATTEMPTS:
            return jsonify(error="Too many attempts. Please request a new code."), 429
        return jsonify(error="Invalid code"), 401

    # Update password + consume OTP
    try:
        user.set_password(new_pw)
        db.session.delete(row)
        db.session.commit()
    except Exception:
        db.session.rollback()
        return jsonify(error="Could not update password. Try again."), 500

    return jsonify(message="Password updated successfully"), 200
