# backend/routes/auth.py
from __future__ import annotations

import os
import time
import jwt
import base64
import secrets
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta
from functools import wraps

from flask import Blueprint, request, jsonify, g, current_app, has_app_context, Response
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
from werkzeug.security import generate_password_hash, check_password_hash

from db import db
from models.user import User
from models.device_token import DeviceToken

# Optional: challenge storage (recommended for login-time MFA)
try:
    from models.mfa_challenge import MfaChallenge
except Exception:
    MfaChallenge = None  # You can still do username-based TOTP verify without the table.

# Optional deps
try:
    import pyotp
except Exception:
    pyotp = None

try:
    from twilio.rest import Client as TwilioClient
except Exception:
    TwilioClient = None

ADDRESS_ALLOWED = {
    "Ramos","Paniqui","Gerona","Tarlac City","Pura",
    "Concepcion","San Manuel","Anao","Others"
}

try:
    from google.oauth2 import id_token as google_id_token
    from google.auth.transport import requests as google_requests
except Exception:
    google_id_token = None
    google_requests = None
from pathlib import Path
# Firebase Admin (for verifying Firebase ID tokens)
#   pip install firebase-admin
import firebase_admin
from firebase_admin import auth as fb_auth, credentials

if not firebase_admin._apps:
    here = Path(__file__).resolve()

    # Primary location: backend/service-account.json  (beside routes/)
    cred_path = here.parent.parent / "service-account.json"

    # Fallback (if you accidentally put it inside routes/ next to auth.py)
    if not cred_path.exists():
        cred_path = here.parent / "service-account.json"

    if not cred_path.exists():
        raise FileNotFoundError(f"Firebase service account JSON not found at: {cred_path}")

    cred = credentials.Certificate(str(cred_path))
    firebase_admin.initialize_app(cred)

auth_bp = Blueprint("auth", __name__, url_prefix="/auth")

# ─────────────────────────── Config helpers ───────────────────────────

def _cfg(key: str, default=None):
    """Use app.config if an app context exists; else fall back to environment."""
    if has_app_context():
        return current_app.config.get(key, os.environ.get(key, default))
    return os.environ.get(key, default)

def _secret_key() -> str:
    # Match Config default to avoid mismatched dev/prod defaults
    return _cfg("SECRET_KEY", "dev_secret")

def _jwt_ttl_hours() -> int:
    try:
        return int(_cfg("JWT_TTL_HOURS", "24"))
    except Exception:
        return 24

def _app_name() -> str:
    return _cfg("APP_NAME", "YourApp")

def _mfa_enforced_roles() -> set[str]:
    raw = _cfg("MFA_ENFORCED_ROLES", "pao,manager,teller")
    return {r.strip().lower() for r in (raw or "").split(",") if r.strip()}

def _google_client_ids() -> set[str]:
    raw = _cfg("GOOGLE_CLIENT_IDS", "") or ""
    return {c.strip() for c in raw.split(",") if c.strip()}

# Twilio (SMS OTP) – optional
def _twilio_client():
    sid   = _cfg("TWILIO_ACCOUNT_SID")
    token = _cfg("TWILIO_AUTH_TOKEN")
    if sid and token and TwilioClient:
        return TwilioClient(sid, token)
    return None

FINISH_EMAIL_HTML = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Verify your email</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,'Helvetica Neue',Arial;
         background:#f6f7fb;margin:0;display:flex;min-height:100vh;align-items:center;justify-content:center}
    .card{background:#fff;max-width:520px;width:92%;padding:28px 24px;border-radius:16px;
          box-shadow:0 12px 30px rgba(0,0,0,.08)}
    h1{margin:0 0 6px;font-size:22px}
    p{color:#4b5563;margin:0 0 18px;line-height:1.5}
    input{width:100%;padding:14px 12px;border:1px solid #d1d5db;border-radius:12px;font-size:16px}
    button{margin-top:14px;width:100%;padding:14px 16px;border:0;border-radius:12px;
           background:#2563eb;color:#fff;font-weight:700;font-size:16px;cursor:pointer}
    button[disabled]{opacity:.6;cursor:not-allowed}
    .ok{color:#065f46}.err{color:#b91c1c}.muted{color:#6b7280;font-size:13px;margin-top:10px}
  </style>
</head>
<body>
  <div class="card">
    <h1>Confirm your email</h1>
    <p>This page will complete your sign-in link.</p>

    <label for="email">Email</label>
    <input id="email" type="email" placeholder="you@example.com" autocomplete="email"/>

    <button id="go">Verify</button>
    <p id="msg" class="muted">Paste/type your email then click Verify.</p>
  </div>

  <!-- Firebase (compat for simple inline usage) -->
  <script src="https://www.gstatic.com/firebasejs/10.13.1/firebase-app-compat.js"></script>
  <script src="https://www.gstatic.com/firebasejs/10.13.1/firebase-auth-compat.js"></script>
  <script>
  const firebaseConfig = {
    apiKey: "AIzaSyANBDzYOOXZ2_bOAWPjwcSXNR9UPp7g0DI",
    authDomain: "maximal-park-331722.firebaseapp.com",
    projectId: "maximal-park-331722",
    storageBucket: "maximal-park-331722.firebasestorage.app",
    messagingSenderId: "284415089867",
    appId: "1:284415089867:web:303d6e0829f359f30e0f4e"
  };
  firebase.initializeApp(firebaseConfig);
  const auth = firebase.auth();

  const $ = (id) => document.getElementById(id);
  const msg = $('msg');
  const emailEl = $('email');
  const go = $('go');
  const url = window.location.href;

  // Pull params passed from the app
  const qs = new URLSearchParams(location.search);
  const redirect = qs.get('redirect') || '';
  const emailFromQS = qs.get('email') || '';
  const firstName   = qs.get('firstName')   || '';
  const lastName    = qs.get('lastName')    || '';
  const phoneNumber = qs.get('phoneNumber') || '';
  const address     = qs.get('address')     || '';

  if (emailFromQS) emailEl.value = emailFromQS;

  function say(t, cls) { msg.className = cls ? cls : 'muted'; msg.textContent = t; }

  if (!auth.isSignInWithEmailLink(url)) {
    say('This link is not a valid sign-in link (or it has expired).', 'err');
    go.disabled = true;
  }

  go.addEventListener('click', async () => {
    try {
      go.disabled = true;
      say('Verifying link…');

      const email = emailEl.value.trim();
      if (!email) throw new Error('Please enter your email.');

      const cred = await auth.signInWithEmailLink(email, url);
      const user = cred.user;
      const idToken = await user.getIdToken(true);

      say('Email verified with Firebase. Finishing on server…');

      const resp = await fetch('/auth/firebase/exchange', {
        method: 'POST',
        headers: { 'Content-Type':'application/json' },
        body: JSON.stringify({ idToken, firstName, lastName, phoneNumber, address })
      });
      const json = await resp.json();
      if (!resp.ok) throw new Error(json.error || 'Server exchange failed');

      say('All set! You can return to the app.', 'ok');

      // bounce back to the app if a redirect was provided
      if (redirect) {
        const sep = redirect.includes('?') ? '&' : '?';
        setTimeout(() => location.replace(redirect + sep + 'verified=1'), 800);
      }
    } catch (e) {
      console.error(e);
      say(e.message || 'Something went wrong.', 'err');
      go.disabled = false;
    }
  });
  </script>
</body>
</html>
"""

@auth_bp.get("/finish-email")
def finish_email_page():
    return Response(FINISH_EMAIL_HTML, mimetype="text/html")

def _now_ts() -> int:
    return int(time.time())

def issue_app_jwt(user: User) -> str:
    """Full-privilege app token."""
    payload = {
        "user_id": user.id,
        "username": user.username,
        "role": user.role,
        "scope": "app",
        "exp": _now_ts() + 3600 * _jwt_ttl_hours(),  # numeric exp avoids tz problems
    }
    return jwt.encode(payload, _secret_key(), algorithm="HS256")

def issue_mfa_setup_jwt(user: User, minutes: int = 15) -> str:
    """Short-lived token for /mfa/totp/enroll + /mfa/totp/activate only."""
    payload = {
        "user_id": user.id,
        "username": user.username,
        "role": user.role,
        "scope": "mfa_setup",
        "exp": _now_ts() + minutes * 60,
    }
    return jwt.encode(payload, _secret_key(), algorithm="HS256")

def _issuer_and_label_for(user: User) -> tuple[str, str]:
    """
    Returns (issuer, label) for otpauth://totp/<label>?secret=...&issuer=<issuer>
    - For PAO: use PAO<assigned_bus_id> if available (PAO1, PAO2, ...)
    - For MANAGER/TELLER: use the role
    - Fallback to APP_NAME
    Label: "<issuer>:<username>"
    """
    role = (getattr(user, "role", "") or "").strip().upper()
    issuer = _app_name()
    if role == "PAO":
        bus_id = getattr(user, "assigned_bus_id", None)
        issuer = f"PAO{bus_id}" if isinstance(bus_id, int) and bus_id > 0 else "PAO"
    elif role in {"MANAGER", "TELLER"}:
        issuer = role
    label = f"{issuer}:{getattr(user, 'username', 'user')}"
    return issuer, label

def _decode_token_from_header():
    authz = request.headers.get("Authorization", "")
    if not authz.startswith("Bearer "):
        raise jwt.InvalidTokenError("Missing token")
    token = authz.split(" ", 1)[1]
    return jwt.decode(token, _secret_key(), algorithms=["HS256"])

def _require_user_any_scope(allowed_scopes={"app", "mfa_setup"}):
    try:
        payload = _decode_token_from_header()
        if payload.get("scope") not in allowed_scopes:
            raise jwt.InvalidTokenError("Scope not allowed")
        user = User.query.get(payload.get("user_id"))
        if not user:
            return None, (jsonify(error="User not found"), 401)
        return user, None
    except jwt.ExpiredSignatureError:
        return None, (jsonify(error="Token has expired"), 401)
    except jwt.InvalidTokenError as e:
        return None, (jsonify(error=str(e)), 401)
    except Exception as e:
        current_app.logger.error(f"_require_user_any_scope error: {e}")
        return None, (jsonify(error="Authentication processing error"), 500)

def _require_app_user():
    return _require_user_any_scope({"app"})

def _new_challenge(user: User, ttl_seconds: int = 300) -> MfaChallenge | None:
    """Create a short-lived MFA challenge (naive UTC to match MySQL DATETIME)."""
    if not MfaChallenge:
        return None
    ch = MfaChallenge(
        id=secrets.token_urlsafe(24),
        user_id=user.id,
        expires_at=datetime.utcnow() + timedelta(seconds=ttl_seconds),
        consumed=False,
    )
    db.session.add(ch)
    db.session.commit()
    return ch

def _save_expo_token_if_present(user: User, data: dict):
    expo_token = (data.get("expoPushToken") or "").strip()
    platform = (data.get("platform") or "").strip()
    if expo_token:
        rec = DeviceToken.query.filter_by(token=expo_token).first()
        if rec:
            rec.user_id = user.id
            rec.platform = platform or rec.platform
        else:
            db.session.add(DeviceToken(user_id=user.id, token=expo_token, platform=platform))
        db.session.commit()

def _mask_email(e: str) -> str:
    try:
        name, dom = e.split("@", 1)
        if len(name) <= 2:
            name_mask = name[0] + "*"
        else:
            name_mask = name[0] + "*" * (len(name) - 2) + name[-1]
        return f"{name_mask}@{dom}"
    except Exception:
        return e

# ─────────────────────────── Senders (SMS / Email) ───────────────────────────

def _send_sms_code(phone_e164: str, code: str) -> None:
    """
    Send a 6-digit OTP via Twilio (Messaging Service SID preferred; falls back to FROM number).
    """
    msg = f"{code} is your verification code. Do not share this code."
    client = _twilio_client()
    if not client:
        current_app.logger.info(f"[DEV] SMS to {phone_e164}: {msg}")
        return

    svc      = _cfg("TWILIO_MESSAGING_SID")
    from_num = _cfg("TWILIO_FROM")
    kwargs = dict(to=phone_e164, body=msg)
    try:
        if svc:
            client.messages.create(messaging_service_sid=svc, **kwargs)
        elif from_num:
            client.messages.create(from_=from_num, **kwargs)
        else:
            current_app.logger.warning("No TWILIO_MESSAGING_SID or TWILIO_FROM configured; SMS not sent.")
    except Exception as e:
        current_app.logger.error(f"Twilio send failed: {e}")
        raise

def _send_email_code(to_email: str, code: str) -> None:
    """
    Send a 6-digit OTP via SMTP. If SMTP is not configured, log to server console.
    """
    host = _cfg("SMTP_HOST")
    port = int(_cfg("SMTP_PORT", "587") or "587")
    user = _cfg("SMTP_USER")
    pwd  = _cfg("SMTP_PASS")
    from_addr = _cfg("SMTP_FROM", "no-reply@example.com")

    if not (host and user and pwd):
        current_app.logger.info(f"[DEV] Email code for {to_email}: {code}")
        return

    msg = EmailMessage()
    msg["Subject"] = "Your verification code"
    msg["From"] = from_addr
    msg["To"] = to_email
    app_name = _app_name()
    msg.set_content(f"{code} is your verification code for {app_name}. Do not share this code.")

    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.send_message(msg)

# ─────────────────────────── Health & Me ───────────────────────────

@auth_bp.get("/ping")
def ping():
    return jsonify(ok=True, ts=time.time()), 200

@auth_bp.get("/me")
def me():
    user, err = _require_app_user()
    if err:
        return err
    u = user
    return jsonify(
        {
            "id": u.id,
            "email": getattr(u, "email", None),
            "email_verified": getattr(u, "email_verified", False),
            "first_name": getattr(u, "first_name", ""),
            "last_name": getattr(u, "last_name", ""),
            "role": getattr(u, "role", None),
            "assigned_bus_id": getattr(u, "assigned_bus_id", None),
        }
    ), 200

# ─────────────────────────── Legacy username/password (optional) ───────────────────────────

@auth_bp.post("/signup")
def signup():
    data = request.get_json() or {}
    required = ["firstName", "lastName", "username", "phoneNumber", "password"]
    if not all(k in data and str(data[k]).strip() for k in required):
        return jsonify(error="Missing fields"), 400

    existing = User.query.filter(
        (User.username == data["username"]) | (User.phone_number == data["phoneNumber"])
    ).first()
    if existing:
        return jsonify(error="Username or phone number already exists"), 409

    user = User(
        first_name=data["firstName"].strip(),
        last_name=data["lastName"].strip(),
        username=data["username"].strip(),
        phone_number=data["phoneNumber"].strip(),
        role="commuter",
    )
    user.set_password(data["password"])
    db.session.add(user)
    db.session.commit()

    return jsonify(message="User registered successfully"), 201

@auth_bp.post("/login")
def login():
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
                    User.totp_secret,
                    User.mfa_enabled,
                    User.phone_number,
                    User.phone_verified,
                    User.email,
                    User.email_verified,
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

    password_ok = bool(user and user.check_password(data["password"]))
    if not (user and password_ok):
        return jsonify(error="Invalid username or password"), 401

    role_needs_mfa = (user.role or "").lower() in _mfa_enforced_roles()
    has_totp = bool(user.mfa_enabled and user.totp_secret)
    has_sms = bool(user.phone_number)  # optionally also check user.phone_verified

    # A) Role requires MFA but user hasn't set up TOTP yet → force setup
    if role_needs_mfa and not has_totp:
        setup_token = issue_mfa_setup_jwt(user, minutes=15)
        return jsonify(
            mfaSetupRequired=True,
            token=setup_token,  # short-lived; only valid for enroll/activate
            role=user.role,
            user={
                "id": user.id,
                "username": user.username,
                "firstName": user.first_name,
                "lastName": user.last_name,
                "phoneNumber": user.phone_number,
                "email": user.email,
            },
        ), 200

    # B) MFA available/enforced → return challenge for verify step
    if role_needs_mfa or has_totp or has_sms:
        ch = _new_challenge(user)
        methods = []
        if has_totp:
            methods.append("totp")
        if has_sms:
            methods.append("sms")

        if not methods:
            token = issue_app_jwt(user)
            _save_expo_token_if_present(user, data)
            return jsonify(
                message="Login successful (no MFA available)",
                token=token,
                role=user.role,
                busId=user.assigned_bus_id,
                user={
                    "id": user.id,
                    "username": user.username,
                    "firstName": user.first_name,
                    "LastName": user.last_name,
                    "phoneNumber": user.phone_number,
                    "email": user.email,
                },
            ), 200

        payload = dict(
            mfaRequired=True,
            mfaMethods=methods,
            role=user.role,
            user={
                "id": user.id,
                "username": user.username,
                "firstName": user.first_name,
                "lastName": user.last_name,
                "phoneNumber": user.phone_number,
                "email": user.email,
            },
        )
        if ch:
            payload["challengeId"] = ch.id
        else:
            payload["username"] = user.username  # fallback without challenge table
        return jsonify(payload), 200

    # C) No MFA needed
    token = issue_app_jwt(user)
    _save_expo_token_if_present(user, data)
    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=user.assigned_bus_id,
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "phoneNumber": user.phone_number,
            "email": user.email,
        },
    ), 200

@auth_bp.post("/firebase/exchange")
def firebase_exchange():
    """
    Body: { idToken, firstName?, lastName?, phoneNumber?, address? }
    - Verifies Firebase ID token (email must be verified)
    - Upserts/updates SQL user (fills profile fields if provided)
    - Enforces TOTP at login for enforced roles (unchanged)
    """
    ADDRESS_ALLOWED = {
        "Ramos","Paniqui","Gerona","Tarlac City","Pura",
        "Concepcion","San Manuel","Anao","Others"
    }

    data = request.get_json(silent=True) or {}
    id_token = (data.get("idToken") or "").strip()
    if not id_token:
        return jsonify(error="Missing idToken"), 400

    # Optional profile fields
    first = (data.get("firstName") or "").strip()
    last  = (data.get("lastName") or "").strip()
    phone = (data.get("phoneNumber") or "").strip()
    addr  = (data.get("address") or "").strip()

    if addr and addr not in ADDRESS_ALLOWED:
        return jsonify(error="Invalid address"), 400

    try:
        decoded = fb_auth.verify_id_token(id_token)  # signature + exp
    except Exception as e:
        msg = str(e)
        current_app.logger.warning(f"Firebase verify_id_token failed: {e}")
        if "Token used too early" in msg:
            time.sleep(2)
            try:
                decoded = fb_auth.verify_id_token(id_token)
            except Exception:
                return jsonify(error="Invalid Firebase token"), 401
        else:
            return jsonify(error="Invalid Firebase token"), 401

    email = (decoded.get("email") or "").lower()
    email_verified = bool(decoded.get("email_verified"))
    uid = decoded.get("uid")
    if not (email and uid):
        return jsonify(error="Token missing email/uid"), 400
    if not email_verified:
        return jsonify(error="Email not verified"), 403

    user = User.query.filter_by(email=email).first()
    if not user:
        # derive username from email (ensure uniqueness)
        base = email.split("@", 1)[0][:30] or f"user{secrets.token_hex(4)}"
        uname = base
        i = 1
        while User.query.filter_by(username=uname).first():
            i += 1
            uname = f"{base}{i}"

        user = User(
            username=uname,
            email=email,
            email_verified=True,
            role="commuter",
            password_hash=generate_password_hash(secrets.token_urlsafe(16)),
        )
        # fill profile if present
        if first: user.first_name = first
        if last:  user.last_name = last
        if phone: user.phone_number = phone
        if addr:  setattr(user, "address", addr)  # ensure column exists
        db.session.add(user)
        db.session.commit()
    else:
        changed = False
        if email_verified and not user.email_verified:
            user.email_verified = True; changed = True
        if first and user.first_name != first:
            user.first_name = first; changed = True
        if last and user.last_name != last:
            user.last_name = last; changed = True
        if phone and user.phone_number != phone:
            user.phone_number = phone; changed = True
        if addr and getattr(user, "address", None) != addr:
            setattr(user, "address", addr); changed = True
        if changed:
            db.session.commit()

    # ── MFA policy (unchanged) ───────────────────────────────────────
    role_needs_mfa = (user.role or "").lower() in _mfa_enforced_roles()
    has_totp = bool(user.mfa_enabled and user.totp_secret)

    if role_needs_mfa and not has_totp:
        setup_token = issue_mfa_setup_jwt(user, minutes=15)
        return jsonify(
            mfaSetupRequired=True,
            token=setup_token,
            role=user.role,
            user={"id": user.id, "username": user.username, "email": user.email},
        ), 200

    if role_needs_mfa or has_totp:
        ch = _new_challenge(user, ttl_seconds=300)
        payload = dict(
            mfaRequired=True,
            mfaMethods=["totp"],
            role=user.role,
            user={"id": user.id, "username": user.username, "email": user.email},
        )
        if ch:
            payload["challengeId"] = ch.id
        else:
            payload["username"] = user.username
        return jsonify(payload), 200

    token = issue_app_jwt(user)
    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=user.assigned_bus_id,
        user={
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "phoneNumber": user.phone_number,
            "address": getattr(user, "address", None),
        },
    ), 200


@auth_bp.post("/mfa/totp/enroll")
def totp_enroll():
    current_user, error = _require_user_any_scope({"app", "mfa_setup"})
    if error:
        return error
    if not pyotp:
        return jsonify(error="pyotp not installed"), 500

    secret = base64.b32encode(secrets.token_bytes(20)).decode("utf-8").replace("=", "")
    current_user.totp_secret = secret
    current_user.mfa_enabled = False
    db.session.commit()

    issuer, label = _issuer_and_label_for(current_user)
    otpauth = pyotp.totp.TOTP(secret).provisioning_uri(name=label, issuer_name=issuer)
    return jsonify(secret=secret, otpauth=otpauth), 200

@auth_bp.post("/mfa/totp/activate")
def totp_activate():
    """User enters a current TOTP from their app to enable MFA."""
    current_user, error = _require_user_any_scope({"app", "mfa_setup"})
    if error:
        return error
    if not (pyotp and current_user.totp_secret):
        return jsonify(error="TOTP not prepared"), 400

    code = str((request.get_json() or {}).get("code") or "").strip()
    totp = pyotp.TOTP(current_user.totp_secret)
    if totp.verify(code, valid_window=2):
        current_user.mfa_enabled = True
        db.session.commit()
        return jsonify(message="TOTP enabled"), 200
    return jsonify(error="Invalid code"), 400

@auth_bp.post("/mfa/totp/verify")
def totp_verify():
    """
    Step-two during login.
    Body: { challengeId, code } OR { username, code } (fallback if no challenge table)
    """
    data = request.get_json() or {}
    code = str(data.get("code") or "").strip()
    challenge_id = data.get("challengeId")
    username = (data.get("username") or "").strip()

    user = None
    if challenge_id and MfaChallenge:
        ch = MfaChallenge.query.get(challenge_id)
        if not ch or ch.consumed or ch.expires_at < datetime.utcnow():
            return jsonify(error="Challenge expired"), 400
        user = User.query.get(ch.user_id)
    elif username:
        user = User.query.filter_by(username=username).first()
    else:
        return jsonify(error="Missing challengeId or username"), 400

    if not (user and user.totp_secret and user.mfa_enabled and pyotp):
        return jsonify(error="TOTP not available"), 400

    totp = pyotp.TOTP(user.totp_secret)
    if not totp.verify(code, valid_window=2):
        return jsonify(error="Invalid code"), 400

    if challenge_id and MfaChallenge:
        ch.consumed = True
        ch.method = "totp"
        db.session.commit()

    token = issue_app_jwt(user)
    return (
        jsonify(
            token=token,
            role=user.role,
            busId=user.assigned_bus_id,
            user={
                "id": user.id,
                "username": user.username,
                "firstName": user.first_name,
                "lastName": user.last_name,
                "phoneNumber": user.phone_number,
                "email": user.email,
            },
        ),
        200,
    )

# ────────────────────────────── SMS OTP (optional) ──────────────────────────────

@auth_bp.post("/mfa/sms/start")
def sms_start():
    """
    Start SMS OTP for an existing challenge.
    Body: { challengeId, phone? }
    """
    if not MfaChallenge:
        return jsonify(error="MFA challenge storage not available"), 500

    data = request.get_json() or {}
    challenge_id = data.get("challengeId")
    phone = (data.get("phone") or "").strip()

    ch = MfaChallenge.query.get(challenge_id)
    if not ch or ch.consumed or ch.expires_at < datetime.utcnow():
        return jsonify(error="Challenge expired"), 400

    user = User.query.get(ch.user_id)
    if not user:
        return jsonify(error="User not found"), 404

    # Use provided phone or the one on record
    phone = phone or (user.phone_number or "")
    if not phone:
        return jsonify(error="No phone on record"), 400

    # Generate and hash a 6-digit code
    code = f"{secrets.randbelow(1_000_000):06d}"
    ch.code_hash = generate_password_hash(code)
    ch.phone = phone
    ch.expires_at = datetime.utcnow() + timedelta(minutes=5)  # refresh window
    db.session.commit()

    try:
        _send_sms_code(phone, code)
    except Exception as e:
        current_app.logger.error(f"SMS send failed: {e}")
        return jsonify(error="Failed to send SMS"), 500

    return jsonify(sent=True), 200

@auth_bp.post("/mfa/sms/verify")
def sms_verify():
    if not MfaChallenge:
        return jsonify(error="MFA challenge storage not available"), 500

    data = request.get_json() or {}
    challenge_id = data.get("challengeId")
    code = str(data.get("code") or "").strip()

    ch = MfaChallenge.query.get(challenge_id)
    if not ch or ch.consumed or ch.expires_at < datetime.utcnow():
        return jsonify(error="Challenge expired"), 400
    if not ch.code_hash or not code:
        return jsonify(error="No SMS code for this challenge"), 400

    if not check_password_hash(ch.code_hash, code):
        return jsonify(error="Invalid code"), 400

    user = User.query.get(ch.user_id)
    if not user:
        return jsonify(error="User not found"), 404

    ch.consumed = True
    ch.method = "sms"
    db.session.commit()

    token = issue_app_jwt(user)
    return (
        jsonify(
            token=token,
            role=user.role,
            busId=user.assigned_bus_id,
            user={
                "id": user.id,
                "username": user.username,
                "firstName": user.first_name,
                "lastName": user.last_name,
                "phoneNumber": user.phone_number,
                "email": user.email,
            },
        ),
        200,
    )

# ────────────────────────────── Email OTP (optional; kept) ──────────────────────────────

@auth_bp.post("/mfa/email/start")
def email_start():
    """Resend or start email OTP for an existing challenge (body: { challengeId })"""
    if not MfaChallenge:
        return jsonify(error="MFA challenge storage not available"), 500
    data = request.get_json(silent=True) or {}
    challenge_id = data.get("challengeId")

    ch = MfaChallenge.query.get(challenge_id)
    if not ch or ch.consumed or ch.expires_at < datetime.utcnow():
        return jsonify(error="Challenge expired"), 400

    user = User.query.get(ch.user_id)
    if not user or not user.email:
        return jsonify(error="User/email not found"), 404

    code = f"{secrets.randbelow(1_000_000):06d}"
    ch.code_hash = generate_password_hash(code)
    ch.email = user.email
    ch.expires_at = datetime.utcnow() + timedelta(minutes=5)
    db.session.commit()

    try:
        _send_email_code(user.email, code)
    except Exception as e:
        current_app.logger.error(f"Email send failed: {e}")
        return jsonify(error="Failed to send email"), 500

    return jsonify(sent=True, sentTo=_mask_email(user.email)), 200

@auth_bp.post("/mfa/email/verify")
def email_verify():
    """Verify the emailed OTP (body: { challengeId, code }) and issue the app JWT."""
    if not MfaChallenge:
        return jsonify(error="MFA challenge storage not available"), 500

    data = request.get_json(silent=True) or {}
    challenge_id = data.get("challengeId")
    code = str(data.get("code") or "").strip()

    ch = MfaChallenge.query.get(challenge_id)
    if not ch or ch.consumed or ch.expires_at < datetime.utcnow():
        return jsonify(error="Challenge expired"), 400
    if not ch.code_hash or not code:
        return jsonify(error="No email code for this challenge"), 400

    if not check_password_hash(ch.code_hash, code):
        return jsonify(error="Invalid code"), 400

    user = User.query.get(ch.user_id)
    if not user:
        return jsonify(error="User not found"), 404

    # Mark challenge consumed and mark email verified (nice-to-have)
    ch.consumed = True
    ch.method = "email"
    if user.email and not getattr(user, "email_verified", False):
        user.email_verified = True
    db.session.commit()

    token = issue_app_jwt(user)
    return jsonify(
        token=token,
        role=user.role,
        busId=user.assigned_bus_id,
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "LastName": user.last_name,
            "email": user.email,
        },
    ), 200

# ────────────────────────────── Google login (optional; kept) ──────────────────────────────

@auth_bp.post("/google/login")
def google_login():
    """
    Body: { idToken, expoPushToken?, platform? }
    Verifies Google ID token, upserts a commuter user, and (for commuters) sends an email OTP.
    Response:
      - If OTP sent: { mfaRequired: true, mfaMethods:["email"], challengeId, sentTo, user, role }
      - Else (non-commuter or policy change): { token, role, user }
    """
    if not (google_id_token and google_requests):
        return jsonify(error="google-auth not installed"), 500

    data = request.get_json(silent=True) or {}
    id_token_str = (data.get("idToken") or "").strip()
    if not id_token_str:
        return jsonify(error="Missing idToken"), 400

    try:
        req = google_requests.Request()
        idinfo = google_id_token.verify_oauth2_token(id_token_str, req, audience=None)

        aud = idinfo.get("aud")
        iss = str(idinfo.get("iss") or "")
        if _google_client_ids() and aud not in _google_client_ids():
            return jsonify(error="Unrecognized Google client ID"), 401
        if iss not in {"accounts.google.com", "https://accounts.google.com"}:
            return jsonify(error="Invalid issuer"), 401

        email = (idinfo.get("email") or "").lower()
        email_verified = bool(idinfo.get("email_verified"))
        sub = idinfo.get("sub")
        given = (idinfo.get("given_name") or "").strip()
        family = (idinfo.get("family_name") or "").strip()
    except Exception as e:
        current_app.logger.warning(f"Google token verify failed: {e}")
        return jsonify(error="Invalid Google token"), 401

    if not (email and sub):
        return jsonify(error="Google token missing email or sub"), 400

    # Link or create user
    user = User.query.filter((User.google_sub == sub) | (User.email == email)).first()
    if not user:
        # derive username from email (ensure uniqueness)
        base = email.split("@", 1)[0].replace("+", ".").replace(" ", ".").lower()[:30] or f"user{secrets.token_hex(4)}"
        uname = base
        suffix = 1
        while User.query.filter_by(username=uname).first():
            suffix += 1
            uname = f"{base}{suffix}"

        user = User(
            username=uname,
            email=email,
            email_verified=email_verified,
            google_sub=sub,
            first_name=given or None,
            last_name=family or None,
            role="commuter",
            password_hash=generate_password_hash(secrets.token_urlsafe(16)),  # random placeholder
        )
        db.session.add(user)
        db.session.commit()
    else:
        changed = False
        if not user.google_sub:
            user.google_sub = sub; changed = True
        if (user.email or "").lower() != email.lower():
            user.email = email; changed = True
        if email_verified and not getattr(user, "email_verified", False):
            user.email_verified = True; changed = True
        if given and not user.first_name:
            user.first_name = given; changed = True
        if family and not user.last_name:
            user.last_name = family; changed = True
        if changed:
            db.session.commit()

    # Save Expo token if the client sent it
    _save_expo_token_if_present(user, data)

    # For commuters: require email OTP as second factor right after Google
    if (user.role or "").lower() == "commuter":
        if not MfaChallenge:
            return jsonify(error="MFA challenge storage not available"), 500
        ch = _new_challenge(user, ttl_seconds=300)
        if not ch:
            return jsonify(error="Could not create challenge"), 500

        code = f"{secrets.randbelow(1_000_000):06d}"
        ch.code_hash = generate_password_hash(code)
        ch.email = user.email
        ch.expires_at = datetime.utcnow() + timedelta(minutes=5)
        db.session.commit()

        try:
            _send_email_code(user.email, code)
        except Exception as e:
            current_app.logger.error(f"Email send failed: {e}")
            return jsonify(error="Failed to send email"), 500

        return jsonify(
            mfaRequired=True,
            mfaMethods=["email"],
            challengeId=ch.id,
            sentTo=_mask_email(user.email),
            role=user.role,
            user={
                "id": user.id,
                "username": user.username,
                "firstName": user.first_name,
                "lastName": user.last_name,
                "email": user.email,
            },
        ), 200

    # (If some other role uses Google login without OTP)
    token = issue_app_jwt(user)
    return jsonify(
        message="Login successful",
        token=token,
        role=user.role,
        busId=user.assigned_bus_id,
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "email": user.email,
        },
    ), 200

# ─────────────────────────── Auth utils ───────────────────────────

def require_role(*roles):
    """
    Usage:
      @require_role("teller")
      @require_role("teller", "pao")
      @require_role()  -> any authenticated 'app' user
    'admin' is always allowed.
    """
    if len(roles) == 1 and isinstance(roles[0], (list, tuple, set)):
        roles = tuple(roles[0])

    allowed = {str(r).lower() for r in roles if r}

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            current_user, error = _require_app_user()
            if error:
                return error

            g.user = current_user
            user_role = (current_user.role or "").lower()
            if allowed and user_role not in allowed and user_role != "admin":
                return jsonify(error="Insufficient permissions"), 403

            return f(*args, **kwargs)

        return decorated_function

    return decorator

@auth_bp.get("/verify-token")
def verify_token():
    try:
        payload = _decode_token_from_header()
        user = User.query.get(payload["user_id"])
        if not user:
            return jsonify(error="User not found"), 401
        return (
            jsonify(valid=True, user={"id": user.id, "username": user.username, "role": user.role}),
            200,
        )
    except jwt.ExpiredSignatureError:
        return jsonify(error="Token has expired"), 401
    except jwt.InvalidTokenError:
        return jsonify(error="Invalid token"), 401

@auth_bp.post("/reset-password")
def reset_password_by_username_phone():
    """
    Reset password for a commuter by matching username + phoneNumber.
    Body: { "username": "...", "phoneNumber": "...", "newPassword": "..." }
    """
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    phone = (data.get("phoneNumber") or "").strip()
    new_pw = (data.get("newPassword") or "").strip()

    if not username or not phone or not new_pw:
        return jsonify(error="username, phoneNumber and newPassword are required"), 400
    if len(new_pw) < 6:
        return jsonify(error="newPassword must be at least 6 characters"), 400

    user = User.query.filter_by(username=username, phone_number=phone, role="commuter").first()
    if not user:
        return jsonify(error="Username and phone number do not match"), 404

    user.set_password(new_pw)
    db.session.commit()

    return jsonify(message="Password updated successfully"), 200


@auth_bp.post("/email/signup-start")
def email_signup_start():
    """
    Body: {
      firstName, lastName, phoneNumber, address, googleIdToken
    }
    - Verifies Google ID token (gets email)
    - Upserts/creates commuter with provided profile fields
    - Starts email OTP challenge and sends code
    Response: { mfaRequired:true, mfaMethods:["email"], challengeId, sentTo, role, user }
    """
    if not (google_id_token and google_requests):
        return jsonify(error="google-auth not installed"), 500

    data = request.get_json(silent=True) or {}
    first = (data.get("firstName") or "").strip()
    last  = (data.get("lastName") or "").strip()
    phone = (data.get("phoneNumber") or "").strip()
    addr  = (data.get("address") or "").strip()
    idtok = (data.get("googleIdToken") or "").strip()

    if not first or not last or not phone or not addr or not idtok:
        return jsonify(error="Missing fields"), 400
    if addr not in ADDRESS_ALLOWED:
        return jsonify(error="Invalid address"), 400

    # Verify Google token to lock email
    try:
        req = google_requests.Request()
        idinfo = google_id_token.verify_oauth2_token(idtok, req, audience=None)
        aud = idinfo.get("aud"); iss = str(idinfo.get("iss") or "")
        if _google_client_ids() and aud not in _google_client_ids():
            return jsonify(error="Unrecognized Google client ID"), 401
        if iss not in {"accounts.google.com", "https://accounts.google.com"}:
            return jsonify(error="Invalid issuer"), 401
        email = (idinfo.get("email") or "").lower()
        email_verified = bool(idinfo.get("email_verified"))
        sub = idinfo.get("sub")
    except Exception as e:
        current_app.logger.warning(f"Google token verify failed: {e}")
        return jsonify(error="Invalid Google token"), 401

    if not (email and sub):
        return jsonify(error="Google token missing email or sub"), 400

    # Upsert commuter
    user = User.query.filter((User.google_sub == sub) | (User.email == email)).first()
    if not user:
        base = email.split("@", 1)[0].replace("+",".").replace(" ",".").lower()[:30] or f"user{secrets.token_hex(4)}"
        uname, i = base, 1
        while User.query.filter_by(username=uname).first():
            i += 1; uname = f"{base}{i}"
        user = User(
            username=uname,
            email=email,
            email_verified=email_verified,
            google_sub=sub,
            first_name=first,
            last_name=last,
            phone_number=phone,
            address=addr,
            role="commuter",
            password_hash=generate_password_hash(secrets.token_urlsafe(16)),
        )
        db.session.add(user)
        db.session.commit()
    else:
        changed = False
        if not user.google_sub: user.google_sub = sub; changed = True
        if (user.email or "").lower() != email: user.email = email; changed = True
        if email_verified and not getattr(user, "email_verified", False): user.email_verified = True; changed = True
        if first and user.first_name != first: user.first_name = first; changed = True
        if last and user.last_name != last: user.last_name = last; changed = True
        if phone and user.phone_number != phone: user.phone_number = phone; changed = True
        if addr and user.address != addr: user.address = addr; changed = True
        if changed: db.session.commit()

    # Create challenge, email code
    if not MfaChallenge:
        return jsonify(error="MFA challenge storage not available"), 500
    ch = _new_challenge(user, ttl_seconds=300)
    if not ch:
        return jsonify(error="Could not create challenge"), 500

    code = f"{secrets.randbelow(1_000_000):06d}"
    ch.code_hash = generate_password_hash(code)
    ch.email = user.email
    ch.expires_at = datetime.utcnow() + timedelta(minutes=5)
    db.session.commit()

    try:
        _send_email_code(user.email, code)   # Make sure SMTP_* env vars are set in prod
    except Exception as e:
        current_app.logger.error(f"Email send failed: {e}")
        return jsonify(error="Failed to send email"), 500

    return jsonify(
        mfaRequired=True,
        mfaMethods=["email"],
        challengeId=ch.id,
        sentTo=_mask_email(user.email),
        role=user.role,
        user={
            "id": user.id,
            "username": user.username,
            "firstName": user.first_name,
            "lastName": user.last_name,
            "email": user.email,
            "address": user.address,
            "phoneNumber": user.phone_number,
        },
    ), 200