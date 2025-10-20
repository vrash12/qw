# backend/routes/auth.py
from __future__ import annotations
from flask import Blueprint, request, jsonify, g, current_app
from models.user import User
from models.device_token import DeviceToken
from db import db
from functools import wraps
from datetime import datetime, timedelta
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import load_only
import jwt, os, time
import re 


auth_bp = Blueprint('auth', __name__, url_prefix='/auth')

@auth_bp.after_request
def add_perf_headers(resp):
    # Keep connections warm across rapid calls (RN fetch honors keep-alive)
    resp.headers['Connection'] = 'keep-alive'
    # Don’t cache auth responses, but allow intermediates to reuse TCP
    resp.headers['Cache-Control'] = 'no-store'
    return resp

@auth_bp.route('/ping', methods=['GET'])
def ping():
    return jsonify(ok=True, ts=time.time()), 200

SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-here')

@auth_bp.route("/me", methods=["GET"])
def me():
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return jsonify(error="unauthorized"), 401

    token = auth.split(" ", 1)[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        uid = payload.get("user_id")
        # db.session.get is a tad faster and clearer than query.get
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

@auth_bp.route('/signup', methods=['POST'])
def signup():
    data = request.get_json() or {}
    required = ['firstName', 'lastName', 'username', 'phoneNumber', 'password']
    if not all(k in data and str(data[k]).strip() for k in required):
        return jsonify(error='Missing fields'), 400

    # Normalize & validate phone number: must be 11 digits, start with 09 (e.g., 09123456789)
    raw_phone = str(data.get('phoneNumber', ''))
    digits = re.sub(r'\D', '', raw_phone)
    if not re.fullmatch(r'09\d{9}', digits):
        return jsonify(error='phoneNumber must start with 09 and be 11 digits (e.g., 09123456789)'), 400

    # Uniqueness checks use the normalized digits
    existing = User.query.filter(
        (User.username == data['username'].strip()) | (User.phone_number == digits)
    ).first()
    if existing:
        return jsonify(error='Username or phone number already exists'), 409

    user = User(
        first_name=data['firstName'].strip(),
        last_name=data['lastName'].strip(),
        username=data['username'].strip(),
        phone_number=digits,
        role='commuter',
    )
    user.set_password(data['password'])
    db.session.add(user)
    db.session.commit()

    return jsonify(message='User registered successfully'), 201

@auth_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json() or {}
    if 'username' not in data or 'password' not in data:
        return jsonify(error='Missing username or password'), 400

    def _get_user():
        # Pull only what we need for password check + response
        return (User.query
                .options(load_only(
                    User.id, User.username, User.role,
                    User.first_name, User.last_name,
                    User.assigned_bus_id, User.password_hash
                ))
                .filter_by(username=data['username'])
                .first())
    try:
        user = _get_user()
    except OperationalError as e:
        # Fast recovery path if pool dropped
        current_app.logger.warning("DB connection dropped; retrying once… %s", e)
        db.session.remove()
        db.engine.dispose()
        user = _get_user()

    password_ok = bool(user and user.check_password(data['password']))
    if not (user and password_ok):
        return jsonify(error='Invalid username or password'), 401

    token = jwt.encode(
        {
            'user_id': user.id,
            'username': user.username,
            'role': user.role,
            'exp': datetime.utcnow() + timedelta(hours=24),
        },
        SECRET_KEY,
        algorithm='HS256',
    )

    # Register push token (sent inline from the app to avoid a second network call)
    expo_token = (data.get('expoPushToken') or '').strip()
    platform   = (data.get('platform') or '').strip()
    if expo_token:
        rec = DeviceToken.query.filter_by(token=expo_token).first()
        if rec:
            # Only commit if something changed
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
        message='Login successful',
        token=token,
        role=user.role,
        busId=user.assigned_bus_id,
        user={
            'id': user.id,
            'username': user.username,
            'firstName': user.first_name,
            'lastName': user.last_name,
            'phoneNumber': user.phone_number,
        },
    ), 200

def require_role(*roles):
    """
    @require_role("teller")
    @require_role("teller", "pao")
    @require_role()  -> any authenticated user
    """
    if len(roles) == 1 and isinstance(roles[0], (list, tuple, set)):
        roles = tuple(roles[0])
    allowed = {str(r).lower() for r in roles if r}

    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            auth = request.headers.get("Authorization", "")
            if not auth.startswith("Bearer "):
                return jsonify(error="Missing token"), 401

            token = auth.split(" ", 1)[1]
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_id = payload.get("user_id")
                current_user = db.session.get(User, user_id)
                if not current_user:
                    return jsonify(error="User not found"), 401

                g.user = current_user
                user_role = (current_user.role or "").lower()
                if allowed and user_role not in allowed and user_role != "admin":
                    return jsonify(error="Insufficient permissions"), 403

            except jwt.ExpiredSignatureError:
                return jsonify(error="Token has expired"), 401
            except jwt.InvalidTokenError:
                return jsonify(error="Invalid token"), 401
            except Exception as e:
                current_app.logger.error(f"Authentication error: {e}")
                return jsonify(error="Authentication processing error"), 500

            return f(*args, **kwargs)
        return decorated_function
    return decorator

@auth_bp.route('/verify-token', methods=['GET'])
def verify_token():
    auth_header = request.headers.get('Authorization', '')
    if not auth_header.startswith('Bearer '):
        return jsonify(error="No token provided"), 401

    token = auth_header.split(' ')[1]
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user = db.session.get(User, payload['user_id'])
        if not user:
            return jsonify(error="User not found"), 401
        return jsonify(valid=True, user={'id': user.id, 'username': user.username, 'role': user.role}), 200
    except jwt.ExpiredSignatureError:
        return jsonify(error="Token has expired"), 401
    except jwt.InvalidTokenError:
        return jsonify(error="Invalid token"), 401

@auth_bp.route('/reset-password', methods=['POST'])
def reset_password_by_username_phone():
    """
    Reset password for a commuter by matching username + phoneNumber.
    Body: { "username": "...", "phoneNumber": "...", "newPassword": "..." }
    """
    data = request.get_json(silent=True) or {}
    username    = (data.get('username') or '').strip()
    phone       = (data.get('phoneNumber') or '').strip()
    new_pw      = (data.get('newPassword') or '').strip()

    if not username or not phone or not new_pw:
        return jsonify(error='username, phoneNumber and newPassword are required'), 400
    if len(new_pw) < 6:
        return jsonify(error='newPassword must be at least 6 characters'), 400

    user = User.query.filter_by(username=username, phone_number=phone, role='commuter').first()
    if not user:
        return jsonify(error='Username and phone number do not match'), 404

    user.set_password(new_pw)
    db.session.commit()

    return jsonify(message='Password updated successfully'), 200
