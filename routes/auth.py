# backend/routes/auth.py
from __future__ import annotations
from flask import Blueprint, request, jsonify, g, current_app
from models.user import User
from db import db
from functools import wraps
from datetime import datetime, timedelta
import jwt
import os
from sqlalchemy.exc import OperationalError
from models.device_token import DeviceToken

auth_bp = Blueprint('auth', __name__, url_prefix='/auth')


import time
from sqlalchemy.orm import load_only

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
        u = User.query.get(payload.get("user_id"))
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

    existing = User.query.filter(
        (User.username == data['username']) | (User.phone_number == data['phoneNumber'])
    ).first()
    if existing:
        return jsonify(error='Username or phone number already exists'), 409

    user = User(
        first_name=data['firstName'].strip(),
        last_name=data['lastName'].strip(),
        username=data['username'].strip(),
        phone_number=data['phoneNumber'].strip(),
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
        return (User.query
                .options(load_only(
                    User.id, User.username, User.role,
                    User.first_name, User.last_name,
                    User.assigned_bus_id, User.password_hash  # whatever your check uses
                ))
                .filter_by(username=data['username'])
                .first())
    try:
        user = _get_user()
    except OperationalError as e:
        current_app.logger.warning("DB connection dropped; retrying once… %s", e)
        db.session.remove()
        db.engine.dispose()
        user = _get_user()

    password_ok = bool(user and user.check_password(data['password']))

    if user and password_ok:
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
        expo_token = (data.get('expoPushToken') or '').strip()
        platform   = (data.get('platform') or '').strip()
        if expo_token:
            rec = DeviceToken.query.filter_by(token=expo_token).first()
            if rec:
                rec.user_id = user.id
                rec.platform = platform or rec.platform
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

    return jsonify(error='Invalid username or password'), 401

def require_role(*roles):
    """
    Usage:
      @require_role("teller")                 -> only teller
      @require_role("teller", "pao")          -> either teller or pao
      @require_role()                         -> any authenticated user
    An 'admin' user is always allowed.
    """
    # Support a single iterable too: @require_role(["teller","pao"])
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
                current_user = User.query.get(user_id)
                if not current_user:
                    return jsonify(error="User not found"), 401

                g.user = current_user

                user_role = (current_user.role or "").lower()
                # If roles were provided, enforce them; if none were provided, just require auth.
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
        user = User.query.get(payload['user_id'])
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

    # Basic validation
    if not username or not phone or not new_pw:
        return jsonify(error='username, phoneNumber and newPassword are required'), 400
    if len(new_pw) < 6:
        return jsonify(error='newPassword must be at least 6 characters'), 400

    # Require a commuter, and require that username + phoneNumber BOTH match the same user
    user = (
        User.query
        .filter_by(username=username, phone_number=phone, role='commuter')
        .first()
    )

    if not user:
        # Don’t reveal which one is wrong—just say the pair doesn’t match.
        return jsonify(error='Username and phone number do not match'), 404

    # Update password
    user.set_password(new_pw)
    db.session.commit()

    return jsonify(message='Password updated successfully'), 200
