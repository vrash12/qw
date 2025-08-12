# backend/routes/auth.py
from flask import Blueprint, request, jsonify, g, current_app
from models.user import User
from db import db
from functools import wraps
from datetime import datetime, timedelta
import jwt
import os
from sqlalchemy.exc import OperationalError

auth_bp = Blueprint('auth', __name__, url_prefix='/api/auth')

SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-here')

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
        return User.query.filter_by(username=data['username']).first()

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


def require_role(role):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            token = None
            if 'Authorization' in request.headers:
                try:
                    token = request.headers['Authorization'].split(" ")[1]
                except IndexError:
                    return jsonify(error="Malformed Authorization header"), 401

            if not token:
                return jsonify(error="Missing token"), 401

            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_id = payload['user_id']
                current_user = User.query.get(user_id)
                if not current_user:
                    return jsonify(error="User not found"), 401

                g.user = current_user
                if current_user.role != role:
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
