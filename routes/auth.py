#backend/routes/auth.py
from flask import Blueprint, request, jsonify, g
from models.user import User
from db import db
from functools import wraps
from datetime import datetime, timedelta
import jwt
import os


auth_bp = Blueprint('auth', __name__)

# Secret key for JWT - in production, use environment variable
SECRET_KEY = os.environ.get('SECRET_KEY', 'your-secret-key-here')

@auth_bp.route('/signup', methods=['POST'])
def signup():
    data = request.json
    required_fields = ['firstName', 'lastName', 'username', 'phoneNumber', 'password']
    if not all(field in data for field in required_fields):
        return jsonify({'error': 'Missing fields'}), 400

    # Check if username or phone number already exists
    existing_user = User.query.filter(
        (User.username == data['username']) | (User.phone_number == data['phoneNumber'])
    ).first()
    if existing_user:
        return jsonify({'error': 'Username or phone number already exists'}), 409

    # Create a new user
    user = User(
        first_name=data['firstName'],
        last_name=data['lastName'],
        username=data['username'],
        phone_number=data['phoneNumber'],
        role='commuter'  # Default role for signup
    )
    user.set_password(data['password'])  # Hash the password
    db.session.add(user)
    db.session.commit()

    return jsonify({'message': 'User registered successfully'}), 201
@auth_bp.route('/login', methods=['POST'])
def login():
    # 1. Grab the JSON payload
    data = request.get_json()
    print("🔍 DEBUG incoming JSON:", data)

    # 2. Check for required fields
    if not data or 'username' not in data or 'password' not in data:
        print("🔍 DEBUG missing username or password fields")
        return jsonify({'error': 'Missing username or password'}), 400

    # 3. Lookup user in DB
    user = User.query.filter_by(username=data['username']).first()
    print("🔍 DEBUG found user object:", user)

    # 4. If found, inspect stored hash and run check_password
    password_ok = False
    if user:
        print("🔍 DEBUG stored password_hash:", user.password_hash)
        password_ok = user.check_password(data['password'])
        print("🔍 DEBUG check_password result:", password_ok)
    else:
        print("🔍 DEBUG no user with that username")

    # 5. If credentials good, issue JWT
    if user and password_ok:
        token = jwt.encode({
            'user_id': user.id,
            'username': user.username,
            'role': user.role,
            'exp': datetime.utcnow() + timedelta(hours=24)
        }, SECRET_KEY, algorithm='HS256')
        print("🔍 DEBUG login successful, issuing token")

        return jsonify({
            'message': 'Login successful',
            'token': token,
            'role': user.role,
            'busId': user.assigned_bus_id,
            'user': {
                'id': user.id,
                'username': user.username,
                'firstName': user.first_name,
                'lastName': user.last_name,
                'phoneNumber': user.phone_number
            }
        }), 200

    # 6. Fallback to invalid credentials
    print("🔍 DEBUG falling through to invalid creds response")
    return jsonify({'error': 'Invalid username or password'}), 401
def require_role(role):
    """
    A decorator to protect routes, ensuring the user has a valid token
    and the specified role.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            token = None
            print(f"[DEBUG][auth] Incoming headers: {dict(request.headers)}")
            if 'Authorization' in request.headers:
                auth_header = request.headers['Authorization']
                print(f"[DEBUG][auth] Found Authorization header: {auth_header}")
                try:
                    token = auth_header.split(" ")[1]
                    print(f"[DEBUG][auth] Extracted token: {token}")
                except IndexError:
                    return jsonify(error="Malformed Authorization header"), 401

            if not token:
                return jsonify(error="Missing token"), 401

            try:
                # ✅ FIX: Use the SAME 'SECRET_KEY' from the top of the file
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                
                user_id = payload['user_id']
                current_user = User.query.get(user_id)
                if not current_user:
                    return jsonify(error="User not found"), 401

                # CRITICAL STEP: Attach the user to the global 'g' object
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



# Optional: Add a route to verify tokens
@auth_bp.route('/verify-token', methods=['GET'])
def verify_token():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return jsonify({"error": "No token provided"}), 401

    token = auth_header.split(' ')[1]
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=['HS256'])
        user = User.query.get(payload['user_id'])
        
        if not user:
            return jsonify({"error": "User not found"}), 401
            
        return jsonify({
            "valid": True,
            "user": {
                "id": user.id,
                "username": user.username,
                "role": user.role
            }
        }), 200
        
    except jwt.ExpiredSignatureError:
        return jsonify({"error": "Token has expired"}), 401
    except jwt.InvalidTokenError:
        return jsonify({"error": "Invalid token"}), 401