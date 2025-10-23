# backend/auth_guard.py
from __future__ import annotations
import os
import jwt
from functools import wraps
from flask import request, jsonify, g, current_app
from db import db
from models.user import User

SECRET_KEY = os.environ.get("SECRET_KEY", "your-secret-key-here")

def require_role(*roles):
    """
    Usage:
      @require_role()                    -> any authenticated user
      @require_role("pao")               -> only PAO (or admin)
      @require_role("pao", "manager")    -> PAO or Manager (or admin)
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
                uid = payload.get("user_id")
                user = db.session.get(User, uid)
                if not user:
                    return jsonify(error="User not found"), 401

                g.user = user
                role = (user.role or "").lower()

                # Admin bypasses role checks
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
        return decorated_function
    return decorator
