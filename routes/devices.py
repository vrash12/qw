# backend/routes/devices.py
from flask import Blueprint, request, jsonify, g, current_app
from models.device_token import DeviceToken
from db import db
from .auth import require_role
from sqlalchemy.exc import IntegrityError
from push import send_expo_push_sync

devices_bp = Blueprint("devices", __name__, url_prefix="/devices")

@devices_bp.route("/register", methods=["POST"])
@require_role()  # any signed-in user
def register():
    data = request.get_json() or {}
    token = (data.get("expoPushToken") or "").strip()
    platform = (data.get("platform") or "").strip()
    user_id = g.user.id

    current_app.logger.info(f"/devices/register user={user_id} token_prefix={token[:18]}")

    if not token:
        return jsonify(error="expoPushToken required"), 400

    try:
        rec = DeviceToken.query.filter_by(token=token).first()
        if rec:
            rec.user_id = user_id
            rec.platform = platform or rec.platform
        else:
            db.session.add(DeviceToken(user_id=user_id, token=token, platform=platform))
        db.session.commit()
        return jsonify(ok=True), 200
    except IntegrityError:
        db.session.rollback()
        return jsonify(ok=True, note="token already recorded"), 200
