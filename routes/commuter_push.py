# backend/routes/commuter_push.py
from flask import Blueprint, request, jsonify, g
from routes.auth import require_role
from db import db
from models.device_token import DeviceToken
from utils.push import send_push_async  # ← use your async sender

commuter_push_bp = Blueprint("commuter_push", __name__, url_prefix="/commuter")

@commuter_push_bp.route("/device-token", methods=["POST"])
@require_role("commuter")
def save_commuter_device_token():
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    platform = (data.get("platform") or "").strip() or None
    if not token:
        return jsonify(error="token required"), 400

    row = DeviceToken.query.filter_by(token=token).first()
    if not row:
        row = DeviceToken(user_id=g.user.id, token=token, platform=platform)
        db.session.add(row)
    else:
        row.user_id = g.user.id
        row.platform = platform or row.platform

    db.session.commit()
    return jsonify(ok=True), 201


@commuter_push_bp.route("/push/test", methods=["POST"])
@require_role("commuter")
def commuter_push_test():
    """Send a quick test notification to the currently signed-in commuter."""
    tokens = [t.token for t in DeviceToken.query.filter_by(user_id=g.user.id).all()]
    if not tokens:
        return jsonify(error="no device tokens saved for this user"), 404

    payload = {
        "type": "wallet_topup",
        "amount": 123.45,
        "newBalance": 999.99,
        "deeplink": "/commuter/wallet",
        "sentAt": int(__import__("time").time() * 1000),
    }

    # Channel "payments" matches the Android channel we'll create in the app
    send_push_async(tokens,
                    "🔔 Test notification",
                    "This is a test push to your device.",
                    payload,
                    channelId="payments",
                    priority="high")
    return jsonify(ok=True, sent=len(tokens)), 200
