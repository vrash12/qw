# backend/app.py
import os
from flask import Flask, jsonify, request
from config import Config
from db import db, migrate
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
# import your models so flask-migrate sees them
from models.user          import User
from models.bus           import Bus
from models.ticket_sale   import TicketSale
from models.device_token  import DeviceToken   # keep even if you move to topics
# import your blueprints
from routes.auth          import auth_bp
from routes.commuter      import commuter_bp
from routes.pao           import pao_bp
from routes.manager       import manager_bp
from models.wallet        import WalletAccount, WalletLedger, TopUp
from routes.teller        import teller_bp
from routes.tickets_static import tickets_bp
from realtime import socketio
from flask_cors import CORS
from mqtt_ingest import start_in_background
from sqlalchemy import event
from tasks.snap_trips import snap_finished_trips

# ---- Firebase Admin (server-side) ----
import firebase_admin
from firebase_admin import credentials, messaging

def _init_firebase_admin():
    """Initialize Firebase Admin using a service account JSON path from env."""
    if firebase_admin._apps:
        return
    sa_path = os.environ.get("FIREBASE_SA_PATH")
    if not sa_path or not os.path.exists(sa_path):
        # Don't crash the app; endpoints will return a helpful error
        print("[firebase] WARNING: FIREBASE_SA_PATH not set or file missing")
        return
    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)
    print("[firebase] Admin SDK initialized")


def create_app():
    app = Flask(__name__)

    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)
    app.config['PREFERRED_URL_SCHEME'] = 'https'
    CORS(app, resources={r"/*": {"origins": "*"}})
    app.config.from_object(Config)

    db.init_app(app)
    migrate.init_app(app, db)

    with app.app_context():
        @event.listens_for(db.engine, "connect")
        def _set_manila_timezone(dbapi_conn, _):
            cur = dbapi_conn.cursor()
            try:
                cur.execute("SET time_zone = '+08:00'")
            finally:
                cur.close()

        _ = (User, Bus, TicketSale, DeviceToken)

        # 🚫 Do NOT auto-start MQTT in web workers
        # start_in_background()

    # ---- Health ----
    @app.route("/")
    def health_check():
        return jsonify(status="ok"), 200

    # ---- Global error handler ----
    @app.errorhandler(Exception)
    def handle_any_error(e):
        if isinstance(e, HTTPException):
            return jsonify(error=e.description), e.code
        return jsonify(error=str(e)), 500

    # ---- Blueprints ----
    app.register_blueprint(auth_bp)
    app.register_blueprint(commuter_bp, url_prefix="/commuter")
    app.register_blueprint(pao_bp,      url_prefix="/pao")
    app.register_blueprint(manager_bp,  url_prefix="/manager")
    app.register_blueprint(tickets_bp,  url_prefix="/tickets")
    app.register_blueprint(teller_bp,   url_prefix="/teller")


    # ---- CLI ----
    @app.cli.command("snap-trips")
    def snap_trips_cmd():
        """Compute and store metrics for finished trips."""
        snap_finished_trips()
        print("Trip snapshots complete.")

    # ---- Socket.io ----
    socketio.init_app(app, cors_allowed_origins="*")

    # ---- Firebase topics endpoints ----
    _init_firebase_admin()

    def _firebase_ready():
        return bool(firebase_admin._apps)

    @app.post("/push/subscribe-topics")
    def push_subscribe_topics():
        """
        Body: { "token": "<fcm-token>", "topics": ["announcements", "bus-12"] }
        Subscribes the given token to the provided topics.
        """
        if not _firebase_ready():
            return jsonify(ok=False, error="Firebase Admin not initialized on server"), 500

        j = request.get_json(force=True, silent=True) or {}
        token = j.get("token")
        topics = j.get("topics") or []
        if not token or not topics:
            return jsonify(ok=False, error="token/topics required"), 400

        # Simple sanitization: topics must be non-empty strings without spaces
        clean_topics = []
        for t in topics:
            if not isinstance(t, str):
                continue
            t = t.strip()
            if not t or " " in t:
                continue
            clean_topics.append(t)

        if not clean_topics:
            return jsonify(ok=False, error="no valid topics"), 400

        # Subscribe per topic (idempotent; Firebase deduplicates)
        for t in clean_topics:
            try:
                messaging.subscribe_to_topic([token], t)
            except Exception as e:
                return jsonify(ok=False, error=f"subscribe failed for {t}: {e}"), 500

        return jsonify(ok=True)

    @app.post("/push/test")
    def push_test():
        """
        Body: { "topic": "announcements", "title": "Test", "body": "Hello", "data": { ... } }
        Sends a test message to a topic.
        """
        if not _firebase_ready():
            return jsonify(ok=False, error="Firebase Admin not initialized on server"), 500

        j = request.get_json(force=True, silent=True) or {}
        topic = j.get("topic") or "announcements"
        title = j.get("title") or "Test"
        body  = j.get("body")  or "Hello from server"
        data  = j.get("data")  or {}

        try:
            msg = messaging.Message(
                topic=topic,
                notification=messaging.Notification(title=title, body=body),
                data={k: str(v) for k, v in data.items()},
                android=messaging.AndroidConfig(
                    priority="high",
                    notification=messaging.AndroidNotification(
                        channel_id="announcements"  # must exist on the app
                    ),
                ),
            )
            mid = messaging.send(msg)
            return jsonify(ok=True, message_id=mid)
        except Exception as e:
            return jsonify(ok=False, error=str(e)), 500

    return app
