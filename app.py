# backend/app.py
from __future__ import annotations

import os
from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS
from sqlalchemy import event

from config import Config
from db import db, migrate
from realtime import socketio

# Ensure models are imported so Flask-Migrate sees them
from models.user import User
from models.bus import Bus
from models.ticket_sale import TicketSale
from models.wallet import WalletAccount, WalletLedger, TopUp

# Blueprints
from routes.auth import auth_bp
from routes.commuter import commuter_bp
from routes.pao import pao_bp
from routes.manager import manager_bp
from routes.teller import teller_bp
from routes.tickets_static import tickets_bp

# Background tasks / CLI
from tasks.snap_trips import snap_finished_trips
# from mqtt_ingest import start_in_background  # keep disabled in web workers


def create_app() -> Flask:
    app = Flask(__name__)

    # Respect reverse proxy headers (scheme / host) for correct URL generation
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)  # type: ignore[arg-type]
    app.config["PREFERRED_URL_SCHEME"] = "https"

    # CORS (open for now; tighten origins for production)
    CORS(app, resources={r"/*": {"origins": "*"}})

    # Load config + init extensions
    app.config.from_object(Config)
    db.init_app(app)
    migrate.init_app(app, db)
    socketio.init_app(app, cors_allowed_origins="*")

    # DB connection tweaks (set timezone to Asia/Manila, a.k.a. +08:00)
    with app.app_context():
        @event.listens_for(db.engine, "connect")
        def _set_manila_timezone(dbapi_conn, _):
            cur = dbapi_conn.cursor()
            try:
                cur.execute("SET time_zone = '+08:00'")
            finally:
                cur.close()

        # Touch models so Alembic/Flask-Migrate registers them
        _ = (User, Bus, TicketSale, WalletAccount, WalletLedger, TopUp)

        # 🚫 Do NOT auto-start MQTT inside web workers / gunicorn
        # start_in_background()

    # Health check
    @app.route("/")
    def health_check():
        return jsonify(status="ok"), 200

    # Global error handler (keep routes free from giant try/except blocks)
    @app.errorhandler(Exception)
    def handle_any_error(e: Exception):
        if isinstance(e, HTTPException):
            return jsonify(error=e.description), e.code
        # Don’t leak internals in prod; this mirrors your earlier behavior
        return jsonify(error=str(e)), 500

    # Register blueprints
    app.register_blueprint(auth_bp)
    app.register_blueprint(commuter_bp, url_prefix="/commuter")
    app.register_blueprint(pao_bp,      url_prefix="/pao")
    app.register_blueprint(manager_bp,  url_prefix="/manager")
    app.register_blueprint(tickets_bp,  url_prefix="/tickets")
    app.register_blueprint(teller_bp,   url_prefix="/teller")

    # CLI: compute and store metrics for finished trips
    @app.cli.command("snap-trips")
    def snap_trips_cmd():
        snap_finished_trips()
        print("Trip snapshots complete.")

    return app


# Optional local entrypoint (useful for quick dev runs)
if __name__ == "__main__":
    app = create_app()
    # Socket.IO server (falls back to Werkzeug in dev)
    socketio.run(
        app,
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "5000")),
        allow_unsafe_werkzeug=True,  # dev convenience
        debug=bool(os.environ.get("FLASK_DEBUG", "")),
    )
