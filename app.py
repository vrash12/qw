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





    return app
