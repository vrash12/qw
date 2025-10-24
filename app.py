# backend/app.py
from __future__ import annotations

import os
import json
from flask import Flask, jsonify
from werkzeug.exceptions import HTTPException
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_cors import CORS
from sqlalchemy import event, func, text

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

# MQTT ingest (we’ll patch some bits for ID compatibility and table name)
try:
    import mqtt_ingest
    from mqtt_ingest import start_in_background as _start_mqtt_ingest
except Exception:
    mqtt_ingest = None
    _start_mqtt_ingest = None


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

        # ───────────────────────────────────────────────────────────────
        # MQTT INGEST START + PATCHES (so "2" works the same as "bus-02")
        # ───────────────────────────────────────────────────────────────
        if mqtt_ingest and _start_mqtt_ingest:
            # 1) Patch device-id → bus resolution to be flexible:
            #    accepts "2", "bus-2", "bus-02", "BUS_02", etc., and also matches Bus.id numerically.
            def _flex_bus_by_device(sess, topic: str):
                try:
                    _, raw_id, _ = topic.split("/", 2)  # device/<device_id>/...
                except ValueError:
                    return None, None

                raw = (raw_id or "").strip()
                # Build candidate identifiers
                cands = [raw.lower()]
                # Pull out the first 1–3 digit run
                import re
                m = re.search(r"(\d{1,3})", raw)
                if m:
                    n = int(m.group(1))
                    cands.append(str(n))                 # "2"
                    cands.append(f"bus-{n:02d}")        # "bus-02"
                    cands.append(f"bus-{n}")            # "bus-2"

                # Try identifier equality (case-insensitive) with candidates
                for ident in cands:
                    bus_row = (
                        sess.query(mqtt_ingest.Bus)
                        .filter(func.lower(mqtt_ingest.Bus.identifier) == ident.lower())
                        .first()
                    )
                    if bus_row:
                        return bus_row, raw_id

                # Last resort: numeric match on Bus.id
                if m:
                    n = int(m.group(1))
                    bus_row = (
                        sess.query(mqtt_ingest.Bus)
                        .filter(mqtt_ingest.Bus.id == n)
                        .first()
                    )
                    if bus_row:
                        return bus_row, raw_id

                return None, raw_id

            # Monkey-patch the lookup used by the ingest
            try:
                mqtt_ingest._bus_by_device = _flex_bus_by_device  # type: ignore[attr-defined]
                app.logger.info("[app] Patched mqtt_ingest._bus_by_device for flexible IDs")
            except Exception:
                app.logger.exception("[app] Failed to patch _bus_by_device")

            # 2) Patch handle_people to fallback to plural table name if needed
            def _handle_people_compat(topic: str, payload_raw: str):
                from sqlalchemy import text as _text
                sess = mqtt_ingest.Session()  # use the ingest’s engine/session
                try:
                    p = json.loads(payload_raw or "{}")
                    bus, device_id = mqtt_ingest._bus_by_device(sess, topic)  # use patched lookup
                    if not bus:
                        app.logger.error("[ingest] No bus match for topic=%s (device=%s)", topic, device_id)
                        return

                    total = int(p.get("total", 0) or 0)

                    # de-dup per bus
                    if mqtt_ingest._last_totals.get(bus.id) == total:
                        return
                    mqtt_ingest._last_totals[bus.id] = total

                    params = dict(
                        in_c=int(p.get("in", 0) or 0),
                        out_c=int(p.get("out", 0) or 0),
                        tot=total,
                        bus_id=bus.id,
                        ts=mqtt_ingest.now_ph(),
                    )

                    # Try singular first
                    try:
                        sess.execute(_text("""
                            INSERT INTO sensor_reading
                              (in_count, out_count, total_count, bus_id, timestamp)
                            VALUES
                              (:in_c, :out_c, :tot, :bus_id, :ts)
                        """), params)
                        sess.commit()
                        return
                    except Exception:
                        sess.rollback()

                    # Fallback to plural table name
                    try:
                        sess.execute(_text("""
                            INSERT INTO sensor_readings
                              (in_count, out_count, total_count, bus_id, timestamp)
                            VALUES
                              (:in_c, :out_c, :tot, :bus_id, :ts)
                        """), params)
                        sess.commit()
                        return
                    except Exception:
                        sess.rollback()
                        app.logger.exception("[ingest] people insert failed (both table names). payload=%s", payload_raw)
                except Exception:
                    sess.rollback()
                    app.logger.exception("[ingest] people ingest failed: %s", payload_raw)
                finally:
                    sess.close()

            try:
                mqtt_ingest.handle_people = _handle_people_compat  # type: ignore[attr-defined]
                app.logger.info("[app] Patched mqtt_ingest.handle_people with table fallback")
            except Exception:
                app.logger.exception("[app] Failed to patch handle_people")

            # 3) Start ingest (dev-friendly). Set MQTT_INGEST=0 to disable.
            if os.environ.get("MQTT_INGEST", "1") != "0":
                try:
                    _start_mqtt_ingest()
                    app.logger.info("[app] MQTT ingest started in background")
                except Exception:
                    app.logger.exception("[app] Failed to start MQTT ingest")

    # Health check
    @app.route("/")
    def health_check():
        return jsonify(status="ok"), 200

    from flask import request

    @app.errorhandler(404)
    def handle_404(e):
        return jsonify(error="Not Found", path=request.path), 404

    # Global error handler
    @app.errorhandler(Exception)
    def handle_any_error(e: Exception):
        if isinstance(e, HTTPException):
            return jsonify(error=e.description), e.code
        return jsonify(error=str(e)), 500

    # --- Debug: list routes ---
    @app.route("/__routes")
    def __routes():
        from flask import Response
        lines = []
        for rule in app.url_map.iter_rules():
            methods = ",".join(sorted(m for m in rule.methods if m not in {"HEAD", "OPTIONS"}))
            lines.append(f"{methods:10s} {rule.rule}")
        lines.sort()
        return Response("\n".join(lines), mimetype="text/plain")

    @app.route("/__whoami")
    def __whoami():
        import time
        return jsonify(
            name="pgt-backup backend",
            pid=os.getpid(),
            started_at=int(time.time())
        )

    # Register blueprints
    app.register_blueprint(auth_bp)
    app.register_blueprint(commuter_bp, url_prefix="/commuter")
    app.register_blueprint(pao_bp,      url_prefix="/pao")
    app.register_blueprint(manager_bp)
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
