# backend/main.py
import os
from flask import Flask, jsonify
from config import Config
from db import db, migrate
from werkzeug.exceptions import HTTPException

# import your models so flask-migrate sees them
from models.user          import User
from models.bus           import Bus
from models.ticket_sale   import TicketSale
from models.device_token  import DeviceToken   # <-- new

# import your blueprints
from routes.auth          import auth_bp
from routes.commuter      import commuter_bp
from routes.pao           import pao_bp
from routes.manager       import manager_bp

from routes.tickets_static import tickets_bp

# util (optional import, not strictly necessary here)
from utils.push            import send_push, push_to_bus

# start your MQTT ingest in the background
from mqtt_ingest import start_in_background


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)

    with app.app_context():
        # ensure all models are registered
        # (Flask-Migrate will pick them up on next `flask db migrate`)
        _ = (User, Bus, TicketSale, DeviceToken)

        # start the MQTT listener
        start_in_background()

    # global error handler
    @app.errorhandler(Exception)
    def handle_any_error(e):
        if isinstance(e, HTTPException):
            return jsonify(error=e.description), e.code
        return jsonify(error=str(e)), 500

    # register blueprints
    app.register_blueprint(auth_bp,           url_prefix='/auth')
    app.register_blueprint(commuter_bp,       url_prefix='/commuter')
    app.register_blueprint(pao_bp,            url_prefix='/pao')
    app.register_blueprint(manager_bp,        url_prefix='/manager')
    app.register_blueprint(tickets_bp,        url_prefix='/tickets')

    return app


if __name__ == '__main__':
    # Use PORT env var if set (for containerized deployments)
    port = int(os.environ.get('PORT', 5000))
    create_app().run(host='0.0.0.0', port=port, debug=True)
