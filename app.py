# backend/app.py
from flask import Flask, jsonify
from config import Config
from db import db, migrate
from werkzeug.exceptions import HTTPException

# import your blueprints
from routes.auth         import auth_bp
from routes.commuter     import commuter_bp
from routes.pao          import pao_bp
from routes.manager      import manager_bp
from routes.ticket_sales import ticket_sales_bp
from routes.tickets_static import tickets_bp
from mqtt_ingest import start_in_background

def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    # initialize extensions
    db.init_app(app)
    migrate.init_app(app, db)

    with app.app_context():
        start_in_background()
    # global error handler
    @app.errorhandler(Exception)
    def handle_any_error(e):
        if isinstance(e, HTTPException):
            return jsonify(error=e.description), e.code
        return jsonify(error=str(e)), 500

    # register all blueprints with their URL prefixes
    app.register_blueprint(auth_bp,         url_prefix='/auth')
    app.register_blueprint(commuter_bp,     url_prefix='/commuter')
    app.register_blueprint(pao_bp,          url_prefix='/pao')
    app.register_blueprint(manager_bp,      url_prefix='/manager')
    app.register_blueprint(ticket_sales_bp, url_prefix='/ticket-sales')
    app.register_blueprint(tickets_bp,      url_prefix='/tickets')

    return app
