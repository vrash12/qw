# models/ticket_stop.py
from db import db

class TicketStop(db.Model):
    __tablename__ = "ticket_stops"

    id        = db.Column(db.Integer, primary_key=True)
    seq       = db.Column(db.SmallInteger, nullable=False)
    stop_name = db.Column(db.String(128), nullable=False)
