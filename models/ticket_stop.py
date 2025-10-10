# models/ticket_stop.py
from db import db

class TicketStop(db.Model):
    __tablename__ = "ticket_stops"

    id        = db.Column(db.Integer, primary_key=True)
    bus_id    = db.Column(db.Integer, db.ForeignKey("buses.id"), nullable=False, index=True)
    seq       = db.Column(db.Integer, nullable=False)   # tinyint in MySQL is fine as Integer here
    stop_name = db.Column(db.String(128), nullable=False)
