from db import db
from datetime import datetime
import uuid
# if you have the class:
from models.ticket_stop import TicketStop  # <-- use TicketStop for the fare grid

class TicketSale(db.Model):
    __tablename__ = 'ticket_sales'

    id          = db.Column(db.Integer, primary_key=True)

    # points to the *commuter* who owns the ticket
    user_id     = db.Column(db.Integer, db.ForeignKey('users.id', ondelete='CASCADE'), nullable=False)

    fare_segment_id = db.Column(db.Integer,
                                db.ForeignKey('fare_segments.id', ondelete='CASCADE'),
                                nullable=True)

    price       = db.Column(db.Numeric(10, 2), nullable=False)
    ticket_uuid = db.Column(db.String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    created_at  = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    reference_no   = db.Column(db.String(16), unique=True, nullable=False)
    passenger_type = db.Column(db.Enum('regular','discount'), nullable=False, server_default='regular')
    paid           = db.Column(db.Boolean, nullable=False, server_default=db.text('0'))
    voided         = db.Column(db.Boolean, nullable=False, server_default=db.text('0'))
    void_reason    = db.Column(db.String(120))

    bus_id    = db.Column(db.Integer, db.ForeignKey("buses.id"), nullable=False)

    # points to the *PAO* user who issued the ticket
    issued_by = db.Column(db.Integer, db.ForeignKey("users.id"), index=True, nullable=True)

    # ---- relationships (disambiguated) ----
    user   = db.relationship('User', foreign_keys=[user_id], back_populates='ticket_sales')
    issuer = db.relationship('User', foreign_keys=[issued_by], back_populates='issued_tickets')

    bus = db.relationship("Bus", back_populates="tickets")

    # Use TicketStop to match your MySQL FKs (fk_ticket_origin_tstop / fk_ticket_destination_tstop)
    origin_stop_time_id      = db.Column(db.Integer, db.ForeignKey('ticket_stops.id', ondelete='SET NULL'), nullable=True)
    destination_stop_time_id = db.Column(db.Integer, db.ForeignKey('ticket_stops.id', ondelete='SET NULL'), nullable=True)

    origin_stop_time      = db.relationship('TicketStop', foreign_keys=[origin_stop_time_id])
    destination_stop_time = db.relationship('TicketStop', foreign_keys=[destination_stop_time_id])
