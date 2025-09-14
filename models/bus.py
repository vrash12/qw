from __future__ import annotations
from db import db

class Bus(db.Model):
    __tablename__ = "buses"

    id          = db.Column(db.Integer, primary_key=True)
    identifier  = db.Column(db.String(64), nullable=False, unique=True)
    capacity    = db.Column(db.Integer, nullable=True)
    description = db.Column(db.String(128), nullable=True)

    # Inverse of User.assigned_bus
    users = db.relationship(
        "User",
        back_populates="assigned_bus",
        foreign_keys="User.assigned_bus_id",
        cascade="save-update",
    )

    trips = db.relationship("Trip", back_populates="bus", cascade="all, delete-orphan")
    sensor_readings = db.relationship("SensorReading", back_populates="bus", cascade="all, delete-orphan")
    tickets = db.relationship("TicketSale", back_populates="bus", cascade="all, delete-orphan")
