from db import db
from models.schedule       import Trip
from models.sensor_reading import SensorReading

class Bus(db.Model):
    __tablename__ = 'buses'

    id           = db.Column(db.Integer, primary_key=True)
    identifier   = db.Column(db.String(64), nullable=False, unique=True)
    capacity     = db.Column(db.Integer, nullable=True)    # e.g. max seating
    description  = db.Column(db.String(128), nullable=True)
    pao = db.relationship("User", back_populates="assigned_bus", uselist=False)
    # one‐to‐many: a bus can run many trips
    trips = db.relationship(
        'Trip',
        back_populates='bus',
        cascade='all, delete-orphan'
    )

    # one‐to‐many: a bus generates many sensor readings
    sensor_readings = db.relationship(
        'SensorReading',
        back_populates='bus',
        cascade='all, delete-orphan'
    )

    tickets = db.relationship(
    "TicketSale",
    back_populates="bus",
    cascade="all, delete-orphan",
    )
