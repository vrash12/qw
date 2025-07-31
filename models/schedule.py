# backend/models/schedule.py

from db import db

class Trip(db.Model):
    __tablename__ = 'trips'

    id           = db.Column(db.Integer,   primary_key=True)
    service_date = db.Column(db.Date,      nullable=False, index=True)
    bus_id       = db.Column(db.Integer,   db.ForeignKey('buses.id'), nullable=True)
    number       = db.Column(db.String(32), nullable=False)
    start_time   = db.Column(db.Time,      nullable=False)
    end_time     = db.Column(db.Time,      nullable=False)

    # ← add this back so Trip.fare_segments exists:
    fare_segments = db.relationship(
        'FareSegment',
        back_populates='trip',
        cascade='all, delete-orphan'
    )

    # relationship to Bus
    bus         = db.relationship('Bus', back_populates='trips')

    # relationship to StopTime
    stop_times  = db.relationship(
        'StopTime',
        back_populates='trip',
        order_by='StopTime.seq',
        cascade='all, delete-orphan'
    )


class StopTime(db.Model):
    __tablename__ = 'stop_times'

    id           = db.Column(db.Integer, primary_key=True)
    trip_id      = db.Column(db.Integer, db.ForeignKey('trips.id'), nullable=False)
    seq          = db.Column(db.Integer, nullable=False)
    stop_name    = db.Column(db.String(128), nullable=False)
    arrive_time  = db.Column(db.Time, nullable=False)
    depart_time  = db.Column(db.Time, nullable=False)

    # link back to Trip
    trip = db.relationship('Trip', back_populates='stop_times')
