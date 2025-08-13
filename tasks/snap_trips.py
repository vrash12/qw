from datetime import datetime, timedelta
from db import db
from models.schedule import Trip
from models.sensor_reading import SensorReading
from models.trip_metric import TripMetric
from sqlalchemy import func

def snap_finished_trips(now=None):
    now = now or datetime.utcnow()
    grace = now - timedelta(minutes=2)

    # trips that ended before 'grace' and have no snapshot
    trips = (
        Trip.query.outerjoin(TripMetric, TripMetric.trip_id == Trip.id)
        .filter(TripMetric.trip_id == None)  # noqa: E711
        .all()
    )

    for t in trips:
        start = datetime.combine(t.service_date, t.start_time)
        end   = datetime.combine(t.service_date, t.end_time)
        if t.end_time <= t.start_time:
            end = end + timedelta(days=1)

        if end > grace:  # not finished long enough
            continue

        end_excl = end + timedelta(minutes=1)
        rows = (
            db.session.query(
                func.date_format(SensorReading.timestamp, "%H:%i").label("hhmm"),
                func.max(SensorReading.total_count).label("pax"),
                func.sum(SensorReading.in_count).label("ins"),
                func.sum(SensorReading.out_count).label("outs"),
            )
            .filter(
                SensorReading.bus_id == t.bus_id,
                SensorReading.timestamp >= start,
                SensorReading.timestamp < end_excl,
            )
            .group_by("hhmm")
            .order_by("hhmm")
            .all()
        )
        if not rows:
            continue

        pax = [int(r.pax or 0) for r in rows]
        ins = sum(int(r.ins or 0) for r in rows)
        outs = sum(int(r.outs or 0) for r in rows)

        snap = TripMetric(
            trip_id=t.id,
            avg_pax=round(sum(pax)/len(pax)),
            peak_pax=max(pax),
            boarded=ins,
            alighted=outs,
            start_pax=pax[0],
            end_pax=pax[-1],
        )
        db.session.add(snap)

    db.session.commit()
