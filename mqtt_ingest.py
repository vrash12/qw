# mqtt_ingest.py
"""
MQTT ingest service for:
- Passenger counts: device/<device_id>/people
- GPS accuracy tests:
    device/<device_id>/test/sample   (per-fix error, JSON)
    device/<device_id>/test/summary  (mean/RMSE/min/max, JSON, retained)

Run modes:
- Blocking:  python -m mqtt_ingest
- Background (for embedding into another app): import start_in_background()

Assumes MySQL/MariaDB and that you've run the provided SQL to create:
  gps_test, gps_test_sample
"""

import json
import logging
import ssl
import time
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt
from dateutil import parser as dtparse  # pip install python-dateutil
from sqlalchemy import create_engine, func, text, event, select
from sqlalchemy.orm import sessionmaker, scoped_session

from config import Config

# ─── LOGGING ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ─── MQTT CONFIG ──────────────────────────────────────────────────────
MQTT_HOST     = "35010b9ea10d41c0be8ac5e9a700a957.s1.eu.hivemq.cloud"
USE_WS        = True
MQTT_PORT     = 8884 if USE_WS else 8883
MQTT_PATH     = "/mqtt"      # only used when USE_WS=True
MQTT_USER     = "vanrodolf"
MQTT_PASS     = "Vanrodolf123."

TOPIC_PEOPLE        = "device/+/people"
TOPIC_TEST_SAMPLE   = "device/+/test/sample"
TOPIC_TEST_SUMMARY  = "device/+/test/summary"

# ─── DB SETUP ─────────────────────────────────────────────────────────
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)

@event.listens_for(engine, "connect")
def _set_manila_timezone(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    try:
        # store civil Manila time in DATETIME columns
        cur.execute("SET time_zone = '+08:00'")
    finally:
        cur.close()

Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

# Lightweight ORM only for 'bus' lookup (your existing table)
from sqlalchemy.orm import declarative_base, mapped_column
from sqlalchemy import Integer, String

Base = declarative_base()

class Bus(Base):
    __tablename__ = "bus"
    id = mapped_column(Integer, primary_key=True)
    identifier = mapped_column(String(255))

# For writes we use SQL text() so you don't need migrations for models.

# ─── MQTT CLIENT ──────────────────────────────────────────────────────
transport = "websockets" if USE_WS else "tcp"
client = mqtt.Client(client_id="pgt-ingest", transport=transport)
client.username_pw_set(MQTT_USER, MQTT_PASS)

ssl_ctx = ssl.create_default_context()
client.tls_set_context(ssl_ctx)
if USE_WS:
    client.ws_set_options(path=MQTT_PATH)

# ─── CACHES ───────────────────────────────────────────────────────────
_last_totals: dict[int, int] = {}          # bus_id -> last total_count
_current_test_id: dict[tuple[int, str], int] = {}  # (bus_id, label) -> gps_test.id

# ─── HELPERS ──────────────────────────────────────────────────────────
def now_ph():
    # If you prefer UTC in DB, use datetime.now(timezone.utc)
    return datetime.utcnow() + timedelta(hours=8)

def _bus_by_device(sess, topic: str):
    # topic: device/<device_id>/...
    try:
        _, device_id, _ = topic.split("/", 2)
    except ValueError:
        return None, None
    bus = (sess.query(Bus)
           .filter(func.lower(Bus.identifier) == device_id.lower())
           .first())
    return bus, device_id

# ─── HANDLERS ─────────────────────────────────────────────────────────
def handle_people(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        bus, device_id = _bus_by_device(sess, topic)
        if not bus:
            logging.error("No bus found for topic=%s", topic); return

        total = int(p.get("total", 0))
        if _last_totals.get(bus.id) == total:
            return
        _last_totals[bus.id] = total

        stmt = text("""
            INSERT INTO sensor_reading
              (in_count, out_count, total_count, bus_id, timestamp)
            VALUES
              (:in_c, :out_c, :tot, :bus_id, :ts)
        """)
        sess.execute(stmt, dict(
            in_c=int(p.get("in", 0)),
            out_c=int(p.get("out", 0)),
            tot=total,
            bus_id=bus.id,
            ts=now_ph(),
        ))
        sess.commit()
        logging.debug("Inserted SensorReading for bus=%s", device_id)

    except Exception:
        sess.rollback(); logging.exception("people ingest failed: %s", payload_raw)
    finally:
        sess.close()

def handle_test_sample(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        bus, device_id = _bus_by_device(sess, topic)
        if not bus:
            logging.error("No bus for topic=%s", topic); return

        label    = str(p.get("label", "test"))
        lat      = float(p["lat"])
        lng      = float(p["lng"])
        lat_true = float(p["lat_true"])
        lng_true = float(p["lng_true"])
        err_m    = float(p["err_m"])
        sats     = int(p.get("sats", -1)) if p.get("sats") is not None else None
        hdop     = float(p.get("hdop", -1.0)) if p.get("hdop") is not None else None
        ts_iso   = p.get("ts") or None
        ts       = dtparse.isoparse(ts_iso) if ts_iso else now_ph()

        key = (bus.id, label)
        test_id = _current_test_id.get(key)

        if not test_id:
            # Create/open a session row in gps_test
            ins = text("""
                INSERT INTO gps_test
                  (bus_id, label, lat_true, lng_true, started_at, samples)
                VALUES
                  (:bus_id, :label, :lat_true, :lng_true, :started_at, 0)
            """)
            res = sess.execute(ins, dict(
                bus_id=bus.id, label=label,
                lat_true=lat_true, lng_true=lng_true,
                started_at=ts
            ))
            sess.commit()

            # Retrieve id (LAST_INSERT_ID works per-connection, so re-select)
            sel = text("""
                SELECT id FROM gps_test
                WHERE bus_id=:bus_id AND label=:label
                ORDER BY started_at DESC LIMIT 1
            """)
            test_id = sess.execute(sel, dict(bus_id=bus.id, label=label)).scalar()
            _current_test_id[key] = test_id
            logging.info("➕ New GpsTest id=%s bus=%s label=%s", test_id, device_id, label)

        # Insert sample
        sess.execute(text("""
            INSERT INTO gps_test_sample
              (test_id, bus_id, ts, lat, lng, err_m, sats, hdop)
            VALUES
              (:test_id, :bus_id, :ts, :lat, :lng, :err_m, :sats, :hdop)
        """), dict(
            test_id=test_id, bus_id=bus.id, ts=ts,
            lat=lat, lng=lng, err_m=err_m, sats=sats, hdop=hdop
        ))

        # Increment sample counter (optional but handy)
        sess.execute(text("""
            UPDATE gps_test SET samples = COALESCE(samples,0) + 1
            WHERE id = :test_id
        """), dict(test_id=test_id))

        sess.commit()

    except Exception:
        sess.rollback(); logging.exception("test/sample ingest failed: %s", payload_raw)
    finally:
        sess.close()

def handle_test_summary(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        bus, device_id = _bus_by_device(sess, topic)
        if not bus:
            logging.error("No bus for topic=%s", topic); return

        label      = str(p.get("label", "test"))
        mean_err_m = float(p.get("mean_err_m", 0))
        rmse_m     = float(p.get("rmse_m", 0))
        min_err_m  = float(p.get("min_err_m", 0))
        max_err_m  = float(p.get("max_err_m", 0))
        samples    = int(p.get("samples", 0))
        lat_true   = float(p.get("lat_true", 0))
        lng_true   = float(p.get("lng_true", 0))
        duration_s = int(p.get("duration_s", 0))

        # Find most recent test session (last 24h) with same label
        cutoff = now_ph() - timedelta(hours=24)
        sel = text("""
            SELECT id, started_at FROM gps_test
            WHERE bus_id=:bus_id AND label=:label AND started_at >= :cutoff
            ORDER BY started_at DESC LIMIT 1
        """)
        row = sess.execute(sel, dict(bus_id=bus.id, label=label, cutoff=cutoff)).first()

        if not row:
            # Summary arrived before samples; create one
            started_at = now_ph() - timedelta(seconds=duration_s)
            ins = text("""
                INSERT INTO gps_test
                (bus_id, label, lat_true, lng_true, started_at)
                VALUES (:bus_id, :label, :lat_true, :lng_true, :started_at)
            """)
            sess.execute(ins, dict(
                bus_id=bus.id, label=label, lat_true=lat_true, lng_true=lng_true,
                started_at=started_at
            ))
            sess.commit()
            row = sess.execute(sel, dict(bus_id=bus.id, label=label, cutoff=cutoff)).first()

        test_id, started_at = row

        upd = text("""
            UPDATE gps_test
            SET mean_err_m=:mean_err_m, rmse_m=:rmse_m,
                min_err_m=:min_err_m, max_err_m=:max_err_m,
                samples = CASE WHEN :samples > 0 THEN :samples ELSE samples END,
                duration_s=:duration_s,
                ended_at = DATE_ADD(:started_at, INTERVAL :duration_s SECOND)
            WHERE id=:id
        """)
        sess.execute(upd, dict(
            mean_err_m=mean_err_m, rmse_m=rmse_m,
            min_err_m=min_err_m, max_err_m=max_err_m,
            samples=samples, duration_s=duration_s,
            started_at=started_at, id=test_id
        ))
        sess.commit()

        # Clear open-session cache so the next /start_test makes a new row
        _current_test_id.pop((bus.id, label), None)

        logging.info("✅ Closed GpsTest id=%s bus=%s label=%s", test_id, device_id, label)

    except Exception:
        sess.rollback(); logging.exception("test/summary ingest failed: %s", payload_raw)
    finally:
        sess.close()

# ─── MQTT CALLBACKS ───────────────────────────────────────────────────
def on_connect(c, _u, _f, rc):
    if rc == 0:
        c.subscribe([(TOPIC_PEOPLE, 1),
                     (TOPIC_TEST_SAMPLE, 1),
                     (TOPIC_TEST_SUMMARY, 1)])
        logging.info("MQTT connected; subscribed to topics.")
    else:
        logging.error("❌ MQTT connect failed rc=%s", rc)

def on_message(_c, _u, msg):
    topic = msg.topic
    payload = msg.payload.decode("utf-8", errors="ignore")

    try:
        if topic.endswith("/people"):
            handle_people(topic, payload); return
        if topic.endswith("/test/sample"):
            handle_test_sample(topic, payload); return
        if topic.endswith("/test/summary"):
            handle_test_summary(topic, payload); return
    except Exception:
        logging.exception("Unhandled error for topic=%s", topic)

# ─── RUNNERS ──────────────────────────────────────────────────────────
def run():
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever()

def start_in_background():
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    logging.info("MQTT ingest started in background (ws=%s)", USE_WS)

# ─── PUBLISH HELPER (optional) ────────────────────────────────────────
def publish(topic: str, message: dict):
    payload = json.dumps(message)
    for _ in range(3):
        try:
            client.publish(topic, payload, qos=1)
            logging.debug("Published %s → %s", topic, payload)
            return
        except:
            time.sleep(0.1)
    logging.error("Failed to publish to %s after retries", topic)

if __name__ == "__main__":
    run()
