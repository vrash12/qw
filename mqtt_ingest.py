# mqtt_ingest.py
"""
MQTT ingest + publish helpers.

Listens for:
- Passenger counts: device/<device_id>/people      (JSON: {"in":N,"out":N,"total":N})
- GPS accuracy tests:
    device/<device_id>/test/sample                 (per-fix sample)
    device/<device_id>/test/summary                (final summary)

Also exposes convenient publishers:
- publish(topic, message, qos=1, retain=False)
- notify_tellers(payload)
- notify_user_event(uid, payload)
- notify_user_wallet(uid, payload)

DB tables used:
  - buses (id, identifier, ...)
  - sensor_readings (id, timestamp, in_count, out_count, total_count, bus_id, trip_id?)
  - gps_test, gps_test_sample  (optional; used by test/* topics)

Notes:
- We set the MySQL/MariaDB session time_zone to +08:00 so naive datetimes are
  stored as Manila civil time.
- Bus matching supports:
    * exact match of buses.identifier
    * numeric device id → matches buses.id
    * "bus-2", "bus-02", "bus-002", "bus-0002" patterns
"""

from __future__ import annotations

import json
import logging
import os
import re
import ssl
import threading
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import uuid4

import paho.mqtt.client as mqtt
from dateutil import parser as dtparse
from sqlalchemy import create_engine, func, text, event
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base, mapped_column
from sqlalchemy import Integer, String, Date, Time

from config import Config

# ───────────────────────── LOGGING ─────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
_log = logging.getLogger("mqtt_ingest")

# ───────────────────────── MQTT CONFIG ─────────────────────
# Switch to TCP (8883) if you prefer; HiveMQ Cloud supports both.
MQTT_HOST = os.getenv("MQTT_HOST", "35010b9ea10d41c0be8ac5e9a700a957.s1.eu.hivemq.cloud")
USE_WS    = os.getenv("MQTT_USE_WS", "1").strip() not in {"0", "false", "False"}
MQTT_PORT = int(os.getenv("MQTT_PORT", "8884" if USE_WS else "8883"))
MQTT_PATH = os.getenv("MQTT_PATH", "/mqtt") if USE_WS else None
MQTT_USER = os.getenv("MQTT_USER", "vanrodolf")
MQTT_PASS = os.getenv("MQTT_PASS", "Vanrodolf123.")

TOPIC_PEOPLE       = "device/+/people"
TOPIC_TEST_SAMPLE  = "device/+/test/sample"
TOPIC_TEST_SUMMARY = "device/+/test/summary"

# ───────────────────────── DB SETUP ────────────────────────
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)

@event.listens_for(engine, "connect")
def _set_manila_timezone(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    try:
        # Ensure DATETIME columns read/write as +08:00
        cur.execute("SET time_zone = '+08:00'")
    finally:
        cur.close()

Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

Base = declarative_base()

class Bus(Base):
    __tablename__ = "buses"
    id         = mapped_column(Integer, primary_key=True)
    identifier = mapped_column(String(255))

class Trip(Base):
    __tablename__ = "trips"
    id           = mapped_column(Integer, primary_key=True)
    bus_id       = mapped_column(Integer)
    service_date = mapped_column(Date)   # yyyy-mm-dd
    start_time   = mapped_column(Time)   # HH:MM:SS
    end_time     = mapped_column(Time)   # HH:MM:SS

# ───────────────────── MQTT CLIENT (global) ─────────────────
transport = "websockets" if USE_WS else "tcp"
CLIENT_ID = f"pgt-ingest-{os.getpid()}-{uuid4().hex[:5]}"
client = mqtt.Client(client_id=CLIENT_ID, clean_session=True, transport=transport, protocol=mqtt.MQTTv311)
client.enable_logger(_log)
client.username_pw_set(MQTT_USER, MQTT_PASS)

ssl_ctx = ssl.create_default_context()
client.tls_set_context(ssl_ctx)
client.tls_insecure_set(False)

if USE_WS and MQTT_PATH:
    client.ws_set_options(path=MQTT_PATH)

client.reconnect_delay_set(min_delay=1, max_delay=30)
client.max_inflight_messages_set(20)
client.max_queued_messages_set(0)  # 0 = unlimited

# ───────────────────── STATE / CACHES ──────────────────────
_started = False
_last_totals: dict[int, int] = {}                 # bus_id -> last observed "total" to dedupe
_current_test_id: dict[tuple[int, str], int] = {} # (bus_id, label) -> gps_test.id

_outbox: deque[tuple[str, str, int, bool]] = deque()
_outbox_lock = threading.Lock()

# ───────────────────── TIME HELPERS ────────────────────────
MNL = timezone(timedelta(hours=8))

def now_ph() -> datetime:
    # We store naive DT with session time_zone = +08:00.
    return datetime.now(MNL).replace(tzinfo=None)

# ───────────────────── BUS & TRIP HELPERS ──────────────────
def _parse_topic_device_id(topic: str) -> Optional[str]:
    # topic: device/<device_id>/...
    try:
        _, device_id, _ = topic.split("/", 2)
        return (device_id or "").strip()
    except ValueError:
        return None

def _find_bus_by_device(sess, device_id: str) -> Optional[Bus]:
    """
    Resolve a device_id to a Bus row.
     - exact match on buses.identifier (case-insensitive)
     - numeric device id -> match buses.id
     - normalized 'bus-0002' shapes for numeric ids
     - also parse 'bus-<num>' identifiers and compare number with buses.id
    """
    dev = (device_id or "").strip()
    if not dev:
        return None

    # 1) exact identifier match
    bus = (
        sess.query(Bus)
        .filter(func.lower(Bus.identifier) == dev.lower())
        .first()
    )
    if bus:
        return bus

    # 2) numeric device → try buses.id
    if dev.isdigit():
        bus = sess.get(Bus, int(dev))
        if bus:
            return bus

    # 3) Try common identifier shapes for numeric device
    if dev.isdigit():
        want = int(dev)
        candidates = [f"bus-{want}", f"bus-{want:02d}", f"bus-{want:03d}", f"bus-{want:04d}"]
        bus = (
            sess.query(Bus)
            .filter(func.lower(Bus.identifier).in_([c.lower() for c in candidates]))
            .first()
        )
        if bus:
            return bus

    # 4) Device looks like bus-XYZ → parse number and compare to buses.id
    m = re.match(r"^bus-?0*(\d+)$", dev, re.IGNORECASE)
    if m:
        num = int(m.group(1))
        bus = sess.get(Bus, num)
        if bus:
            return bus

    # 5) Fallback: scan and compare numeric suffix
    for b in sess.query(Bus).all():
        ident = (b.identifier or "").strip().lower()
        m2 = re.match(r"^bus-?0*(\d+)$", ident)
        if m2 and m2.group(1).isdigit() and int(m2.group(1)) == int(dev) if dev.isdigit() else False:
            return b

    return None

def _active_trip_for(sess, bus_id: int, ts: datetime) -> Optional[int]:
    """
    Find an active trip id for bus_id at timestamp ts.
    We check service_date ∈ {today, yesterday} (in PH time) and compute windows in Python.
    """
    day = ts.date()
    prev = (ts - timedelta(days=1)).date()
    trips = (
        sess.query(Trip)
        .filter(Trip.bus_id == bus_id, Trip.service_date.in_([day, prev]))
        .all()
    )
    for t in trips:
        start = datetime.combine(t.service_date, t.start_time)
        end   = datetime.combine(t.service_date, t.end_time)
        if t.end_time <= t.start_time:  # crosses midnight
            end += timedelta(days=1)
        if start <= ts < end:
            return t.id
    return None

# ───────────────────── OUTBOX / PUBLISH ────────────────────
def _flush_outbox():
    if not client.is_connected():
        return
    with _outbox_lock:
        while _outbox:
            topic, payload, qos, retain = _outbox[0]
            info = client.publish(topic, payload=payload, qos=qos, retain=retain)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                break
            _outbox.popleft()

def publish(topic: str, message, qos: int = 1, retain: bool = False) -> bool:
    """Fire-and-forget publish; queues when offline and flushes on reconnect."""
    _ensure_started()
    payload = json.dumps(message, separators=(",", ":")) if isinstance(message, (dict, list)) else str(message)
    if not client.is_connected():
        with _outbox_lock:
            _outbox.append((topic, payload, qos, retain))
        _log.debug("queued (offline) → %s", topic)
        return True
    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            with _outbox_lock:
                _outbox.append((topic, payload, qos, retain))
            _log.warning("publish refused rc=%s; re-queued topic=%s", info.rc, topic)
            return False
        return True
    except Exception:
        _log.exception("Failed to publish to %s", topic)
        with _outbox_lock:
            _outbox.append((topic, payload, qos, retain))
        return False

def notify_tellers(payload: dict) -> bool:
    if "sentAt" not in payload:
        payload["sentAt"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    return publish("tellers/topups", payload)

def notify_user_event(uid: int, payload: dict) -> bool:
    if "sentAt" not in payload:
        payload["sentAt"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    ok = True
    for root in ("user", "users"):
        ok = publish(f"{root}/{int(uid)}/events", payload) and ok
    return ok

def notify_user_wallet(uid: int, payload: dict) -> bool:
    if "sentAt" not in payload:
        payload["sentAt"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    ok = True
    for root in ("user", "users"):
        ok = publish(f"{root}/{int(uid)}/wallet", payload) and ok
    return ok

# ───────────────────── MQTT CALLBACKS ──────────────────────
def on_connect(c, _u, _f, rc):
    if rc == 0:
        subs = [(TOPIC_PEOPLE, 1), (TOPIC_TEST_SAMPLE, 1), (TOPIC_TEST_SUMMARY, 1)]
        try:
            c.subscribe(subs)
        except Exception:
            _log.exception("Subscribe failed")
        _log.info("MQTT connected; subscribed. client_id=%s", CLIENT_ID)
        _flush_outbox()
    else:
        _log.error("❌ MQTT connect failed rc=%s client_id=%s", rc, CLIENT_ID)

def on_disconnect(_c, _u, rc):
    _log.warning("MQTT disconnected rc=%s client_id=%s", rc, CLIENT_ID)

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
        _log.exception("Unhandled error for topic=%s", topic)

# ───────────────────── STARTUP GUARD ───────────────────────
def _ensure_started():
    global _started
    if _started:
        return
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=45)
    client.loop_start()
    _started = True
    _log.info("MQTT client auto-started (ws=%s) client_id=%s", USE_WS, CLIENT_ID)

# ───────────────────── INGEST HANDLERS ─────────────────────
def handle_people(topic: str, payload_raw: str):
    """
    Payload example: {"in":1,"out":0,"total":12}
    Inserts into sensor_readings (and links trip_id if a trip is active).
    """
    sess = Session()
    try:
        p = json.loads(payload_raw)
        device_id = _parse_topic_device_id(topic)
        if not device_id:
            _log.error("Bad topic (no device id): %s", topic); return

        bus = _find_bus_by_device(sess, device_id)
        if not bus:
            _log.error("No bus matched for device_id=%s (topic=%s)", device_id, topic)
            return

        in_c  = int(p.get("in", 0))
        out_c = int(p.get("out", 0))
        total = int(p.get("total", 0))

        # Deduplicate on same total (to reduce DB noise)
        if _last_totals.get(bus.id) == total:
            return
        _last_totals[bus.id] = total

        ts = now_ph()

        # Optional: attach active trip if within a trip window
        trip_id = _active_trip_for(sess, bus.id, ts)

        sess.execute(
            text("""
                INSERT INTO sensor_readings
                    (in_count, out_count, total_count, bus_id, trip_id, timestamp)
                VALUES
                    (:in_c, :out_c, :tot, :bus_id, :trip_id, :ts)
            """),
            dict(in_c=in_c, out_c=out_c, tot=total, bus_id=bus.id, trip_id=trip_id, ts=ts),
        )
        sess.commit()
        _log.info("🚍 sensor_readings ← bus=%s (id=%s) in=%s out=%s total=%s%s",
                  bus.identifier, bus.id, in_c, out_c, total,
                  f" trip_id={trip_id}" if trip_id else "")
    except Exception:
        sess.rollback()
        _log.exception("people ingest failed: %s", payload_raw)
    finally:
        sess.close()

def handle_test_sample(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        device_id = _parse_topic_device_id(topic)
        bus = _find_bus_by_device(sess, device_id or "")
        if not bus:
            _log.error("No bus for topic=%s", topic)
            return

        label    = str(p.get("label", "test"))
        lat      = float(p["lat"]);      lng      = float(p["lng"])
        lat_true = float(p["lat_true"]); lng_true = float(p["lng_true"])
        err_m    = float(p["err_m"])
        sats     = int(p.get("sats", -1)) if p.get("sats") is not None else None
        hdop     = float(p.get("hdop", -1.0)) if p.get("hdop") is not None else None
        ts_iso   = p.get("ts") or None
        ts       = dtparse.isoparse(ts_iso).astimezone(MNL).replace(tzinfo=None) if ts_iso else now_ph()

        key = (bus.id, label)
        test_id = _current_test_id.get(key)
        if not test_id:
            sess.execute(text("""
                INSERT INTO gps_test
                    (bus_id, label, lat_true, lng_true, started_at, samples)
                VALUES
                    (:bus_id, :label, :lat_true, :lng_true, :started_at, 0)
            """), dict(bus_id=bus.id, label=label, lat_true=lat_true, lng_true=lng_true, started_at=ts))
            sess.commit()
            test_id = sess.execute(
                text("""SELECT id FROM gps_test
                        WHERE bus_id=:bus_id AND label=:label
                        ORDER BY started_at DESC LIMIT 1"""),
                dict(bus_id=bus.id, label=label),
            ).scalar()
            _current_test_id[key] = test_id
            _log.info("➕ New gps_test id=%s bus=%s label=%s", test_id, device_id, label)

        sess.execute(text("""
            INSERT INTO gps_test_sample
                (test_id, bus_id, ts, lat, lng, err_m, sats, hdop)
            VALUES
                (:test_id, :bus_id, :ts, :lat, :lng, :err_m, :sats, :hdop)
        """), dict(test_id=test_id, bus_id=bus.id, ts=ts, lat=lat, lng=lng, err_m=err_m, sats=sats, hdop=hdop))

        sess.execute(text("UPDATE gps_test SET samples = COALESCE(samples,0) + 1 WHERE id = :test_id"),
                     dict(test_id=test_id))
        sess.commit()
    except Exception:
        sess.rollback()
        _log.exception("test/sample ingest failed: %s", payload_raw)
    finally:
        sess.close()

def handle_test_summary(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        device_id = _parse_topic_device_id(topic)
        bus = _find_bus_by_device(sess, device_id or "")
        if not bus:
            _log.error("No bus for topic=%s", topic)
            return

        label      = str(p.get("label", "test"))
        mean_err_m = float(p.get("mean_err_m", 0))
        rmse_m     = float(p.get("rmse_m", 0))
        min_err_m  = float(p.get("min_err_m", 0))
        max_err_m  = float(p.get("max_err_m", 0))
        samples    = int(p.get("samples", 0))
        lat_true   = float(p.get("lat_true", 0))
        lng_true   = float(p.get("lng_true", 0))
        duration_s = int(p.get("duration_s", 0))

        cutoff = now_ph() - timedelta(hours=24)
        row = sess.execute(text("""
            SELECT id, started_at FROM gps_test
            WHERE bus_id=:bus_id AND label=:label AND started_at >= :cutoff
            ORDER BY started_at DESC LIMIT 1
        """), dict(bus_id=bus.id, label=label, cutoff=cutoff)).first()

        if not row:
            started_at = now_ph() - timedelta(seconds=duration_s)
            sess.execute(text("""
                INSERT INTO gps_test
                    (bus_id, label, lat_true, lng_true, started_at)
                VALUES
                    (:bus_id, :label, :lat_true, :lng_true, :started_at)
            """), dict(bus_id=bus.id, label=label, lat_true=lat_true, lng_true=lng_true, started_at=started_at))
            sess.commit()
            row = sess.execute(text("""
                SELECT id, started_at FROM gps_test
                WHERE bus_id=:bus_id AND label=:label AND started_at >= :cutoff
                ORDER BY started_at DESC LIMIT 1
            """), dict(bus_id=bus.id, label=label, cutoff=cutoff)).first()

        test_id, started_at = row
        sess.execute(text("""
            UPDATE gps_test
            SET mean_err_m=:mean_err_m, rmse_m=:rmse_m,
                min_err_m=:min_err_m, max_err_m=:max_err_m,
                samples = CASE WHEN :samples > 0 THEN :samples ELSE samples END,
                duration_s=:duration_s,
                ended_at = DATE_ADD(:started_at, INTERVAL :duration_s SECOND)
            WHERE id=:id
        """), dict(mean_err_m=mean_err_m, rmse_m=rmse_m, min_err_m=min_err_m, max_err_m=max_err_m,
                   samples=samples, duration_s=duration_s, started_at=started_at, id=test_id))
        sess.commit()
        _current_test_id.pop((bus.id, label), None)
        _log.info("✅ Closed gps_test id=%s bus=%s label=%s", test_id, device_id, label)
    except Exception:
        sess.rollback()
        _log.exception("test/summary ingest failed: %s", payload_raw)
    finally:
        sess.close()

# ───────────────────── RUNNERS ─────────────────────────────
def run():
    """Blocking runner (CLI): keeps the ingest loop alive."""
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=45)
    client.loop_forever()

def start_in_background():
    """Non-blocking runner (embed in Flask, celery, etc.)."""
    _ensure_started()
    _log.info("MQTT ingest started in background (ws=%s)", USE_WS)

if __name__ == "__main__":
    run()
