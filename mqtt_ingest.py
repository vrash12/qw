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

from __future__ import annotations

import json
import logging
import os
import ssl
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from typing import Optional

import paho.mqtt.client as mqtt
from dateutil import parser as dtparse  # pip install python-dateutil
from sqlalchemy import create_engine, func, text, event
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base, mapped_column
from sqlalchemy import Integer, String

from config import Config

# ─── LOGGING ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
_log = logging.getLogger("pgt.mqtt")

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
Base = declarative_base()

class Bus(Base):
    __tablename__ = "bus"
    id = mapped_column(Integer, primary_key=True)
    identifier = mapped_column(String(255))

# ─── MQTT CLIENT (single global) ──────────────────────────────────────
transport = "websockets" if USE_WS else "tcp"

CLIENT_ID = f"pgt-ingest-{os.getpid()}-{uuid4().hex[:6]}"  # ✅ unique per process
client = mqtt.Client(
    client_id=CLIENT_ID,
    clean_session=True,
    transport=transport,
    protocol=mqtt.MQTTv311
)
client.enable_logger(_log)
client.username_pw_set(MQTT_USER, MQTT_PASS)

ssl_ctx = ssl.create_default_context()
client.tls_set_context(ssl_ctx)
client.tls_insecure_set(False)

if USE_WS:
    client.ws_set_options(path=MQTT_PATH)

# Backoff & inflight settings
client.reconnect_delay_set(min_delay=1, max_delay=30)
client.max_inflight_messages_set(20)  # tune as needed
client.max_queued_messages_set(0)     # 0 = unlimited (or set a cap)

# ─── STATE/CACHES ─────────────────────────────────────────────────────
_started = False
_last_totals: dict[int, int] = {}                    # bus_id -> last total_count
_current_test_id: dict[tuple[int, str], int] = {}    # (bus_id, label) -> gps_test.id

# Outbox queue to survive offline periods
_outbox: deque[tuple[str, str, int, bool]] = deque()      # (topic, payload, qos, retain)
_outbox_lock = threading.Lock()

# ─── TIME/HELPERS ─────────────────────────────────────────────────────
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

def _flush_outbox():
    if not client.is_connected():
        return
    with _outbox_lock:
        while _outbox:
            topic, payload, qos, retain = _outbox[0]
            info = client.publish(topic, payload=payload, qos=qos, retain=retain)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                # Stop flushing and retry next time we connect or publish
                break
            _outbox.popleft()

# ─── MQTT CALLBACKS ───────────────────────────────────────────────────
def on_connect(c, _u, _f, rc):
    if rc == 0:
        subs = [(TOPIC_PEOPLE, 1), (TOPIC_TEST_SAMPLE, 1), (TOPIC_TEST_SUMMARY, 1)]
        try:
            c.subscribe(subs)
        except Exception:
            _log.exception("Subscribe failed")
        _log.info("MQTT connected; subscribed to topics. client_id=%s", CLIENT_ID)
        _flush_outbox()   # ✅ send anything queued during downtime
    else:
        _log.error("❌ MQTT connect failed rc=%s client_id=%s", rc, CLIENT_ID)

def on_disconnect(_c, _u, rc):
    # With loop_start() running, Paho will try to reconnect using our backoff
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

# ─── CLIENT STARTUP GUARD ─────────────────────────────────────────────
def _ensure_started():
    """Idempotently start the MQTT client and its network loop."""
    global _started
    if _started:
        return
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    # connect_async + loop_start → non-blocking and auto-reconnect
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=45)
    client.loop_start()
    _started = True
    _log.info("MQTT client auto-started (publisher/ingest mode) client_id=%s", CLIENT_ID)

# ─── PUBLISH HELPERS ──────────────────────────────────────────────────
def publish(topic: str, message: dict | list | str, qos: int = 1, retain: bool = False) -> bool:
    """
    Fire-and-forget publish suitable for web request threads.
    - Starts the client if needed.
    - Queues the message when offline and flushes on reconnect.
    - Returns True unless we synchronously know broker refused (still queued).
    """
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
        # keep the payload; add to outbox
        with _outbox_lock:
            _outbox.append((topic, payload, qos, retain))
        return False

def publish_sync(topic: str, message: dict | list | str, qos: int = 1, retain: bool = False, timeout: float = 2.0) -> bool:
    """
    Synchronous publish (avoid in Flask request threads).
    """
    _ensure_started()
    payload = json.dumps(message, separators=(",", ":")) if isinstance(message, (dict, list)) else str(message)
    try:
        info = client.publish(topic, payload=payload, qos=qos, retain=retain)
        info.wait_for_publish(timeout=timeout)
        return info.is_published()
    except Exception:
        _log.exception("Failed to publish_sync to %s", topic)
        return False

# ─── HANDLERS (INGEST) ────────────────────────────────────────────────
def handle_people(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        bus, device_id = _bus_by_device(sess, topic)
        if not bus:
            _log.error("No bus found for topic=%s", topic)
            return

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
        _log.debug("Inserted SensorReading for bus=%s", device_id)

    except Exception:
        sess.rollback()
        _log.exception("people ingest failed: %s", payload_raw)
    finally:
        sess.close()

def handle_test_sample(topic: str, payload_raw: str):
    sess = Session()
    try:
        p = json.loads(payload_raw)
        bus, device_id = _bus_by_device(sess, topic)
        if not bus:
            _log.error("No bus for topic=%s", topic)
            return

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
            sess.execute(ins, dict(
                bus_id=bus.id, label=label,
                lat_true=lat_true, lng_true=lng_true,
                started_at=ts
            ))
            sess.commit()

            # Retrieve id (LAST_INSERT_ID is connection-scoped; re-select)
            sel = text("""
                SELECT id FROM gps_test
                WHERE bus_id=:bus_id AND label=:label
                ORDER BY started_at DESC LIMIT 1
            """)
            test_id = sess.execute(sel, dict(bus_id=bus.id, label=label)).scalar()
            _current_test_id[key] = test_id
            _log.info("➕ New GpsTest id=%s bus=%s label=%s", test_id, device_id, label)

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

        # Increment sample counter (optional)
        sess.execute(text("""
            UPDATE gps_test SET samples = COALESCE(samples,0) + 1
            WHERE id = :test_id
        """), dict(test_id=test_id))

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
        bus, device_id = _bus_by_device(sess, topic)
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

        _log.info("✅ Closed GpsTest id=%s bus=%s label=%s", test_id, device_id, label)

    except Exception:
        sess.rollback()
        _log.exception("test/summary ingest failed: %s", payload_raw)
    finally:
        sess.close()

# ─── RUNNERS ──────────────────────────────────────────────────────────
def run():
    """Blocking runner (CLI): keeps the ingest loop alive."""
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=45)
    client.loop_forever()

def start_in_background():
    """Non-blocking runner (embed in Flask, etc.)."""
    _ensure_started()
    _log.info("MQTT ingest started in background (ws=%s)", USE_WS)

if __name__ == "__main__":
    run()
