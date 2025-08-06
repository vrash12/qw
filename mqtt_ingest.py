# backend/mqtt_ingest.py
"""
MQTT ingest service: listens for “people” messages over MQTT+WebSockets/TLS
and writes them to sensor_readings—but only when the count actually changes.
"""

import json
import logging
import ssl
import time

import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, scoped_session

from config import Config

# ─── MODEL IMPORTS ────────────────────────────────────────────────────
# Ensure relationship() targets resolve
from models.fare_segment    import FareSegment   # noqa: F401
from models.schedule        import Trip, StopTime  # noqa: F401
from models.bus             import Bus
from models.sensor_reading  import SensorReading
from models.ticket_sale     import TicketSale     # noqa: F401
from models.ticket_stop     import TicketStop     # noqa: F401
from models.qr_template     import QRTemplate     # noqa: F401
from models.user            import User           # noqa: F401

# ─── LOGGING ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s: %(message)s"
)

# ─── MQTT CONFIG ──────────────────────────────────────────────────────
MQTT_HOST     = "35010b9ea10d41c0be8ac5e9a700a957.s1.eu.hivemq.cloud"
USE_WS        = True
MQTT_PORT     = 8884 if USE_WS else 8883
MQTT_PATH     = "/mqtt"      # only used when USE_WS=True
MQTT_USER     = "vanrodolf"
MQTT_PASS     = "Vanrodolf123."
TOPIC_PEOPLE  = "device/+/people"

# ─── DB SETUP ─────────────────────────────────────────────────────────
engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
Session = scoped_session(sessionmaker(bind=engine, expire_on_commit=False))

# ─── MQTT CLIENT SETUP ────────────────────────────────────────────────
# Choose WebSocket transport if USE_WS
transport = "websockets" if USE_WS else "tcp"
client = mqtt.Client(client_id="pgt-ingest", transport=transport)
client.username_pw_set(MQTT_USER, MQTT_PASS)

# Configure TLS
ssl_ctx = ssl.create_default_context()
# if you're on a network without the public CAs, you could disable verification:
# ssl_ctx.check_hostname = False
# ssl_ctx.verify_mode   = ssl.CERT_NONE

client.tls_set_context(ssl_ctx)

# For websockets you must tell Paho the WS path:
if USE_WS:
    client.ws_set_options(path=MQTT_PATH)

# ─── DEDUP CACHE ──────────────────────────────────────────────────────
_last_totals: dict[int,int] = {}

# ─── CALLBACKS ────────────────────────────────────────────────────────
def on_connect(c, _u, _f, rc):
    if rc == 0:
        c.subscribe([(TOPIC_PEOPLE, 1)])
    else:
        logging.error("❌ MQTT connect failed rc=%s", rc)

def on_message(_c, _u, msg):
    # only handle /people messages
    if not msg.topic.endswith("/people"):
        return

    sess = Session()
    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        _, device_id, _ = msg.topic.split("/", 2)

        bus = (sess.query(Bus)
                  .filter(func.lower(Bus.identifier) == device_id.lower())
                  .first())
        if not bus:
            logging.error("No bus found for device_id=%s", device_id)
            return

        total = int(payload.get("total", 0))
        if _last_totals.get(bus.id) == total:
            return
        _last_totals[bus.id] = total

        reading = SensorReading(
            in_count    = int(payload.get("in",    0)),
            out_count   = int(payload.get("out",   0)),
            total_count = total,
            bus_id      = bus.id
        )
        sess.add(reading)
        sess.commit()
        logging.debug("Inserted SensorReading id=%s for bus=%s",
                      reading.id, device_id)

    except Exception:
        sess.rollback()
        logging.exception("Error ingesting %s → %s", msg.topic, msg.payload)
    finally:
        sess.close()

# ─── RUNNERS ──────────────────────────────────────────────────────────
def run():
    """
    Blocking: start MQTT loop on this thread.
    Usage: python -m mqtt_ingest
    """
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_forever()

def start_in_background():
    """
    Non-blocking: start MQTT loop in background.
    Call this from your Flask app factory.
    """
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.loop_start()
    logging.info("MQTT ingest started in background (ws=%s)", USE_WS)

# ─── PUBLISH HELPER ───────────────────────────────────────────────────
def publish(topic: str, message: dict):
    """
    Publish JSON to broker (with retries).
    """
    payload = json.dumps(message)
    for _ in range(3):
        try:
            client.publish(topic, payload, qos=1)
            logging.debug("Published %s → %s", topic, payload)
            return
        except:
            time.sleep(0.1)
    logging.error("Failed to publish to %s after retries", topic)

# ─── MODULE ENTRYPOINT ───────────────────────────────────────────────
if __name__ == "__main__":
    run()
