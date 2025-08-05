# backend/mqtt_ingest.py
import json, threading, logging
import paho.mqtt.client as mqtt
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker, scoped_session
from models.bus            import Bus
from models.sensor_reading import SensorReading
from config import Config

MQTT_HOST = "35010b9ea10d41c0be8ac5e9a700a957.s1.eu.hivemq.cloud"
MQTT_PORT = 8883
MQTT_USER = "vanrodolf"
MQTT_PASS = "Vanrodolf123."
TOPIC     = "device/+/people"

# ── one engine / one session maker, detached from Flask ────────────────
engine  = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
Session = scoped_session(
    sessionmaker(bind=engine, expire_on_commit=False)   # keep attrs usable after commit
)
def on_message(client, userdata, msg):
    """Insert people–counter readings coming from device/<id>/people messages."""
    sess = Session()
    try:
        # --------------------- 1) basic JSON parse --------------------------
        data = json.loads(msg.payload.decode("utf-8", errors="ignore"))

        # --------------------- 2) figure out which bus ----------------------
        # a) explicit in the payload
        device_id = str(data.get("deviceId") or data.get("bus") or "").strip()

        # b) derive it from the MQTT topic   device/<BUS-ID>/people
        if not device_id:
            parts = msg.topic.split("/")
            if len(parts) >= 3 and parts[0] == "device":
                device_id = parts[1]          # bus-01 / PGT-001 / 2 …
        if not device_id:
            raise ValueError("cannot deduce deviceId from payload or topic")

        # look it up (case-insensitive)
        bus = (
            sess.query(Bus)
                .filter(func.lower(Bus.identifier) == device_id.lower())
                .first()
        )
        if not bus:
            raise ValueError(f"no bus in DB matching {device_id!r}")
        if msg.topic.startswith("pao/") and payload.get("type") == "request":
            bus_id_str = msg.topic.split("/")[1]        # pao/<bus>/passenger/updates
            bus_id = int(bus_id_str.replace("bus-", ""))
            push_to_bus(
                bus_id,
                "🚍 New Pickup Request",
                f"Commuter #{payload['id']} is waiting.",
                {"commuterId": payload["id"]}
            )
        # --------------------- 3) numeric values ----------------------------
        in_c   = int(data["in"])
        out_c  = int(data["out"])
        total  = int(data["total"])

        # --------------------- 4) insert ------------------------------------
        reading = SensorReading(
            in_count=in_c, out_count=out_c, total_count=total, bus_id=bus.id
        )
        sess.add(reading)
        sess.commit()

        logging.debug("saved SensorReading id=%s for %s", reading.id, device_id)

    except (KeyError, ValueError, TypeError, json.JSONDecodeError) as e:
        logging.error("MQTT ingest skipped payload %s → %s", msg.payload, e)
        sess.rollback()
    except Exception:
        sess.rollback()
        logging.exception("MQTT ingest unexpected error")
    finally:
        sess.close()

def run():
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    client.subscribe(TOPIC, qos=1)
    client.loop_forever()

def start_in_background():
    threading.Thread(target=run, daemon=True, name="mqtt-ingest").start()
