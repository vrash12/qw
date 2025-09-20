# utils/notify_user.py
import os, ssl, json, time
import paho.mqtt.client as mqtt

MQTT_HOST  = os.getenv("MQTT_HOST",  "35010b9ea10d41c0be8ac5e9a700a957.s1.eu.hivemq.cloud")
MQTT_PORT  = int(os.getenv("MQTT_PORT", "8883"))  # TLS over TCP
MQTT_USER  = os.getenv("MQTT_USER",  "vanrodolf")
MQTT_PASS  = os.getenv("MQTT_PASS",  "Vanrodolf123.")
TOPIC_TELLERS = os.getenv("MQTT_TOPIC_TELLERS", "tellers/topups")

def _oneshot_publish(topic: str, payload: dict, qos: int = 1, retain: bool = False, timeout: float = 5.0) -> bool:
    client = mqtt.Client(client_id=f"server-{int(time.time()*1000)}", clean_session=True)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set(cert_reqs=ssl.CERT_REQUIRED)  # system CAs
    # client.tls_insecure_set(True)  # uncomment only if you really need to bypass cert checks

    rc = client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    if rc != 0:
        return False

    client.loop_start()
    info = client.publish(topic, json.dumps(payload or {}), qos=qos, retain=retain)
    ok = info.wait_for_publish(timeout=timeout)
    client.loop_stop()
    try: client.disconnect()
    except: pass
    return bool(ok)

def notify_tellers(payload: dict) -> bool:
    p = dict(payload or {})
    p.setdefault("type", "broadcast")
    p.setdefault("sentAt", int(time.time()*1000))
    return _oneshot_publish(TOPIC_TELLERS, p, qos=1, retain=False)

def notify_user(user_id: int, payload: dict) -> bool:
    topic = f"user/{int(user_id)}/notify"
    p = dict(payload or {})
    p.setdefault("sentAt", int(time.time()*1000))
    return _oneshot_publish(topic, p, qos=1, retain=False)
