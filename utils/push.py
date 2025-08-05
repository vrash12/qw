# util/push.py
import requests, os, json
from models.user import User
from models.device_token import DeviceToken

EXPO_URL = "https://exp.host/--/api/v2/push/send"

def send_push(tokens: list[str], title: str, body: str, data=None):
    if not tokens:
        return

    # Expo allows up to 100 messages per call; chunk if needed.
    payload = [{
        "to": t,
        "title": title,
        "body": body,
        "sound": "default",
        "data": data or {},
    } for t in tokens]

    r = requests.post(EXPO_URL, json=payload, timeout=10)
    if r.status_code != 200:
        current_app.logger.error("Expo push error %s – %s", r.status_code, r.text)
        
def push_to_bus(bus_id: int, title: str, body: str, extra=None):
    """Send to *every PAO* whose assigned_bus_id == bus_id."""
    pao_ids = [u.id for u in User.query.filter_by(role="pao", assigned_bus_id=bus_id)]
    tokens  = [t.token for t in DeviceToken.query.filter(DeviceToken.user_id.in_(pao_ids))]
    send_push(tokens, title, body, extra)