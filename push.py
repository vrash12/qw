# push.py
import firebase_init  # ensures admin is initialized
from firebase_admin import messaging

def subscribe_topics(token: str, topics: list[str]):
    for t in topics:
        # topic names: letters, numbers, _ or - (no spaces)
        messaging.subscribe_to_topic([token], t)

def send_to_topic(topic: str, title: str, body: str, data: dict | None = None):
    msg = messaging.Message(
        topic=topic,
        notification=messaging.Notification(title=title, body=body),
        data={k: str(v) for k, v in (data or {}).items()},
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(channel_id="announcements"),
        ),
    )
    return messaging.send(msg)
