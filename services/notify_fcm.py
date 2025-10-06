# services/notify_fcm.py
from firebase_admin import messaging

TOPIC_COMMUTERS_BUS = "commuters.bus.{bus_id}"

def notify_commuters_announcement_fcm(*, bus_id: int, message: str) -> str:
    topic = TOPIC_COMMUTERS_BUS.format(bus_id=int(bus_id))
    msg = messaging.Message(
        notification=messaging.Notification(
            title="🚌 Bus announcement",
            body=message,
        ),
        data={
            "type": "announcement",
            "bus_id": str(int(bus_id)),
            "deeplink": "/commuter/announcements",
        },
        topic=topic,
        android=messaging.AndroidConfig(
            priority="high",
            notification=messaging.AndroidNotification(channel_id="announcements")
        ),
        apns=messaging.APNSConfig(
            headers={"apns-priority": "10"},
            payload=messaging.APNSPayload(aps=messaging.Aps(sound="default"))
        ),
    )
    return messaging.send(msg)  # returns message_id
