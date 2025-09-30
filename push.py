# backend/push.py
from exponent_server_sdk import PushClient, PushMessage

push_client = PushClient()

def _msg(token, title, body, data):
    return PushMessage(
        to=token, title=title, body=body, data=data,
        sound="default", channel_id="payments", priority="high"
    )

async def send_expo_push(tokens: list[str], title: str, body: str, data: dict):
    await push_client.publish_multiple([_msg(t, title, body, data) for t in tokens])

def send_expo_push_sync(tokens, title, body, data):
    import asyncio
    asyncio.get_event_loop().run_until_complete(send_expo_push(tokens, title, body, data))
