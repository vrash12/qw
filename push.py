# backend/push.py
from exponent_server_sdk import PushClient, PushMessage, PushTicketError, PushServerError
import asyncio

# initialize a client that can send pushes
push_client = PushClient()

async def send_expo_push(tokens: list[str], title: str, body: str, data: dict):
    messages = [
        PushMessage(to=token, title=title, body=body, data=data)
        for token in tokens
    ]
    try:
        # this will batch & send them concurrently
        tickets = await push_client.publish_multiple(messages)
    except PushServerError as e:
        # Something went wrong at the HTTP / Expo server level
        print(f"[Push] Server error: {e.message}")
        for ticket in e.errors:
            print(ticket.details)
    except Exception as e:
        # Some other non-Expo error
        print(f"[Push] Unexpected error: {e}")

# if you need a sync wrapper:
def send_expo_push_sync(tokens, title, body, data):
    asyncio.get_event_loop().run_until_complete(
        send_expo_push(tokens, title, body, data)
    )
