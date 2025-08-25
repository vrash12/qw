# backend/realtime.py
from flask_socketio import SocketIO, emit, join_room, leave_room

# one shared instance for the whole app
socketio = SocketIO(cors_allowed_origins="*", ping_interval=25, ping_timeout=20)

NS = "/rt"

@socketio.on("connect", namespace=NS)
def on_connect(auth):
    # Optional: validate JWT in `auth.get("token")`
    emit("connected", {"ok": True})

@socketio.on("disconnect", namespace=NS)
def on_disconnect():
    pass

@socketio.on("subscribe", namespace=NS)
def on_subscribe(data):
    bus_id = (data or {}).get("bus_id")
    if bus_id:
        join_room(f"bus:{bus_id}")

@socketio.on("unsubscribe", namespace=NS)
def on_unsubscribe(data):
    bus_id = (data or {}).get("bus_id")
    if bus_id:
        leave_room(f"bus:{bus_id}")

def emit_announcement(payload: dict, *, bus_id: int | None):
    """
    Broadcast a freshly created announcement to:
      - everyone on /rt (for 'All' filter)
      - and a per-bus room if bus_id is provided
    """
    socketio.emit("announcement:new", payload, namespace=NS)  # global
    if bus_id:
        socketio.emit("announcement:new", payload, room=f"bus:{bus_id}", namespace=NS)
