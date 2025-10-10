# wsgi.py
import os
from app import create_app
from realtime import socketio  # shared SocketIO instance

app = create_app()

# Local dev only: `python wsgi.py`
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    socketio.run(app, host="0.0.0.0", port=port)
