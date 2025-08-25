# wsgi.py
import os
from app import create_app
from realtime import socketio   # ⬅️ import the shared SocketIO instance

# Create Flask app
app = create_app()

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    host = os.getenv("HOST", "0.0.0.0")

    # Run with Socket.IO server
    # In dev this works fine; in prod use gunicorn with -k eventlet/gevent
    socketio.run(app, host=host, port=port)
