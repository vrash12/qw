# backend/config.py
import os

class Config:
    DEBUG = True
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev_secret')

    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'mysql+pymysql://u782952718_eee:Vanrodolf123.@srv667.hstgr.io/u782952718_eee'
    )

    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Engine options to prevent stale connection errors
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,      # Check connections before using them
        "pool_recycle": 180,        # Recycle before MySQL’s wait_timeout
        "pool_size": 5,             # Base pool size
        "max_overflow": 10,         # Extra conns allowed above pool_size
        "pool_timeout": 30,         # Wait max 30s for a conn
        "connect_args": {
            "connect_timeout": 10,  # Fail fast on network issues
            "read_timeout": 10,
            "write_timeout": 10,
        },
    }

    WALLET_QR_SECRET = os.environ.get("WALLET_QR_SECRET", "dev-wallet-secret-change-me")
    MIN_TOPUP_CENTS = int(os.environ.get("MIN_TOPUP_CENTS", "2000"))       # ₱20
    MAX_TOPUP_CENTS = int(os.environ.get("MAX_TOPUP_CENTS", "200000"))     # ₱2,000
    PAO_DAILY_TOPUP_LIMIT_CENTS = int(os.environ.get("PAO_DAILY_TOPUP_LIMIT_CENTS", "5000000"))  # ₱50,000

