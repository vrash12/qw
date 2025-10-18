# backend/config.py
import os

def _to_bool(val: str | None, default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}

def _to_int(val: str | None, default: int) -> int:
    try:
        return int(val) if val is not None else default
    except ValueError:
        return default


class Config:
    # ── Core ─────────────────────────────────────────────────────────────────
    DEBUG = _to_bool(os.environ.get("DEBUG") or os.environ.get("FLASK_DEBUG"), True)
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev_secret")  # ← override in prod!

    SQLALCHEMY_DATABASE_URI = os.environ.get(
        "DATABASE_URL",
        "mysql+pymysql://u782952718_eee:Vanrodolf123.@srv667.hstgr.io/u782952718_eee",
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    PREFERRED_URL_SCHEME = os.environ.get("PREFERRED_URL_SCHEME", "https")
    APP_TIMEZONE = os.environ.get("APP_TIMEZONE", "Asia/Manila")

    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
        "pool_recycle": 180,
        "pool_size": _to_int(os.environ.get("DB_POOL_SIZE"), 5),
        "max_overflow": _to_int(os.environ.get("DB_MAX_OVERFLOW"), 10),
        "pool_timeout": _to_int(os.environ.get("DB_POOL_TIMEOUT"), 30),
        "connect_args": {
            "connect_timeout": _to_int(os.environ.get("DB_CONNECT_TIMEOUT"), 10),
            "read_timeout": _to_int(os.environ.get("DB_READ_TIMEOUT"), 10),
            "write_timeout": _to_int(os.environ.get("DB_WRITE_TIMEOUT"), 10),
        },
    }

    # ── App/Wallet ──────────────────────────────────────────────────────────
    WALLET_QR_SECRET = os.environ.get("WALLET_QR_SECRET", "dev-wallet-secret-change-me")
    APP_NAME = os.environ.get("APP_NAME", "YourApp")

    # ── Auth / JWT ──────────────────────────────────────────────────────────
    JWT_TTL_HOURS = _to_int(os.environ.get("JWT_TTL_HOURS"), 24)
    MFA_ENFORCED_ROLES = os.environ.get("MFA_ENFORCED_ROLES", "pao,manager,teller")

    # ── Twilio (SMS OTP) ────────────────────────────────────────────────────
    TWILIO_ACCOUNT_SID  = os.environ.get("TWILIO_ACCOUNT_SID")
    TWILIO_AUTH_TOKEN   = os.environ.get("TWILIO_AUTH_TOKEN")
    TWILIO_FROM         = os.environ.get("TWILIO_FROM")            # e.g. +12565550123
    TWILIO_MESSAGING_SID= os.environ.get("TWILIO_MESSAGING_SID")   # e.g. MGxxxxxxxx...
    TWILIO_VERIFY_SID   = os.environ.get("TWILIO_VERIFY_SID")      # only if you use Verify

    TWILIO_ENABLED = bool(TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and (TWILIO_FROM or TWILIO_MESSAGING_SID))


class ProductionConfig(Config):
    DEBUG = False
    SECRET_KEY = os.environ.get("SECRET_KEY")


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = os.environ.get("TEST_DATABASE_URL", "sqlite:///:memory:")
