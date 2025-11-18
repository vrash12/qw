#!/usr/bin/env python3
"""
Seed 7 PAO and 7 Driver users using SQLAlchemy and the same DATABASE_URL
style as backend/config.py.

Env:
  DATABASE_URL (defaults to the one from your config.py)
  PAO_PASSWORD (default: pao12345)
  DRV_PASSWORD (default: drv12345)

Usage:
  pip install SQLAlchemy PyMySQL werkzeug
  python seed_staff_sqlalchemy.py
"""
import os
import sys
from typing import Dict, List, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

try:
    from werkzeug.security import generate_password_hash
    _HAS_WERKZEUG = True
except Exception:
    _HAS_WERKZEUG = False

DEFAULT_DBURL = 'mysql+pymysql://u782952718_eee:Vanrodolf123.@srv667.hstgr.io/u782952718_eee'
DATABASE_URL = os.environ.get('DATABASE_URL', DEFAULT_DBURL)

PAO_PASSWORD = os.environ.get('PAO_PASSWORD', 'pao12345')
DRV_PASSWORD = os.environ.get('DRV_PASSWORD', 'drv12345')

PAO_USERS: List[Dict] = [
    {"first_name": "Arvin",   "last_name": "Santos",    "username": "pao4",  "phone_number": "09171110001"},
    {"first_name": "Jessa",   "last_name": "Dela Cruz", "username": "pao5",  "phone_number": "09171110002"},
    {"first_name": "Mark",    "last_name": "Reyes",     "username": "pao6",  "phone_number": "09171110003"},
    {"first_name": "Katrina", "last_name": "Mendoza",   "username": "pao7",  "phone_number": "09171110004"},
    {"first_name": "Jerome",  "last_name": "Garcia",    "username": "pao8",  "phone_number": "09171110005"},
    {"first_name": "Aubrey",  "last_name": "Ramos",     "username": "pao9",  "phone_number": "09171110006"},
    {"first_name": "Noel",    "last_name": "Bautista",  "username": "pao10", "phone_number": "09171110007"},
]

DRV_USERS: List[Dict] = [
    {"first_name": "Joel",     "last_name": "Dizon",      "username": "drv01", "phone_number": "09172220001"},
    {"first_name": "Rogelio",  "last_name": "Cruz",       "username": "drv02", "phone_number": "09172220002"},
    {"first_name": "Liza",     "last_name": "Navarro",    "username": "drv03", "phone_number": "09172220003"},
    {"first_name": "Maricar",  "last_name": "Velasco",    "username": "drv04", "phone_number": "09172220004"},
    {"first_name": "Nestor",   "last_name": "Aquino",     "username": "drv05", "phone_number": "09172220005"},
    {"first_name": "Ruby",     "last_name": "Sarmiento",  "username": "drv06", "phone_number": "09172220006"},
    {"first_name": "Carlo",    "last_name": "Villanueva", "username": "drv07", "phone_number": "09172220007"},
]

def pw_hash(raw: str) -> str:
    if not _HAS_WERKZEUG:
        raise RuntimeError("werkzeug is required: pip install werkzeug")
    try:
        return generate_password_hash(raw, method="scrypt")
    except Exception:
        return generate_password_hash(raw)

def ensure_driver_enum(engine: Engine) -> bool:
    with engine.begin() as conn:
        row = conn.execute(text(
            "SELECT COLUMN_TYPE FROM information_schema.columns "
            "WHERE table_schema = DATABASE() AND table_name = 'users' AND column_name = 'role'"
        )).mappings().first()
        if not row:
            raise RuntimeError("users.role column not found")
        if "driver" in (row["COLUMN_TYPE"] or ""):
            return False
        conn.execute(text(
            "ALTER TABLE `users` "
            "MODIFY `role` ENUM('commuter','pao','manager','teller','driver') "
            "NOT NULL DEFAULT 'commuter'"
        ))
        return True

def upsert_user(engine: Engine, u: Dict, role: str, password: str) -> Tuple[bool, int]:
    with engine.begin() as conn:
        # check if exists
        r = conn.execute(
            text("SELECT id FROM users WHERE username=:u OR phone_number=:p LIMIT 1"),
            {"u": u["username"], "p": u["phone_number"]},
        ).mappings().first()
        if r:
            return False, int(r["id"])
        ph = pw_hash(password)
        res = conn.execute(text("""
            INSERT INTO users
            (first_name,last_name,username,phone_number,password_hash,role,assigned_bus_id,created_at,updated_at)
            VALUES (:fn,:ln,:un,:phn,:phash,:role,NULL,NOW(),NOW())
        """), {
            "fn": u["first_name"], "ln": u["last_name"], "un": u["username"],
            "phn": u["phone_number"], "phash": ph, "role": role
        })
        new_id = res.lastrowid
        return True, int(new_id)

def main():
    print("Connecting via:", DATABASE_URL.split('@')[-1])  # avoid echoing credentials
    engine = create_engine(DATABASE_URL, pool_pre_ping=True, pool_recycle=180, future=True)

    altered = ensure_driver_enum(engine)
    print("✅ Added 'driver' to users.role" if altered else "ℹ️ 'driver' already in users.role")

    created = {"pao": 0, "driver": 0}
    ids: List[Tuple[str, str, int]] = []

    for u in PAO_USERS:
        ok, uid = upsert_user(engine, u, "pao", PAO_PASSWORD)
        created["pao"] += 1 if ok else 0
        ids.append(("pao", u["username"], uid))

    for u in DRV_USERS:
        ok, uid = upsert_user(engine, u, "driver", DRV_PASSWORD)
        created["driver"] += 1 if ok else 0
        ids.append(("driver", u["username"], uid))

    print("\nSummary:")
    print(f"  PAO created:    {created['pao']} / {len(PAO_USERS)}")
    print(f"  Driver created: {created['driver']} / {len(DRV_USERS)}")
    print("\nUsers:")
    for role, uname, uid in ids:
        print(f"  [{role:6}] id={uid:<4} username={uname}")

if __name__ == "__main__":
    try:
        main()
    except SQLAlchemyError as e:
        print("SQLAlchemy ERROR:", e)
        sys.exit(2)
    except Exception as e:
        print("ERROR:", e)
        sys.exit(1)
