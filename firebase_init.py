# firebase_init.py
import os, logging
from pathlib import Path
import firebase_admin
from firebase_admin import credentials

# 1) Prefer explicit env var
sa_path = os.environ.get("FIREBASE_SA_PATH")

# 2) Fallback: resolve relative to this file (works regardless of cwd)
if not sa_path:
    here = Path(__file__).resolve().parent
    candidate = here / "etc" / "secrets" / "firebase-sa.json"
    sa_path = str(candidate)

# 3) Validate and log what we’re using
p = Path(sa_path)
if not p.is_file():
    logging.warning(
        "[firebase] WARNING: FIREBASE_SA_PATH not set or file missing (wanted: %s; cwd=%s)",
        sa_path, os.getcwd()
    )
    raise FileNotFoundError(f"Firebase service account JSON not found: {sa_path}")

# 4) Initialize once
try:
    firebase_admin.get_app()
except ValueError:
    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)
