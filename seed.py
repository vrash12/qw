#!/usr/bin/env python3
# scripts/seed_teller.py

from werkzeug.security import generate_password_hash
from app import create_app      # ← replace with your actual app factory import
from db import db
from models.user import User

# Teller account definition
TELLER_USERNAME = "teller"
TELLER_PASSWORD = "password"

def seed_teller():
    """
    Creates or updates a Teller user in the database.

    This script ensures a user with the username 'teller' exists, has the role
    'teller', and has their password set to 'password'. It can be run multiple
    times without causing errors.
    """
    app = create_app()
    with app.app_context():
        # Check if the teller user already exists
        user = User.query.filter_by(username=TELLER_USERNAME).first()

        if not user:
            # If the user doesn't exist, create a new one.
            user = User(
                first_name="Teller",
                last_name="Account",
                username=TELLER_USERNAME,
                phone_number="09170008355", # Using 8355 for "TELL"
                role="teller",
                # assigned_bus_id is left as NULL by default
            )
            user.password_hash = generate_password_hash(TELLER_PASSWORD)
            db.session.add(user)
            print(f"➕ Created Teller account `{TELLER_USERNAME}`.")
        else:
            # If the user already exists, update their role and password to
            # ensure they are correctly configured.
            user.role = "teller"
            user.password_hash = generate_password_hash(TELLER_PASSWORD)
            print(f"🔄 Updated Teller account `{TELLER_USERNAME}` with a fresh password.")

        db.session.commit()
        print("✅ Seeded the Teller account successfully.")

if __name__ == "__main__":
    seed_teller()
