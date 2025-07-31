#!/usr/bin/env python3
# scripts/seed_pao.py

from werkzeug.security import generate_password_hash
from app import create_app      # ← replace with your actual app factory import
from db import db
from models.user import User
from models.bus  import Bus

# PAO account definitions: (username, phone_number_suffix, bus_id)
PAO_DEFINITIONS = [
    ("pao1", "01", 1),
    ("pao2", "02", 2),
    ("pao3", "03", 3),
]

def seed_pao():
    app = create_app()
    with app.app_context():
        for username, suffix, bus_id in PAO_DEFINITIONS:
            # 1) Ensure the Bus row exists
            bus = Bus.query.get(bus_id)
            if not bus:
                bus = Bus(
                    id=bus_id,
                    identifier=f"PGT-00{bus_id}",
                    capacity=30,
                    description=f"Demo Route Bus {bus_id}"
                )
                db.session.add(bus)
                db.session.flush()  # get bus.id

            # 2) Upsert the PAO user
            user = User.query.filter_by(username=username).first()
            if not user:
                user = User(
                    first_name="PAO",
                    last_name=suffix,
                    username=username,
                    phone_number=f"0917{suffix}00000",
                    role="pao",
                    assigned_bus_id=bus.id
                )
                user.password_hash = generate_password_hash("password")
                db.session.add(user)
                print(f"➕ Created PAO `{username}` for bus {bus_id}")
            else:
                # update existing
                user.role = "pao"
                user.assigned_bus_id = bus.id
                user.password_hash = generate_password_hash("password")
                print(f"🔄 Updated PAO `{username}` to bus {bus_id}")

        db.session.commit()
        print("✅ Seeded all PAO accounts.")

if __name__ == "__main__":
    seed_pao()
