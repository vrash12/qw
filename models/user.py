from db import db
from werkzeug.security import generate_password_hash, check_password_hash

class User(db.Model):
    __tablename__ = 'users'

    id            = db.Column(db.Integer, primary_key=True)
    first_name    = db.Column(db.String(64), nullable=False)
    last_name     = db.Column(db.String(64), nullable=False)
    username      = db.Column(db.String(64), unique=True, nullable=False)
    phone_number  = db.Column(db.String(32), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    role = db.Column(db.Enum("commuter", "pao", "manager"), nullable=False, default="commuter")
    
    # ... (assigned_bus_id and assigned_bus relationship are correct)
    assigned_bus_id = db.Column(db.Integer, db.ForeignKey("buses.id"))
    assigned_bus    = db.relationship("Bus", back_populates="pao", uselist=False)

    # ... (ticket_sales relationship is correct)
    ticket_sales = db.relationship('TicketSale', back_populates='user', cascade='all, delete-orphan')


    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)
    # ─── helpers ───────────────────────────────────────────────
    @property
    def is_pao(self):
        return self.role == "pao"

    @property
    def is_manager(self):
        return self.role == "manager"
    @property
    def is_pao(self):
        return self.role == 'pao'

    @property
    def is_manager(self):
        return self.role == 'manager'
    
    ticket_sales = db.relationship(
        'TicketSale',
        back_populates='user',
        cascade='all, delete-orphan'
    )