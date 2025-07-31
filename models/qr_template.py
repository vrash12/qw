from db import db
from datetime import datetime

class QRTemplate(db.Model):
    __tablename__ = 'qr_templates'

    id               = db.Column(db.Integer, primary_key=True)
    file_path        = db.Column(db.String(256), nullable=False)
    price            = db.Column(db.Numeric(10, 2), nullable=False)

    # this is critical: declare the FK here
    fare_segment_id  = db.Column(
                         db.Integer,
                         db.ForeignKey('fare_segments.id', ondelete='CASCADE'),
                         nullable=False
                       )

    created_at       = db.Column(db.DateTime, default=datetime.utcnow)

    # and tell SQLAlchemy explicitly which column to join on
    fare_segment     = db.relationship(
                         'FareSegment',
                         foreign_keys=[fare_segment_id],
                         back_populates='qr_templates'
                       )
