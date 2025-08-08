# utils/qr.py
from datetime import datetime

def build_qr_payload(ticket) -> str:
    """
    Produce a pipe-separated payload that contains everything a scanner
    would need.  Feel free to change the fields / order later.
      PGT|<REF>|<TYPE>|<FARE>|<ISO8601>|<PAID 0/1>
    """
    return  "PGT|" \
          + ticket.reference_no + "|" \
          + ticket.passenger_type + "|" \
          + f"{float(ticket.price):.2f}|" \
          + ticket.created_at.replace(tzinfo=None).isoformat(timespec="seconds") + "|" \
          + ("1" if ticket.paid else "0")
