# backend/mailer.py
from __future__ import annotations
import os, smtplib, ssl
from email.message import EmailMessage

SMTP_HOST = os.getenv("SMTP_HOST", "smtp-relay.brevo.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")            # 9b5024001@smtp-brevo.com
SMTP_PASS = os.getenv("SMTP_PASS")            # 1W4b5m9vqHAS7NBd
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER or "no-reply@example.com")
FROM_NAME  = os.getenv("FROM_NAME", "PGT Transport")

def send_email(to: str, subject: str, html: str, text: str | None = None) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to
    if text:
        msg.set_content(text)
    msg.add_alternative(html, subtype="html")

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as s:
        s.starttls(context=ctx)
        if SMTP_USER and SMTP_PASS:
            s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)
