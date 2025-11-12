# utils/mail.py
import os
import smtplib
import ssl
import socket
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta

MNL_TZ = timezone(timedelta(hours=8))

BREVO_HOST = os.getenv("BREVO_HOST", "smtp-relay.brevo.com")
BREVO_LOGIN = os.getenv("BREVO_LOGIN")       # e.g. "9b5024001@smtp-brevo.com"
BREVO_PASSWORD = os.getenv("BREVO_PASSWORD") # your new xsmtpsib-… key
MAIL_FROM = os.getenv("MAIL_FROM")           # e.g. "PGT <pgtmanagement045@gmail.com>"

_PORT_PLAN = [("STARTTLS", 587), ("STARTTLS", 2525), ("SSL", 465)]

def _mask(s: str) -> str:
    if not s: return s
    if "@" in s:
        user, dom = s.split("@", 1)
        return (user[:1] + "***@" + dom[:1] + "***")
    return s[:6] + "…"

def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=MNL_TZ).astimezone(timezone.utc)

def send_email(*, to: str, subject: str, html: str = "", text: str = "") -> None:
    if not BREVO_LOGIN:
        raise RuntimeError("BREVO_LOGIN env var is not set.")
    if not BREVO_PASSWORD:
        raise RuntimeError("BREVO_PASSWORD env var is not set.")
    if not MAIL_FROM:
        raise RuntimeError("MAIL_FROM env var is not set.")

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(text or "")
    if html:
        msg.add_alternative(html, subtype="html")

    last_err = None
    for mode, port in _PORT_PLAN:
        try:
            if mode == "SSL":
                ctx = ssl.create_default_context()
                with smtplib.SMTP_SSL(BREVO_HOST, port, context=ctx, timeout=20) as s:
                    s.login(BREVO_LOGIN, BREVO_PASSWORD)
                    s.send_message(msg)
            else:
                ctx = ssl.create_default_context()
                with smtplib.SMTP(BREVO_HOST, port, timeout=20) as s:
                    s.ehlo(); s.starttls(context=ctx); s.ehlo()
                    s.login(BREVO_LOGIN, BREVO_PASSWORD)
                    s.send_message(msg)
            print(f"[mail] sent via {BREVO_HOST}:{port} as {_mask(BREVO_LOGIN)} from {_mask(MAIL_FROM)} to {_mask(to)}")
            return
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPException, OSError, socket.error) as e:
            last_err = e
            print(f"[mail] attempt {mode} {BREVO_HOST}:{port} failed: {e!r}")
    raise RuntimeError(f"All SMTP attempts failed; last error: {last_err!r}")
