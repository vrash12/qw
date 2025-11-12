# utils/mail.py
import os
import smtplib
import ssl
import socket
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta

# NEW: load .env when this module is imported
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads .env from the current working dir
except Exception:
    pass

MNL_TZ = timezone(timedelta(hours=8))

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
    # READ ENV **AT CALL TIME** (so .env or container env is respected)
    host        = os.getenv("BREVO_HOST", "smtp-relay.brevo.com")
    login       = os.getenv("BREVO_LOGIN")
    password    = os.getenv("BREVO_PASSWORD")
    mail_from   = os.getenv("MAIL_FROM")

    if not login:
        raise RuntimeError("BREVO_LOGIN env var is not set.")
    if not password:
        raise RuntimeError("BREVO_PASSWORD env var is not set.")
    if not mail_from:
        raise RuntimeError("MAIL_FROM env var is not set.")

    msg = EmailMessage()
    msg["From"] = mail_from
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
                with smtplib.SMTP_SSL(host, port, context=ctx, timeout=20) as s:
                    s.login(login, password)
                    s.send_message(msg)
            else:
                ctx = ssl.create_default_context()
                with smtplib.SMTP(host, port, timeout=20) as s:
                    s.ehlo(); s.starttls(context=ctx); s.ehlo()
                    s.login(login, password)
                    s.send_message(msg)
            print(f"[mail] sent via {host}:{port} as {_mask(login)} from {_mask(mail_from)} to {_mask(to)}")
            return
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPException, OSError, socket.error) as e:
            last_err = e
            print(f"[mail] attempt {mode} {host}:{port} failed: {e!r}")
    raise RuntimeError(f"All SMTP attempts failed; last error: {last_err!r}")
