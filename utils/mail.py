# utils/mail.py
import os
import smtplib
import ssl
import socket
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from typing import Optional

# Load .env in local/dev; harmless on Cloud Run if python-dotenv isn't present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

__all__ = ["send_email"]

MNL_TZ = timezone(timedelta(hours=8))
_PORT_PLAN = [("STARTTLS", 587), ("STARTTLS", 2525), ("SSL", 465)]

def _mask(s: Optional[str]) -> str:
    if not s:
        return ""
    if "@" in s:
        user, dom = s.split("@", 1)
        return f"{user[:1]}***@{dom[:1]}***"
    return (s[:6] + "…") if len(s) > 6 else s

def _to_utc(dt: datetime) -> datetime:
    # Kept for parity; handy if you ever need timestamp normalization
    if dt.tzinfo:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=MNL_TZ).astimezone(timezone.utc)

def send_email(*, to: str, subject: str, html: str = "", text: str = "") -> None:
    """
    Sends an email via Brevo/Sendinblue SMTP.
    Required env vars (set in Cloud Run UI or via Secret Manager):
      - BREVO_LOGIN       (e.g. 9b....@smtp-brevo.com)
      - BREVO_PASSWORD    (xsmtpsib-... key; use Secret Manager)
      - MAIL_FROM         (e.g. 'PGT <pgtmanagement045@gmail.com>')
    Optional:
      - BREVO_HOST        (default: smtp-relay.brevo.com)
    """
    host      = os.getenv("BREVO_HOST", "smtp-relay.brevo.com")
    login     = os.getenv("BREVO_LOGIN")
    password  = os.getenv("BREVO_PASSWORD")
    mail_from = os.getenv("MAIL_FROM")

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

    last_err: Optional[Exception] = None

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
                    s.ehlo()
                    s.starttls(context=ctx)
                    s.ehlo()
                    s.login(login, password)
                    s.send_message(msg)

            print(f"[mail] sent via {host}:{port} as {_mask(login)} from {_mask(mail_from)} to {_mask(to)}")
            return
        except (smtplib.SMTPAuthenticationError, smtplib.SMTPException, OSError, socket.error) as e:
            last_err = e
            print(f"[mail] attempt {mode} {host}:{port} failed: {e!r}")

    raise RuntimeError(f"All SMTP attempts failed; last error: {last_err!r}")

if __name__ == "__main__":
    # Optional local smoke test:
    to = os.getenv("TEST_TO")
    if not to:
        print("Set TEST_TO to try a local send, e.g. TEST_TO=you@example.com")
    else:
        send_email(
            to=to,
            subject="SMTP smoke test",
            text="Hello from the app (SMTP).",
            html="<strong>Hello</strong> from the app (SMTP).",
        )
        print("OK")
