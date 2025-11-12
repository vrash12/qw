# utils/mail.py
import os
import smtplib
import ssl
from email.message import EmailMessage

BREVO_HOST = os.getenv("BREVO_SMTP_HOST", "smtp-relay.brevo.com")
BREVO_PORT = int(os.getenv("BREVO_SMTP_PORT", "587"))

# Must be the exact “Login” shown in Brevo’s SMTP page (looks like 9b...@smtp-brevo.com)
BREVO_LOGIN = os.getenv("BREVO_SMTP_LOGIN")
# The SMTP key you just generated in Brevo
BREVO_PASSWORD = os.getenv("BREVO_SMTP_PASSWORD")

# Must be a verified sender in Brevo (your Gmail sender is fine if it’s verified)
MAIL_FROM = os.getenv("MAIL_FROM", "PGT <pgtmanagement045@gmail.com>")

def _need(name: str, val: str | None) -> str:
    if not val:
        raise RuntimeError(f"Missing required env var: {name}")
    return val

def send_email(*, to: str, subject: str, html: str = "", text: str = "") -> None:
    login = _need("BREVO_SMTP_LOGIN", BREVO_LOGIN)
    password = _need("BREVO_SMTP_PASSWORD", BREVO_PASSWORD)

    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(text or "")
    if html:
        msg.add_alternative(html, subtype="html")

    try:
        with smtplib.SMTP(BREVO_HOST, BREVO_PORT, timeout=20) as s:
            s.ehlo()
            s.starttls(context=ssl.create_default_context())
            s.ehlo()
            s.login(login, password)
            s.send_message(msg)
    except smtplib.SMTPAuthenticationError as e:
        raise RuntimeError(
            f"SMTP auth failed (check BREVO_SMTP_LOGIN/ BREVO_SMTP_PASSWORD). Server said: {e.smtp_error!r}"
        ) from e
