# utils/mail.py
import smtplib
import ssl
import socket
from email.message import EmailMessage

# ----------------------------
# HARDCODED SMTP SETTINGS
# ----------------------------
# Brevo (Sendinblue) SMTP relay host
BREVO_HOST = "smtp-relay.brevo.com"

# Your exact “Login” from Brevo's SMTP page (looks like 9b....@smtp-brevo.com)
BREVO_LOGIN = "9b5024001@smtp-brevo.com"  # e.g., "9b5024001@smtp-brevo.com"

# Your generated SMTP key from Brevo (starts with xsmtpsib-...)
BREVO_PASSWORD = "xsmtpsib-06d1207a556fb0d49e6b194b1a7e96fec14b37ac9b89ef6723af6b00fa97097b-S2NbGwMXfw29ZPrQ"  # e.g., "xsmtpsib-xxxxxxxx..."

# The FROM address must be verified in Brevo
MAIL_FROM = "PGT <pgtmanagement045@gmail.com>"

# Try common SMTP ports. 587/2525 use STARTTLS; 465 uses SSL.
_PORT_PLAN = [
    ("STARTTLS", 587),
    ("STARTTLS", 2525),
    ("SSL",      465),
]

# ----------------------------
# Implementation
# ----------------------------
def _mask(s: str) -> str:
    if not s:
        return s
    if "@" in s:
        user, dom = s.split("@", 1)
        return (user[:1] + "***@" + dom[:1] + "***")
    return s[:6] + "…"

def send_email(*, to: str, subject: str, html: str = "", text: str = "") -> None:
    if not BREVO_LOGIN or "REPLACE_WITH" in BREVO_LOGIN:
        raise RuntimeError("BREVO_LOGIN is not set in code.")
    if not BREVO_PASSWORD or "REPLACE_WITH" in BREVO_PASSWORD:
        raise RuntimeError("BREVO_PASSWORD is not set in code.")
    if not MAIL_FROM:
        raise RuntimeError("MAIL_FROM is empty. Use a verified sender address.")

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
                    s.ehlo()
                    s.starttls(context=ctx)
                    s.ehlo()
                    s.login(BREVO_LOGIN, BREVO_PASSWORD)
                    s.send_message(msg)

            print(f"[mail] sent via {BREVO_HOST}:{port} as {_mask(BREVO_LOGIN)} from {_mask(MAIL_FROM)} to {_mask(to)}")
            return  # success

        except (smtplib.SMTPAuthenticationError, smtplib.SMTPException, OSError, socket.error) as e:
            last_err = e
            print(f"[mail] attempt {mode} {BREVO_HOST}:{port} failed: {e!r}")

    raise RuntimeError(f"All SMTP attempts failed; last error: {last_err!r}")

# ----------------------------
# Quick local/manual test
# ----------------------------
if __name__ == "__main__":
    # Change this to your inbox to smoke-test from the server:
    TEST_TO = "your.address@example.com"
    try:
        send_email(
            to=TEST_TO,
            subject="SMTP smoke test",
            text="Hello from the app (SMTP in-code).",
            html="<strong>Hello</strong> from the app (SMTP in-code).",
        )
        print("OK: message dispatched")
    except Exception as e:
        print("ERROR:", e)
