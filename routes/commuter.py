# routes/commuter.py
from __future__ import annotations
import datetime as dt
from typing import Any, Dict, List, Optional

from flask import (
    Blueprint, request, jsonify, g, current_app, url_for,
    redirect, send_file, make_response
)
from sqlalchemy import func, text
from sqlalchemy.orm import joinedload
from models.wallet import WalletAccount, WalletLedger, TopUp
from utils.wallet_qr import build_wallet_token
from db import db
from routes.auth import require_role
from models.schedule import Trip, StopTime
from models.sensor_reading import SensorReading
from models.announcement import Announcement
from models.ticket_sale import TicketSale
from models.bus import Bus
from models.user import User
from models.ticket_stop import TicketStop
from models.device_token import DeviceToken
from utils.qr import build_qr_payload
from utils.push import push_to_user
from io import BytesIO
from PIL import Image, ImageDraw, ImageFont
import qrcode
import traceback
from werkzeug.exceptions import HTTPException
from datetime import timedelta
from sqlalchemy.exc import OperationalError

from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

try:
    from zoneinfo import ZoneInfo
    try:
        LOCAL_TZ = ZoneInfo("Asia/Manila")
    except Exception:
        LOCAL_TZ = dt.timezone(dt.timedelta(hours=8))
except Exception:
    LOCAL_TZ = dt.timezone(dt.timedelta(hours=8))

commuter_bp = Blueprint("commuter", __name__, url_prefix="/commuter")

THEMES = {
    "light": {
        "bg": (248, 250, 248),
        "card": (255, 255, 255),
        "text": (28, 32, 28),
        "subtle": (102, 114, 102),
        "brand": (16, 122, 82),
        "muted": (160, 168, 160),
        "line": (226, 232, 226),
        "accent": (20, 164, 108),
        "qr_bg": (245, 247, 245),
        "ribbon": (20, 164, 108),
    },
    "dark": {
        "bg": (18, 18, 18),
        "card": (28, 28, 28),
        "text": (240, 240, 240),
        "subtle": (189, 195, 199),
        "brand": (66, 194, 133),
        "muted": (120, 120, 120),
        "line": (52, 52, 52),
        "accent": (66, 194, 133),
        "qr_bg": (36, 36, 36),
        "ribbon": (50, 160, 110),
    },
}

def _as_local(dt_obj: dt.datetime) -> dt.datetime:
    """Convert naive (assumed UTC) or aware datetime to LOCAL_TZ."""
    if dt_obj is None:
        return dt.datetime.now(LOCAL_TZ)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(LOCAL_TZ)

def _debug_enabled() -> bool:
    return (request.args.get("debug") or request.headers.get("X-Debug") or "").lower() in {"1","true","yes"}

# ──────────────────────────────────────────────────────────────────────────────
# Wallet schema autodetect (supports *_pesos or legacy *_cents while migrating)
# ──────────────────────────────────────────────────────────────────────────────
def _has_column(table: str, column: str) -> bool:
    try:
        row = db.session.execute(
            text("""
                SELECT 1
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t AND COLUMN_NAME = :c
                LIMIT 1
            """),
            {"t": table, "c": column},
        ).first()
        if row:
            return True
    except Exception:
        pass
    try:
        row = db.session.execute(text(f"SHOW COLUMNS FROM {table} LIKE :c"), {"c": column}).first()
        return bool(row)
    except Exception:
        return False

def _wallet_cols() -> Dict[str, str]:
    """
    Returns the actual column names present in DB:
      - balance: 'balance_pesos' or 'balance_cents'
      - amount:  'amount_pesos'  or 'amount_cents'
      - running: 'running_balance_pesos' or 'running_balance_cents'
      - topup_amount: 'amount_pesos' or 'amount_cents'
    """
    bal = "balance_pesos" 
    amt = "amount_pesos" 
    run = "running_balance_pesos"
    tup = "amount_pesos"
    return {"balance": bal, "amount": amt, "running": run, "topup_amount": tup}

def _to_pesos_from_db(value: Optional[int], came_from: str) -> int:
    """
    Convert DB value to whole pesos. If column is *_cents, convert 100c == 1 peso.
    If it is *_pesos already, just coerce to int.
    """
    if value is None:
        return 0
    cols = _wallet_cols()
    if came_from.endswith("_cents"):
        return int(round(int(value) / 100.0))
    return int(value)

# ──────────────────────────────────────────────────────────────────────────────

SALT_USER_QR = "user-qr-v1"

def _user_qr_sign(uid: int) -> str:
    s = URLSafeTimedSerializer(current_app.config["SECRET_KEY"], salt=SALT_USER_QR)
    return s.dumps({"uid": int(uid)})

@commuter_bp.route("/users/me/qr.png", methods=["GET"])
@require_role("commuter")
def commuter_my_wallet_qr_png():
    """
    Returns a PNG QR for the logged-in commuter.
    The QR encodes a URL to /pao/users/scan?token=...
    """
    size = max(240, min(1024, int(request.args.get("size", 360) or 360)))
    token = _user_qr_sign(g.user.id)
    scan_url = url_for("pao.user_qr_scan", _external=True) + f"?token={token}"

    qr = qrcode.QRCode(box_size=10, border=2)
    qr.add_data(scan_url)
    qr.make(fit=True)
    out = qr.make_image(fill_color="black", back_color="white").convert("RGB")
    out = out.resize((size, size))

    bio = BytesIO()
    out.save(bio, format="PNG", optimize=True)
    bio.seek(0)

    resp = make_response(send_file(bio, mimetype="image/png"))
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

@commuter_bp.route("/notify-test", methods=["POST"])
@require_role("commuter")
def commuter_notify_test():
    ok = push_to_user(
        db, DeviceToken, g.user.id,
        "🔔 Test notification",
        "If you see this, push is working!",
        {"deeplink": "/commuter/notifications"},
        channelId="announcements", priority="high", ttl=600,
    )
    return jsonify(ok=ok), (200 if ok else 202)

@commuter_bp.route("/tickets/<int:ticket_id>/image.jpg", methods=["GET"])
def commuter_ticket_image(ticket_id: int):
    """
    High-contrast, LARGE-TYPE JPG receipt renderer.
    Optional: ?download=1 to force download.
    """
    t = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.bus).joinedload(Bus.pao),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Resolve stop names (StopTime or TicketStop fallback)
    if t.origin_stop_time:
        origin_name = t.origin_stop_time.stop_name
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin_name = ts.stop_name if ts else ""

    if t.destination_stop_time:
        destination_name = t.destination_stop_time.stop_name
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination_name = tsd.stop_name if tsd else ""

    issuer_via_field = getattr(t, "issued_by", None)
    issuer_via_bus   = getattr(getattr(getattr(t, "bus", None), "pao", None), "id", None)
    issuer_id        = issuer_via_field or issuer_via_bus

    try:
        current_app.logger.info(
            "[receipt:image] ticket_id=%s ref=%s price=%.2f paid=%s "
            "issued_by_field=%s bus.pao.id=%s chosen_issuer=%s",
            t.id, t.reference_no, float(t.price or 0), bool(t.paid),
            issuer_via_field, issuer_via_bus, issuer_id
        )
    except Exception:
        pass

    # QR that points back to THIS image URL
    img_link = url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True)
    qr = qrcode.QRCode(box_size=12, border=2)
    qr.add_data(img_link)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    # Canvas + styles
    W, H = 1440, 2200
    M = 80
    DARK_GREEN    = (21, 87, 36)
    LIGHT_GREEN   = (236, 248, 239)
    ACCENT_GREEN  = (34, 139, 58)
    TEXT_DARK     = (33, 37, 41)
    TEXT_MEDIUM   = (73, 80, 87)
    TEXT_MUTED    = (108, 117, 125)
    BORDER_LIGHT  = (222, 226, 230)
    WHITE         = (255, 255, 255)
    BG_PAPER      = (250, 251, 252)
    SUCCESS_BG    = (212, 237, 218)
    SUCCESS_TEXT  = (21, 87, 36)
    ERROR_BG      = (248, 215, 218)
    ERROR_TEXT    = (114, 28, 36)

    bg = Image.new("RGB", (W, H), BG_PAPER)
    draw = ImageDraw.Draw(bg)

    def _safe_font(candidates, size):
        for candidate in candidates:
            try:
                return ImageFont.truetype(candidate, size)
            except Exception:
                continue
        return ImageFont.load_default()

    ft_title   = _safe_font(["Arial Bold", "DejaVuSans-Bold.ttf", "arial.ttf"], 80)
    ft_header  = _safe_font(["Arial Bold", "DejaVuSans-Bold.ttf", "arial.ttf"], 60)
    ft_label   = _safe_font(["Arial", "DejaVuSans.ttf", "arial.ttf"], 40)
    ft_value   = _safe_font(["Arial Bold", "DejaVuSans-Bold.ttf", "arial.ttf"], 52)
    ft_big     = _safe_font(["Arial Black", "DejaVuSans-Bold.ttf", "arialbd.ttf"], 96)
    ft_medium  = _safe_font(["Arial", "DejaVuSans.ttf", "arial.ttf"], 44)
    ft_small   = _safe_font(["Arial", "DejaVuSans.ttf", "arial.ttf"], 34)

    def tw(text, font):
        if not font or not text:
            return 0
        try:
            return draw.textlength(text, font=font)
        except Exception:
            return len(text) * 10

    def ellipsize(s: str, max_chars: int) -> str:
        if len(s) <= max_chars:
            return s
        keep = max(8, max_chars // 2 - 1)
        return s[:keep] + "…" + s[-(max_chars - keep - 1):]

    # Card & header
    shadow_offset = 10
    draw.rectangle((M + shadow_offset, M + shadow_offset, W - M + shadow_offset, H - M + shadow_offset), fill=(0, 0, 0))
    draw.rectangle((M, M, W - M, H - M), fill=WHITE, outline=BORDER_LIGHT, width=2)

    y = M + 40
    header_h = 180
    draw.rectangle((M, y, W - M, y + header_h), fill=LIGHT_GREEN, outline=DARK_GREEN, width=3)
    if ft_title:
        draw.text((M + 48, y + (header_h - 80) // 2), "PGT Onboard — Official Receipt", fill=DARK_GREEN, font=ft_title)
    y += header_h + 12
    draw.rectangle((M + 48, y, W - M - 48, y + 5), fill=ACCENT_GREEN)
    y += 40

    # fields
    L = M + 48
    R = W - M - 48
    COL_GAP = 60
    COL_W = (R - L - COL_GAP) // 2

    def field(x, y, label, value, color=TEXT_DARK):
        if ft_label:
            draw.text((x, y), label.upper(), fill=TEXT_MUTED, font=ft_label)
        y2 = y + 54
        display_value = value if tw(value, ft_value) <= COL_W else ellipsize(value, 36)
        if ft_value:
            draw.text((x, y2), display_value, fill=color, font=ft_value)
        return y2 + 58 + 28

    if t.origin_stop_time:
        origin_name = t.origin_stop_time.stop_name
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin_name = ts.stop_name if ts else ""

    if t.destination_stop_time:
        destination_name = t.destination_stop_time.stop_name
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination_name = tsd.stop_name if tsd else ""

    yL = y
    yR = y
    passenger_name = f"{t.user.first_name} {t.user.last_name}" if t.user else "—"
    yL = field(L, yL, "Reference No.", t.reference_no or "—")
    yR = field(L + COL_W + COL_GAP, yR, "Destination", destination_name or "—")

    date_str = t.created_at.strftime('%B %d, %Y')
    time_str = t.created_at.strftime('%I:%M %p').lstrip('0').lower()
    if ft_label:
        draw.text((L, yL), "DATE & TIME", fill=TEXT_MUTED, font=ft_label)
    y_value = yL + 54
    if ft_value:
        d_disp = date_str if tw(date_str, ft_value) <= COL_W else ellipsize(date_str, 36)
        draw.text((L, y_value), d_disp, fill=TEXT_DARK, font=ft_value)
        t_disp = time_str if tw(time_str, ft_value) <= COL_W else ellipsize(time_str, 36)
        draw.text((L, y_value + 52), t_disp, fill=TEXT_DARK, font=ft_value)
    yL = y_value + 52 + 60

    yR = field(L + COL_W + COL_GAP, yR, "Passenger Type", (t.passenger_type or "").title() or "—")
    yL = field(L, yL, "Origin", origin_name or "—")
    yR = field(L + COL_W + COL_GAP, yR, "Passenger", passenger_name)

    y = max(yL, yR) + 20
    draw.rectangle((L, y, R, y + 4), fill=BORDER_LIGHT)
    y += 48

    # amount + pill (NO CENTS)
    amount_y = y
    if ft_label:
        draw.text((L, amount_y), "TOTAL AMOUNT", fill=TEXT_MUTED, font=ft_label)
    if ft_big:
        fare_pesos = int(round(float(t.price or 0)))
        draw.text((L, amount_y + 44), f"₱{fare_pesos}", fill=ACCENT_GREEN, font=ft_big)

    state_txt = "PAID" if t.paid else "UNPAID"
    state_bg = SUCCESS_BG if t.paid else ERROR_BG
    state_text_color = SUCCESS_TEXT if t.paid else ERROR_TEXT

    if ft_header:
        pill_w = int(tw(state_txt, ft_header) + 64)
        pill_h = 76
        pill_x1 = R - pill_w
        pill_y1 = amount_y + 8
        draw.rectangle((pill_x1 + 14, pill_y1, pill_x1 + pill_w - 14, pill_y1 + pill_h), fill=state_bg)
        draw.rectangle((pill_x1, pill_y1 + 14, pill_x1 + pill_w, pill_y1 + pill_h - 14), fill=state_bg)
        text_x = pill_x1 + (pill_w - tw(state_txt, ft_header)) // 2
        text_y = pill_y1 + (pill_h - 60) // 2
        draw.text((text_x, text_y), state_txt, fill=state_text_color, font=ft_header)

    y += 170

    qr_section_bg = (247, 251, 247)
    qr_size = 480
    qr_padding = 36
    panel_w = qr_size + qr_padding * 2
    panel_h = qr_size + qr_padding * 2 + 96

    draw.rectangle((L, y, L + panel_w, y + panel_h), fill=qr_section_bg, outline=BORDER_LIGHT, width=2)
    bg.paste(qr_img.resize((qr_size, qr_size)), (L + qr_padding, y + qr_padding))
    if ft_medium:
        draw.text((L + qr_padding, y + qr_padding + qr_size + 24), "Scan to view/download receipt", fill=TEXT_MEDIUM, font=ft_medium)

    right_x = L + panel_w + 56
    right_y = y + 24
    if ft_label:
        draw.text((right_x, right_y), "PAYMENT STATUS", fill=TEXT_MUTED, font=ft_label)
    if ft_header:
        draw.text((right_x, right_y + 44), state_txt, fill=(ACCENT_GREEN if t.paid else ERROR_TEXT), font=ft_header)

    info_y = right_y + 140
    info_items = [
        ("Bus ID", str(getattr(t, "bus_id", "") or "—")),
        ("Issued By (PAO ID)", str(issuer_id or "—")),
    ]
    for label, value in info_items:
        if ft_label:
            draw.text((right_x, info_y), label.upper(), fill=TEXT_MUTED, font=ft_label)
        if ft_value:
            draw.text((right_x, info_y + 36), value, fill=TEXT_MEDIUM, font=ft_value)
        info_y += 96

    footer_y = H - M - 72
    if ft_small:
        from datetime import datetime as _dt
        link_display = (img_link[:80] + "…") if len(img_link) > 80 else img_link
        draw.text((L, footer_y), link_display, fill=TEXT_MUTED, font=ft_small)
        draw.text((L, footer_y + 36), _dt.now().strftime("Generated on %B %d, %Y at %I:%M %p"), fill=TEXT_MUTED, font=ft_small)

    bio = BytesIO()
    bg.save(bio, format="JPEG", quality=95, optimize=True)
    bio.seek(0)

    as_download = (request.args.get("download") or "").lower() in {"1", "true", "yes"}
    resp = make_response(
        send_file(
            bio,
            mimetype="image/jpeg",
            as_attachment=as_download,
            download_name=f"receipt_{t.reference_no}.jpg",
        )
    )
    try:
        resp.headers["X-Debug-Issued-By"] = str(issuer_id or "")
        resp.headers["X-Debug-Issued-By-Field"] = str(issuer_via_field or "")
        resp.headers["X-Debug-Issued-By-BusPao"] = str(issuer_via_bus or "")
    except Exception:
        pass
    resp.headers["Cache-Control"] = "public, max-age=86400"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

# ──────────────────────────────────────────────────────────────────────────────
# Wallet helpers that try *_pesos first, then fall back to *_cents on 1054.
# This avoids relying on INFORMATION_SCHEMA and survives mid-migration states.
# ──────────────────────────────────────────────────────────────────────────────

def _is_unknown_col(err: Exception) -> bool:
    # MySQL error code for "Unknown column": 1054
    try:
        return isinstance(err, OperationalError) and getattr(err.orig, "args", [None])[0] == 1054
    except Exception:
        return False

def _get_or_create_wallet_account(user_id: int):
    # PESOS-ONLY: never reference *cents columns
    row = db.session.execute(
        text("""
            SELECT user_id, balance_pesos AS bal, qr_token
            FROM wallet_accounts
            WHERE user_id = :uid
        """),
        {"uid": user_id},
    ).mappings().first()

    if not row:
        # create if missing (idempotent)
        db.session.execute(
            text("""
                INSERT INTO wallet_accounts (user_id, balance_pesos)
                VALUES (:uid, 0)
                ON DUPLICATE KEY UPDATE user_id = user_id
            """),
            {"uid": user_id},
        )
        db.session.commit()
        row = {"user_id": user_id, "bal": 0, "qr_token": None}

    class _Acct: ...
    acct = _Acct()
    acct.user_id = int(row["user_id"])
    acct.balance_pesos = int(row["bal"] or 0)
    acct.qr_token = row.get("qr_token")
    return acct


@commuter_bp.route("/wallet/me", methods=["GET"])
@require_role("commuter")
def wallet_me():
    acct = _get_or_create_wallet_account(g.user.id)
    bal = int(getattr(acct, "balance_pesos", 0) or 0)
    return jsonify(balance_pesos=bal, balance_php=bal), 200
    
@commuter_bp.route("/wallet/ledger", methods=["GET"])
@require_role("commuter")
def wallet_ledger():
    page = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, request.args.get("page_size", type=int, default=5))
    offset = (page - 1) * page_size
    aid = g.user.id

    total = int(
        db.session.execute(
            text("SELECT COUNT(*) FROM wallet_ledger WHERE account_id = :aid"),
            {"aid": aid},
        ).scalar() or 0
    )

    rows = db.session.execute(
        text("""
            SELECT
                id,
                account_id,
                direction,
                event,
                amount_pesos           AS amount_val,
                running_balance_pesos  AS running_val,
                ref_table,
                ref_id,
                created_at
            FROM wallet_ledger
            WHERE account_id = :aid
            ORDER BY id DESC
            LIMIT :lim OFFSET :off
        """),
        {"aid": aid, "lim": page_size, "off": offset},
    ).mappings().all()

    topup_ref_ids = [
        int(r["ref_id"]) for r in rows
        if r.get("ref_table") == "wallet_topups" and r.get("ref_id")
           and (r.get("event") or "").startswith("topup")
    ]
    methods_by_topup_id = {}
    if topup_ref_ids:
        for tid, method in db.session.query(TopUp.id, TopUp.method).filter(TopUp.id.in_(topup_ref_ids)).all():
            methods_by_topup_id[int(tid)] = (method or "cash")

    items = []
    for r in rows:
        items.append({
            "id": int(r["id"]),
            "direction": r["direction"],
            "event": r["event"],
            "amount_pesos": int(r["amount_val"] or 0),
            "running_balance_pesos": int(r["running_val"] or 0),
            "created_at": (r["created_at"].isoformat() if r.get("created_at") else None),
            "method": (
                methods_by_topup_id.get(int(r["ref_id"]))
                if (r.get("ref_table") == "wallet_topups" and r.get("ref_id"))
                else None
            ),
        })

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200


@commuter_bp.route("/wallet/qrcode", methods=["GET"])
@require_role("commuter")
def wallet_qrcode():
    token = build_wallet_token(g.user.id)  # now stable per commuter
    deep_link = f"https://pay.example/charge?wallet_token={token}&autopay=1"
    return jsonify(wallet_token=token, deep_link=deep_link), 200

@commuter_bp.route("/wallet/qrcode/rotate", methods=["POST"])
@require_role("commuter")
def wallet_qrcode_rotate():
    from secrets import token_urlsafe
    new_tok = token_urlsafe(24)

    db.session.execute(
        text("""
            INSERT INTO wallet_accounts (user_id, balance_pesos, qr_token)
            VALUES (:uid, 0, :tok)
            ON DUPLICATE KEY UPDATE qr_token = :tok
        """),
        {"uid": g.user.id, "tok": new_tok},
    )
    db.session.commit()

    deep_link = f"https://pay.example/charge?wallet_token={new_tok}&autopay=1"
    return jsonify(wallet_token=new_tok, deep_link=deep_link), 200


@commuter_bp.route("/tickets/<int:ticket_id>/receipt-qr.png", methods=["GET"])
def commuter_ticket_receipt_qr(ticket_id: int):
    img_link = url_for("commuter.commuter_ticket_image", ticket_id=ticket_id, _external=True)
    qr = qrcode.QRCode(box_size=10, border=2)
    qr.add_data(img_link)
    qr.make(fit=True)
    out = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    bio = BytesIO()
    out.save(bio, format="PNG", optimize=True)
    bio.seek(0)

    resp = make_response(send_file(bio, mimetype="image/png"))
    resp.headers["Cache-Control"] = "public, max-age=86400"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp

@commuter_bp.app_errorhandler(Exception)
def _commuter_errors(e: Exception):
    current_app.logger.exception("Unhandled error on %s %s", request.method, request.path)
    if isinstance(e, HTTPException) and not _debug_enabled():
        return e
    status = getattr(e, "code", 500)
    if _debug_enabled():
        return jsonify({
            "ok": False,
            "type": e.__class__.__name__,
            "error": str(e),
            "endpoint": request.endpoint,
            "path": request.path,
            "traceback": traceback.format_exc(),
        }), status
    return jsonify({"error": "internal server error"}), status

# -------- time helpers --------
def _as_time(v: Any) -> Optional[dt.time]:
    if v is None:
        return None
    if isinstance(v, dt.datetime):
        return v.time().replace(tzinfo=None)
    if isinstance(v, dt.time):
        return v.replace(tzinfo=None)
    if isinstance(v, str):
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                return dt.datetime.strptime(v, fmt).time()
            except ValueError:
                pass
    return None

@commuter_bp.route("/device-token", methods=["POST"])
@require_role("commuter")
def save_device_token():
    data = request.get_json(silent=True) or {}
    token = (data.get("token") or "").strip()
    platform = (data.get("platform") or "").strip() or None
    if not token:
        return jsonify(error="token required"), 400

    created = False
    row = DeviceToken.query.filter_by(user_id=g.user.id, token=token).first()
    if not row:
        row = DeviceToken(user_id=g.user.id, token=token, platform=platform)
        db.session.add(row)
    else:
        row.platform = platform or row.platform
    db.session.commit()
    current_app.logger.info(f"[push] saved token token={token[:12]}… uid={g.user.id} created={created} platform={row.platform}")
    return jsonify(ok=True, created=created), (201 if created else 200)

@commuter_bp.route("/qr/ticket/<int:ticket_id>.jpg", methods=["GET"])
def qr_image_for_ticket(ticket_id: int):
    t = TicketSale.query.get_or_404(ticket_id)
    amount = int(round(float(t.price or 0)))
    prefix = "discount" if t.passenger_type == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"
    return redirect(url_for("static", filename=f"qr/{filename}", _external=True), code=302)

@commuter_bp.route("/tickets/<int:ticket_id>/view", methods=["GET"])
def commuter_ticket_view(ticket_id: int):
    img_url = url_for("commuter.commuter_ticket_image", ticket_id=ticket_id, _external=True)
    dl_url = img_url + ("&" if "?" in img_url else "?") + "download=1"
    return (
        f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Receipt #{ticket_id}</title>
    <style>
      body {{ margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Inter, sans-serif; background:#0b0b0b; color:#f2f2f2; }}
      .wrap {{ max-width: 720px; margin: 24px auto; padding: 16px; }}
      .card {{ background:#1c1c1c; border-radius:16px; padding:16px; box-shadow:0 10px 30px rgba(0,0,0,.3) }}
      img {{ width:100%; height:auto; border-radius:12px; display:block }}
      .actions {{ margin-top:12px; display:flex; gap:8px }}
      a.btn {{ text-decoration:none; padding:12px 16px; border-radius:12px; background:#42c285; color:#0b0b0b; font-weight:600; display:inline-block }}
      a.link {{ color:#bdbdbd }}
    </style>
  </head>
  <body>
    <div class="wrap">
      <div class="card">
        <img src="{img_url}" alt="Receipt image"/>
        <div class="actions">
          <a class="btn" href="{dl_url}">Download JPG</a>
          <a class="link" href="{img_url}">Open image</a>
        </div>
      </div>
    </div>
  </body>
</html>""",
        200,
        {"Content-Type": "text/html; charset=utf-8"},
    )

@commuter_bp.route("/tickets/<int:ticket_id>", methods=["GET"])
@require_role("commuter")
def commuter_get_ticket(ticket_id: int):
    t = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.bus).joinedload(Bus.pao),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id, TicketSale.user_id == g.user.id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    if t.origin_stop_time:
        origin_name = t.origin_stop_time.stop_name
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin_name = ts.stop_name if ts else ""

    if t.destination_stop_time:
        destination_name = t.destination_stop_time.stop_name
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination_name = tsd.stop_name if tsd else ""

    # Choose QR asset (amount already integer pesos)
    if t.passenger_type == "discount":
        base = int(round(float(t.price or 0) / 0.8))
        prefix = "discount"
    else:
        base = int(round(float(t.price or 0)))
        prefix = "regular"
    filename = f"{prefix}_{base}.jpg"
    qr_url = url_for("static", filename=f"qr/{filename}", _external=True)

    payload = build_qr_payload(
        t,
        origin_name=origin_name,
        destination_name=destination_name,
    )
    qr_link = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)

    issuer_id = getattr(t, "issued_by", None) or (t.bus.pao.id if (t.bus and t.bus.pao) else None)

    return jsonify({
        "id": t.id,
        "referenceNo": t.reference_no,
        "date": t.created_at.strftime("%B %d, %Y"),
        "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "origin": origin_name,
        "destination": destination_name,
        "passengerType": t.passenger_type.title(),
        "commuter": f"{t.user.first_name} {t.user.last_name}",
        "fare": int(round(float(t.price or 0))),  # NO CENTS
        "paid": bool(t.paid),
        "qr": payload,
        "qr_link": qr_link,
        "qr_url": qr_url,
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
        "paoId": issuer_id,
    }), 200

@commuter_bp.route("/dashboard", methods=["GET"])
@require_role("commuter")
def dashboard():
    """
    Compact dashboard payload + accurate live_now using local time.
    Debug: ?debug=1 | ?date=YYYY-MM-DD | ?now=HH:MM
    """
    debug_on = (request.args.get("debug") or "").lower() in {"1", "true", "yes"}

    now_local = dt.datetime.now(LOCAL_TZ) if LOCAL_TZ else dt.datetime.now()
    date_arg = (request.args.get("date") or "").strip()
    force_now = (request.args.get("now") or request.args.get("force_now") or "").strip()

    if date_arg:
        try:
            today_local = dt.datetime.strptime(date_arg, "%Y-%m-%d").date()
        except ValueError:
            today_local = now_local.date()
    else:
        today_local = now_local.date()

    if force_now:
        try:
            hh, mm = map(int, force_now.split(":")[:2])
            now_local = now_local.replace(hour=hh, minute=mm, second=0, microsecond=0)
        except Exception:
            pass

    now_time_local = now_local.time().replace(tzinfo=None)

    def _choose_greeting() -> str:
        hr = now_local.hour
        if hr < 12:
            return "Good morning"
        elif hr < 18:
            return "Good afternoon"
        return "Good evening"

    next_trip_row = (
        db.session.query(Trip, Bus.identifier.label("bus_identifier"))
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(
            Trip.service_date == today_local,
            Trip.start_time >= now_time_local,
        )
        .order_by(Trip.start_time.asc())
        .first()
    )
    if next_trip_row:
        trip, identifier = next_trip_row
        next_trip = {
            "bus": (identifier or "").replace("bus-", "Bus "),
            "start": _as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
            "end": _as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
        }
    else:
        next_trip = None

    unread_msgs = Announcement.query.count()

    last_ann_row = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)
        .order_by(Announcement.timestamp.desc())
        .first()
    )
    last_announcement = None
    if last_ann_row:
        ann, fn, ln, bid = last_ann_row
        last_announcement = {
            "message": ann.message,
            "timestamp": ann.timestamp.isoformat(),
            "author_name": f"{fn} {ln}",
            "bus_identifier": bid or "unassigned",
        }

    def _is_live_window(now_t: dt.time, s: Optional[dt.time], e: Optional[dt.time], *, grace_min: int = 3) -> bool:
        if not s or not e:
            return False
        if s == e:
            base = dt.datetime.combine(today_local, s)
            nowd = dt.datetime.combine(today_local, now_t)
            return abs((nowd - base).total_seconds()) <= grace_min * 60
        return s <= now_t < e

    live_now: List[Dict[str, Any]] = []
    debug_trips: List[Dict[str, Any]] = []

    trips_today = (
        db.session.query(Trip, Bus.identifier.label("bus_identifier"))
        .join(Bus, Trip.bus_id == Bus.id)
        .filter(Trip.service_date == today_local)
        .order_by(Trip.start_time.asc())
        .all()
    )

    for t, bid in trips_today:
        sts = (
            StopTime.query.filter_by(trip_id=t.id)
            .order_by(StopTime.seq.asc(), StopTime.id.asc())
            .all()
        )

        events: List[Dict[str, Any]] = []
        if len(sts) < 2:
            events.append({
                "type": "trip",
                "label": "In Transit",
                "start": _as_time(t.start_time),
                "end": _as_time(t.end_time),
                "description": "",
            })
        else:
            for idx, st in enumerate(sts):
                s = _as_time(st.arrive_time or st.depart_time)
                e = _as_time(st.depart_time or st.arrive_time)
                if s or e:
                    events.append({
                        "type": "stop",
                        "label": "At Stop",
                        "start": s,
                        "end": e,
                        "description": st.stop_name,
                    })
                if idx < len(sts) - 1:
                    nxt = sts[idx + 1]
                    s2 = _as_time(st.depart_time or st.arrive_time)
                    e2 = _as_time(nxt.arrive_time or nxt.depart_time)
                    if s2 and e2 and s2 != e2:
                        events.append({
                            "type": "trip",
                            "label": "In Transit",
                            "start": s2,
                            "end": e2,
                            "description": f"{st.stop_name} → {nxt.stop_name}",
                        })

        chosen = None
        for ev in events:
            if _is_live_window(now_time_local, ev["start"], ev["end"], grace_min=3):
                chosen = ev
                live_now.append({
                    "bus_id": t.bus_id,
                    "bus": (bid or "").replace("bus-", "Bus "),
                    "trip_id": t.id,
                    "type": ev["type"],
                    "label": ev["label"],
                    "start": ev["start"].strftime("%H:%M"),
                    "end": ev["end"].strftime("%H:%M"),
                    "description": ev["description"],
                })
                break

        ts = _as_time(t.start_time)
        te = _as_time(t.end_time)
        if not chosen and ts and te and _is_live_window(now_time_local, ts, te, grace_min=0):
            live_now.append({
                "bus_id": t.bus_id,
                "bus": (bid or "").replace("bus-", "Bus "),
                "trip_id": t.id,
                "type": "trip",
                "label": "In Transit",
                "start": ts.strftime("%H:%M"),
                "end": te.strftime("%H:%M"),
                "description": "",
            })

        if debug_on:
            def _fmt(x: Optional[dt.time]) -> Optional[str]:
                return x.strftime("%H:%M") if x else None
            debug_trips.append({
                "trip_id": t.id,
                "bus": (bid or "").replace("bus-", "Bus "),
                "events": [
                    {
                        "type": ev["type"],
                        "label": ev["label"],
                        "start": _fmt(ev["start"]),
                        "end": _fmt(ev["end"]),
                        "desc": ev["description"],
                        "hit": _is_live_window(now_time_local, ev["start"], ev["end"], grace_min=3),
                    } for ev in events
                ],
                "chosen": None if not chosen else {
                    "type": chosen["type"],
                    "start": _fmt(chosen["start"]),
                    "end": _fmt(chosen["end"]),
                },
            })

    current_app.logger.info(
        "dashboard live_now=%d now=%s date=%s trips=%d",
        len(live_now),
        now_time_local,
        today_local,
        len(trips_today),
    )

    payload: Dict[str, Any] = {
        "greeting": _choose_greeting(),
        "user_name": f"{g.user.first_name} {g.user.last_name}",
        "next_trip": next_trip,
        "unread_messages": int(unread_msgs or 0),
        "last_announcement": last_announcement,
        "live_now": live_now,
    }
    if debug_on:
        payload["debug"] = {
            "now_local": now_local.strftime("%Y-%m-%d %H:%M:%S"),
            "today_local": str(today_local),
            "live_now_len": len(live_now),
            "trips_today_len": len(trips_today),
            "first_trip_debug": debug_trips[0] if debug_trips else None,
        }

    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp, 200

@commuter_bp.route("/trips", methods=["GET"])
def list_all_trips():
    date_str = request.args.get("date")
    if not date_str:
        return jsonify(error="A 'date' parameter is required."), 400
    try:
        svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="Invalid date format. Use YYYY-MM-DD."), 400

    first_stop_sq = (
        db.session.query(StopTime.trip_id, func.min(StopTime.seq).label("min_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    first_stop_name_sq = (
        db.session.query(StopTime.trip_id, StopTime.stop_name.label("origin"))
        .join(first_stop_sq, (StopTime.trip_id == first_stop_sq.c.trip_id) & (StopTime.seq == first_stop_sq.c.min_seq))
        .subquery()
    )
    last_stop_sq = (
        db.session.query(StopTime.trip_id, func.max(StopTime.seq).label("max_seq"))
        .group_by(StopTime.trip_id)
        .subquery()
    )
    last_stop_name_sq = (
        db.session.query(StopTime.trip_id, StopTime.stop_name.label("destination"))
        .join(last_stop_sq, (StopTime.trip_id == last_stop_sq.c.trip_id) & (StopTime.seq == last_stop_sq.c.max_seq))
        .subquery()
    )

    trips = (
        db.session.query(Trip, Bus.identifier, first_stop_name_sq.c.origin, last_stop_name_sq.c.destination)
        .join(Bus, Trip.bus_id == Bus.id)
        .outerjoin(first_stop_name_sq, Trip.id == first_stop_name_sq.c.trip_id)
        .outerjoin(last_stop_name_sq, Trip.id == last_stop_name_sq.c.trip_id)
        .filter(Trip.service_date == svc_date)
        .order_by(Trip.start_time.asc())
        .all()
    )

    result = [
        {
            "id": trip.id,
            "bus_identifier": identifier,
            "start_time": _as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
            "end_time": _as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
            "origin": origin or "N/A",
            "destination": destination or "N/A",
        }
        for trip, identifier, origin, destination in trips
    ]
    return jsonify(result), 200

@commuter_bp.route("/buses", methods=["GET"])
def list_buses():
    date_str = request.args.get("date")
    q = Bus.query
    if date_str:
        try:
            svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        q = q.join(Trip, Bus.id == Trip.bus_id).filter(Trip.service_date == svc_date)
    buses = q.order_by(Bus.identifier.asc()).all()
    return jsonify([{"id": b.id, "identifier": b.identifier} for b in buses]), 200

@commuter_bp.route("/bus-trips", methods=["GET"])
@require_role("commuter")
def commuter_bus_trips():
    bus_id = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")
    if not (bus_id and date_str):
        return jsonify(error="bus_id and date are required"), 400
    try:
        svc_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return jsonify(error="date must be YYYY-MM-DD"), 400

    trips = (
        Trip.query.filter_by(bus_id=bus_id, service_date=svc_date)
        .order_by(Trip.start_time.asc())
        .all()
    )
    return jsonify([
        {
            "id": t.id,
            "number": t.number,
            "start_time": _as_time(t.start_time).strftime("%H:%M") if _as_time(t.start_time) else "",
            "end_time": _as_time(t.end_time).strftime("%H:%M") if _as_time(t.end_time) else "",
        }
        for t in trips
    ]), 200

@commuter_bp.route("/stop-times", methods=["GET"])
@require_role("commuter")
def commuter_stop_times():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = StopTime.query.filter_by(trip_id=trip_id).order_by(StopTime.seq.asc()).all()
    return jsonify([
        {
            "stop_name": st.stop_name,
            "arrive_time": (_as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else ""),
            "depart_time": (_as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else ""),
        }
        for st in sts
    ]), 200

@commuter_bp.route("/location", methods=["GET"])
@require_role("commuter")
def vehicle_location():
    sr = SensorReading.query.order_by(SensorReading.timestamp.desc()).first()
    if not sr:
        return jsonify(error="no sensor data"), 404
    return jsonify(
        lat=sr.lat,
        lng=sr.lng,
        occupied=sr.occupied,
        timestamp=sr.timestamp.isoformat(),
    ), 200

def _local_day_bounds_utc(day: dt.date):
    start_local = dt.datetime.combine(day, dt.time(0, 0, 0), tzinfo=LOCAL_TZ)
    end_local   = start_local + dt.timedelta(days=1)
    start_utc   = start_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    end_utc     = end_local.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return start_utc, end_utc

@commuter_bp.route("/tickets/mine", methods=["GET"])
@require_role("commuter")
def my_receipts():
    page      = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, request.args.get("page_size", type=int, default=5))
    date_str  = request.args.get("date")
    days      = request.args.get("days")
    bus_id    = request.args.get("bus_id", type=int)
    light     = (request.args.get("light") or "").lower() in {"1", "true", "yes"}

    qs = (
        db.session.query(TicketSale)
        .options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.user_id == g.user.id)
    )

    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        qs = qs.filter(func.date(TicketSale.created_at) == day)
    elif days in {"7", "30"}:
        cutoff = dt.datetime.utcnow() - timedelta(days=int(days))
        qs = qs.filter(TicketSale.created_at >= cutoff)

    if bus_id:
        if hasattr(TicketSale, "bus_id"):
            qs = qs.filter(TicketSale.bus_id == bus_id)
        else:
            qs = qs.join(Trip, TicketSale.trip_id == Trip.id).filter(Trip.bus_id == bus_id)

    total = qs.count()
    rows = qs.order_by(TicketSale.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()

    items = []
    for t in rows:
        amount = int(round(float(t.price or 0)))
        prefix = "discount" if t.passenger_type == "discount" else "regular"
        filename = f"{prefix}_{amount}.jpg"
        qr_url = url_for("static", filename=f"qr/{filename}", _external=True)

        if t.origin_stop_time:
            origin_name = t.origin_stop_time.stop_name
        else:
            ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
            origin_name = ts.stop_name if ts else ""

        if t.destination_stop_time:
            destination_name = t.destination_stop_time.stop_name
        else:
            tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
            destination_name = tsd.stop_name if tsd else ""

        payload = build_qr_payload(
            t,
            origin_name=origin_name,
            destination_name=destination_name,
        )
        qr_link = url_for("commuter.qr_image_for_ticket", ticket_id=t.id, _external=True)

        items.append({
            "id": t.id,
            "referenceNo": t.reference_no,
            "date": t.created_at.strftime("%B %d, %Y"),
            "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "origin": origin_name,
            "destination": destination_name,
            "passengerType": t.passenger_type.title(),
            "commuter": f"{t.user.first_name} {t.user.last_name}",
            "fare": amount,  # NO CENTS
            "paid": bool(t.paid),
            "qr_url": qr_url,
            "qr": payload if not light else payload,
            "qr_link": qr_link,
        })

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200

@commuter_bp.route("/trips/<int:trip_id>", methods=["GET"])
@require_role("commuter")
def get_trip(trip_id: int):
    trip = Trip.query.get_or_404(trip_id)
    first_stop = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .first()
    )
    last_stop = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.desc(), StopTime.id.desc())
        .first()
    )

    return jsonify(
        id=trip.id,
        number=trip.number,
        origin=first_stop.stop_name if first_stop else "",
        destination=last_stop.stop_name if last_stop else "",
        start_time=_as_time(trip.start_time).strftime("%H:%M") if _as_time(trip.start_time) else "",
        end_time=_as_time(trip.end_time).strftime("%H:%M") if _as_time(trip.end_time) else "",
    ), 200

@commuter_bp.route("/timetable", methods=["GET"])
@require_role("commuter")
def timetable():
    trip_id = request.args.get("trip_id", type=int)
    if not trip_id:
        return jsonify(error="trip_id is required"), 400

    sts = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )
    return jsonify([
        {
            "stop": st.stop_name,
            "arrive": (_as_time(st.arrive_time).strftime("%H:%M") if _as_time(st.arrive_time) else ""),
            "depart": (_as_time(st.depart_time).strftime("%H:%M") if _as_time(st.depart_time) else ""),
        }
        for st in sts
    ]), 200

@commuter_bp.route("/schedule", methods=["GET"])
@require_role("commuter")
def schedule():
    trip_id = request.args.get("trip_id", type=int)
    date_str = request.args.get("date")
    if not trip_id or not date_str:
        return jsonify(error="trip_id and date are required"), 400

    trip = Trip.query.get_or_404(trip_id)
    stops = (
        StopTime.query.filter_by(trip_id=trip_id)
        .order_by(StopTime.seq.asc(), StopTime.id.asc())
        .all()
    )

    def fmt(t):
        tt = _as_time(t)
        return tt.strftime("%H:%M") if tt else ""

    events = []
    if len(stops) == 0:
        events.append({
            "id": 1,
            "type": "trip",
            "label": "In Transit",
            "start_time": fmt(trip.start_time),
            "end_time": fmt(trip.end_time),
            "description": "",
        })
    else:
        for idx, st in enumerate(stops):
            s = _as_time(st.arrive_time) or _as_time(st.depart_time)
            e = _as_time(st.depart_time) or _as_time(st.arrive_time)
            if s or e:
                events.append({
                    "id": idx * 2 + 1,
                    "type": "stop",
                    "label": "At Stop",
                    "start_time": fmt(s),
                    "end_time": fmt(e),
                    "description": st.stop_name,
                })
            if idx < len(stops) - 1:
                nxt = stops[idx + 1]
                s2 = _as_time(st.depart_time) or _as_time(st.arrive_time)
                e2 = _as_time(nxt.arrive_time) or _as_time(nxt.depart_time)
                if s2 and e2 and s2 != e2:
                    events.append({
                        "id": idx * 2 + 2,
                        "type": "trip",
                        "label": "In Transit",
                        "start_time": fmt(s2),
                        "end_time": fmt(e2),
                        "description": f"{st.stop_name} → {nxt.stop_name}",
                    })

    return jsonify(events=events), 200

@commuter_bp.route("/announcements", methods=["GET"])
def announcements():
    """
    GET /commuter/announcements
      Optional:
        bus_id=<int>        # only announcements authored by PAOs assigned to this bus
        date=YYYY-MM-DD     # local calendar day (defaults to today in LOCAL_TZ)
        limit=<int>         # cap the number of rows (newest first)
    """
    bus_id   = request.args.get("bus_id", type=int)
    date_str = request.args.get("date")
    limit    = request.args.get("limit", type=int)

    q = (
        db.session.query(
            Announcement,
            User.first_name,
            User.last_name,
            Bus.identifier.label("bus_identifier"),
        )
        .join(User, Announcement.created_by == User.id)
        .outerjoin(Bus, User.assigned_bus_id == Bus.id)
    )
    if bus_id:
        q = q.filter(User.assigned_bus_id == bus_id)

    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
    else:
        day = (dt.datetime.now(LOCAL_TZ) if LOCAL_TZ else dt.datetime.now()).date()

    start_utc, end_utc = _local_day_bounds_utc(day)
    q = q.filter(Announcement.timestamp >= start_utc, Announcement.timestamp < end_utc)
    q = q.order_by(Announcement.timestamp.desc())
    if isinstance(limit, int) and limit > 0:
        q = q.limit(limit)

    rows = q.all()
    anns = [
        {
            "id": ann.id,
            "message": ann.message,
            "timestamp": (ann.timestamp.replace(tzinfo=dt.timezone.utc)).isoformat(),
            "author_name": f"{first} {last}",
            "bus_identifier": bus_identifier or "unassigned",
        }
        for ann, first, last, bus_identifier in rows
    ]
    return jsonify(anns), 200
