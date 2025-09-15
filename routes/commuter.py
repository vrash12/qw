# routes/commuter.py
from __future__ import annotations
import datetime as dt
from typing import Any, Dict, List, Optional

from flask import (
    Blueprint, request, jsonify, g, current_app, url_for,
    redirect, send_file, make_response
)
from sqlalchemy import func, text, or_

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

from sqlalchemy import desc
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
import os, uuid, time
from werkzeug.utils import secure_filename
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

RECEIPTS_DIR = "topup_receipts"
ALLOWED_EXTS = {"jpg", "jpeg", "png", "webp"}

def _unique_ref(prefix: str) -> str:
    return f"{prefix}-{int(time.time()*1000)}-{uuid.uuid4().hex[:8]}"

def _save_receipt(file_storage, topup_id: int) -> Optional[str]:
    if not file_storage or not getattr(file_storage, "filename", None):
        return None
    fname = secure_filename(file_storage.filename or "")
    ext = (fname.rsplit(".", 1)[-1].lower() if "." in fname else "jpg")
    if ext not in ALLOWED_EXTS:
        ext = "jpg"

    base_dir = os.path.join(current_app.root_path, "static", RECEIPTS_DIR)
    os.makedirs(base_dir, exist_ok=True)

    path = os.path.join(base_dir, f"{topup_id}.{ext}")
    file_storage.save(path)
    return url_for("static", filename=f"{RECEIPTS_DIR}/{topup_id}.{ext}", _external=True)


def _receipt_abs_path(tid: int, ext: str) -> str:
    safe = f"{tid}{ext.lower()}"
    return os.path.join(current_app.root_path, "static", RECEIPTS_DIR, safe)
def _is_ticket_void(t) -> bool:
    """
    Robustly determine if a ticket is voided, regardless of schema.
    True if:
      - a boolean/integer column 'voided' exists and is truthy, OR
      - a 'status' column exists and is one of: void, voided, refunded, cancelled.
    """
    try:
        if bool(getattr(t, "voided", False)):
            return True
    except Exception:
        pass
    try:
        st = (getattr(t, "status", None) or "").strip().lower()
        if st in {"void", "voided", "refunded", "cancelled"}:
            return True
    except Exception:
        pass
    return False

def _receipt_url_if_exists(tid: int) -> str | None:
    for ext in (".jpg", ".jpeg", ".png", ".webp"):
        p = _receipt_abs_path(tid, ext)
        if os.path.exists(p):
            return url_for("static", filename=f"{RECEIPTS_DIR}/{os.path.basename(p)}", _external=True)
    return None

def _as_local(dt_obj: dt.datetime) -> dt.datetime:
    """Convert naive (assumed UTC) or aware datetime to LOCAL_TZ."""
    if dt_obj is None:
        return dt.datetime.now(LOCAL_TZ)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    return dt_obj.astimezone(LOCAL_TZ)

def _debug_enabled() -> bool:
    return (request.args.get("debug") or request.headers.get("X-Debug") or "").lower() in {"1","true","yes"}



@commuter_bp.route("/topup-requests", methods=["GET"])
@require_role("commuter")
def list_my_topup_requests():
    """
    GET /commuter/topup-requests
      Optional query:
        status=pending|approved|rejected|succeeded|cancelled
        page=<int> default 1
        page_size=<int> default 20
    """
    status = (request.args.get("status") or "").strip().lower()
    page = max(1, request.args.get("page", type=int, default=1))
    page_size = max(1, min(100, request.args.get("page_size", type=int, default=20)))

    q = TopUp.query.filter(TopUp.account_id == g.user.id)
    if status:
        q = q.filter(TopUp.status == status)

    total = q.count()
    rows = (
        q.order_by(desc(getattr(TopUp, "created_at", TopUp.id)))
         .offset((page - 1) * page_size)
         .limit(page_size)
         .all()
    )

    items = []
    for t in rows:
        items.append({
            "id": int(t.id),
            "account_id": int(t.account_id),
            "amount_pesos": int(getattr(t, "amount_pesos", 0) or 0),
            "method": getattr(t, "method", "cash"),
            "status": getattr(t, "status", "pending"),
            "created_at": (
                t.created_at.isoformat() if getattr(t, "created_at", None) else None
            ),
            # Optional fields (add only if your schema has them)
            "note": getattr(t, "note", None) or getattr(t, "external_ref", None) or getattr(t, "provider_ref", None),
            "receipt_url": getattr(t, "receipt_url", None),
        })

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200

@commuter_bp.route("/topup-requests", methods=["POST"])
@require_role("commuter")
def create_topup_request():
    """
    Create a pending commuter top-up request (GCash with receipt).
    Accepts multipart/form-data:
      - amount_pesos (int)
      - method = 'gcash' (required for commuter)
      - note (optional) -> used as provider_ref if present
      - receipt (image file)
    """
    form = request.form
    files = request.files

    # Only GCash is supported for commuter self-requests (cash goes via teller scan)
    method = (form.get("method") or "gcash").strip().lower()
    if method != "gcash":
        return jsonify(error="unsupported method"), 400

    # amount
    try:
        amount_pesos = int(form.get("amount_pesos") or form.get("amount_php") or 0)
    except Exception:
        amount_pesos = 0
    if amount_pesos <= 0:
        return jsonify(error="invalid amount"), 400

    # provider fields (MUST NOT be NULL in your DB)
    provider = "gcash"
    # Prefer note/external ref if given; else synthesize a unique one
    note = (form.get("note") or "").strip()
    provider_ref = (form.get("external_ref") or "").strip() or note or _unique_ref(provider)

    # Require a receipt image
    receipt_fs = files.get("receipt")
    if not receipt_fs or not getattr(receipt_fs, "filename", None):
        return jsonify(error="receipt image is required"), 400

    # Insert pending top-up
    tu = TopUp(
        account_id=int(g.user.id),
        method=method,
        amount_pesos=amount_pesos,
        status="pending",
        provider=provider,
        provider_ref=provider_ref,
    )
    db.session.add(tu)
    db.session.flush()  # get tu.id without committing yet

    # Save receipt to static/topup_receipts/<id>.<ext>
    receipt_url = _save_receipt(receipt_fs, tu.id)

    # Commit
    db.session.commit()

    return jsonify({
        "id": tu.id,
        "account_id": tu.account_id,
        "amount_pesos": int(tu.amount_pesos or 0),
        "method": tu.method,
        "status": tu.status,
        "provider": tu.provider,
        "provider_ref": tu.provider_ref,
        "receipt_url": receipt_url,
        "created_at": tu.created_at.isoformat() if getattr(tu, "created_at", None) else None,
    }), 201

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

# ADD this near your other wallet routes in routes/commuter.py

@commuter_bp.route("/wallet/share-text", methods=["GET"])
@require_role("commuter")
def wallet_share_text():
    """
    Returns a pre-filled message the commuter can copy/share to a Teller.
    Optional query params:
      - amount: int pesos (e.g., 250)
      - receipt: str (free-form GCash text, link or reference no.)
      - method: 'gcash' | 'cash' (default 'gcash')
    """
    amount = max(0, request.args.get("amount", type=int, default=0))
    method = (request.args.get("method") or "gcash").lower().strip()
    receipt = (request.args.get("receipt") or "").strip()

    # Stable wallet token + deep link (you already have this)
    token = build_wallet_token(g.user.id)
    deep_link = f"https://pay.example/charge?wallet_token={token}&autopay=1"

    # Compose a short, Teller-friendly note
    pretty = []
    pretty.append("PGT Onboard — Wallet Top-up")
    pretty.append(f"Name: {g.user.first_name} {g.user.last_name}")
    pretty.append(f"Method: {'GCash' if method == 'gcash' else 'Cash'}")
    if amount > 0:
        pretty.append(f"Amount: ₱{amount:,}")
    if receipt:
        pretty.append(f"Receipt/Ref: {receipt}")
    pretty.append(f"Wallet Token: {token}")
    pretty.append(f"Deep Link: {deep_link}")

    return jsonify({
        "wallet_token": token,
        "deep_link": deep_link,
        "message": "\n".join(pretty),
    }), 200


@commuter_bp.route("/users/me/qr.png", methods=["GET"])
@require_role("commuter")
def commuter_my_wallet_qr_png():
    """
    Returns a PNG QR for the logged-in commuter.
    The QR encodes a URL to /teller/users/scan?token=...
    """
    size = max(240, min(1024, int(request.args.get("size", 360) or 360)))

    # signed short-lived token that represents this user id
    token = _user_qr_sign(g.user.id)

    # 🔁 moved from PAO → Teller
    scan_url = url_for("teller.user_qr_scan", _external=True) + f"?token={token}"

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
def _day_range_filter(q, date_str: Optional[str], days: Optional[str]):
    """Apply date/day window filters (created_at) in LOCAL_TZ semantics."""
    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError("date must be YYYY-MM-DD")
        start_utc, end_utc = _local_day_bounds_utc(day)
        q = q.filter(TicketSale.created_at >= start_utc,
                     TicketSale.created_at <  end_utc)
    elif days in {"7", "30"}:
        cutoff = dt.datetime.utcnow() - timedelta(days=int(days))
        q = q.filter(TicketSale.created_at >= cutoff)
    return q

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

    Supports both:
      • legacy "batch" (multiple TicketSale rows tied by batch_id), and
      • single-row "group" tickets with is_group/group_* columns.

    Also renders VOIDED tickets (refunded to wallet).
    """
    t = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.bus),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Resolve stop names + (optional) seq for fare math
    o_seq = d_seq = None
    if t.origin_stop_time:
        origin_name = t.origin_stop_time.stop_name
        try:
            o_seq = int(getattr(t.origin_stop_time, "seq", None) or 0)
        except Exception:
            o_seq = None
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin_name = ts.stop_name if ts else ""
        o_seq = int(getattr(ts, "seq", 0) or 0) if ts else None

    if t.destination_stop_time:
        destination_name = t.destination_stop_time.stop_name
        try:
            d_seq = int(getattr(t.destination_stop_time, "seq", None) or 0)
        except Exception:
            d_seq = None
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination_name = tsd.stop_name if tsd else ""
        d_seq = int(getattr(tsd, "seq", 0) or 0) if tsd else None

    # Who issued (for right-panel info)
    issuer_via_field = getattr(t, "issued_by", None)
    issuer_via_bus = db.session.query(User.id) \
        .filter(User.assigned_bus_id == t.bus_id, User.role == "pao") \
        .order_by(User.id.desc()) \
        .scalar()
    issuer_id = issuer_via_field or issuer_via_bus

    # Group-aware details (single-row model)
    is_group = bool(getattr(t, "is_group", False))
    g_reg = int(getattr(t, "group_regular", 0) or 0)
    g_dis = int(getattr(t, "group_discount", 0) or 0)
    group_qty = g_reg + g_dis if is_group else 1
    total_pesos = int(round(float(t.price or 0)))

    # Fare math (only if we can infer hops). Same formula used in PAO create flow.
    def _fare_for_each(pt: str) -> Optional[int]:
        if o_seq is None or d_seq is None:
            return None
        hops = abs(int(o_seq) - int(d_seq))
        base = 10 + max(hops - 1, 0) * 2
        return int(round(base * 0.8)) if pt == "discount" else int(base)

    reg_each = _fare_for_each("regular")
    dis_each = _fare_for_each("discount")

    # Build "batch-like" summary for the renderer
    if is_group and group_qty > 1:
        batch_qty = group_qty
        if reg_each is not None and dis_each is not None:
            total_by_types = (g_reg * reg_each) + (g_dis * dis_each)
            batch_total = total_by_types if total_by_types > 0 else total_pesos
        else:
            batch_total = total_pesos

        breakdown = {}
        if g_reg > 0:
            breakdown["regular"] = {
                "qty": g_reg,
                "total": (g_reg * reg_each) if reg_each is not None else None,
                "each": reg_each
            }
        if g_dis > 0:
            breakdown["discount"] = {
                "qty": g_dis,
                "total": (g_dis * dis_each) if dis_each is not None else None,
                "each": dis_each
            }

        # Line items text (compact)
        line_items = []
        if g_reg > 0:
            if reg_each is not None:
                line_items.append(f"Regular × {g_reg}  ·  ₱{reg_each} each  ·  ₱{(g_reg*reg_each)}")
            else:
                line_items.append(f"Regular × {g_reg}")
        if g_dis > 0:
            if dis_each is not None:
                line_items.append(f"Discount × {g_dis}  ·  ₱{dis_each} each  ·  ₱{(g_dis*dis_each)}")
            else:
                line_items.append(f"Discount × {g_dis}")
    else:
        # Legacy batch path (multiple rows tied together) — keep working if present
        bid = getattr(t, "batch_id", None) or t.id
        rows = (
            db.session.query(
                TicketSale.id,
                TicketSale.reference_no,
                TicketSale.passenger_type,
                TicketSale.price
            )
            .filter(func.coalesce(TicketSale.batch_id, TicketSale.id) == bid,
                    TicketSale.paid.is_(True))
            .order_by(TicketSale.id.asc())
            .all()
        )
        batch_qty = len(rows)
        batch_total = sum(int(round(float(r.price or 0))) for r in rows)
        breakdown = {}
        for r in rows:
            ptype = (r.passenger_type or "regular").lower()
            breakdown.setdefault(ptype, {"qty": 0, "total": 0})
            breakdown[ptype]["qty"] += 1
            breakdown[ptype]["total"] += int(round(float(r.price or 0)))
        line_items = [
            f"{(r.reference_no or str(r.id))}  ·  {(r.passenger_type or 'regular').title()}  ·  ₱{int(round(float(r.price or 0)))}"
            for r in rows
        ]

    # Log for debugging
    try:
        current_app.logger.info(
            "[receipt:image] ticket_id=%s model=%s qty=%s total=%s reg=%s dis=%s",
            t.id,
            ("group" if (is_group and group_qty > 1) else "batch/solo"),
            (batch_qty if (is_group and group_qty > 1) else (batch_qty if 'batch_qty' in locals() else 1)),
            (batch_total if 'batch_total' in locals() else total_pesos),
            g_reg, g_dis
        )
    except Exception:
        pass

    # QR that points back to THIS image URL
    img_link = url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True)
    qr = qrcode.QRCode(box_size=12, border=2)
    qr.add_data(img_link)
    qr.make(fit=True)
    qr_img = qr.make_image(fill_color="black", back_color="white").convert("RGB")

    # ─── Drawing ────────────────────────────────────────────────────────────
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
    WARN_BG       = (255, 243, 205)
    WARN_TEXT     = (133, 100, 4)
    DANGER_BG     = (248, 215, 218)
    DANGER_TEXT   = (114, 28, 36)

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

    # Card
    shadow_offset = 10
    draw.rectangle(
        (M + shadow_offset, M + shadow_offset, W - M + shadow_offset, H - M + shadow_offset),
        fill=(0, 0, 0),
    )
    draw.rectangle((M, M, W - M, H - M), fill=WHITE, outline=BORDER_LIGHT, width=2)

    y = M + 40
    header_h = 180
    draw.rectangle((M, y, W - M, y + header_h), fill=LIGHT_GREEN, outline=DARK_GREEN, width=3)
    if ft_title:
        title = "PGT Onboard — Official Receipt"
        if is_group and group_qty > 1:
            title += " — Group"
        draw.text((M + 48, y + (header_h - 80) // 2), title, fill=DARK_GREEN, font=ft_title)
    y += header_h + 12
    draw.rectangle((M + 48, y, W - M - 48, y + 5), fill=ACCENT_GREEN)
    y += 40

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

    yL = y
    yR = y
    passenger_name = f"{t.user.first_name} {t.user.last_name}" if t.user else "—"
    # Left/Right columns
    yL = field(L, yL, "Reference No.", t.reference_no or "—")
    yR = field(L + COL_W + COL_GAP, yR, "Destination", destination_name or "—")

    date_str = t.created_at.strftime('%B %d, %Y')
    time_str = t.created_at.strftime('%I:%M %p').lstrip('0').lower()
    if ft_label:
        draw.text((L, yL), "DATE & TIME", fill=TEXT_MUTED, font=ft_label)
    y_value = yL + 54
    if ft_value:
        draw.text((L, y_value), date_str, fill=TEXT_DARK, font=ft_value)
        draw.text((L, y_value + 52), time_str, fill=TEXT_DARK, font=ft_value)
    yL = y_value + 52 + 60

    # Passenger field: show group hint when applicable
    right_passenger_value = passenger_name if not (is_group and group_qty > 1) else f"{passenger_name}  (Group of {group_qty})"
    yR = field(L + COL_W + COL_GAP, yR, "Passenger", right_passenger_value)
    yL = field(L, yL, "Origin", origin_name or "—")
    yR = field(L + COL_W + COL_GAP, yR, "Passenger Type", (t.passenger_type or "").title() or "—")

    y = max(yL, yR) + 20
    draw.rectangle((L, y, R, y + 4), fill=BORDER_LIGHT)
    y += 48

    # Robust "voided" check (use module helper if present)
    try:
        is_void = _is_ticket_void(t)  # type: ignore[name-defined]
    except NameError:
        st = (getattr(t, "status", None) or "").strip().lower()
        is_void = bool(getattr(t, "voided", False)) or st in {"void", "voided", "refunded", "cancelled"}

    # amount + pill (NO CENTS)
    amount_y = y
    if ft_label:
        draw.text((L, amount_y), "TOTAL AMOUNT", fill=TEXT_MUTED, font=ft_label)
    if ft_big:
        fare_pesos = total_pesos
        draw.text((L, amount_y + 44), f"₱{fare_pesos}", fill=ACCENT_GREEN, font=ft_big)

    # State pill (PAID / UNPAID / VOIDED)
    state_txt = "PAID"
    pill_bg = SUCCESS_BG
    pill_fg = SUCCESS_TEXT
    if not bool(t.paid):
        state_txt = "UNPAID"
        pill_bg = WARN_BG
        pill_fg = WARN_TEXT
    if is_void:
        state_txt = "VOIDED"
        pill_bg = DANGER_BG
        pill_fg = DANGER_TEXT

    if ft_header:
        pill_w = int(tw(state_txt, ft_header) + 64)
        pill_h = 76
        pill_x1 = R - pill_w
        pill_y1 = amount_y + 8
        draw.rectangle((pill_x1 + 14, pill_y1, pill_x1 + pill_w - 14, pill_y1 + pill_h), fill=pill_bg)
        draw.rectangle((pill_x1, pill_y1 + 14, pill_x1 + pill_w, pill_y1 + pill_h - 14), fill=pill_bg)
        text_x = pill_x1 + (pill_w - tw(state_txt, ft_header)) // 2
        text_y = pill_y1 + (pill_h - 60) // 2
        draw.text((text_x, text_y), state_txt, fill=pill_fg, font=ft_header)

    # Add a small hint below amount when voided
    if is_void and ft_medium:
        draw.text((L, amount_y + 44 + 96), "Refunded to wallet", fill=TEXT_MEDIUM, font=ft_medium)

    y += 170

    # QR panel (left)
    qr_section_bg = (247, 251, 247)
    qr_size = 480
    qr_padding = 36
    panel_w = qr_size + qr_padding * 2
    panel_h = qr_size + qr_padding * 2 + 96

    draw.rectangle((L, y, L + panel_w, y + panel_h), fill=qr_section_bg, outline=BORDER_LIGHT, width=2)
    bg.paste(qr_img.resize((qr_size, qr_size)), (L + qr_padding, y + qr_padding))
    if ft_medium:
        draw.text((L + qr_padding, y + qr_padding + qr_size + 24), "Scan to view/download receipt", fill=TEXT_MEDIUM, font=ft_medium)

    # Right panel: Payment + Group/Batch Summary
    right_x = L + panel_w + 56
    right_y = y + 24
    if ft_label:
        draw.text((right_x, right_y), "PAYMENT STATUS", fill=TEXT_MUTED, font=ft_label)

    right_status_color = pill_fg  # mirror pill color
    if ft_header:
        draw.text((right_x, right_y + 44), state_txt, fill=right_status_color, font=ft_header)

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

    # Summary block
    if (is_group and group_qty > 1) or ("batch_total" in locals() and batch_qty > 1):
        section_y = info_y + 12
        if ft_label:
            draw.text((right_x, section_y), "GROUP SUMMARY" if (is_group and group_qty > 1) else "BATCH SUMMARY", fill=TEXT_MUTED, font=ft_label)
        section_y += 52
        if ft_value:
            draw.text((right_x, section_y), f"Passengers: {batch_qty}", fill=TEXT_DARK, font=ft_value)
            section_y += 52
            draw.text((right_x, section_y), f"Total Fare: ₱{batch_total}", fill=ACCENT_GREEN, font=ft_value)
            section_y += 64

        if ft_label:
            draw.text((right_x, section_y), "Breakdown", fill=TEXT_MUTED, font=ft_label)
        section_y += 44

        # group-aware breakdown printing
        for ptype in ("regular", "discount"):
            if ptype in breakdown:
                data = breakdown[ptype]
                if isinstance(data.get("total"), int) and isinstance(data.get("each"), int):
                    line = f"{ptype.title()}: {data['qty']}  ·  ₱{data['each']} each  ·  ₱{data['total']}"
                elif isinstance(data.get("total"), int):
                    line = f"{ptype.title()}: {data['qty']}  ·  ₱{data['total']}"
                else:
                    line = f"{ptype.title()}: {data['qty']}"
                if ft_medium:
                    draw.text((right_x, section_y), line, fill=TEXT_MEDIUM, font=ft_medium)
                section_y += 44

    y = y + panel_h + 48

    # Ticket line items (for legacy multi-row batches) or synthesized lines for group
    if (is_group and group_qty > 1) or ("line_items" in locals() and len(line_items) > 0):
        if ft_header:
            draw.text((L, y), "Tickets in this group" if (is_group and group_qty > 1) else "Tickets in this batch", fill=TEXT_DARK, font=ft_header)
        y += 64
        draw.rectangle((L, y, R, y + 4), fill=BORDER_LIGHT)
        y += 24

        lines = (line_items if (is_group and group_qty > 1) else line_items)
        top_n = 15
        for idx, txt in enumerate(lines[:top_n], start=1):
            if ft_medium:
                draw.text((L + 12, y), (txt if (is_group and group_qty > 1) else f"{idx:>2}.  {txt}"), fill=TEXT_MEDIUM, font=ft_medium)
            y += 50
        if len(lines) > top_n:
            if ft_medium:
                draw.text((L + 12, y), f"+{len(lines) - top_n} more…", fill=TEXT_MUTED, font=ft_medium)
            y += 50

        y += 16
        draw.rectangle((L, y, R, y + 4), fill=BORDER_LIGHT)
        y += 24

    # Footer
    footer_y = max(y + 36, H - M - 72)
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
        resp.headers["X-Debug-Model"] = "group" if (is_group and group_qty > 1) else "batch/solo"
        resp.headers["X-Debug-Group-Regular"] = str(g_reg)
        resp.headers["X-Debug-Group-Discount"] = str(g_dis)
        resp.headers["X-Debug-Group-Qty"] = str(group_qty)
        # surface state in headers too
        resp.headers["X-Debug-State"] = ("voided" if is_void else ("paid" if bool(t.paid) else "unpaid"))
    except Exception:
        pass
    resp.headers["Cache-Control"] = "public, max-age=86400"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    return resp


def _is_unknown_col(err: Exception) -> bool:
    # MySQL error code for "Unknown column": 1054
    try:
        return isinstance(err, OperationalError) and getattr(err.orig, "args", [None])[0] == 1054
    except Exception:
        return False


@commuter_bp.route("/wallet/qr-token", methods=["GET"])
@require_role("commuter")
def wallet_qr_token_alias():
    # Same payload as /wallet/qrcode
    return wallet_qrcode()

@commuter_bp.route("/wallet/qr-token/rotate", methods=["POST"])
@require_role("commuter")
def wallet_qr_token_rotate_alias():
    # Same behavior as /wallet/qrcode/rotate
    return wallet_qrcode_rotate()


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

    row = DeviceToken.query.filter_by(user_id=g.user.id, token=token).first()
    if not row:
        row = DeviceToken(user_id=g.user.id, token=token, platform=platform, provider="expo")
        db.session.add(row)
    else:
        row.platform = platform or row.platform
    db.session.commit()

    current_app.logger.info("[push] saved token uid=%s platform=%s token=%s…",
                            g.user.id, platform, token[:16])
    return jsonify(ok=True), 200
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
            joinedload(TicketSale.bus),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id == ticket_id)
        .first()
    )
    if not t:
        return jsonify(error="ticket not found"), 404

    # Resolve stop names + (optional) seq (for per-type fare estimates)
    if t.origin_stop_time:
        origin_name = t.origin_stop_time.stop_name
        o_seq = getattr(t.origin_stop_time, "seq", None)
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin_name = ts.stop_name if ts else ""
        o_seq = getattr(ts, "seq", None)

    if t.destination_stop_time:
        destination_name = t.destination_stop_time.stop_name
        d_seq = getattr(t.destination_stop_time, "seq", None)
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination_name = tsd.stop_name if tsd else ""
        d_seq = getattr(tsd, "seq", None)

    # Group meta
    is_group = bool(getattr(t, "is_group", False))
    g_reg = int(getattr(t, "group_regular", 0) or 0)
    g_dis = int(getattr(t, "group_discount", 0) or 0)
    group_qty = g_reg + g_dis if is_group else 1
    total_pesos = int(round(float(t.price or 0)))

    # Per-type each (best-effort)
    def _fare_for_each(pt: str):
        try:
            if o_seq is None or d_seq is None:
                return None
            hops = abs(int(o_seq) - int(d_seq))
            base = 10 + max(hops - 1, 0) * 2
            return int(round(base * 0.8)) if pt == "discount" else int(base)
        except Exception:
            return None

    reg_each = _fare_for_each("regular")
    dis_each = _fare_for_each("discount")

    breakdown = []
    if is_group and group_qty > 1:
        if g_reg:
            subtotal = (g_reg * reg_each) if reg_each is not None else None
            breakdown.append({"passenger_type": "regular", "quantity": g_reg, "fare_each": reg_each, "subtotal": subtotal})
        if g_dis:
            subtotal = (g_dis * dis_each) if dis_each is not None else None
            breakdown.append({"passenger_type": "discount", "quantity": g_dis, "fare_each": dis_each, "subtotal": subtotal})

    # QR helpers
    amount = total_pesos
    prefix = "discount" if (t.passenger_type or "").lower() == "discount" else "regular"
    filename = f"{prefix}_{amount}.jpg"
    qr_url  = url_for("static", filename=f"qr/{filename}", _external=True)
    qr_link = url_for("commuter.commuter_ticket_receipt_qr", ticket_id=t.id, _external=True)
    payload = build_qr_payload(t, origin_name=origin_name, destination_name=destination_name)

    issuer_id = getattr(t, "issued_by", None) or getattr(g, "user", None).id

    # Robust "voided" check (use module helper if present)
    try:
        is_void = _is_ticket_void(t)  # type: ignore[name-defined]
    except NameError:
        st = (getattr(t, "status", None) or "").strip().lower()
        is_void = bool(getattr(t, "voided", False)) or st in {"void", "voided", "refunded", "cancelled"}

    out = {
        "id": t.id,
        "referenceNo": t.reference_no,
        "date": t.created_at.strftime("%B %d, %Y"),
        "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "origin": origin_name,
        "destination": destination_name,
        "passengerType": (t.passenger_type or "").title(),
        "commuter": f"{t.user.first_name} {t.user.last_name}" if t.user else "Guest",
        "fare": total_pesos,
        "paid": bool(t.paid) and not is_void,           # never show paid=true when voided
        "voided": bool(is_void),
        "state": ("voided" if is_void else ("paid" if bool(t.paid) else "unpaid")),
        "qr": payload,
        "qr_link": qr_link,
        "qr_url": qr_url,
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
        "paoId": issuer_id,
        # Group-aware extras:
        "is_group": is_group,
        "group": {
            "regular": g_reg,
            "discount": g_dis,
            "total": group_qty,
            "breakdown": breakdown,
            "total_fare": total_pesos,
        } if is_group else None,
    }
    return jsonify(out), 200


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
    group     = (request.args.get("group") or "").lower() in {"1", "true", "yes"}

    # ---------- UNGROUPED (one row per ticket, PAID-ONLY) ----------
    if not group:
        q = (
            TicketSale.query.options(
                joinedload(TicketSale.user),
                joinedload(TicketSale.origin_stop_time),
                joinedload(TicketSale.destination_stop_time),
            )
            .filter(TicketSale.user_id == g.user.id, TicketSale.paid.is_(True))
        )
        # time window
        try:
            q = _day_range_filter(q, date_str, days)
        except ValueError as e:
            return jsonify(error=str(e)), 400

        if bus_id:
            q = q.filter(TicketSale.bus_id == bus_id)

        total = q.count()
        rows = (
            q.order_by(TicketSale.id.desc())
             .offset((page - 1) * page_size)
             .limit(page_size)
             .all()
        )

        items = []
        for t in rows:
            # names via StopTime or TicketStop fallback
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

            fare = int(round(float(t.price or 0)))
            base = {
                "id": t.id,
                "referenceNo": t.reference_no,
                "date": t.created_at.strftime("%B %d, %Y"),
                "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
                "origin": origin_name,
                "destination": destination_name,
                "fare": fare,
                "paid": True,  # PAO-only receipts
                "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
                "view_url": url_for("commuter.commuter_ticket_view", ticket_id=t.id, _external=True),
            }
            if not light:
                base.update({
                    "passengerType": (t.passenger_type or "").title(),
                    "commuter": f"{t.user.first_name} {t.user.last_name}" if t.user else "Guest",
                    "bus_id": t.bus_id,
                    "batch_id": int(getattr(t, "batch_id", None) or t.id),
                })
            items.append(base)

        return jsonify(
            items=items,
            page=page,
            page_size=page_size,
            total=total,
            has_more=(page * page_size) < total,
        ), 200

    # ---------- GROUPED (one row per batch, PAID-ONLY) ----------
    where_dates = []
    params = {}

    if date_str:
        try:
            day = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return jsonify(error="date must be YYYY-MM-DD"), 400
        where_dates.append("DATE(created_at) = :oneday")
        params["oneday"] = day
    elif days in {"7", "30"}:
        cutoff = dt.datetime.utcnow() - timedelta(days=int(days))
        where_dates.append("created_at >= :cutoff")
        params["cutoff"] = cutoff

    if bus_id:
        where_dates.append("bus_id = :bus_id")
        params["bus_id"] = bus_id

    # Only this user's PAID tickets
    base_where = "user_id = :uid AND paid = 1"
    params["uid"] = g.user.id
    if where_dates:
        base_where += " AND " + " AND ".join(where_dates)

    total = int(db.session.execute(
        text(f"""
            SELECT COUNT(*) FROM (
              SELECT COALESCE(batch_id, id) AS batch_key
              FROM ticket_sales
              WHERE {base_where}
              GROUP BY COALESCE(batch_id, id)
            ) AS g
        """),
        params,
    ).scalar() or 0)

    groups = db.session.execute(
        text(f"""
            SELECT
              COALESCE(batch_id, id) AS batch_id,
              MIN(id)  AS head_id,
              COUNT(*) AS qty,
              SUM(CAST(price AS SIGNED)) AS total_pesos,
              MAX(created_at) AS latest_created_at
            FROM ticket_sales
            WHERE {base_where}
            GROUP BY COALESCE(batch_id, id)
            ORDER BY head_id DESC
            LIMIT :lim OFFSET :off
        """),
        {**params, "lim": page_size, "off": (page - 1) * page_size},
    ).mappings().all()

    if not groups:
        return jsonify(items=[], page=page, page_size=page_size, total=0, has_more=False), 200

    head_ids = [int(r["head_id"]) for r in groups]
    heads = (
        TicketSale.query.options(
            joinedload(TicketSale.user),
            joinedload(TicketSale.origin_stop_time),
            joinedload(TicketSale.destination_stop_time),
        )
        .filter(TicketSale.id.in_(head_ids))
        .all()
    )
    head_map = {t.id: t for t in heads}

    items = []
    for grow in groups:
        head = head_map.get(int(grow["head_id"]))
        if not head:
            continue

        if head.origin_stop_time:
            origin_name = head.origin_stop_time.stop_name
        else:
            ts = TicketStop.query.get(getattr(head, "origin_stop_time_id", None))
            origin_name = ts.stop_name if ts else ""

        if head.destination_stop_time:
            destination_name = head.destination_stop_time.stop_name
        else:
            tsd = TicketStop.query.get(getattr(head, "destination_stop_time_id", None))
            destination_name = tsd.stop_name if tsd else ""

        items.append({
            "id": head.id,
            "batch_id": int(grow["batch_id"]),
            "referenceNo": head.reference_no,
            "date": head.created_at.strftime("%B %d, %Y"),
            "time": head.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "origin": origin_name,
            "destination": destination_name,
            "commuter": f"{head.user.first_name} {head.user.last_name}" if head.user else "Guest",
            "fare": int(grow["total_pesos"] or 0),
            "paid": True,
            "passengers": int(grow["qty"] or 0),
            "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=head.id, _external=True),
            "view_url": url_for("commuter.commuter_ticket_view", ticket_id=head.id, _external=True),
        })

    return jsonify(
        items=items,
        page=page,
        page_size=page_size,
        total=total,
        has_more=(page * page_size) < total,
    ), 200

# Alias: /commuter/my/receipts  → same payload as /commuter/tickets/mine
@commuter_bp.route("/my/receipts", methods=["GET"])
@require_role("commuter")
def my_receipts_alias():
    return my_receipts()

# Alias: /commuter/tickets?mine=1  → same payload as /commuter/tickets/mine
@commuter_bp.route("/tickets", methods=["GET"])
@require_role("commuter")
def tickets_index():
    mine = (request.args.get("mine") or "").lower() in {"1", "true", "yes"}
    # If client asks for mine=1, return the same as /tickets/mine (supports all same query params)
    if mine or True:
        return my_receipts()
    # (If you really want a 404 when mine is missing, replace the line above with:)
    # return jsonify(error="not found"), 404


@commuter_bp.route("/tickets/<int:ticket_id>/batch", methods=["GET"])
@require_role("commuter")
def commuter_get_ticket_batch(ticket_id: int):
    t = TicketSale.query.filter_by(id=ticket_id).first()
    if not t or (t.user_id != g.user.id):
        return jsonify(error="not found"), 404

    # names
    if t.origin_stop_time:
        origin = t.origin_stop_time.stop_name
    else:
        ts = TicketStop.query.get(getattr(t, "origin_stop_time_id", None))
        origin = ts.stop_name if ts else ""

    if t.destination_stop_time:
        destination = t.destination_stop_time.stop_name
    else:
        tsd = TicketStop.query.get(getattr(t, "destination_stop_time_id", None))
        destination = tsd.stop_name if tsd else ""

    # Group-first; fall back to legacy batch
    is_group = bool(getattr(t, "is_group", False))
    if is_group and (int(getattr(t, "group_regular", 0) or 0) + int(getattr(t, "group_discount", 0) or 0) > 1):
        g_reg = int(getattr(t, "group_regular", 0) or 0)
        g_dis = int(getattr(t, "group_discount", 0) or 0)
        passengers = g_reg + g_dis
        total = int(round(float(t.price or 0)))
        breakdown = []
        if g_reg: breakdown.append({"passenger_type": "regular", "quantity": g_reg})
        if g_dis: breakdown.append({"passenger_type": "discount", "quantity": g_dis})

        return jsonify({
            "batch_id": int(getattr(t, "batch_id", None) or t.id),
            "head_ticket_id": int(t.id),
            "referenceNo": t.reference_no,
            "date": t.created_at.strftime("%B %d, %Y"),
            "time": t.created_at.strftime("%I:%M %p").lstrip("0").lower(),
            "origin": origin,
            "destination": destination,
            "passengers": passengers,
            "fare_total": total,
            "breakdown": breakdown,
            "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=t.id, _external=True),
        }), 200

    # legacy multi-row batch
    bid = getattr(t, "batch_id", None) or t.id
    rows = (
        TicketSale.query
        .filter(func.coalesce(TicketSale.batch_id, TicketSale.id) == bid, TicketSale.paid.is_(True))
        .order_by(TicketSale.id.asc())
        .all()
    )
    if not rows:
        return jsonify(error="not found"), 404

    total = sum(int(round(float(r.price or 0))) for r in rows)
    types = {}
    for r in rows:
        types[r.passenger_type or "regular"] = types.get(r.passenger_type or "regular", 0) + 1

    head = rows[0]
    return jsonify({
        "batch_id": int(bid),
        "head_ticket_id": int(head.id),
        "referenceNo": head.reference_no,
        "date": head.created_at.strftime("%B %d, %Y"),
        "time": head.created_at.strftime("%I:%M %p").lstrip("0").lower(),
        "origin": origin,
        "destination": destination,
        "passengers": len(rows),
        "fare_total": total,
        "breakdown": [{"passenger_type": k, "quantity": v} for k, v in types.items()],
        "receipt_image": url_for("commuter.commuter_ticket_image", ticket_id=head.id, _external=True),
    }), 200


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
