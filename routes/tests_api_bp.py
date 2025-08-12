# routes/tests_api_bp.py
from flask import Blueprint, jsonify, Response
from sqlalchemy import text
from db import db

tests_bp = Blueprint("tests_api", __name__)

@tests_bp.get("/tests")
def list_tests():
    rows = db.session.execute(text("""
        SELECT id, bus_id, label, lat_true, lng_true,
               started_at, ended_at, duration_s, samples,
               mean_err_m, rmse_m, min_err_m, max_err_m
        FROM gps_test
        ORDER BY started_at DESC
        LIMIT 200
    """)).mappings().all()
    return jsonify([dict(r) for r in rows]), 200

@tests_bp.get("/tests/<int:test_id>/samples.csv")
def export_csv(test_id: int):
    rows = db.session.execute(text("""
        SELECT ts, lat, lng, err_m, sats, hdop
        FROM gps_test_sample
        WHERE test_id = :tid
        ORDER BY ts ASC
    """), dict(tid=test_id)).all()

    def generate():
        yield "ts,lat,lng,err_m,sats,hdop\n"
        for ts, lat, lng, err, sats, hdop in rows:
            sats = "" if sats is None else sats
            hdop = "" if hdop is None else hdop
            yield f"{ts.isoformat()},{lat:.6f},{lng:.6f},{err:.2f},{sats},{hdop}\n"

    return Response(generate(),
                    mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=test_{test_id}.csv"})
