# tests_api.py
from flask import Flask, jsonify, Response
from sqlalchemy import create_engine, text, event
from config import Config

app = Flask(__name__)

engine = create_engine(Config.SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)

@event.listens_for(engine, "connect")
def _set_manila_timezone(dbapi_conn, _):
    cur = dbapi_conn.cursor()
    try:
        cur.execute("SET time_zone = '+08:00'")
    finally:
        cur.close()

@app.get("/")
def health():
    return jsonify(status="ok"), 200

@app.get("/tests")
def list_tests():
    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, bus_id, label, lat_true, lng_true,
                   started_at, ended_at, duration_s, samples,
                   mean_err_m, rmse_m, min_err_m, max_err_m
            FROM gps_test
            ORDER BY started_at DESC
            LIMIT 200
        """)).mappings().all()
        return jsonify([dict(r) for r in rows]), 200

@app.get("/tests/<int:test_id>/samples.csv")
def export_csv(test_id: int):
    with engine.begin() as conn:
        rows = conn.execute(text("""
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)
