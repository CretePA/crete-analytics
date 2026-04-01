"""
Crete Analytics — Databricks Analytics Platform
"""

import os
import logging
from pathlib import Path
from contextlib import contextmanager
from decimal import Decimal
from datetime import date, datetime

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import requests as http_requests
from databricks import sql as dbsql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Crete Analytics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "")

# ---------------------------------------------------------------------------
# Databricks SQL connection
# ---------------------------------------------------------------------------
def _env(*names, default=""):
    for n in names:
        v = os.environ.get(n, "")
        if v:
            return v
    return default

_HOST = _env("DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_HOST", "DATABRICKS_INSTANCE")
_WAREHOUSE_ID = _env("DATABRICKS_WAREHOUSE_ID")
_HTTP_PATH = _env("DATABRICKS_HTTP_PATH")
_TOKEN = _env("DATABRICKS_TOKEN")
_CLIENT_ID = _env("DATABRICKS_CLIENT_ID")
_CLIENT_SECRET = _env("DATABRICKS_CLIENT_SECRET")

if not _HTTP_PATH and _WAREHOUSE_ID:
    _HTTP_PATH = f"/sql/1.0/warehouses/{_WAREHOUSE_ID}"

def _get_oauth_token():
    resp = http_requests.post(
        f"https://{_HOST}/oidc/v1/token",
        data={"grant_type": "client_credentials", "client_id": _CLIENT_ID,
              "client_secret": _CLIENT_SECRET, "scope": "all-apis"},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def _new_connection():
    if _TOKEN:
        token = _TOKEN
    elif _CLIENT_ID and _CLIENT_SECRET:
        token = _get_oauth_token()
    else:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        token = w.config.authenticate()
        if callable(token):
            headers = token()
            token = headers.get("Authorization", "").replace("Bearer ", "")
    return dbsql.connect(server_hostname=_HOST, http_path=_HTTP_PATH, access_token=token)

@contextmanager
def get_cursor():
    conn = _new_connection()
    try:
        cursor = conn.cursor()
        yield cursor
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

def _serialize(val):
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (date, datetime)):
        return val.isoformat()
    return val

def run_query(sql):
    """Execute SQL and return {columns, rows} as dicts."""
    with get_cursor() as cur:
        cur.execute(sql)
        cols = [desc[0] for desc in cur.description]
        rows = [dict(zip(cols, [_serialize(v) for v in row])) for row in cur.fetchall()]
    return {"columns": cols, "rows": rows}


# ---------------------------------------------------------------------------
# Weekly Flash SQL — converted from Domo MySQL to Databricks SQL
# Uses sandbox.gold star schema (fact + dim tables)
# ---------------------------------------------------------------------------
CATALOG = "sandbox"
SCHEMA = "gold"

def _tbl(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

def _flash_query(as_of_date=None, member_firm=None):
    """Return helper functions for building Weekly Flash queries."""
    dt = f"DATE('{as_of_date}')" if as_of_date else "CURRENT_DATE()"

    def firm_filter(col="t.member_firm_id"):
        if member_firm:
            return f" AND {col} = '{member_firm}'"
        return ""

    def date_sub(days):
        if as_of_date:
            return f"DATE_SUB(DATE('{as_of_date}'), {days})"
        return f"DATE_SUB(CURRENT_DATE(), {days})"

    return dt, firm_filter, date_sub


@app.get("/api/flash/firms")
def flash_firms():
    """List available member firms."""
    sql = f"""
        SELECT DISTINCT mf.member_firm_id, mf.member_firm_name
        FROM {_tbl('dim_member_firm')} mf
        WHERE mf.is_current = true
        ORDER BY mf.member_firm_name
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/time-gauges")
def flash_time_gauges(as_of_date: str = None, member_firm: str = None):
    """Time entry gauges: hours, TVP, write-up/down — MTD/YTD current vs prior year."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN hours ELSE 0 END) as cy_mtd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN production_amount ELSE 0 END) as cy_mtd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN hours ELSE 0 END) as py_mtd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN production_amount ELSE 0 END) as py_mtd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) THEN hours ELSE 0 END) as py_full_month_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) THEN production_amount ELSE 0 END) as py_full_month_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND entry_date <= {dt} THEN hours ELSE 0 END) as cy_ytd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND entry_date <= {dt} THEN production_amount ELSE 0 END) as cy_ytd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR({dt}) THEN hours ELSE 0 END) as py_ytd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR({dt}) THEN production_amount ELSE 0 END) as py_ytd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 THEN hours ELSE 0 END) as py_full_year_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 THEN production_amount ELSE 0 END) as py_full_year_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN COALESCE(write_up_down, 0) ELSE 0 END) as cy_mtd_wuwd,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN COALESCE(write_up_down, 0) ELSE 0 END) as py_mtd_wuwd,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND entry_date <= {dt} THEN COALESCE(write_up_down, 0) ELSE 0 END) as cy_ytd_wuwd,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR({dt}) THEN COALESCE(write_up_down, 0) ELSE 0 END) as py_ytd_wuwd
        FROM {_tbl('fct_time_entry')} t
        WHERE t.is_billable = true
        {ff()}
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/invoice-gauges")
def flash_invoice_gauges(as_of_date: str = None, member_firm: str = None):
    """Invoice (billings) gauges — MTD/YTD current vs prior year."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT
            SUM(CASE WHEN YEAR(invoice_date) = YEAR({dt}) AND MONTH(invoice_date) = MONTH({dt}) AND DAY(invoice_date) <= DAY({dt}) THEN invoice_amount ELSE 0 END) as cy_mtd_billings,
            SUM(CASE WHEN YEAR(invoice_date) = YEAR({dt}) - 1 AND MONTH(invoice_date) = MONTH({dt}) AND DAY(invoice_date) <= DAY({dt}) THEN invoice_amount ELSE 0 END) as py_mtd_billings,
            SUM(CASE WHEN YEAR(invoice_date) = YEAR({dt}) - 1 AND MONTH(invoice_date) = MONTH({dt}) THEN invoice_amount ELSE 0 END) as py_full_month_billings,
            SUM(CASE WHEN YEAR(invoice_date) = YEAR({dt}) AND invoice_date <= {dt} THEN invoice_amount ELSE 0 END) as cy_ytd_billings,
            SUM(CASE WHEN YEAR(invoice_date) = YEAR({dt}) - 1 AND DAYOFYEAR(invoice_date) <= DAYOFYEAR({dt}) THEN invoice_amount ELSE 0 END) as py_ytd_billings,
            SUM(CASE WHEN YEAR(invoice_date) = YEAR({dt}) - 1 THEN invoice_amount ELSE 0 END) as py_full_year_billings
        FROM {_tbl('fct_invoice')} t
        {f"WHERE 1=1 {ff()}"}
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/cash-gauges")
def flash_cash_gauges(as_of_date: str = None, member_firm: str = None):
    """Cash receipts gauges — MTD/YTD current vs prior year."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT
            SUM(CASE WHEN YEAR(receipt_date) = YEAR({dt}) AND MONTH(receipt_date) = MONTH({dt}) AND DAY(receipt_date) <= DAY({dt}) THEN receipt_amount ELSE 0 END) as cy_mtd_cash,
            SUM(CASE WHEN YEAR(receipt_date) = YEAR({dt}) - 1 AND MONTH(receipt_date) = MONTH({dt}) AND DAY(receipt_date) <= DAY({dt}) THEN receipt_amount ELSE 0 END) as py_mtd_cash,
            SUM(CASE WHEN YEAR(receipt_date) = YEAR({dt}) - 1 AND MONTH(receipt_date) = MONTH({dt}) THEN receipt_amount ELSE 0 END) as py_full_month_cash,
            SUM(CASE WHEN YEAR(receipt_date) = YEAR({dt}) AND receipt_date <= {dt} THEN receipt_amount ELSE 0 END) as cy_ytd_cash,
            SUM(CASE WHEN YEAR(receipt_date) = YEAR({dt}) - 1 AND DAYOFYEAR(receipt_date) <= DAYOFYEAR({dt}) THEN receipt_amount ELSE 0 END) as py_ytd_cash,
            SUM(CASE WHEN YEAR(receipt_date) = YEAR({dt}) - 1 THEN receipt_amount ELSE 0 END) as py_full_year_cash
        FROM {_tbl('fct_cash_receipt')} t
        {f"WHERE 1=1 {ff()}"}
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/service-lines")
def flash_service_lines(as_of_date: str = None, member_firm: str = None):
    """Service line breakdown — hours + TVP, MTD/YTD."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT service_line,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN hours ELSE 0 END) as cy_mtd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN production_amount ELSE 0 END) as cy_mtd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN hours ELSE 0 END) as py_mtd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND MONTH(entry_date) = MONTH({dt}) AND DAY(entry_date) <= DAY({dt}) THEN production_amount ELSE 0 END) as py_mtd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND entry_date <= {dt} THEN hours ELSE 0 END) as cy_ytd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) AND entry_date <= {dt} THEN production_amount ELSE 0 END) as cy_ytd_tvp,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR({dt}) THEN hours ELSE 0 END) as py_ytd_hours,
            SUM(CASE WHEN YEAR(entry_date) = YEAR({dt}) - 1 AND DAYOFYEAR(entry_date) <= DAYOFYEAR({dt}) THEN production_amount ELSE 0 END) as py_ytd_tvp
        FROM {_tbl('fct_time_entry')} t
        WHERE t.is_billable = true
        {ff()}
        GROUP BY service_line
        ORDER BY service_line
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/utilization")
def flash_utilization(as_of_date: str = None, member_firm: str = None):
    """Utilization by staff level — current week."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT p.staff_level, p.staff_level_index,
            COUNT(DISTINCT t.person_key) as headcount,
            SUM(t.hours) as total_hours,
            SUM(t.production_amount) as total_tvp
        FROM {_tbl('fct_time_entry')} t
        JOIN {_tbl('dim_person')} p ON t.person_key = p.person_key
        WHERE t.is_billable = true
            AND WEEKOFYEAR(t.entry_date) = WEEKOFYEAR({dt})
            AND YEAR(t.entry_date) = YEAR({dt})
            AND p.staff_level IS NOT NULL AND p.staff_level != ''
            {ff()}
        GROUP BY p.staff_level, p.staff_level_index
        ORDER BY p.staff_level_index
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/ar-aging")
def flash_ar_aging(as_of_date: str = None, member_firm: str = None):
    """AR aging buckets from snapshot table."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT
            SUM(ar_0_30) as current_bucket,
            SUM(ar_31_60) as bucket_31_60,
            SUM(ar_61_90) as bucket_61_90,
            SUM(ar_91_120) as bucket_91_120,
            SUM(ar_121_150 + ar_151_180 + ar_181_364 + ar_365_plus) as bucket_120_plus,
            SUM(ar_total) as total
        FROM {_tbl('fct_ar_aging_snapshot')} t
        WHERE t.report_date = (
            SELECT MAX(report_date) FROM {_tbl('fct_ar_aging_snapshot')}
            WHERE report_date <= {dt}
        )
        {ff()}
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/wip-aging")
def flash_wip_aging(as_of_date: str = None, member_firm: str = None):
    """WIP aging buckets from snapshot table."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT
            SUM(wip_0_30) as wip_0_30,
            SUM(wip_31_60) as wip_31_60,
            SUM(wip_61_90) as wip_61_90,
            SUM(wip_91_120) as wip_91_120,
            SUM(wip_121_150 + wip_151_180 + wip_181_210 + wip_211_240 + wip_241_270 + wip_271_300 + wip_301_330 + wip_331_364 + wip_1yr_2yr + wip_2yr_plus) as wip_120_plus,
            SUM(wip_total) as total
        FROM {_tbl('fct_wip_aging_snapshot')} t
        WHERE t.report_date = (
            SELECT MAX(report_date) FROM {_tbl('fct_wip_aging_snapshot')}
            WHERE report_date <= {dt}
        )
        {ff()}
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/wip-risk")
def flash_wip_risk(as_of_date: str = None, member_firm: str = None):
    """At-risk WIP by client (>60 days unbilled)."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT c.client_name, c.partner_name,
            SUM(t.wip_61_90 + t.wip_91_120 + t.wip_121_150 + t.wip_151_180 +
                t.wip_181_210 + t.wip_211_240 + t.wip_241_270 + t.wip_271_300 +
                t.wip_301_330 + t.wip_331_364 + t.wip_1yr_2yr + t.wip_2yr_plus) as wip_amount
        FROM {_tbl('fct_wip_aging_snapshot')} t
        JOIN {_tbl('dim_client')} c ON t.client_key = c.client_key
        WHERE t.report_date = (
            SELECT MAX(report_date) FROM {_tbl('fct_wip_aging_snapshot')}
            WHERE report_date <= {dt}
        )
        {ff()}
        GROUP BY c.client_name, c.partner_name
        HAVING wip_amount > 0
        ORDER BY wip_amount DESC
        LIMIT 50
    """
    return JSONResponse(run_query(sql))


@app.get("/api/flash/ar-risk")
def flash_ar_risk(as_of_date: str = None, member_firm: str = None):
    """At-risk AR by client (>60 days outstanding)."""
    dt, ff, ds = _flash_query(as_of_date, member_firm)
    sql = f"""
        SELECT c.client_name, c.partner_name,
            SUM(t.ar_61_90 + t.ar_91_120 + t.ar_121_150 + t.ar_151_180 +
                t.ar_181_364 + t.ar_365_plus) as ar_amount
        FROM {_tbl('fct_ar_aging_snapshot')} t
        JOIN {_tbl('dim_client')} c ON t.client_key = c.client_key
        WHERE t.report_date = (
            SELECT MAX(report_date) FROM {_tbl('fct_ar_aging_snapshot')}
            WHERE report_date <= {dt}
        )
        {ff()}
        GROUP BY c.client_name, c.partner_name
        HAVING ar_amount > 0
        ORDER BY ar_amount DESC
        LIMIT 50
    """
    return JSONResponse(run_query(sql))


# ---------------------------------------------------------------------------
# Genie
# ---------------------------------------------------------------------------
def _get_genie_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


class GenieQuestion(BaseModel):
    question: str
    conversation_id: Optional[str] = None


@app.post("/api/genie/ask")
def genie_ask(body: GenieQuestion):
    if not GENIE_SPACE_ID:
        return JSONResponse({"error": "GENIE_SPACE_ID not configured"}, status_code=500)
    try:
        w = _get_genie_client()
        if body.conversation_id:
            resp = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID, conversation_id=body.conversation_id, content=body.question)
        else:
            resp = w.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID, content=body.question)

        attachments = []
        for att in (resp.attachments or []):
            if att.text:
                attachments.append({"type": "text", "content": att.text.content})
            if att.query:
                a = {"type": "query", "sql": getattr(att.query, 'query', '') or ''}
                aid = getattr(att, 'attachment_id', None) or getattr(att.query, 'id', None)
                if aid:
                    try:
                        qr = w.genie.get_message_query_result(
                            space_id=GENIE_SPACE_ID, conversation_id=resp.conversation_id,
                            message_id=resp.id, attachment_id=aid)
                        columns = [c.name for c in (qr.statement_response.manifest.schema.columns or [])]
                        rows = []
                        for chunk in (qr.statement_response.result.data_array or []):
                            rows.append(dict(zip(columns, chunk)))
                        a["columns"] = columns
                        a["rows"] = rows[:200]
                    except Exception as e:
                        logger.warning("Could not fetch query result: %s", e)
                attachments.append(a)

        return JSONResponse({
            "conversation_id": resp.conversation_id,
            "message_id": resp.id,
            "attachments": attachments,
        })
    except Exception as e:
        logger.exception("Genie error")
        return JSONResponse({"error": str(e)}, status_code=500)


# ---------------------------------------------------------------------------
# User identity
# ---------------------------------------------------------------------------
@app.get("/api/whoami")
def whoami(request: Request):
    email = (request.headers.get("x-forwarded-email", "")
             or request.headers.get("x-databricks-user-email", "")
             or request.headers.get("x-real-email", ""))
    user = (request.headers.get("x-forwarded-user", "")
            or request.headers.get("x-databricks-user", "")
            or request.headers.get("x-real-user", ""))
    name = email.split("@")[0].split(".")[0].capitalize() if email else user or ""
    return JSONResponse({"email": email, "user": user, "name": name})


@app.get("/api/health")
def health():
    return {"status": "ok"}


# ---- Serve React build ----
build_dir = Path(__file__).parent.parent / "frontend" / "build"
if build_dir.exists():
    app.mount("/static", StaticFiles(directory=build_dir / "static"), name="static")

    @app.get("/{full_path:path}")
    def serve_react(full_path: str):
        file = build_dir / full_path
        if file.exists() and file.is_file():
            return FileResponse(file)
        return FileResponse(build_dir / "index.html")
else:
    logger.warning("No frontend build found at %s", build_dir)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
