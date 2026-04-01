"""
Crete Analytics — Databricks Analytics Platform
"""

import os
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

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
# Genie helpers
# ---------------------------------------------------------------------------
def _get_genie_client():
    """Return a WorkspaceClient for Genie API calls."""
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


class GenieQuestion(BaseModel):
    question: str
    conversation_id: Optional[str] = None


@app.post("/api/genie/ask")
def genie_ask(body: GenieQuestion):
    """Send a question to Genie and return the response."""
    if not GENIE_SPACE_ID:
        return JSONResponse({"error": "GENIE_SPACE_ID not configured"}, status_code=500)

    try:
        w = _get_genie_client()

        if body.conversation_id:
            resp = w.genie.create_message_and_wait(
                space_id=GENIE_SPACE_ID,
                conversation_id=body.conversation_id,
                content=body.question,
            )
        else:
            resp = w.genie.start_conversation_and_wait(
                space_id=GENIE_SPACE_ID,
                content=body.question,
            )

        attachments = []
        for att in (resp.attachments or []):
            if att.text:
                attachments.append({"type": "text", "content": att.text.content})
            if att.query:
                a = {"type": "query", "sql": att.query.query}
                # Try to fetch query results
                if att.query.attachment_id:
                    try:
                        qr = w.genie.get_message_query_result(
                            space_id=GENIE_SPACE_ID,
                            conversation_id=resp.conversation_id,
                            message_id=resp.id,
                            attachment_id=att.query.attachment_id,
                        )
                        columns = [c.name for c in (qr.statement_response.manifest.schema.columns or [])]
                        rows = []
                        for chunk in (qr.statement_response.result.data_array or []):
                            rows.append(dict(zip(columns, chunk)))
                        a["columns"] = columns
                        a["rows"] = rows[:200]  # cap for payload size
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
    """Return the logged-in user from Databricks App proxy headers."""
    email = (
        request.headers.get("x-forwarded-email", "")
        or request.headers.get("x-databricks-user-email", "")
        or request.headers.get("x-real-email", "")
    )
    user = (
        request.headers.get("x-forwarded-user", "")
        or request.headers.get("x-databricks-user", "")
        or request.headers.get("x-real-user", "")
    )
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
    logger.warning("No frontend build found at %s — run `npm run build` in frontend/", build_dir)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
