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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Crete Analytics")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_methods=["GET"],
    allow_headers=["*"],
)


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
