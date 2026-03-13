"""
main.py
-------
FastAPI application entry point for the NBA Dashboard backend.
"""

import os
import logging
from contextlib import asynccontextmanager
from datetime import datetime

import joblib
from fastapi import FastAPI
from fastapi.responses import Response
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")
_scheduler = None


# ── Lifespan ───────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    # Load models
    try:
        clf_path = os.path.join(MODELS_DIR, "classifier.pkl")
        reg_path = os.path.join(MODELS_DIR, "regressor.pkl")
        if os.path.exists(clf_path):
            app.state.classifier = joblib.load(clf_path)
            logger.info("Classifier loaded")
        else:
            app.state.classifier = None
            logger.warning("classifier.pkl not found")

        if os.path.exists(reg_path):
            app.state.regressor = joblib.load(reg_path)
            logger.info("Regressor loaded")
        else:
            app.state.regressor = None
            logger.warning("regressor.pkl not found")
    except Exception as e:
        logger.error("Model loading failed: %s", e)
        app.state.classifier = None
        app.state.regressor = None

    # Start scheduler
    try:
        from scheduler import start_scheduler
        _scheduler = start_scheduler()
        logger.info("Scheduler started")
    except Exception as e:
        logger.error("Scheduler failed to start: %s", e, exc_info=True)

    yield

    # Shutdown
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="NBA Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS
# In production set CORS_ORIGINS env var to your Vercel domain (comma-separated).
# Locally (no env var) we inject the header directly for any origin.
_cors_origins_raw = os.environ.get("CORS_ORIGINS", "")
if _cors_origins_raw:
    _allowed_origins = {o.strip() for o in _cors_origins_raw.split(",") if o.strip()}
else:
    _allowed_origins = None  # None → allow all


class _CORSMiddleware:
    """Raw ASGI CORS middleware — injects Access-Control-Allow-Origin before headers are sent."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers = dict(scope.get("headers", []))
        origin = headers.get(b"origin", b"").decode()
        method = scope.get("method", "")

        if _allowed_origins is None:
            allow_origin = origin or "*"
        elif origin in _allowed_origins:
            allow_origin = origin
        else:
            allow_origin = None

        # Handle preflight
        if method == "OPTIONS":
            cors_headers = [
                (b"access-control-allow-methods", b"GET, POST, PUT, DELETE, OPTIONS, PATCH"),
                (b"access-control-allow-headers", b"*"),
                (b"access-control-max-age", b"600"),
                (b"content-length", b"0"),
            ]
            if allow_origin:
                cors_headers.insert(0, (b"access-control-allow-origin", allow_origin.encode()))
            await send({"type": "http.response.start", "status": 204, "headers": cors_headers})
            await send({"type": "http.response.body", "body": b""})
            return

        async def send_with_cors(message):
            if message["type"] == "http.response.start" and allow_origin:
                existing = list(message.get("headers", []))
                existing.append((b"access-control-allow-origin", allow_origin.encode()))
                message = {**message, "headers": existing}
            await send(message)

        await self.app(scope, receive, send_with_cors)


app.add_middleware(_CORSMiddleware)


# ── Health ─────────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "models": {
            "classifier": app.state.classifier is not None,
            "regressor": app.state.regressor is not None,
        },
    }


# ── Routers ────────────────────────────────────────────────────────────────────

from routes.games        import router as games_router
from routes.standings    import router as standings_router
from routes.players      import router as players_router
from routes.injuries     import router as injuries_router
from routes.predictions  import router as predictions_router
from routes.playoffs     import router as playoffs_router
from routes.transactions import router as transactions_router
from routes.model_info   import router as model_router

app.include_router(games_router,        prefix="/api/games",        tags=["games"])
app.include_router(standings_router,    prefix="/api/standings",    tags=["standings"])
app.include_router(players_router,      prefix="/api/players",      tags=["players"])
app.include_router(injuries_router,     prefix="/api/injuries",     tags=["injuries"])
app.include_router(predictions_router,  prefix="/api/predictions",  tags=["predictions"])
app.include_router(playoffs_router,     prefix="/api/playoffs",     tags=["playoffs"])
app.include_router(transactions_router, prefix="/api/transactions",  tags=["transactions"])
app.include_router(model_router,        prefix="/api/model",        tags=["model"])
