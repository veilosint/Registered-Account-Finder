import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sse_starlette.sse import EventSourceResponse
from pydantic import BaseModel

from user_scanner.core import engine

# --- Configuration ---
RATE_LIMIT = "3/minute"
active_scans = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    active_scans.clear()


app = FastAPI(title="Email Scanner Live", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https://.*\.(lovable\.app|lovableproject\.com|lovable\.dev)|http://localhost:\d+",
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ScanRequest(BaseModel):
    email: str


@app.get("/health")
async def health_check():
    return {"status": "ok", "service": "email-scanner-live"}


def _result_to_payload(result):
    """Convert a user-scanner Result object into a JSON-friendly dict."""
    payload = {}

    # Try the built-in as_dict() first
    if hasattr(result, "as_dict"):
        try:
            payload = result.as_dict()
            if isinstance(payload, dict):
                return payload
        except Exception:
            pass

    # Fallback: read common attributes manually
    for attr in ("site", "name", "url", "category", "status", "exists", "extra"):
        if hasattr(result, attr):
            try:
                payload[attr] = getattr(result, attr)
            except Exception:
                pass

    return payload


def _result_found(result) -> bool:
    """Determine whether a Result represents a real hit."""
    # Try common attributes for 'found' state
    for attr in ("exists", "found", "is_found"):
        if hasattr(result, attr):
            try:
                val = getattr(result, attr)
                if callable(val):
                    val = val()
                return bool(val)
            except Exception:
                pass
    # If the payload has a url, assume it's a hit
    payload = _result_to_payload(result)
    return bool(payload.get("url"))


async def run_scan_and_stream(queue: asyncio.Queue, email: str):
    """Run the scan and push each hit into the queue as it completes."""
    try:
        results = await engine.check_all(email, is_email=True)

        for result in results:
            if not _result_found(result):
                continue

            payload = _result_to_payload(result)
            await queue.put({"type": "hit", "data": payload})
            await asyncio.sleep(0)

        await queue.put({"type": "done", "data": {}})
    except Exception as e:
        await queue.put({"type": "error", "data": {"detail": str(e)}})


@app.post("/api/scan/start")
async def start_scan(body: ScanRequest):
    email = body.email.strip()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email")

    scan_id = f"scan_{email}_{id(body)}"
    queue: asyncio.Queue = asyncio.Queue()
    active_scans[scan_id] = queue

    asyncio.create_task(run_scan_and_stream(queue, email))

    return {"scan_id": scan_id}


@app.get("/api/scan/stream/{scan_id}")
async def stream_scan(scan_id: str):
    if scan_id not in active_scans:
        raise HTTPException(status_code=404, detail="Scan not found")

    queue = active_scans[scan_id]

    async def event_generator():
        try:
            yield {"event": "started", "data": json.dumps({"scan_id": scan_id})}

            while True:
                item = await queue.get()
                if item["type"] == "hit":
                    yield {"event": "hit", "data": json.dumps(item["data"])}
                elif item["type"] == "done":
                    yield {"event": "done", "data": "{}"}
                    break
                elif item["type"] == "error":
                    yield {"event": "error", "data": json.dumps(item["data"])}
                    break
        finally:
            active_scans.pop(scan_id, None)

    return EventSourceResponse(event_generator())
