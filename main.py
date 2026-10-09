import asyncio
import json
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from user_scanner.core import engine

# Global dictionary to track active scan tasks
active_scans: dict[str, asyncio.Queue] = {}

RATE_LIMIT = "3/minute"
limiter = Limiter(key_func=get_remote_address)
app = FastAPI(title="User Scanner API")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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
    return {"status": "ok", "service": "live-scanner"}


async def run_scan_and_stream(queue: asyncio.Queue, email: str):
    """Run the scan and push hits into the queue as they are found."""
    try:
        # This is the key part: we need to iterate over results as they complete.
        # The user-scanner engine might not expose a streaming API directly,
        # so we run the full scan and push results as they become available.
        # For true per-site streaming, you would need to modify the engine internals.
        results = await engine.check_all(email, is_email=True)

        for result in results:
            if result.get("exists"):  # Adjust based on actual Result object attribute
                await queue.put({"type": "hit", "data": result.as_dict()})
                await asyncio.sleep(0)  # Let the event loop breathe

        await queue.put({"type": "done", "data": {}})
    except Exception as e:
        await queue.put({"type": "error", "data": {"detail": str(e)}})


@app.post("/api/scan/start")
@limiter.limit(RATE_LIMIT)
async def start_scan(request: Request, body: ScanRequest):
    email = body.email.strip()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email")

    # Create a unique ID for this scan (in a real app, use a UUID)
    scan_id = f"scan_{email}_{id(body)}"
    queue: asyncio.Queue = asyncio.Queue()
    active_scans[scan_id] = queue

    # Fire off the scan in the background
    asyncio.create_task(run_scan_and_stream(queue, email))

    return {"scan_id": scan_id}


@app.get("/api/scan/stream/{scan_id}")
async def stream_scan(scan_id: str):
    if scan_id not in active_scans:
        raise HTTPException(status_code=404, detail="Scan not found")

    queue = active_scans[scan_id]

    async def event_generator():
        try:
            # Send a "started" event
            yield f"event: started\ndata: {json.dumps({'scan_id': scan_id})}\n\n"

            while True:
                item = await queue.get()
                if item["type"] == "hit":
                    yield f"event: hit\ndata: {json.dumps(item['data'])}\n\n"
                elif item["type"] == "done":
                    yield f"event: done\ndata: {{}}\n\n"
                    break
                elif item["type"] == "error":
                    yield f"event: error\ndata: {json.dumps(item['data'])}\n\n"
                    break
        finally:
            # Cleanup when the client disconnects
            active_scans.pop(scan_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # Disables buffering on Render/nginx [citation:3]
        },
    )
