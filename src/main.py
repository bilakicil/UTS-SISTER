
from contextlib import asynccontextmanager
import asyncio
import time
from typing import List, Union

from fastapi import FastAPI, Query

from .aggregator import LogAggregator
from .models import LogEvent
from .databse import DedupStore


db = DedupStore()
persisted_received, persisted_duplicate_dropped, persisted_uptime = db.get_persisted_stats()
aggregator = LogAggregator(
    db,
    initial_received=persisted_received,
    initial_duplicate_dropped=persisted_duplicate_dropped,
)
start_time = time.time()
base_uptime_seconds = persisted_uptime


@asynccontextmanager
async def lifespan(app: FastAPI):
    worker_task = asyncio.create_task(aggregator.start_worker())
    try:
        yield
    finally:
        db.add_uptime(int(time.time() - start_time))
        worker_task.cancel()
        try:
            await worker_task
        except asyncio.CancelledError:
            pass
        db.close()


app = FastAPI(lifespan=lifespan)


@app.post("/publish")
async def publish(data: Union[LogEvent, List[LogEvent]]):
    if isinstance(data, list):
        for event in data:
            await aggregator.publish(event)
        return {"status": "accepted", "count": len(data)}

    await aggregator.publish(data)
    return {"status": "accepted", "event_id": data.event_id}


@app.get("/stats")
async def get_stats():
    unique_count, topics = db.get_stats()
    uptime_seconds = base_uptime_seconds + int(time.time() - start_time)
    return {
        "received": aggregator.stats["received"],
        "unique_processed": unique_count,
        "duplicate_dropped": aggregator.stats["duplicate_dropped"],
        "topics": topics,
        "uptime_seconds": uptime_seconds,
    }


@app.get("/events")
async def get_events(topic: str = Query(...)):
    events = db.get_events_by_topic(topic)
    return {"topic": topic, "events": events}