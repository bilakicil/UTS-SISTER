import asyncio
from .databse import DedupStore

class LogAggregator:
    def __init__(self, db: DedupStore, initial_received: int = 0, initial_duplicate_dropped: int = 0):
        self.db = db
        self.queue = asyncio.Queue()
        self.stats = {
            "received": initial_received,
            "duplicate_dropped": initial_duplicate_dropped
        }

    async def start_worker(self):
        """Worker yang memproses event secara background"""
        print("[INFO] Worker started. Monitoring queue...")
        while True:
            event = await self.queue.get()
            
            # Cek Deduplication
            if self.db.is_duplicate(event.topic, event.event_id):
                self.stats["duplicate_dropped"] += 1
                self.db.increment_duplicate_dropped()
                print(f"[DEDUP] Ignored duplicate event: {event.event_id}")
            else:
                # Simpan ke DB (Idempotent Action)
                self.db.save_event(event)
                print(f"[PROCESS] Event {event.event_id} stored.")
            
            self.queue.task_done()

    async def publish(self, event):
        self.stats["received"] += 1
        self.db.increment_received()
        await self.queue.put(event)