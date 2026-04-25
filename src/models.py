from pydantic import BaseModel, Field
from datetime import datetime
from typing import Any, Dict

class LogEvent(BaseModel):
    topic: str
    event_id: str
    timestamp: datetime
    source: str
    payload: Dict[str, Any]