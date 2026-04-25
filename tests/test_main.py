import os
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from src.main import app
from src.models import LogEvent


@pytest.fixture(scope="module")
def client():
    # Pastikan startup event jalan tiap test context
    with TestClient(app) as c:
        yield c


def wait_until(predicate, timeout=15.0, interval=0.1):
    start = time.time()
    while time.time() - start < timeout:
        if predicate():
            return True
        time.sleep(interval)
    return False

def test_deduplication_logic(client):
    unique_id = f"test-{uuid.uuid4()}"
    payload = {
        "topic": "unittest",
        "event_id": unique_id,
        "timestamp": "2026-04-25T10:00:00Z",
        "source": "pytest",
        "payload": {"temp": 25},
    }

    before = client.get("/stats").json()["duplicate_dropped"]

    client.post("/publish", json=payload)
    client.post("/publish", json=payload)
    client.post("/publish", json=payload)

    assert wait_until(
        lambda: client.get("/stats").json()["duplicate_dropped"] >= before + 2,
        timeout=10.0,
    )

def test_persistence():
    from src.databse import DedupStore
    db = DedupStore("data/test_persistence.db") # Pakai DB test
    event_id = "persist-123"
    
    # Simpan data
    db.save_event(LogEvent(topic="T",event_id=event_id,timestamp="2026-04-25T10:00:00Z",source="pytest",payload={}))
    
    # Simulasikan restart dengan membuat instance baru
    new_db = DedupStore("data/test_persistence.db")
    assert new_db.is_duplicate("T", event_id) is True # Data harus tetap ada

def test_event_schema_validation(client):
    bad_payload = {
        "event_id": "err-123",
        "timestamp": "2026-04-25T10:00:00Z",
        "source": "test",
        "payload": {},
    }
    response = client.post("/publish", json=bad_payload)
    assert response.status_code == 422

def test_api_consistency(client):
    topic_name = f"topic-{uuid.uuid4()}"
    event_id = f"cons-{uuid.uuid4()}"
    payload = {
        "topic": topic_name,
        "event_id": event_id,
        "timestamp": "2026-04-25T10:00:00Z",
        "source": "pytest",
        "payload": {"data": "verified"},
    }

    client.post("/publish", json=payload)

    assert wait_until(
        lambda: len(client.get(f"/events?topic={topic_name}").json()["events"]) >= 1,
        timeout=10.0,
    )

    resp = client.get(f"/events?topic={topic_name}")
    assert resp.status_code == 200
    assert resp.json()["events"][0][1] == event_id

def test_stress_small(client):
    start_time = time.time()
    for i in range(100):
        response = client.post("/publish", json={
            "topic": "small-stress",
            "event_id": f"small-{i}-{uuid.uuid4()}",
            "timestamp": "2026-04-25T10:00:00Z",
            "source": "pytest",
            "payload": {}
        })
        assert response.status_code == 200
    
    duration = time.time() - start_time
    assert duration < 2.0

def test_high_load_performance(client):
    total_target = 5000
    duplicate_count = 1000
    unique_count = total_target - duplicate_count

    topic = f"load-test-{uuid.uuid4()}"
    batch_data = []

    unique_ids = [str(uuid.uuid4()) for _ in range(unique_count)]
    for uid in unique_ids:
        batch_data.append(
            {
                "topic": topic,
                "event_id": uid,
                "timestamp": "2026-04-25T10:00:00Z",
                "source": "load-test",
                "payload": {"status": "ok"},
            }
        )

    for i in range(duplicate_count):
        batch_data.append(
            {
                "topic": topic,
                "event_id": unique_ids[i],
                "timestamp": "2026-04-25T10:00:00Z",
                "source": "load-test",
                "payload": {"status": "retry"},
            }
        )

    before = client.get("/stats").json()["duplicate_dropped"]

    start_time = time.time()
    chunk_size = 1000
    for i in range(0, len(batch_data), chunk_size):
        chunk = batch_data[i : i + chunk_size]
        r = client.post("/publish", json=chunk)
        assert r.status_code == 200
    duration = time.time() - start_time

    assert duration < 40.0, "Sistem terlalu lambat saat publish batch"

    assert wait_until(
        lambda: client.get("/stats").json()["duplicate_dropped"] >= before + duplicate_count,
        timeout=120.0,
    )