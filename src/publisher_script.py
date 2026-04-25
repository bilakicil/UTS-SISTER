import requests
import time
import uuid

URL = "http://aggregator:8080/publish"

def send_batch():
    print("Publisher: Menunggu Aggregator siap...")
    time.sleep(5)  
    
    print("Publisher: Mengirim 5000 event (20% duplikat)...")
    unique_ids = [str(uuid.uuid4()) for _ in range(4000)]
    
    all_events = []
    for uid in unique_ids:
        all_events.append({"topic": "docker_compose_test", "event_id": uid, "timestamp": "2026-04-25T10:00:00Z", "source": "publisher-service", "payload": {}})
    for i in range(1000):
        all_events.append({"topic": "docker_compose_test", "event_id": unique_ids[i], "timestamp": "2026-04-25T10:00:00Z", "source": "publisher-service", "payload": {}})

    # Kirim per batch 1000
    for i in range(0, len(all_events), 1000):
        chunk = all_events[i:i+1000]
        try:
            requests.post(URL, json=chunk)
            print(f"Publisher: Terkirim {i+1000} events...")
        except Exception as e:
            print(f"Publisher Error: {e}")

if __name__ == "__main__":
    send_batch()
    print("Publisher: Tugas selesai. Tetap standby.")
    while True: time.sleep(10) # Biar container tidak mati