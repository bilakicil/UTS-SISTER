import os
import sqlite3
import threading
import time


class DedupStore:
	def __init__(self, db_path: str = "data/dedup.db"):
		db_dir = os.path.dirname(db_path)
		os.makedirs(db_dir, exist_ok=True)
		abs_db_path = os.path.abspath(db_path)
		self.conn = sqlite3.connect(abs_db_path, check_same_thread=False, timeout=30)
		self._lock = threading.RLock()
		# Improve concurrent read/write behavior for API + worker access.
		self.conn.execute("PRAGMA journal_mode = WAL")
		self.conn.execute("PRAGMA synchronous = NORMAL")
		self.conn.execute("PRAGMA busy_timeout = 5000")
		self._create_table()
		self._init_stats_row()

	def _execute_with_retry(self, sql: str, params=(), fetch: str = "none"):
		max_retry = 6
		delay = 0.05
		for attempt in range(max_retry):
			try:
				with self._lock:
					cursor = self.conn.execute(sql, params)
					if fetch == "one":
						return cursor.fetchone()
					if fetch == "all":
						return cursor.fetchall()
					return None
			except sqlite3.OperationalError as exc:
				if "locked" not in str(exc).lower() or attempt == max_retry - 1:
					raise
				time.sleep(delay)
				delay *= 2

	def _create_table(self):
		with self._lock:
			with self.conn:
				self.conn.execute(
					"""
					CREATE TABLE IF NOT EXISTS processed_events (
						topic TEXT,
						event_id TEXT,
						timestamp TEXT,
						source TEXT,
						payload TEXT,
						PRIMARY KEY (topic, event_id)
					)
					"""
				)
				self.conn.execute(
					"""
					CREATE TABLE IF NOT EXISTS stats_state (
						id INTEGER PRIMARY KEY CHECK (id = 1),
						received INTEGER NOT NULL DEFAULT 0,
						duplicate_dropped INTEGER NOT NULL DEFAULT 0,
						total_uptime_seconds INTEGER NOT NULL DEFAULT 0,
						last_updated_at TEXT NOT NULL
					)
					"""
				)

	def _init_stats_row(self):
		with self._lock:
			with self.conn:
				self.conn.execute(
					"""
					INSERT OR IGNORE INTO stats_state
					(id, received, duplicate_dropped, total_uptime_seconds, last_updated_at)
					VALUES (1, 0, 0, 0, ?)
					""",
					(time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),),
				)

	def get_persisted_stats(self):
		row = self._execute_with_retry(
			"SELECT received, duplicate_dropped, total_uptime_seconds FROM stats_state WHERE id = 1",
			fetch="one",
		)
		if row is None:
			return 0, 0, 0
		return row[0], row[1], row[2]

	def increment_received(self, count: int = 1):
		with self._lock:
			with self.conn:
				self.conn.execute(
					"""
					UPDATE stats_state
					SET received = received + ?, last_updated_at = ?
					WHERE id = 1
					""",
					(count, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
				)

	def increment_duplicate_dropped(self, count: int = 1):
		with self._lock:
			with self.conn:
				self.conn.execute(
					"""
					UPDATE stats_state
					SET duplicate_dropped = duplicate_dropped + ?, last_updated_at = ?
					WHERE id = 1
					""",
					(count, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
				)

	def add_uptime(self, seconds: int):
		seconds = max(0, int(seconds))
		if seconds == 0:
			return
		with self._lock:
			with self.conn:
				self.conn.execute(
					"""
					UPDATE stats_state
					SET total_uptime_seconds = total_uptime_seconds + ?, last_updated_at = ?
					WHERE id = 1
					""",
					(seconds, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
				)

	def is_duplicate(self, topic: str, event_id: str) -> bool:
		row = self._execute_with_retry(
			"SELECT 1 FROM processed_events WHERE topic = ? AND event_id = ?",
			(topic, event_id),
			fetch="one",
		)
		return row is not None

	def save_event(self, event) -> bool:
		try:
			with self._lock:
				with self.conn:
					self.conn.execute(
						"INSERT INTO processed_events VALUES (?, ?, ?, ?, ?)",
						(
							event.topic,
							event.event_id,
							event.timestamp.isoformat(),
							event.source,
							str(event.payload),
						),
					)
			return True
		except sqlite3.IntegrityError:
			return False

	def get_events_by_topic(self, topic: str):
		return self._execute_with_retry(
			"SELECT * FROM processed_events WHERE topic = ?",
			(topic,),
			fetch="all",
		)

	def get_stats(self):
		unique_row = self._execute_with_retry(
			"SELECT COUNT(*) FROM processed_events",
			fetch="one",
		)
		unique_count = unique_row[0] if unique_row is not None else 0
		topic_rows = self._execute_with_retry(
			"SELECT DISTINCT topic FROM processed_events",
			fetch="all",
		)
		topics = [row[0] for row in topic_rows]
		return unique_count, topics

	def close(self):
		with self._lock:
			self.conn.close()