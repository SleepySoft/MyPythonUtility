"""
A comprehensive resource-usage management system with grouping and dual-threshold
availability checks.

The module tracks arbitrary resources that may reset in three ways:
  - unlimited        – pure counter, never resets
  - sliding_window   – time window of N seconds
  - cron             – fixed wall-clock moments (cron syntax)

Every resource belongs to exactly one logical group (default = "default") and
carries two optional thresholds:
  - soft_threshold   – usage ≥ soft  ⇒  not recommended
  - hard_threshold   – usage ≥ hard  ⇒  unavailable
"""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
#  Enums                                                                      #
# --------------------------------------------------------------------------- #
class ResetType(Enum):
    UNLIMITED = "unlimited"
    SLIDING_WINDOW = "sliding_window"
    CRON = "cron"


class TimeUnit(Enum):
    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"


class ResourceUnit(Enum):
    RPM = "requests_per_minute"
    TPM = "tokens_per_minute"
    RPD = "requests_per_day"
    TOKEN = "tokens"
    COUNT = "count"
    MB = "megabytes"
    GB = "gigabytes"
    CUSTOM = "custom"


class CronFrequency(Enum):
    HOURLY = "0 * * * *"
    DAILY = "0 0 * * *"
    WEEKLY = "0 0 * * 0"
    MONTHLY = "0 0 1 * *"
    YEARLY = "0 0 1 1 *"


# --------------------------------------------------------------------------- #
#  Cron pattern helper                                                        #
# --------------------------------------------------------------------------- #
class CronPattern:
    """
    Lightweight cron parser that understands standard 5-field patterns
    (minute hour day month weekday).  Only 'next reset' is computed.
    """

    def __init__(
        self,
        pattern: Optional[str] = None,
        frequency: Optional[CronFrequency] = None,
    ):
        if pattern is None and frequency is None:
            raise ValueError("Either pattern or frequency must be provided")
        self.pattern = frequency.value if frequency else pattern
        self.minute, self.hour, self.day, self.month, self.weekday = self.pattern.split()

    # --------------------------------------------------------------------- #
    #  Next reset calculation – good enough for most use-cases              #
    # --------------------------------------------------------------------- #
    def get_next_reset(self, current_time: Optional[float] = None) -> float:
        if current_time is None:
            current_time = time.time()
        dt = datetime.fromtimestamp(current_time)

        # Built-ins
        if self.pattern == CronFrequency.HOURLY.value:
            nxt = dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        elif self.pattern == CronFrequency.DAILY.value:
            nxt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
        elif self.pattern == CronFrequency.WEEKLY.value:
            days_ahead = (6 - dt.weekday()) % 7 or 7
            nxt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
        elif self.pattern == CronFrequency.MONTHLY.value:
            if dt.month == 12:
                nxt = dt.replace(year=dt.year + 1, month=1, day=1)
            else:
                nxt = dt.replace(month=dt.month + 1, day=1)
            nxt = nxt.replace(hour=0, minute=0, second=0, microsecond=0)
        elif self.pattern == CronFrequency.YEARLY.value:
            nxt = dt.replace(year=dt.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
        else:
            # Fallback – daily reset
            nxt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

        return nxt.timestamp()

    # --------------------------------------------------------------------- #
    #  Serialisation                                                        #
    # --------------------------------------------------------------------- #
    def to_dict(self) -> Dict[str, str]:
        return {"pattern": self.pattern}

    @classmethod
    def from_dict(cls, data: Dict[str, str]) -> "CronPattern":
        return cls(pattern=data["pattern"])


# --------------------------------------------------------------------------- #
#  Resource                                                                   #
# --------------------------------------------------------------------------- #
class Resource:
    """
    Single resource with usage tracking, reset logic, grouping and thresholds.
    """

    def __init__(
        self,
        name: str,
        reset_type: ResetType = ResetType.UNLIMITED,
        limit: int = 0,
        unit: Union[ResourceUnit, str] = ResourceUnit.COUNT,
        window_seconds: Optional[int] = None,
        cron_pattern: Optional[Union[CronPattern, str, CronFrequency]] = None,
        last_reset: Optional[float] = None,
        current_usage: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
        group: str = "default",
        soft_threshold: Optional[int] = None,
        hard_threshold: Optional[int] = None,
    ):
        self.name = name
        self.reset_type = reset_type
        self.limit = limit
        self.current_usage = current_usage
        self.unit = unit.value if isinstance(unit, ResourceUnit) else unit

        # Sliding-window specific
        if reset_type == ResetType.SLIDING_WINDOW and window_seconds is None:
            raise ValueError("window_seconds required for sliding_window")
        self.window_seconds = window_seconds

        # Cron specific
        self.cron_pattern: Optional[CronPattern] = None
        if reset_type == ResetType.CRON:
            if isinstance(cron_pattern, CronFrequency):
                self.cron_pattern = CronPattern(frequency=cron_pattern)
            elif isinstance(cron_pattern, str):
                self.cron_pattern = CronPattern(pattern=cron_pattern)
            elif isinstance(cron_pattern, CronPattern):
                self.cron_pattern = cron_pattern
            else:
                raise ValueError("cron_pattern required for cron reset")
        self.last_reset = last_reset or time.time()
        self.next_reset = (
            self.cron_pattern.get_next_reset(self.last_reset)
            if self.cron_pattern
            else None
        )

        # Grouping and thresholds
        self.metadata = metadata or {}
        self.metadata.setdefault("group", group)
        self.soft_threshold = soft_threshold
        self.hard_threshold = hard_threshold

        # Sliding-window history – list[tuple[timestamp, amount]]
        self.usage_history: List[tuple[float, int]] = []

    # ------------------------------------------------------------------ #
    #  Core usage API                                                    #
    # ------------------------------------------------------------------ #
    def record_usage(self, amount: int = 1) -> bool:
        now = time.time()
        self._check_reset(now)

        if self.reset_type == ResetType.SLIDING_WINDOW:
            self._clean_old_records(now)
            if 0 < self.limit < (self.current_usage + amount):
                return False
            self.usage_history.append((now, amount))
            self.current_usage += amount
        else:
            if 0 < self.limit < (self.current_usage + amount):
                return False
            self.current_usage += amount
        return True

    # ------------------------------------------------------------------ #
    #  Reset handling                                                    #
    # ------------------------------------------------------------------ #
    def _check_reset(self, now: float) -> None:
        if self.reset_type == ResetType.CRON and self.next_reset and now >= self.next_reset:
            self.reset()
            self.last_reset = now
            if self.cron_pattern:
                self.next_reset = self.cron_pattern.get_next_reset(now)

    def _clean_old_records(self, now: float) -> None:
        if self.reset_type != ResetType.SLIDING_WINDOW or not self.window_seconds:
            return
        cutoff = now - self.window_seconds
        self.usage_history = [(ts, amt) for ts, amt in self.usage_history if ts >= cutoff]
        self.current_usage = sum(amt for _, amt in self.usage_history)

    def reset(self) -> None:
        self.current_usage = 0
        self.last_reset = time.time()
        if self.reset_type == ResetType.SLIDING_WINDOW:
            self.usage_history.clear()

    # ------------------------------------------------------------------ #
    #  Threshold helpers                                                 #
    # ------------------------------------------------------------------ #
    def recommendation_status(self) -> Dict[str, Any]:
        """
        Return availability info:
        {
          'available': bool,      # False if hard threshold exceeded
          'recommended': bool,    # False if soft threshold exceeded
          'usage_percentage': float
        }
        """
        pct = self.get_usage_percentage()
        usage = self.current_usage
        hard = self.hard_threshold is not None and usage >= self.hard_threshold
        soft = self.soft_threshold is not None and usage >= self.soft_threshold
        return {
            "available": not hard,
            "recommended": not soft and not hard,
            "usage_percentage": pct,
        }

    # ------------------------------------------------------------------ #
    #  Utilities                                                         #
    # ------------------------------------------------------------------ #
    def get_usage_percentage(self) -> float:
        return 0.0 if self.limit == 0 else (self.current_usage / self.limit) * 100

    def time_until_reset(self) -> Optional[float]:
        if self.next_reset is None:
            return None
        return max(0.0, self.next_reset - time.time())

    def is_limited(self) -> bool:
        return self.limit > 0

    @property
    def group(self) -> str:
        return self.metadata.get("group", "default")

    # ------------------------------------------------------------------ #
    #  Serialisation                                                     #
    # ------------------------------------------------------------------ #
    def to_dict(self) -> Dict[str, Any]:
        data = {
            "name": self.name,
            "reset_type": self.reset_type.value,
            "limit": self.limit,
            "unit": self.unit,
            "current_usage": self.current_usage,
            "last_reset": self.last_reset,
            "metadata": self.metadata,
            "soft_threshold": self.soft_threshold,
            "hard_threshold": self.hard_threshold,
        }
        if self.reset_type == ResetType.SLIDING_WINDOW:
            data["window_seconds"] = self.window_seconds
        if self.reset_type == ResetType.CRON and self.cron_pattern:
            data["cron_pattern"] = self.cron_pattern.to_dict()
            data["next_reset"] = self.next_reset
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Resource":
        cp = None
        if data.get("cron_pattern"):
            cp = CronPattern.from_dict(data["cron_pattern"])
        elif data.get("reset_type") == ResetType.CRON.value:
            cp = CronPattern(frequency=CronFrequency.DAILY)

        return cls(
            name=data["name"],
            reset_type=ResetType(data["reset_type"]),
            limit=data["limit"],
            unit=data["unit"],
            window_seconds=data.get("window_seconds"),
            cron_pattern=cp,
            last_reset=data.get("last_reset"),
            current_usage=data.get("current_usage", 0),
            metadata=data.get("metadata"),
            soft_threshold=data.get("soft_threshold"),
            hard_threshold=data.get("hard_threshold"),
        )


# --------------------------------------------------------------------------- #
#  Manager                                                                    #
# --------------------------------------------------------------------------- #
class ResourceUsageManager:
    """
    Thread-safe manager for many resources with persistence.
    """

    def __init__(self, db_path: str = "resource_usage.db"):
        self.db_path = db_path
        self.resources: Dict[str, Resource] = {}
        self.lock = threading.RLock()
        self._init_database()
        self._load_resources()

    # ------------------------------------------------------------------ #
    #  Database bootstrap                                                #
    # ------------------------------------------------------------------ #
    def _init_database(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS resources (
                    name             TEXT PRIMARY KEY,
                    reset_type       TEXT NOT NULL,
                    limit_value      INTEGER NOT NULL,
                    unit             TEXT NOT NULL,
                    window_seconds   INTEGER,
                    cron_pattern     TEXT,
                    last_reset       REAL NOT NULL,
                    next_reset       REAL,
                    current_usage    INTEGER NOT NULL,
                    metadata         TEXT,
                    soft_threshold   INTEGER,
                    hard_threshold   INTEGER,
                    created_at       REAL NOT NULL,
                    updated_at       REAL NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS usage_history (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    resource_name TEXT NOT NULL,
                    timestamp     REAL NOT NULL,
                    amount        INTEGER NOT NULL,
                    FOREIGN KEY (resource_name) REFERENCES resources(name) ON DELETE CASCADE
                """
            )
            conn.commit()

    # ------------------------------------------------------------------ #
    #  Load resources + history                                          #
    # ------------------------------------------------------------------ #
    def _load_resources(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM resources")
            for row in cur.fetchall():
                try:
                    meta = json.loads(row["metadata"]) if row["metadata"] else {}
                    data = {
                        "name": row["name"],
                        "reset_type": ResetType(row["reset_type"]),
                        "limit": row["limit_value"],
                        "unit": row["unit"],
                        "window_seconds": row["window_seconds"],
                        "cron_pattern": json.loads(row["cron_pattern"])
                        if row["cron_pattern"]
                        else None,
                        "last_reset": row["last_reset"],
                        "current_usage": row["current_usage"],
                        "metadata": meta,
                        "soft_threshold": row["soft_threshold"],
                        "hard_threshold": row["hard_threshold"],
                    }
                    res = Resource.from_dict(data)
                    self.resources[res.name] = res
                except Exception as exc:
                    logger.error("Skipping corrupted row %s: %s", row["name"], exc)

            # Reload sliding-window history
            cur.execute("SELECT resource_name, timestamp, amount FROM usage_history")
            for row in cur.fetchall():
                res = self.resources.get(row["resource_name"])
                if res and res.reset_type == ResetType.SLIDING_WINDOW:
                    res.usage_history.append((row["timestamp"], row["amount"]))
            logger.info("Loaded %d resources", len(self.resources))

    # ------------------------------------------------------------------ #
    #  Resource lifecycle                                                #
    # ------------------------------------------------------------------ #
    def create_resource(
        self,
        name: Optional[str] = None,
        reset_type: ResetType = ResetType.UNLIMITED,
        limit: int = 0,
        unit: Union[ResourceUnit, str] = ResourceUnit.COUNT,
        window_seconds: Optional[int] = None,
        cron_pattern: Optional[Union[CronPattern, str, CronFrequency]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        group: str = "default",
        soft_threshold: Optional[int] = None,
        hard_threshold: Optional[int] = None,
    ) -> Resource:
        if name is None:
            name = str(uuid.uuid4())
        with self.lock:
            if name in self.resources:
                raise ValueError(f"Resource '{name}' already exists")
            res = Resource(
                name=name,
                reset_type=reset_type,
                limit=limit,
                unit=unit,
                window_seconds=window_seconds,
                cron_pattern=cron_pattern,
                metadata=metadata,
                group=group,
                soft_threshold=soft_threshold,
                hard_threshold=hard_threshold,
            )
            self.resources[name] = res
            self._save_resource(res)
            return res

    def get_resource(self, name: str) -> Optional[Resource]:
        return self.resources.get(name)

    def delete_resource(self, name: str) -> bool:
        with self.lock:
            if name not in self.resources:
                return False
            del self.resources[name]
            with sqlite3.connect(self.db_path) as conn:
                cur = conn.cursor()
                cur.execute("DELETE FROM resources WHERE name = ?", (name,))
                cur.execute("DELETE FROM usage_history WHERE resource_name = ?", (name,))
                conn.commit()
            return True

    # ------------------------------------------------------------------ #
    #  Usage API                                                         #
    # ------------------------------------------------------------------ #
    def record_usage(self, resource_name: str, amount: int = 1) -> bool:
        with self.lock:
            res = self.resources.get(resource_name)
            if res is None:
                raise ValueError(f"Resource '{resource_name}' not found")
            ok = res.record_usage(amount)
            if ok:
                self._save_resource(res)
                if res.reset_type == ResetType.SLIDING_WINDOW:
                    self._record_usage_history(resource_name, time.time(), amount)
            return ok

    def _record_usage_history(self, name: str, ts: float, amount: int) -> None:
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO usage_history(resource_name, timestamp, amount) VALUES (?,?,?)",
                (name, ts, amount),
            )
            conn.commit()

    def reset_resource(self, name: str) -> None:
        with self.lock:
            res = self.resources.get(name)
            if res is None:
                raise ValueError(f"Resource '{name}' not found")
            res.reset()
            self._save_resource(res)

    def set_usage(self, name: str, usage: int) -> None:
        with self.lock:
            res = self.resources.get(name)
            if res is None:
                raise ValueError(f"Resource '{name}' not found")
            res.current_usage = usage
            self._save_resource(res)

    def set_limit(self, name: str, limit: int) -> None:
        with self.lock:
            res = self.resources.get(name)
            if res is None:
                raise ValueError(f"Resource '{name}' not found")
            res.limit = limit
            self._save_resource(res)

    # ------------------------------------------------------------------ #
    #  Threshold / availability API                                      #
    # ------------------------------------------------------------------ #
    def check_availability(self, name: str) -> Optional[Dict[str, Any]]:
        res = self.resources.get(name)
        return None if res is None else res.recommendation_status()

    # ------------------------------------------------------------------ #
    #  Group helpers                                                     #
    # ------------------------------------------------------------------ #
    def list_groups(self) -> List[str]:
        return sorted({res.group for res in self.resources.values()})

    def get_group_stats(self, group: str) -> Dict[str, Any]:
        members = [r for r in self.resources.values() if r.group == group]
        return {
            "group": group,
            "member_count": len(members),
            "total_usage": sum(r.current_usage for r in members),
            "total_limit": sum(r.limit for r in members if r.is_limited()),
            "resources": {r.name: r.get_usage_stats() for r in members},
        }

    def recommend_from_group(
        self,
        group: str,
        *,
        prefer_highest_usage: bool = True,
    ) -> Optional[Resource]:
        """
        Pick ONE available resource from the group.
        Available: hard threshold not exceeded.
        Recommended: soft threshold not exceeded.
        If prefer_highest_usage=True  →  take highest usage among recommended,
        otherwise lowest usage among recommended.
        Falls back to any available (non-recommended) if none recommended.
        """
        candidates = [r for r in self.resources.values() if r.group == group]
        if not candidates:
            return None

        # 1. Only recommended
        recommended = [
            r for r in candidates if r.recommendation_status()["recommended"]
        ]
        if recommended:
            key = (lambda r: r.current_usage) if prefer_highest_usage else (
                lambda r: -r.current_usage
            )
            return max(recommended, key=key)

        # 2. Any available
        available = [r for r in candidates if r.recommendation_status()["available"]]
        if available:
            key = (lambda r: r.current_usage) if prefer_highest_usage else (
                lambda r: -r.current_usage
            )
            return max(available, key=key)

        return None

    # ------------------------------------------------------------------ #
    #  Statistics / export                                               #
    # ------------------------------------------------------------------ #
    def get_usage_stats(self, name: str) -> Optional[Dict[str, Any]]:
        res = self.resources.get(name)
        return None if res is None else res.get_usage_stats()

    def get_all_usage_stats(self) -> Dict[str, Dict[str, Any]]:
        return {n: r.get_usage_stats() for n, r in self.resources.items()}

    def export_resources(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self.resources.values()]

    def import_resources(self, data: List[Dict[str, Any]]) -> None:
        with self.lock:
            for item in data:
                try:
                    res = Resource.from_dict(item)
                    self.resources[res.name] = res
                    self._save_resource(res)
                except Exception as exc:
                    raise ValueError(f"Invalid resource data: {exc}") from exc

    # ------------------------------------------------------------------ #
    #  Persistence                                                       #
    # ------------------------------------------------------------------ #
    def _save_resource(self, res: Resource) -> None:
        with self.lock, sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cron_json = json.dumps(res.cron_pattern.to_dict()) if res.cron_pattern else None
            meta_json = json.dumps(res.metadata) if res.metadata else None
            now = time.time()

            cur.execute("SELECT 1 FROM resources WHERE name = ?", (res.name,))
            exists = cur.fetchone() is not None

            if exists:
                cur.execute(
                    """
                    UPDATE resources
                    SET reset_type = ?, limit_value = ?, unit = ?, window_seconds = ?,
                        cron_pattern = ?, last_reset = ?, next_reset = ?, current_usage = ?,
                        metadata = ?, soft_threshold = ?, hard_threshold = ?, updated_at = ?
                    WHERE name = ?
                    """,
                    (
                        res.reset_type.value,
                        res.limit,
                        res.unit,
                        res.window_seconds,
                        cron_json,
                        res.last_reset,
                        res.next_reset,
                        res.current_usage,
                        meta_json,
                        res.soft_threshold,
                        res.hard_threshold,
                        now,
                        res.name,
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO resources
                    (name, reset_type, limit_value, unit, window_seconds, cron_pattern,
                     last_reset, next_reset, current_usage, metadata,
                     soft_threshold, hard_threshold, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        res.name,
                        res.reset_type.value,
                        res.limit,
                        res.unit,
                        res.window_seconds,
                        cron_json,
                        res.last_reset,
                        res.next_reset,
                        res.current_usage,
                        meta_json,
                        res.soft_threshold,
                        res.hard_threshold,
                        now,
                        now,
                    ),
                )
            conn.commit()

    # ------------------------------------------------------------------ #
    #  Maintenance                                                       #
    # ------------------------------------------------------------------ #
    def cleanup_old_usage_data(self, older_than_days: int = 30) -> int:
        cutoff = time.time() - older_than_days * 86400
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM usage_history WHERE timestamp < ?", (cutoff,))
            deleted = cur.rowcount
            conn.commit()
        return deleted


# ----------------------------------------------------------------------------------------------------------------------

if __name__ == '__main__':
    manager = ResourceUsageManager()

    # Create two resources in group "openai"
    manager.create_resource(
        name="gpt4-rpm",
        reset_type=ResetType.SLIDING_WINDOW,
        limit=10_000,
        window_seconds=60,
        group="openai",
        soft_threshold=8_000,
        hard_threshold=10_000,
    )
    manager.create_resource(
        name="gpt4-tpm",
        reset_type=ResetType.SLIDING_WINDOW,
        limit=300_000,
        window_seconds=60,
        group="openai",
        soft_threshold=250_000,
        hard_threshold=300_000,
    )

    # Use the recommendation API
    best = manager.recommend_from_group("openai")
    if best:
        print("Recommended:", best.name, best.current_usage)
    else:
        print("No available resource in group")

    # Check single resource
    print(manager.check_availability("gpt4-rpm"))
    # {'available': True, 'recommended': False, 'usage_percentage': 85.0}
