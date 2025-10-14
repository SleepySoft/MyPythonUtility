import json
import time
import sqlite3
import croniter
import threading
from enum import Enum
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any, Tuple
from dataclasses import dataclass, asdict


class ResetType(Enum):
    """Enumeration of resource reset types"""
    NONE = "none"  # No reset, pure usage tracking
    SLIDING_WINDOW = "sliding_window"  # Time-based sliding window
    FIXED_SCHEDULE = "fixed_schedule"  # Cron-like fixed schedule


class AvailabilityStatus(Enum):
    """Resource availability status"""
    AVAILABLE = "available"
    WARNING = "warning"  # Exceeded first threshold
    UNAVAILABLE = "unavailable"  # Exceeded second threshold


@dataclass
class ResourceLimit:
    """Represents a single limit constraint for a resource"""
    max_quantity: float
    reset_config: Dict[str, Any]  # Reset configuration
    limit_name: str = "default"

    def __post_init__(self):
        """Validate limit configuration"""
        if self.max_quantity <= 0:
            raise ValueError("Max quantity must be positive")


class ResetStrategy(ABC):
    """Abstract base class for reset strategies"""

    @abstractmethod
    def needs_reset(self, last_reset: float) -> bool:
        """Check if resource needs reset"""
        pass

    @abstractmethod
    def get_usage_period(self) -> Tuple[float, float]:
        """Get start and end time for usage calculation"""
        pass


class NoResetStrategy(ResetStrategy):
    """Strategy for resources that never reset"""

    def needs_reset(self, last_reset: float) -> bool:
        return False

    def get_usage_period(self) -> Tuple[float, float]:
        return 0, float('inf')


class SlidingWindowStrategy(ResetStrategy):
    """Strategy for sliding window reset"""

    def __init__(self, window_seconds: int):
        self.window_seconds = window_seconds

    def needs_reset(self, last_reset: float) -> bool:
        return False  # Sliding window doesn't need explicit reset

    def get_usage_period(self) -> Tuple[float, float]:
        end_time = time.time()
        start_time = end_time - self.window_seconds
        return start_time, end_time


class FixedScheduleStrategy(ResetStrategy):
    """Strategy for fixed schedule reset using cron expressions"""

    def __init__(self, cron_expression: str):
        self.cron_expression = cron_expression

    def needs_reset(self, last_reset: float) -> bool:
        """Check if we've passed a cron boundary since last reset"""
        iter = croniter.croniter(self.cron_expression, datetime.fromtimestamp(last_reset))
        next_time = iter.get_next(datetime)
        return time.time() >= next_time.timestamp()

    def get_usage_period(self) -> Tuple[float, float]:
        """For fixed schedule, usage is since last cron execution"""
        now = time.time()
        iter = croniter.croniter(self.cron_expression, datetime.fromtimestamp(now))
        last_execution = iter.get_prev(datetime)
        return last_execution.timestamp(), now


class Resource:
    """
    Represents a single resource with multiple constraints and usage tracking.
    """

    def __init__(self, name: str, group: Optional[str], unit: str,
                 limits: List[ResourceLimit], current_usage: float = 0,
                 last_reset_time: Optional[float] = None,
                 extra_data: Optional[Dict] = None,
                 manager: Optional['ResourceUsageManager'] = None):
        """
        Initialize a Resource instance.

        Args:
            name (str): Unique resource name.
            group (str, optional): Group name for categorization.
            unit (str): Display unit for the resource.
            limits (List[ResourceLimit]): List of constraints for this resource.
            current_usage (float): Current usage amount.
            last_reset_time (float, optional): Timestamp of last reset.
            extra_data (dict, optional): Additional user data.
        """
        self.name = name
        self.group = group
        self.unit = unit
        self.limits = limits
        self.current_usage = current_usage
        self.last_reset_time = last_reset_time or time.time()
        self.created_time = time.time()
        self.extra_data = extra_data or {}
        self.manager = manager

        # Default thresholds (80% warning, 95% critical)
        self.warning_threshold = 0.8
        self.critical_threshold = 0.95

        # Initialize reset strategies for each limit
        self.reset_strategies = []
        for limit in self.limits:
            self.reset_strategies.append(self._create_reset_strategy(limit))

        # Check if reset is needed
        self._check_and_reset()

    def _create_reset_strategy(self, limit: ResourceLimit) -> ResetStrategy:
        """Create appropriate reset strategy based on limit configuration"""
        reset_type = limit.reset_config.get("type")

        if reset_type == ResetType.SLIDING_WINDOW.value:
            window_seconds = limit.reset_config.get("window_seconds", 3600)
            return SlidingWindowStrategy(window_seconds)
        elif reset_type == ResetType.FIXED_SCHEDULE.value:
            cron_expression = limit.reset_config.get("cron_expression", "0 0 * * *")
            return FixedScheduleStrategy(cron_expression)
        else:
            return NoResetStrategy()

    def record_usage(self, quantity: float = 1.0) -> bool:
        """
        Record usage of this resource.

        Args:
            quantity (float): Amount to record.

        Returns:
            bool: True if usage was recorded successfully.
        """
        # Check if we can record this usage
        if not self._can_use(quantity):
            return False

        # Record usage in database
        if self.manager:
            self.manager._record_usage_event(self.name, quantity)

        self.current_usage += quantity
        return True

    def _can_use(self, quantity: float) -> bool:
        """Check if the resource can be used with the given quantity"""
        # Check all limits using the strictest constraint
        for i, limit in enumerate(self.limits):
            strategy = self.reset_strategies[i]
            if not self._check_limit(limit, strategy, quantity):
                return False
        return True

    def _check_limit(self, limit: ResourceLimit, strategy: ResetStrategy,
                     quantity: float) -> bool:
        """Check a specific limit with its reset strategy"""
        current_usage = self._get_current_usage(strategy)
        return current_usage + quantity <= limit.max_quantity

    def _get_current_usage(self, strategy: ResetStrategy) -> float:
        """Get current usage based on the reset strategy"""
        if isinstance(strategy, SlidingWindowStrategy):
            # For sliding window, calculate usage from history
            if self.manager:
                return self.manager._get_window_usage(self.name, strategy)
        return self.current_usage

    def check_availability(self, required_quantity: float = 1.0) -> AvailabilityStatus:
        """
        Check availability status of the resource.

        Args:
            required_quantity (float): Quantity needed.

        Returns:
            AvailabilityStatus: Availability status.
        """
        if not self._can_use(required_quantity):
            return AvailabilityStatus.UNAVAILABLE

        # Find the strictest limit to check against thresholds
        max_usable = float('inf')
        for i, limit in enumerate(self.limits):
            strategy = self.reset_strategies[i]
            current_usage = self._get_current_usage(strategy)
            remaining = limit.max_quantity - current_usage
            if remaining < max_usable:
                max_usable = remaining

        if max_usable <= 0:
            return AvailabilityStatus.UNAVAILABLE

        usage_ratio = 1 - (max_usable / self._get_max_limit())

        if usage_ratio >= self.critical_threshold:
            return AvailabilityStatus.UNAVAILABLE
        elif usage_ratio >= self.warning_threshold:
            return AvailabilityStatus.WARNING
        else:
            return AvailabilityStatus.AVAILABLE

    def _get_max_limit(self) -> float:
        """Get the maximum limit value among all constraints"""
        return max(limit.max_quantity for limit in self.limits)

    def _check_and_reset(self) -> None:
        """Check if resource needs reset based on its limits"""
        for i, limit in enumerate(self.limits):
            strategy = self.reset_strategies[i]
            if strategy.needs_reset(self.last_reset_time):
                self._reset_usage(strategy)
                break  # Reset once per check

    def _reset_usage(self, strategy: ResetStrategy) -> None:
        """Reset usage based on strategy"""
        if isinstance(strategy, (FixedScheduleStrategy, NoResetStrategy)):
            self.current_usage = 0
            self.last_reset_time = time.time()

            # Clean up old usage records
            if self.manager:
                self.manager._cleanup_old_records(self.name)

    def to_dict(self) -> Dict[str, Any]:
        """Convert resource to dictionary for JSON serialization"""
        return {
            "name": self.name,
            "group": self.group,
            "unit": self.unit,
            "current_usage": self.current_usage,
            "last_reset_time": self.last_reset_time,
            "created_time": self.created_time,
            "warning_threshold": self.warning_threshold,
            "critical_threshold": self.critical_threshold,
            "limits": [asdict(limit) for limit in self.limits],
            "extra_data": self.extra_data
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any],
                  manager: Optional['ResourceUsageManager'] = None) -> 'Resource':
        """Create Resource from dictionary"""
        limits = [ResourceLimit(**limit_data) for limit_data in data.get("limits", [])]

        return cls(
            name=data["name"],
            group=data.get("group"),
            unit=data.get("unit", "count"),
            limits=limits,
            current_usage=data.get("current_usage", 0),
            last_reset_time=data.get("last_reset_time"),
            extra_data=data.get("extra_data", {}),
            manager=manager
        )


class ResourceUsageManager:
    """
    A comprehensive resource usage manager that handles various types of resource limitations
    with multiple constraint types and reset mechanisms.
    """

    def __init__(self, db_path: str = "resource_usage.db"):
        """
        Initialize the ResourceUsageManager with SQLite database storage.

        Args:
            db_path (str): Path to the SQLite database file
        """
        self.db_path = db_path
        self.lock = threading.RLock()
        self.resources: Dict[str, Resource] = {}
        self.groups: Dict[str, List[str]] = {}

        # Initialize database and load existing resources
        self._init_database()
        self._load_resources()

    def _init_database(self) -> None:
        """Initialize database tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Resources table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS resources (
                    name TEXT PRIMARY KEY,
                    resource_group TEXT,
                    unit TEXT,
                    limits_json TEXT,
                    current_usage REAL DEFAULT 0,
                    last_reset_time REAL,
                    created_time REAL NOT NULL,
                    extra_data TEXT,
                    warning_threshold REAL DEFAULT 0.8,
                    critical_threshold REAL DEFAULT 0.95
                )
            """)

            # Usage history table for sliding window resources
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usage_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    resource_name TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    timestamp REAL NOT NULL,
                    FOREIGN KEY (resource_name) REFERENCES resources (name)
                )
            """)

            conn.commit()

    def _load_resources(self) -> None:
        """Load resources from database on startup"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM resources")

            for row in cursor.fetchall():
                (name, group, unit, limits_json, current_usage,
                 last_reset_time, created_time, extra_data,
                 warning_threshold, critical_threshold) = row

                limits_data = json.loads(limits_json)
                limits = [ResourceLimit(**limit_data) for limit_data in limits_data]

                resource = Resource(
                    name=name,
                    group=group,
                    unit=unit,
                    limits=limits,
                    current_usage=current_usage,
                    last_reset_time=last_reset_time,
                    extra_data=json.loads(extra_data) if extra_data else {},
                    manager=self
                )

                resource.warning_threshold = warning_threshold
                resource.critical_threshold = critical_threshold

                self.resources[name] = resource

                # Add to group
                if group:
                    if group not in self.groups:
                        self.groups[group] = []
                    self.groups[group].append(name)

                print(f"Loaded resource: {name} (group: {group}), usage: {current_usage}")

    def _record_usage_event(self, resource_name: str, quantity: float) -> None:
        """Record a usage event in the database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO usage_history (resource_name, quantity, timestamp)
                VALUES (?, ?, ?)
            """, (resource_name, quantity, time.time()))
            conn.commit()

    def _get_window_usage(self, resource_name: str, strategy: SlidingWindowStrategy) -> float:
        """Calculate usage within a time window"""
        start_time, end_time = strategy.get_usage_period()

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COALESCE(SUM(quantity), 0) 
                FROM usage_history 
                WHERE resource_name = ? AND timestamp BETWEEN ? AND ?
            """, (resource_name, start_time, end_time))

            result = cursor.fetchone()
            return result[0] if result else 0.0

    def _cleanup_old_records(self, resource_name: str, older_than_days: int = 30) -> None:
        """Clean up old usage history records"""
        cutoff_time = time.time() - (older_than_days * 24 * 3600)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM usage_history 
                WHERE resource_name = ? AND timestamp < ?
            """, (resource_name, cutoff_time))
            conn.commit()

    def manual_reset(self, resource_name: str) -> bool:
        """
        Manually reset a resource's usage to zero.

        Args:
            resource_name (str): Name of the resource to reset.

        Returns:
            bool: True if reset successful, False if resource doesn't exist.
        """
        with self.lock:
            if resource_name not in self.resources:
                return False

            resource = self.resources[resource_name]
            resource.current_usage = 0
            resource.last_reset_time = time.time()

            # Clear usage history for this resource
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM usage_history WHERE resource_name = ?",
                               (resource_name,))
                conn.commit()

            self._update_resource_in_db(resource)
            return True

    def calibrate_usage(self, resource_name: str, new_usage: float) -> bool:
        """
        Manually set/calibrate the current usage of a resource.

        Args:
            resource_name (str): Name of the resource.
            new_usage (float): New usage value to set.

        Returns:
            bool: True if calibration successful, False if resource doesn't exist.
        """
        with self.lock:
            if resource_name not in self.resources:
                return False

            resource = self.resources[resource_name]
            resource.current_usage = new_usage
            resource.last_reset_time = time.time()

            self._update_resource_in_db(resource)
            return True

    def get_statistics(self, resource_name: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive statistics for a resource.

        Args:
            resource_name (str): Name of the resource.

        Returns:
            Optional[Dict]: Statistics dictionary or None if resource doesn't exist.
        """
        with self.lock:
            if resource_name not in self.resources:
                return None

            resource = self.resources[resource_name]
            stats = resource.to_dict()

            # Add additional statistics
            stats['availability_status'] = resource.check_availability().value

            # Calculate usage percentages for each limit
            usage_breakdown = []
            for i, limit in enumerate(resource.limits):
                strategy = resource.reset_strategies[i]
                current_usage = resource._get_current_usage(strategy)
                usage_pct = (current_usage / limit.max_quantity) * 100

                usage_breakdown.append({
                    'limit_name': limit.limit_name,
                    'max_quantity': limit.max_quantity,
                    'current_usage': current_usage,
                    'usage_percentage': usage_pct,
                    'remaining': limit.max_quantity - current_usage
                })

            stats['usage_breakdown'] = usage_breakdown
            stats['strictest_limit'] = min(
                usage_breakdown,
                key=lambda x: x['remaining']
            ) if usage_breakdown else None

            return stats

    def export_configuration(self, resource_name: str) -> Optional[str]:
        """
        Export resource configuration as JSON.

        Args:
            resource_name (str): Name of the resource.

        Returns:
            Optional[str]: JSON string of configuration, or None if resource doesn't exist.
        """
        with self.lock:
            if resource_name not in self.resources:
                return None

            resource = self.resources[resource_name]
            config = resource.to_dict()
            return json.dumps(config, indent=2, default=str)

    def import_configuration(self, config_json: str) -> str:
        """
        Import resource configuration from JSON.

        Args:
            config_json (str): JSON string containing resource configuration.

        Returns:
            str: Name of the imported resource.
        """
        config = json.loads(config_json)
        resource = Resource.from_dict(config, self)

        with self.lock:
            if resource.name in self.resources:
                raise ValueError(f"Resource '{resource.name}' already exists")

            self.resources[resource.name] = resource
            self._save_resource_to_db(resource)

            if resource.group:
                if resource.group not in self.groups:
                    self.groups[resource.group] = []
                self.groups[resource.group].append(resource.name)

            return resource.name

    def _save_resource_to_db(self, resource: Resource) -> None:
        """Save resource to database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            limits_json = json.dumps([asdict(limit) for limit in resource.limits])
            extra_data_json = json.dumps(resource.extra_data)

            cursor.execute("""
                INSERT OR REPLACE INTO resources 
                (name, resource_group, unit, limits_json, current_usage, 
                 last_reset_time, created_time, extra_data, warning_threshold, critical_threshold)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (resource.name, resource.group, resource.unit, limits_json,
                  resource.current_usage, resource.last_reset_time,
                  resource.created_time, extra_data_json,
                  resource.warning_threshold, resource.critical_threshold))

            conn.commit()

    def _update_resource_in_db(self, resource: Resource) -> None:
        """Update resource in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE resources 
                SET current_usage = ?, last_reset_time = ?, extra_data = ?
                WHERE name = ?
            """, (resource.current_usage, resource.last_reset_time,
                  json.dumps(resource.extra_data), resource.name))

            conn.commit()

    def create_resource(self, name: Optional[str] = None, group: Optional[str] = None,
                        unit: str = "count", limits: Optional[List[ResourceLimit]] = None) -> str:
        """
        Create a new resource with automatic name generation if not provided.

        Args:
            name (str, optional): Unique resource name. Auto-generated if None.
            group (str, optional): Resource group name.
            unit (str): Measurement unit for display purposes.
            limits (List[ResourceLimit]): List of limits for this resource.

        Returns:
            str: The resource name (generated or provided).
        """
        with self.lock:
            if name is None:
                name = f"resource_{int(time.time())}_{len(self.resources)}"

            if name in self.resources:
                raise ValueError(f"Resource '{name}' already exists")

            if limits is None:
                limits = [ResourceLimit(max_quantity=1000, reset_config={"type": ResetType.NONE.value})]

            resource = Resource(
                name=name,
                group=group,
                unit=unit,
                limits=limits,
                current_usage=0,
                last_reset_time=time.time(),
                extra_data={}
            )

            self.resources[name] = resource

            if group:
                if group not in self.groups:
                    self.groups[group] = []
                self.groups[group].append(name)

            # Save to database
            self._save_resource_to_db(resource)

            return name

    def record_usage(self, resource_name: str, quantity: float = 1.0) -> bool:
        """
        Record usage of a resource.

        Args:
            resource_name (str): Name of the resource.
            quantity (float): Amount of resource used.

        Returns:
            bool: True if usage was recorded successfully, False if resource doesn't exist.
        """
        with self.lock:
            if resource_name not in self.resources:
                return False

            resource = self.resources[resource_name]
            success = resource.record_usage(quantity)

            if success:
                self._update_resource_in_db(resource)

            return success

    def check_availability(self, resource_name: str, required_quantity: float = 1.0) -> AvailabilityStatus:
        """
        Check availability status of a resource.

        Args:
            resource_name (str): Name of the resource.
            required_quantity (float): Quantity needed.

        Returns:
            AvailabilityStatus: Availability status.
        """
        with self.lock:
            if resource_name not in self.resources:
                return AvailabilityStatus.UNAVAILABLE

            resource = self.resources[resource_name]
            return resource.check_availability(required_quantity)

    def get_recommended_resource(self, group: str) -> Optional[str]:
        """
        Get a recommended resource from a group that has the highest usage
        while still being available (to avoid fragmentation).

        Args:
            group (str): Group name.

        Returns:
            Optional[str]: Recommended resource name or None if no available resources.
        """
        with self.lock:
            if group not in self.groups:
                return None

            available_resources = []
            for resource_name in self.groups[group]:
                resource = self.resources[resource_name]
                status = resource.check_availability(1.0)
                if status != AvailabilityStatus.UNAVAILABLE:
                    # Prefer resources with higher usage to avoid fragmentation
                    available_resources.append((resource.current_usage, resource_name))

            if not available_resources:
                return None

            # Return the resource with highest current usage
            available_resources.sort(reverse=True)
            return available_resources[0][1]


# Convenience functions for common resource types
def create_hourly_limit(manager: ResourceUsageManager, name: str, limit: int,
                        group: str = "hourly") -> str:
    """Create a resource that resets hourly"""
    resource_limit = ResourceLimit(
        max_quantity=limit,
        reset_config={"type": ResetType.FIXED_SCHEDULE.value, "cron_expression": "0 * * * *"},
        limit_name="hourly"
    )
    return manager.create_resource(name=name, group=group, unit="requests", limits=[resource_limit])


def create_daily_limit(manager: ResourceUsageManager, name: str, limit: int,
                       group: str = "daily") -> str:
    """Create a resource that resets daily at midnight"""
    resource_limit = ResourceLimit(
        max_quantity=limit,
        reset_config={"type": ResetType.FIXED_SCHEDULE.value, "cron_expression": "0 0 * * *"},
        limit_name="daily"
    )
    return manager.create_resource(name=name, group=group, unit="uses", limits=[resource_limit])


def create_sliding_window_limit(manager: ResourceUsageManager, name: str, limit: int,
                                window_seconds: int, group: str = "sliding") -> str:
    """Create a sliding window limited resource"""
    resource_limit = ResourceLimit(
        max_quantity=limit,
        reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": window_seconds},
        limit_name="sliding_window"
    )
    return manager.create_resource(name=name, group=group, unit="requests", limits=[resource_limit])


# Usage Example and Convenience Functions
def create_rate_limit_resource(manager: ResourceUsageManager, name: str, requests_per_minute: int,
                               group: str = "api") -> str:
    """Convenience function to create a rate-limited resource"""
    limit = ResourceLimit(
        max_quantity=requests_per_minute,
        reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": 60}
    )
    return manager.create_resource(name=name, group=group, unit="requests", limits=[limit])


def main():
    # Create manager instance
    manager = ResourceUsageManager("test.db")

    # Create a rate-limited API resource
    api_resource = create_rate_limit_resource(manager, "api_requests", 1000, "api")

    # Check availability
    status = manager.check_availability(api_resource, 1)
    print(f"API availability: {status}")

    # Record usage
    manager.record_usage(api_resource, 1)

    # Get recommended resource from group
    recommended = manager.get_recommended_resource("api")
    print(f"Recommended resource: {recommended}")



# Example usage
if __name__ == "__main__":
    main()
