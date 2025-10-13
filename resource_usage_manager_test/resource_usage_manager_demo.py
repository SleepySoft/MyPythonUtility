#!/usr/bin/env python3
"""
Comprehensive demo for ResourceUsageManager
Shows all major features: thresholds, groups, persistence, sliding-window,
cron reset, recommendations, etc.
"""

import os
import sys
import time
import random
import traceback

from litellm.litellm_core_utils.dd_tracing import tracer

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from ResourceUsageManager import (
    ResourceUsageManager,
    ResetType,
    ResourceUnit,
    CronFrequency,
)

DB_FILE = "demo_resource_usage.db"

def print_title(msg: str):
    print(f"\n{'='*70}\n{msg}\n{'='*70}")

def main():
    # Remove old DB if you want a fresh start every demo run
    # os.remove(DB_FILE) if os.path.exists(DB_FILE) else None

    manager = ResourceUsageManager(DB_FILE)

    print_title("1. Create resources with different reset patterns")

    # 1) Unlimited resource
    manager.create_resource(
        name="unlimited_api_calls",
        reset_type=ResetType.UNLIMITED,
        unit=ResourceUnit.COUNT,
        group="public",
    )

    # 2) Sliding-window RPM with thresholds
    manager.create_resource(
        name="gpt4_rpm",
        reset_type=ResetType.SLIDING_WINDOW,
        limit=100,
        window_seconds=60,
        unit=ResourceUnit.RPM,
        group="openai",
        soft_threshold=80,
        hard_threshold=100,
    )

    # 3) Cron-based daily reset
    manager.create_resource(
        name="daily_newsletter",
        reset_type=ResetType.CRON,
        limit=5,
        cron_pattern=CronFrequency.DAILY,
        unit=ResourceUnit.COUNT,
        group="marketing",
        soft_threshold=3,
        hard_threshold=5,
    )

    print("Resources created -> groups:", manager.list_groups())

    print_title("2. Simulate usage and check availability")

    # Burst usage on GPT-4 RPM
    for i in range(85):
        ok = manager.record_usage("gpt4_rpm")
        if not ok:
            print("GPT-4 RPM limit hit at request", i)
            break
    print("GPT-4 status after burst:", manager.check_availability("gpt4_rpm"))

    print_title("3. Group recommendation (highest usage among available)")

    best = manager.recommend_from_group("openai", prefer_highest_usage=True)
    if best:
        print("Recommended resource in 'openai':", best.name,
              "usage =", best.current_usage)
    else:
        print("No available resource in 'openai'")

    print_title("4. Cron reset demo (daily resource)")

    news = manager.get_resource("daily_newsletter")
    print("Daily newsletter before usage ->",
          news.current_usage, "/", news.limit)
    for _ in range(4):
        manager.record_usage("daily_newsletter")
    print("After 4 sends ->", manager.check_availability("daily_newsletter"))

    print_title("5. Persistence demo – re-load from disk")

    del manager
    print("Manager deleted, re-opening same DB...")
    manager2 = ResourceUsageManager(DB_FILE)
    print("Loaded resources:", list(manager2.resources.keys()))
    print("GPT-4 RPM usage after reload:",
          manager2.get_resource("gpt4_rpm").current_usage)

    print_title("6. Sliding-window auto-cleanup (30 s history)")

    # Create 30-second window resource
    manager2.create_resource(
        name="short_window",
        reset_type=ResetType.SLIDING_WINDOW,
        limit=10,
        window_seconds=30,
        unit=ResourceUnit.COUNT,
        group="test",
    )
    for _ in range(10):
        manager2.record_usage("short_window")
    print("short_window usage:", manager2.get_usage_stats("short_window")["current_usage"])
    print("Sleeping 31 s to let records expire...")
    time.sleep(31)
    # Trigger internal cleanup by querying
    manager2.get_resource("short_window")._clean_old_records(time.time())
    print("After cleanup:", manager2.get_usage_stats("short_window")["current_usage"])

    print_title("7. Full export / import")

    exported = manager2.export_resources()
    print("Exported", len(exported), "resources")
    # Simulate transfer to another instance
    new_mgr = ResourceUsageManager(":memory:")
    new_mgr.import_resources(exported)
    print("Imported into new in-memory manager ->",
          new_mgr.get_all_usage_stats().keys())

    print_title("Demo finished – DB file retained at " + DB_FILE)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e))
        traceback.print_exc()
