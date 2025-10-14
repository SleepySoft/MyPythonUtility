import os
import sys
import time
import json
import traceback

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from MyPythonUtility.ResourceUsageManager import ResourceUsageManager, ResourceLimit, ResetType, create_hourly_limit, \
    create_daily_limit, create_sliding_window_limit


TEST_DB_PATH = 'resource_usage_manager_demo.db'


def demo_resource_usage_manager():
    """
    Demo program to showcase all features of ResourceUsageManager
    """
    print("=== ResourceUsageManager Demo ===\n")

    # 1. Initialize manager with in-memory database for demo
    manager = ResourceUsageManager(TEST_DB_PATH)
    print("✓ ResourceUsageManager initialized with in-memory database")

    # 2. Create different types of resources
    print("\n1. Creating Resources with Different Limit Types")
    print("-" * 50)

    # 2.1 Pure usage resource (no reset)
    pure_resource = manager.create_resource(
        name="api_total_requests",
        group="api",
        unit="requests",
        limits=[ResourceLimit(
            max_quantity=10000,
            reset_config={"type": ResetType.NONE.value},
            limit_name="lifetime_limit"
        )]
    )
    print(f"✓ Created pure usage resource: {pure_resource}")

    # 2.2 Sliding window resource (per minute)
    sliding_resource = manager.create_resource(
        name="api_rate_limit",
        group="api",
        unit="requests",
        limits=[ResourceLimit(
            max_quantity=100,
            reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": 60},
            limit_name="per_minute"
        )]
    )
    print(f"✓ Created sliding window resource (60s): {sliding_resource}")

    # 2.3 Fixed schedule resource (daily reset)
    daily_resource = manager.create_resource(
        name="daily_api_calls",
        group="api",
        unit="calls",
        limits=[ResourceLimit(
            max_quantity=5000,
            reset_config={"type": ResetType.FIXED_SCHEDULE.value, "cron_expression": "0 0 * * *"},
            limit_name="daily"
        )]
    )
    print(f"✓ Created daily reset resource: {daily_resource}")

    # 2.4 Composite limits resource (multiple constraints)
    composite_resource = manager.create_resource(
        name="premium_api",
        group="premium",
        unit="requests",
        limits=[
            ResourceLimit(
                max_quantity=1000,
                reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": 60},
                limit_name="per_minute"
            ),
            ResourceLimit(
                max_quantity=20000,
                reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": 3600},
                limit_name="per_hour"
            )
        ]
    )
    print(f"✓ Created composite limits resource: {composite_resource}")

    # 2.5 Resource with custom thresholds
    threshold_resource = manager.create_resource(
        name="sensitive_operation",
        group="operations",
        unit="operations",
        limits=[ResourceLimit(
            max_quantity=100,
            reset_config={"type": ResetType.NONE.value},
            limit_name="sensitive_limit"
        )]
    )
    # Set custom thresholds (60% warning, 80% critical)
    manager.resources[threshold_resource].warning_threshold = 0.6
    manager.resources[threshold_resource].critical_threshold = 0.8
    print(f"✓ Created resource with custom thresholds: {threshold_resource}")

    # 3. Demonstrate usage recording and availability checking
    print("\n2. Usage Recording and Availability Checking")
    print("-" * 50)

    # Record usage on different resources
    test_resources = [sliding_resource, composite_resource, threshold_resource]
    usage_patterns = [5, 3, 2]  # Different usage amounts

    for i, resource_name in enumerate(test_resources):
        quantity = usage_patterns[i]
        success = manager.record_usage(resource_name, quantity)
        status = manager.check_availability(resource_name, 1)
        current_usage = manager.resources[resource_name].current_usage

        print(f"✓ Recorded {quantity} {manager.resources[resource_name].unit} on {resource_name}")
        print(f"  Current usage: {current_usage}, Status: {status.value}")

    # 4. Demonstrate availability thresholds
    print("\n3. Availability Threshold Demonstration")
    print("-" * 50)

    # Test threshold resource by recording significant usage
    test_quantities = [30, 25, 20]  # Will push past 60% and 80% thresholds

    print(f"Testing {threshold_resource} with thresholds: 60% warning, 80% critical")
    for i, quantity in enumerate(test_quantities):
        before_status = manager.check_availability(threshold_resource, quantity)
        success = manager.record_usage(threshold_resource, quantity)
        after_status = manager.check_availability(threshold_resource, 1)
        current_usage = manager.resources[threshold_resource].current_usage
        max_limit = manager.resources[threshold_resource]._get_max_limit()
        usage_percentage = (current_usage / max_limit) * 100

        print(f"  Use {quantity}: {before_status.value} → {after_status.value} "
              f"({usage_percentage:.1f}% used)")

    # 5. Demonstrate group-based resource recommendation
    print("\n4. Group-based Resource Recommendation")
    print("-" * 50)

    # Create additional resources in the same group
    api_resources = []
    for i in range(3):
        resource_name = manager.create_resource(
            name=f"api_endpoint_{i}",
            group="api",
            unit="requests",
            limits=[ResourceLimit(
                max_quantity=1000,
                reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": 3600},
                limit_name="hourly"
            )]
        )
        api_resources.append(resource_name)

    # Record different usage patterns to simulate real scenario
    usage_patterns = [100, 300, 50]  # Different usage levels
    for i, resource_name in enumerate(api_resources):
        manager.record_usage(resource_name, usage_patterns[i])
        usage = manager.resources[resource_name].current_usage
        print(f"  {resource_name}: {usage}/1000 used")

    # Get recommendation (should choose the most used available resource)
    recommended = manager.get_recommended_resource("api")
    if recommended:
        status = manager.check_availability(recommended, 1)
        usage = manager.resources[recommended].current_usage
        print(f"✓ Recommended resource: {recommended} (usage: {usage}, status: {status.value})")

    # 6. Demonstrate manual operations
    print("\n5. Manual Operations")
    print("-" * 50)

    # Manual reset
    resource_to_reset = api_resources[0]
    before_reset = manager.resources[resource_to_reset].current_usage
    manager.manual_reset(resource_to_reset)
    after_reset = manager.resources[resource_to_reset].current_usage
    print(f"✓ Manual reset: {resource_to_reset} {before_reset} → {after_reset}")

    # Manual calibration
    resource_to_calibrate = api_resources[1]
    manager.calibrate_usage(resource_to_calibrate, 150)
    calibrated_usage = manager.resources[resource_to_calibrate].current_usage
    print(f"✓ Manual calibration: {resource_to_calibrate} → {calibrated_usage}")

    # 7. Demonstrate statistics and monitoring
    print("\n6. Statistics and Monitoring")
    print("-" * 50)

    # Get detailed statistics for a complex resource
    stats = manager.get_statistics(composite_resource)
    if stats:
        print(f"Statistics for {composite_resource}:")
        print(f"  Group: {stats['group']}, Unit: {stats['unit']}")
        print(f"  Current Usage: {stats['current_usage']}")
        print(f"  Overall Status: {stats['availability_status']}")

        print("  Limit Breakdown:")
        for limit_info in stats['usage_breakdown']:
            print(f"    {limit_info['limit_name']}: {limit_info['current_usage']}/"
                  f"{limit_info['max_quantity']} ({limit_info['usage_percentage']:.1f}%)")

        strictest = stats['strictest_limit']
        if strictest:
            print(f"  Strictest Limit: {strictest['limit_name']} "
                  f"({strictest['usage_percentage']:.1f}% used)")

    # 8. Demonstrate configuration export/import
    print("\n7. Configuration Export/Import")
    print("-" * 50)

    # Export resource configuration
    config_json = manager.export_configuration(composite_resource)
    if config_json:
        config_data = json.loads(config_json)
        print(f"✓ Exported {composite_resource} configuration:")
        print(f"  Name: {config_data['name']}, Limits: {len(config_data['limits'])}")

        # Modify and import as new resource
        config_data['name'] = f"cloned_{config_data['name']}"
        new_config_json = json.dumps(config_data)

        try:
            new_resource_name = manager.import_configuration(new_config_json)
            print(f"✓ Imported as new resource: {new_resource_name}")
        except ValueError as e:
            print(f"  Import failed: {e}")

    # 9. Demonstrate convenience functions for common patterns
    print("\n8. Convenience Functions for Common Patterns")
    print("-" * 50)

    # Create resources using convenience functions
    hourly_resource = create_hourly_limit(manager, "hourly_jobs", 500, "batch")
    daily_resource = create_daily_limit(manager, "daily_reports", 100, "reports")
    sliding_resource = create_sliding_window_limit(manager, "real_time_api", 200, 30, "realtime")

    print(f"✓ Created hourly limited resource: {hourly_resource}")
    print(f"✓ Created daily limited resource: {daily_resource}")
    print(f"✓ Created 30s sliding window resource: {sliding_resource}")

    # 10. Simulate time-based behavior (conceptual)
    print("\n9. Time-based Behavior Simulation")
    print("-" * 50)
    print("Conceptual demonstration of time-based features:")
    print("• Sliding window resources automatically expire old usage records")
    print("• Fixed schedule resources reset based on cron expressions")
    print("• Database maintains usage history for accurate window calculations")
    print("• Automatic reset checks occur on every usage recording")

    # 11. Show comprehensive group overview
    print("\n10. Group Overview")
    print("-" * 50)

    for group_name, resources in manager.groups.items():
        print(f"Group '{group_name}':")
        for resource_name in resources:
            resource = manager.resources[resource_name]
            status = resource.check_availability(1)
            usage_pct = (resource.current_usage / resource._get_max_limit()) * 100

            print(f"  {resource_name}: {resource.current_usage:.0f}/{resource._get_max_limit():.0f} "
                  f"({usage_pct:.1f}%) - {status.value}")

    print("\n=== Demo Completed ===")
    print(f"Total resources managed: {len(manager.resources)}")
    print(f"Groups defined: {len(manager.groups)}")

    return manager


def stress_test_demo():
    """
    Additional demo showing performance under load
    """
    print("\n" + "=" * 60)
    print("STRESS TEST DEMO")
    print("=" * 60)

    manager = ResourceUsageManager(TEST_DB_PATH)

    # Create a resource for stress testing
    stress_resource = manager.create_resource(
        name="stress_test",
        group="performance",
        unit="operations",
        limits=[ResourceLimit(
            max_quantity=10000,
            reset_config={"type": ResetType.SLIDING_WINDOW.value, "window_seconds": 10},
            limit_name="stress_test"
        )]
    )

    print("Testing concurrent-like usage pattern...")
    start_time = time.time()

    # Simulate rapid usage recording
    operations = 1000
    success_count = 0

    for i in range(operations):
        if manager.record_usage(stress_resource, 1):
            success_count += 1

    end_time = time.time()
    duration = end_time - start_time

    stats = manager.get_statistics(stress_resource)
    print(f"Completed {operations} operations in {duration:.3f} seconds")
    print(f"Successful recordings: {success_count}/{operations}")
    print(f"Final usage: {stats['current_usage'] if stats else 'N/A'}")
    print(f"Performance: {operations / duration:.1f} operations/second")


def main():
    # Run main demo
    manager = demo_resource_usage_manager()

    # Run stress test
    stress_test_demo()

    print("\n" + "=" * 60)
    print("DEMO SUMMARY")
    print("=" * 60)
    print("This demo showcased:")
    print("✓ Multiple resource types (pure, sliding window, fixed schedule)")
    print("✓ Composite limits with multiple constraints")
    print("✓ Threshold-based availability checking")
    print("✓ Group-based resource management and recommendation")
    print("✓ Manual operations (reset, calibration)")
    print("✓ Comprehensive statistics and monitoring")
    print("✓ Configuration export/import functionality")
    print("✓ Convenience functions for common patterns")
    print("✓ SQLite persistence and usage history tracking")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e))
        print(traceback.format_exc())
