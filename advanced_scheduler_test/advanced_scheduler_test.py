import os
import sys
import unittest
import time
import threading
from datetime import datetime, timedelta
from functools import partial
from unittest.mock import Mock, patch
import logging

from aiofiles.os import replace

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from AdvancedScheduler import AdvancedScheduler  # Import your actual class

# Configure logging for tests
logging.basicConfig(level=logging.WARNING)  # Reduce logging noise during tests


mock_count = 0


def mock_function_counter():
    global mock_count
    mock_count += 1
    print(f'Execute mock function: {mock_count}')


class TestAdvancedScheduler(unittest.TestCase):
    """
    Comprehensive test suite for AdvancedScheduler class.
    Tests cover all major functionalities and edge cases.
    """

    def setUp(self):
        """Initialize a fresh scheduler instance before each test."""
        self.scheduler = AdvancedScheduler(use_background_scheduler=True, default_thread_pool_size=5)
        self.test_counter = 0
        self.execution_events = []
        self.mock_function = Mock()
        self.scheduler.start_scheduler()

    def tearDown(self):
        """Clean up after each test - shutdown scheduler and reset state."""
        if self.scheduler and hasattr(self, 'scheduler'):
            self.scheduler.shutdown(wait=False)
        self.mock_function.reset_mock()
        self.execution_events.clear()

    def _track_execution(self, task_name="default"):
        """Helper method to track task executions."""
        self.execution_events.append((task_name, datetime.now()))
        self.test_counter += 1

    def test_01_initialization_and_shutdown(self):
        """Test scheduler initialization and proper shutdown."""
        # Test initialization with different configurations
        bg_scheduler = AdvancedScheduler(use_background_scheduler=True)
        bg_scheduler.start_scheduler()
        self.assertTrue(bg_scheduler.scheduler.running)
        bg_scheduler.shutdown()

        # blocking_scheduler = AdvancedScheduler(use_background_scheduler=False)
        # self.assertTrue(blocking_scheduler.scheduler.running)
        # blocking_scheduler.shutdown()

    def test_02_interval_task_basic(self):
        """Test basic interval task functionality."""
        task_id = "interval_test"
        interval_seconds = 1
        wait_tolerance = 1.1
        check_times = 5

        # Add interval task
        job_id = self.scheduler.add_interval_task(
            self.mock_function, interval_seconds, task_id
        )

        self.assertEqual(job_id, task_id)

        for i in range(check_times):
            time.sleep(interval_seconds * wait_tolerance)
            self.assertGreaterEqual(self.mock_function.call_count, i)

    def test_03_interval_task_with_args(self):
        """Test interval task with arguments."""
        task_id = "interval_with_args"
        test_args = [1, 2, 3]
        test_kwargs = {'param': 'value'}

        job_id = self.scheduler.add_interval_task(
            self.mock_function, 1, task_id,
            args=test_args, kwargs=test_kwargs
        )

        self.assertEqual(job_id, task_id)

        time.sleep(1.1)

        # Verify function was called with correct arguments
        self.mock_function.assert_called_with(*test_args, **test_kwargs)

    def test_04_cron_task_functionality(self):
        """Test cron-style task scheduling."""
        task_id = "cron_test"

        # Add cron task that should run every minute at second 0
        current_time = datetime.now()
        target_second = (current_time.second + 2) % 60  # Run in 2 seconds

        job_id = self.scheduler.add_cron_task(
            self.mock_function, task_id, second=str(target_second)
        )

        # Wait for the target second to pass
        time.sleep(2.5)

        self.mock_function.assert_called()

    def test_05_daily_task_scheduling(self):
        """Test daily task scheduling."""
        task_id = "daily_test"

        # Schedule for a specific time (next minute)
        now = datetime.now()
        target_minute = (now.minute + 1) % 60
        target_hour = now.hour

        job_id = self.scheduler.add_daily_task(
            self.mock_function, task_id,
            hour=target_hour, minute=target_minute
        )

        # Verify task was added
        self.assertEqual(job_id, task_id)

        # Check task status
        status = self.scheduler.get_task_status(task_id)
        self.assertIsNotNone(status)
        self.assertEqual(status['id'], task_id)

    def test_06_weekly_task_scheduling(self):
        """Test weekly task scheduling."""
        task_id = "weekly_test"

        job_id = self.scheduler.add_weekly_task(
            self.mock_function, task_id, 'mon', hour=10, minute=0
        )

        self.assertEqual(job_id, task_id)

        # Test pausing and resuming
        self.assertTrue(self.scheduler.pause_task(task_id))
        self.assertTrue(self.scheduler.resume_task(task_id))

    def test_07_monthly_task_scheduling(self):
        """Test monthly task scheduling."""
        task_id = "monthly_test"

        job_id = self.scheduler.add_monthly_task(
            self.mock_function, task_id, day=1, hour=9, minute=0
        )

        self.assertEqual(job_id, task_id)

    def test_08_manual_task_execution_without_reset(self):
        """Test manual task execution with delay."""
        task_id = "manual_test"

        self.scheduler.add_interval_task(self.mock_function, 2, task_id)

        # Test immediate execution
        self.scheduler.execute_task(task_id, delay_seconds=0)
        self.assertGreaterEqual(self.mock_function.call_count, 1)

        # Test delayed execution
        self.scheduler.execute_task(task_id, delay_seconds=1)
        time.sleep(1.1)
        self.assertGreaterEqual(self.mock_function.call_count, 2)

        time.sleep(1)
        self.assertGreaterEqual(self.mock_function.call_count, 3)

    def test_09_manual_task_execution_with_reset(self):
        """Test manual task execution with delay."""
        task_id = "manual_test"

        self.scheduler.add_interval_task(self.mock_function, 2, task_id)

        # Test immediate execution
        self.scheduler.execute_task(task_id, delay_seconds=0, reset_timer=True)
        self.assertGreaterEqual(self.mock_function.call_count, 1)

        # Test delayed execution
        self.scheduler.execute_task(task_id, delay_seconds=1, reset_timer=True)
        time.sleep(1.1)
        self.assertGreaterEqual(self.mock_function.call_count, 2)

        time.sleep(1.1)
        self.assertGreaterEqual(self.mock_function.call_count, 2)

        time.sleep(2)
        self.assertGreaterEqual(self.mock_function.call_count, 3)


    # def test_10_task_timeout_management(self):
    #     """Test task timeout functionality."""
    #     task_id = "timeout_test"
    #
    #     # Set timeout for a task
    #     self.scheduler.set_task_timeout(task_id, 2)
    #
    #     # Create a long-running function
    #     def long_running_task():
    #         time.sleep(5)  # This should timeout
    #         self._track_execution("long_running")
    #
    #     # Add task with timeout
    #     job_id = self.scheduler.add_interval_task(
    #         long_running_task, 1, task_id
    #     )
    #
    #     time.sleep(3)  # Wait for timeout to occur
    #
    #     # Task should have been interrupted by timeout
    #     status = self.scheduler.get_task_status(task_id)
    #     self.assertIsNotNone(status)

    def test_11_task_reset_functionality(self):
        """Test timer reset for periodic tasks."""
        task_id = "reset_test"

        self.scheduler.add_interval_task(self.mock_function, 2, task_id)

        time.sleep(2.1)
        self.assertEqual(self.mock_function.call_count, 1)

        time.sleep(1.8)
        self.assertEqual(self.mock_function.call_count, 1)
        self.assertTrue(self.scheduler.reset_task_timer(task_id))       # <---- Reset time here.

        time.sleep(1.5)
        self.assertEqual(self.mock_function.call_count, 1)

        time.sleep(0.6)
        self.assertEqual(self.mock_function.call_count, 2)

        # Try to reset non-existent task
        self.assertFalse(self.scheduler.reset_task_timer("non_existent_task"))

    def test_12_task_removal(self):
        """Test task removal functionality."""
        task_id = "removal_test"

        job_id = self.scheduler.add_interval_task(
            self.mock_function, 5, task_id
        )

        # Verify task exists
        self.assertIsNotNone(self.scheduler.get_task_status(task_id))

        # Remove task
        self.assertTrue(self.scheduler.remove_task(task_id))

        # Verify task no longer exists
        self.assertIsNone(self.scheduler.get_task_status(task_id))

        # Try to remove non-existent task
        self.assertFalse(self.scheduler.remove_task("non_existent_task"))

    def test_13_concurrent_task_execution(self):
        """Test concurrent task execution handling."""
        task_id = "concurrent_test"
        execution_count = [0]
        lock = threading.Lock()

        def concurrent_task():
            with lock:
                execution_count[0] += 1
            time.sleep(0.1)

        # Add multiple rapid-fire tasks
        for i in range(3):
            self.scheduler.add_interval_task(
                concurrent_task, 0.5, f"{task_id}_{i}"
            )

        time.sleep(2)

        # Should have multiple executions
        self.assertGreater(execution_count[0], 1)

    def test_14_edge_case_duplicate_task_ids(self):
        """Test behavior with duplicate task IDs when replace=False and replace=True."""
        task_id = "duplicate_test"

        # Add first task with 2-second interval
        job1_id = self.scheduler.add_interval_task(
            self.mock_function, 2, task_id
        )

        # Verify the first job was added and has correct interval
        self.assertIsNotNone(job1_id)
        initial_status = self.scheduler.get_task_status(task_id)
        self.assertIsNotNone(initial_status)

        # Test that adding duplicate ID without replace raises ValueError
        with self.assertRaises(ValueError) as context:
            self.scheduler.add_interval_task(
                self.mock_function, 3, task_id, replace=False
            )

        # Verify the exception message is informative
        self.assertIn("already exists", str(context.exception))
        self.assertIn(task_id, str(context.exception))

        # Test that adding with replace=True succeeds
        job2_id = self.scheduler.add_interval_task(
            self.mock_function, 3, task_id, replace=True
        )

        # Verify both job references have the same ID
        self.assertEqual(job1_id, job2_id)

        # Get updated status and verify it reflects the new task configuration
        updated_status = self.scheduler.get_task_status(task_id)
        self.assertIsNotNone(updated_status)

        # Additional verification: check that only one job with this ID exists in the scheduler
        all_jobs = self.scheduler.list_tasks()
        task_ids = [job['id'] for job in all_jobs]
        assert task_ids.count(
            task_id) == 1, f"Should only be one job with ID {task_id}, found {task_ids.count(task_id)}"

    def test_15_edge_case_invalid_parameters(self):
        """Test behavior with invalid parameters."""
        # Test with invalid function
        with self.assertRaises(ValueError):
            self.scheduler.add_interval_task(
                None, 1, "invalid_function_test"
            )

        # Test with negative interval
        with self.assertRaises(ValueError):
            self.scheduler.add_interval_task(
                self.mock_function, -1, "negative_interval_test"
            )

    def test_16_task_pause_and_resume(self):
        """Test task pausing and resuming functionality."""
        task_id = "pause_resume_test"

        job_id = self.scheduler.add_interval_task(
            self.mock_function, 1, task_id
        )

        # Pause the task
        self.assertTrue(self.scheduler.pause_task(task_id))

        time.sleep(1.5)
        call_count_after_pause = self.mock_function.call_count

        # Resume the task
        self.assertTrue(self.scheduler.resume_task(task_id))

        time.sleep(1.5)

        # Should have more calls after resuming
        self.assertGreater(self.mock_function.call_count, call_count_after_pause)

    def test_17_boundary_condition_very_short_interval(self):
        """Test very short interval tasks."""
        task_id = "short_interval_test"

        job_id = self.scheduler.add_interval_task(
            self.mock_function, 0.1, task_id  # 100ms interval
        )

        time.sleep(0.5)  # Should trigger multiple times

        self.assertGreaterEqual(self.mock_function.call_count, 3)

    def test_18_boundary_condition_very_long_interval(self):
        """Test very long interval tasks."""
        task_id = "long_interval_test"

        job_id = self.scheduler.add_interval_task(
            self.mock_function, 3600, task_id  # 1 hour interval
        )

        status = self.scheduler.get_task_status(task_id)
        self.assertIsNotNone(status)

        # Task should exist but not have executed yet
        self.assertEqual(self.mock_function.call_count, 0)

    def test_19_error_handling_in_tasks(self):
        """Test error handling in task execution."""
        task_id = "error_test"

        def failing_task():
            raise ValueError("Intentional test error")

        job_id = self.scheduler.add_interval_task(
            failing_task, 1, task_id
        )

        # Task should be added despite potential errors
        self.assertIsNotNone(self.scheduler.get_task_status(task_id))

        # Wait and verify scheduler is still running
        time.sleep(1.1)
        self.assertTrue(self.scheduler.scheduler.running)

    def test_20_thread_safety_operations(self):
        """Test thread safety of scheduler operations."""
        results = []

        def parallel_operation(op_type, task_id):
            try:
                if op_type == "add":
                    self.scheduler.add_interval_task(
                        self.mock_function, 2, task_id
                    )
                    results.append(f"add_success_{task_id}")
                elif op_type == "remove":
                    self.scheduler.remove_task(task_id)
                    results.append(f"remove_success_{task_id}")
            except Exception as e:
                results.append(f"error_{task_id}: {str(e)}")

        # Create multiple threads performing operations
        threads = []
        for i in range(5):
            t = threading.Thread(
                target=parallel_operation,
                args=("add", f"thread_test_{i}")
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # All operations should complete without exceptions
        self.assertEqual(len(results), 5)
        self.assertTrue(all("success" in r for r in results))

    def test_21_comprehensive_lifecycle_management(self):
        """Test comprehensive lifecycle management of tasks and threads."""
        task_ids = []

        # Create multiple tasks with different configurations
        for i in range(3):
            task_id = f"lifecycle_test_{i}"
            task_ids.append(task_id)

            self.scheduler.add_interval_task(
                self.mock_function, 1 + i, task_id  # Different intervals
            )

        # Verify all tasks exist
        for task_id in task_ids:
            self.assertIsNotNone(self.scheduler.get_task_status(task_id))

        # Perform various operations
        self.assertTrue(self.scheduler.pause_task(task_ids[0]))
        self.assertTrue(self.scheduler.reset_task_timer(task_ids[1]))
        self.assertTrue(self.scheduler.remove_task(task_ids[2]))

        # Verify operations took effect
        self.assertIsNone(self.scheduler.get_task_status(task_ids[2]))

        # Clean shutdown
        self.scheduler.shutdown(wait=True)
        self.assertFalse(self.scheduler.scheduler.running)

        # Avoid shutdown again in teardown.
        self.scheduler = None


if __name__ == "__main__":
    # Run the tests
    unittest.main(verbosity=2, exit=False)
