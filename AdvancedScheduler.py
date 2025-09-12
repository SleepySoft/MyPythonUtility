from datetime import datetime, timedelta
from typing import Callable, Any, Optional, Union, Dict, List
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MAX_INSTANCES, EVENT_JOB_MISSED, JobExecutionEvent
from concurrent.futures import ThreadPoolExecutor as ConcurrentThreadPoolExecutor, TimeoutError as FutureTimeoutError
import threading
import time
import logging
from logging import Logger
from functools import wraps


def _thread_wrapper(func: Callable[..., Any], task_id: str, logger: Logger, *args, **kwargs) -> None:
    """
    A wrapper function that executes the target function in a new daemon thread.
    Also captures and logs any exceptions that occur in the thread.

    Args:
        func: The target function to execute.
        task_id: ID of the task for logging.
        logger: Logger instance for logging.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.
    """
    def run() -> None:
        try:
            func(*args, **kwargs)
            logger.debug(f"Task '{task_id}' executed successfully in a new thread.")
        except Exception as e:
            logger.error(f"Error in task '{task_id}' execution (threaded): {e}", exc_info=True)
    # Create and start a daemon thread
    thread = threading.Thread(target=run, name=f"TaskThread-{task_id}-{datetime.now().timestamp()}")
    thread.daemon = True  # Daemon thread will exit if main program exits
    thread.start()
    logger.info(f"Task '{task_id}' started in a new thread (ID: {thread.ident}).")


class AdvancedScheduler:
    """
    An advanced scheduler class based on APScheduler with extended functionality.
    Supports various scheduling types, thread management, and manual triggering.
    """

    def __init__(self, use_background_scheduler: bool = True, default_thread_pool_size: int = 10, logger: Optional[Logger] = None):
        """
        Initialize the advanced scheduler.

        Args:
            use_background_scheduler: If True, uses BackgroundScheduler (non-blocking),
                                     else uses BlockingScheduler (blocking)
            default_thread_pool_size: Default size of thread pool for task execution
            logger: Custom logger instance. If not provided, a default one will be created.
        """
        # Configure logging
        self.logger = logger or logging.getLogger(__name__)
        if not self.logger.handlers:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Configure job stores and executors
        jobstores = {
            'default': MemoryJobStore()
        }

        executors = {
            'default': ThreadPoolExecutor(default_thread_pool_size),
            'processpool': ProcessPoolExecutor(5)
        }

        job_defaults = {
            'coalesce': False,          # 是否合并多次错过的执行
            'max_instances': 3,         # 允许的并发实例数
            'misfire_grace_time': 30    # 允许的误执行时间
        }

        # Create scheduler instance
        if use_background_scheduler:
            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone='UTC'
            )
            self.logger.info("BackgroundScheduler initialized.")
        else:
            self.scheduler = BlockingScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone='UTC'
            )
            self.logger.info("BlockingScheduler initialized.")

        # Dictionary to track task timeouts
        self.task_timeouts = {}
        # Dictionary to track running tasks and their futures for timeout handling
        self.running_tasks = {}
        self._running_tasks_lock = threading.RLock()  # Use RLock for potential nested locking

        # Set up event listeners for job execution events
        self.scheduler.add_listener(self._job_listener,
                                    EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MAX_INSTANCES | EVENT_JOB_MISSED)

        # Start the scheduler
        self.scheduler.start()
        self.logger.info("AdvancedScheduler initialized and started successfully")

    def _job_listener(self, event: JobExecutionEvent) -> None:
        """Listen to job events and handle timeouts/errors"""
        if event.code == EVENT_JOB_ERROR and event.exception:
            self.logger.error(f"Job {event.job_id} failed with exception: {event.exception}")
        elif event.code == EVENT_JOB_EXECUTED:
            self.logger.info(f"Job {event.job_id} executed successfully")
        elif event.code == EVENT_JOB_MAX_INSTANCES:
            self.logger.warning(f"Job {event.job_id} reached maximum instances, skipping run.")
        elif event.code == EVENT_JOB_MISSED:
            self.logger.warning(f"Job {event.job_id} missed its scheduled run time.")

    def _wrap_function_for_threading(self, func: Callable[..., Any], task_id: str, use_new_thread: bool) -> Callable[..., Any]:
        """
        Optionally wrap the function to execute in a new thread.

        Args:
            func: The original function to be executed.
            task_id: The ID of the task.
            use_new_thread: If True, wrap the function to run in a new thread.

        Returns:
            The wrapped function or the original function.
        """
        if not use_new_thread:
            return func  # Return the original function if no threading is requested.

        @wraps(func)
        def wrapper(*args, **kwargs):
            """Wrapper that starts the function in a new thread and returns immediately."""
            _thread_wrapper(func, task_id, self.logger, *args, **kwargs)
        return wrapper

    def add_interval_task(self, func: Callable[..., Any], interval_seconds: int,
                          task_id: str, args: list = None, kwargs: dict = None,
                          use_new_thread: bool = False, start_immediately: bool = True) -> str:
        """
        Add a periodic task that runs at fixed intervals.

        Args:
            func: Function to be executed
            interval_seconds: Interval in seconds between executions
            task_id: Unique identifier for the task
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            use_new_thread: If True, the task will be executed in a new daemon thread.
            start_immediately: Whether to start first execution immediately

        Returns:
            Job ID of the created task
        """
        args = args or []
        kwargs = kwargs or {}
        trigger_args = {'seconds': interval_seconds}
        if not start_immediately:
            trigger_args['start_date'] = datetime.now() + timedelta(seconds=interval_seconds)

        # Wrap the function if use_new_thread is True
        wrapped_func = self._wrap_function_for_threading(func, task_id, use_new_thread)

        job = self.scheduler.add_job(
            wrapped_func,  # Use the potentially wrapped function
            trigger='interval',
            id=task_id,
            args=args,
            kwargs=kwargs,
            **trigger_args
        )

        thread_info = " (in a new thread)" if use_new_thread else ""
        self.logger.info(f"Interval task '{task_id}' added with {interval_seconds}s interval{thread_info}.")
        return job.id

    def add_cron_task(self, func: Callable[..., Any], task_id: str,
                      year: str = None, month: str = None, day: str = None,
                      week: str = None, day_of_week: str = None,
                      hour: str = None, minute: str = None, second: str = None,
                      args: list = None, kwargs: dict = None,
                      use_new_thread: bool = False) -> str:
        """
        Add a cron-style task with flexible scheduling options.

        Args:
            func: Function to be executed
            task_id: Unique identifier for the task
            year: Year expression (e.g., '2023', '2023-2025')
            month: Month expression (e.g., '1-12', '*/3')
            day: Day of month expression (e.g., '1,15,31')
            week: Week expression (e.g., '1-52')
            day_of_week: Day of week expression (e.g., 'mon-fri', '0-6')
            hour: Hour expression (e.g., '0-23', '*/2')
            minute: Minute expression (e.g., '0-59', '*/15')
            second: Second expression (e.g., '0-59', '*/30')
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            use_new_thread: If True, the task will be executed in a new daemon thread.

        Returns:
            Job ID of the created task
        """
        args = args or []
        kwargs = kwargs or {}

        # Wrap the function if use_new_thread is True
        wrapped_func = self._wrap_function_for_threading(func, task_id, use_new_thread)

        job = self.scheduler.add_job(
            wrapped_func,  # Use the potentially wrapped function
            trigger='cron',
            id=task_id,
            year=year,
            month=month,
            day=day,
            week=week,
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            second=second,
            args=args,
            kwargs=kwargs
        )

        thread_info = " (in a new thread)" if use_new_thread else ""
        self.logger.info(f"Cron task '{task_id}' added with custom schedule{thread_info}.")
        return job.id

    def add_daily_task(self, func: Callable[..., Any], task_id: str,
                       hour: int = 0, minute: int = 0, second: int = 0,
                       args: list = None, kwargs: dict = None,
                       use_new_thread: bool = False) -> str:
        """
        Add a task that runs daily at specified time.

        Args:
            func: Function to be executed
            task_id: Unique identifier for the task
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
            second: Second of minute (0-59)
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            use_new_thread: If True, the task will be executed in a new daemon thread.

        Returns:
            Job ID of the created task
        """
        return self.add_cron_task(
            func, task_id, hour=str(hour), minute=str(minute), second=str(second),
            args=args, kwargs=kwargs, use_new_thread=use_new_thread
        )

    def add_weekly_task(self, func: Callable[..., Any], task_id: str,
                        day_of_week: str, hour: int = 0, minute: int = 0, second: int = 0,
                        args: list = None, kwargs: dict = None,
                        use_new_thread: bool = False) -> str:
        """
        Add a task that runs weekly on specified day and time.

        Args:
            func: Function to be executed
            task_id: Unique identifier for the task
            day_of_week: Day of week ('mon', 'tue', etc. or '0-6')
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
            second: Second of minute (0-59)
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            use_new_thread: If True, the task will be executed in a new daemon thread.

        Returns:
            Job ID of the created task
        """
        return self.add_cron_task(
            func, task_id, day_of_week=day_of_week, hour=str(hour), minute=str(minute), second=str(second),
            args=args, kwargs=kwargs, use_new_thread=use_new_thread
        )

    def add_monthly_task(self, func: Callable[..., Any], task_id: str,
                         day: int = 1, hour: int = 0, minute: int = 0, second: int = 0,
                         args: list = None, kwargs: dict = None,
                         use_new_thread: bool = False) -> str:
        """
        Add a task that runs monthly on specified day and time.

        Args:
            func: Function to be executed
            task_id: Unique identifier for the task
            day: Day of month (1-31)
            hour: Hour of day (0-23)
            minute: Minute of hour (0-59)
            second: Second of minute (0-59)
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            use_new_thread: If True, the task will be executed in a new daemon thread.

        Returns:
            Job ID of the created task
        """
        return self.add_cron_task(
            func, task_id, day=str(day), hour=str(hour), minute=str(minute), second=str(second),
            args=args, kwargs=kwargs, use_new_thread=use_new_thread
        )

    def execute_task(self, func: Callable[..., Any], task_id: str,
                     delay_seconds: int = 0, args: list = None,
                     kwargs: dict = None, use_new_thread: bool = False) -> str:
        """
        Manually trigger a task execution with optional delay.

        Args:
            func: Function to be executed
            task_id: Unique identifier for the task
            delay_seconds: Delay in seconds before execution (0 for immediate)
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
            use_new_thread: If True, the task will be executed in a new daemon thread.

        Returns:
            Job ID of the created task
        """
        args = args or []
        kwargs = kwargs or {}
        run_date = datetime.now() + timedelta(seconds=delay_seconds)
        # Generate a more unique ID for manual execution to avoid conflicts
        unique_id = f"manual_{task_id}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

        # Wrap the function if use_new_thread is True
        wrapped_func = self._wrap_function_for_threading(func, task_id, use_new_thread)

        job = self.scheduler.add_job(
            wrapped_func,  # Use the potentially wrapped function
            trigger='date',
            run_date=run_date,
            id=unique_id,
            args=args,
            kwargs=kwargs
        )

        thread_info = " (in a new thread)" if use_new_thread else ""
        self.logger.info(f"Manual task '{task_id}' scheduled with {delay_seconds}s delay{thread_info} (Job ID: {unique_id}).")
        return job.id

    def reset_task_timer(self, task_id: str) -> bool:
        """
        Reset the timer for a periodic task. This will re-calculate the next run time
        based on the current time and the original trigger settings.

        Args:
            task_id: ID of the task to reset

        Returns:
            True if successful, False otherwise
        """
        job = self.scheduler.get_job(task_id)
        if job:
            original_trigger = job.trigger
            # For interval and cron triggers, we can reschedule with the same trigger
            # which will recalculate the next run time from now.
            if isinstance(original_trigger, (IntervalTrigger, CronTrigger)):
                try:
                    job.reschedule(trigger=original_trigger)
                    self.logger.info(f"Timer reset for task {task_id}. Next run: {job.next_run_time}")
                    return True
                except Exception as e:
                    self.logger.error(f"Failed to reset timer for task {task_id}: {e}")
                    return False
            else:
                self.logger.warning(f"Task {task_id} does not have a resettable trigger (Interval or Cron).")
                return False
        else:
            self.logger.warning(f"Task {task_id} not found. Cannot reset timer.")
            return False

    def set_task_timeout(self, task_id: str, timeout_seconds: int) -> None:
        """
        Set timeout for a task execution. Note: This requires the task to be executed
        using the `execute_task_with_timeout` method or similar mechanism to be effective.
        For scheduled jobs, a wrapper would be needed.

        Args:
            task_id: ID of the task to set timeout for
            timeout_seconds: Timeout duration in seconds
        """
        self.task_timeouts[task_id] = timeout_seconds
        self.logger.info(f"Timeout of {timeout_seconds}s set for task {task_id}")

    def _execute_with_timeout(self, func: Callable[..., Any], task_id: str,
                              args: list, kwargs: dict) -> Any:
        """
        Execute a function with timeout protection.

        Args:
            func: Function to execute
            task_id: ID of the task
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Function result or None if timeout occurs
        """
        timeout = self.task_timeouts.get(task_id)
        if timeout is None:
            # No timeout set, execute normally
            return func(*args, **kwargs)

        with ConcurrentThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)
            try:
                result = future.result(timeout=timeout)
                self.logger.info(f"Task {task_id} completed within {timeout}s timeout")
                return result
            except FutureTimeoutError:
                self.logger.warning(f"Task {task_id} timed out after {timeout}s")
                future.cancel()
                raise TimeoutError(f"Task {task_id} exceeded timeout of {timeout} seconds")
            except Exception as e:
                self.logger.error(f"Task {task_id} encountered an error: {e}")
                raise

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the scheduler and all running tasks.

        Args:
            wait: Whether to wait for running tasks to complete
        """
        self.scheduler.shutdown(wait=wait)
        self.logger.info("Scheduler shutdown completed")

    def get_task_status(self, task_id: str) -> Optional[Dict]:
        """
        Get current status of a task.

        Args:
            task_id: ID of the task to check

        Returns:
            Dictionary with task status information or None if not found
        """
        job = self.scheduler.get_job(task_id)
        if job:
            return {
                'id': job.id,
                'next_run_time': job.next_run_time,
                'pending': job.pending,
                'trigger': str(job.trigger)
            }
        return None

    def pause_task(self, task_id: str) -> bool:
        """
        Pause a scheduled task.

        Args:
            task_id: ID of the task to pause

        Returns:
            True if successful, False otherwise
        """
        job = self.scheduler.get_job(task_id)
        if job:
            job.pause()
            self.logger.info(f"Task {task_id} paused")
            return True
        return False

    def resume_task(self, task_id: str) -> bool:
        """
        Resume a paused task.

        Args:
            task_id: ID of the task to resume

        Returns:
            True if successful, False otherwise
        """
        job = self.scheduler.get_job(task_id)
        if job:
            job.resume()
            self.logger.info(f"Task {task_id} resumed")
            return True
        return False

    def remove_task(self, task_id: str) -> bool:
        """
        Remove a task from the scheduler.

        Args:
            task_id: ID of the task to remove

        Returns:
            True if successful, False otherwise
        """
        try:
            self.scheduler.remove_job(task_id)
            self.logger.info(f"Task {task_id} removed")
            return True
        except Exception as e:
            self.logger.error(f"Failed to remove task {task_id}: {e}")
            return False

    def list_tasks(self) -> List[Dict]:
        """
        List all scheduled tasks.

        Returns:
            A list of dictionaries containing task information
        """
        jobs = self.scheduler.get_jobs()
        task_list = []
        for job in jobs:
            task_list.append({
                'id': job.id,
                'name': job.name,
                'next_run_time': job.next_run_time,
                'trigger': str(job.trigger)
            })
        return task_list

    def modify_interval_task(self, task_id: str, new_interval_seconds: int) -> bool:
        """
        Modify the interval of an existing interval task.

        Args:
            task_id: ID of the task to modify
            new_interval_seconds: New interval in seconds

        Returns:
            True if successful, False otherwise
        """
        job = self.scheduler.get_job(task_id)
        if job and isinstance(job.trigger, IntervalTrigger):
            new_trigger = IntervalTrigger(seconds=new_interval_seconds)
            job.reschedule(trigger=new_trigger)
            self.logger.info(f"Interval for task {task_id} modified to {new_interval_seconds}s")
            return True
        return False

# Example usage and demonstration
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create scheduler instance (BackgroundScheduler)
    scheduler = AdvancedScheduler(use_background_scheduler=True)


    # Example tasks
    def sample_task(name: str = "default"):
        print(f"Executing task: {name} at {datetime.now()}. Thread: {threading.current_thread().name}")


    def long_running_task():
        time.sleep(8)
        print(f"Long running task completed. Thread: {threading.current_thread().name}")


    # Add various types of tasks, some with use_new_thread=True
    scheduler.add_interval_task(sample_task, 10, "interval_task_normal", kwargs={'name': 'interval_normal'}, use_new_thread=False)
    scheduler.add_interval_task(sample_task, 15, "interval_task_threaded", kwargs={'name': 'interval_threaded'}, use_new_thread=True)

    scheduler.add_daily_task(sample_task, "daily_task_threaded", hour=datetime.now().hour, minute=(datetime.now().minute + 1) % 60, kwargs={'name': 'daily_threaded'}, use_new_thread=True) # Schedule roughly 1 minute from now

    # Manual execution examples with and without threading
    scheduler.execute_task(sample_task, "immediate_task", delay_seconds=5, kwargs={'name': 'immediate_normal'}, use_new_thread=False)
    scheduler.execute_task(long_running_task, "delayed_long_task", delay_seconds=3, kwargs={}, use_new_thread=True) # This long task will run in its own thread

    # Keep the program running
    try:
        while True:
            time.sleep(2)
    except KeyboardInterrupt:
        scheduler.shutdown(wait=True)
