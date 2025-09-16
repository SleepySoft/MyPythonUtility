import traceback
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


class TaskWrapper:
    """
    A wrapper class for tasks, preserving execution context and enabling execution via thread pool.
    Adds debug prints before and after task execution.
    """

    def __init__(self, func: Callable[..., Any], task_id: str,
                 logger: Logger, use_new_thread: bool, *args, **kwargs):
        """
        Initialize the TaskWrapper with execution context.

        Args:
            func: The target function to execute.
            task_id: Unique identifier for the task, used for logging.
            logger: Logger instance for logging messages.
            use_new_thread: Flag indicating whether the task should run in a new thread.
            *args: Positional arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.
        """
        self.func = func
        self.task_id = task_id
        self.logger = logger
        self.use_new_thread = use_new_thread  # Save the threading preference
        self.args = args
        self.kwargs = kwargs
        # Preserve the context when the task was created
        self.creation_time = datetime.now()
        self.creation_thread = threading.current_thread().name

    def run(self) -> None:
        """
        Execute the task. This method is designed to be called by a thread pool.
        Adds debug prints before and after execution.
        """
        # Debug print before execution
        self.logger.debug(f"Task '{self.task_id}' starting execution. "
                         f"Created at {self.creation_time} in thread '{self.creation_thread}', "
                         f"executing in thread '{threading.current_thread().name}'. "
                         f"Use_new_thread: {self.use_new_thread}")

        try:
            # Execute the actual task function
            self.func(*self.args, **self.kwargs)
            # Debug print after successful execution
            self.logger.debug(f"Task '{self.task_id}' executed successfully.")
        except Exception as e:
            # Log any exceptions that occur during execution
            self.logger.error(f"Error in task '{self.task_id}' execution: {e}", exc_info=True)


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
        self._running_tasks_lock = threading.RLock()  # Use RLock for potential nested locking
        self.thread_pool = ConcurrentThreadPoolExecutor(
            max_workers=default_thread_pool_size,
            thread_name_prefix="AdvancedSchedulerThread")

        # Set up event listeners for job execution events
        self.scheduler.add_listener(self._job_listener,
                                    EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MAX_INSTANCES | EVENT_JOB_MISSED)

        # Start the scheduler
        # self.scheduler.start()
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

    def start_scheduler(self):
        """Start the scheduler. Note that if using BlockingScheduler, this call will block."""
        self.scheduler.start()
        # If it is BlockingScheduler, the following log will be printed only after the scheduler ends.
        self.logger.info("Scheduler started.")

    def shutdown(self, wait: bool = True) -> None:
        """
        Shutdown the scheduler and all running tasks.
        """
        self.scheduler.shutdown(wait=wait)
        self.thread_pool.shutdown(wait=wait)  # Add this line
        self.logger.info("Scheduler shutdown completed")

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

        # Create TaskWrapper instance
        task_wrapper = TaskWrapper(
            func=func,
            task_id=task_id,
            logger=self.logger,
            use_new_thread=use_new_thread,
            *args,
            **kwargs
        )

        try:
            job = self.scheduler.add_job(
                task_wrapper.run,  # Use the run method of TaskWrapper
                trigger='interval',
                id=task_id,
                **trigger_args
            )
            self.logger.info(f"Interval task '{task_id}' added with {interval_seconds}s interval. "
                             f"Use new thread: {use_new_thread}")
            return job.id
        except Exception as e:
            self.logger.error(f"Failed to add interval task '{task_id}': {e}")
            raise

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

        # Create TaskWrapper instance
        task_wrapper = TaskWrapper(
            func=func,
            task_id=task_id,
            logger=self.logger,
            use_new_thread=use_new_thread,
            *args,
            **kwargs
        )

        job = self.scheduler.add_job(
            task_wrapper.run,  # Use the run method of TaskWrapper
            trigger='cron',
            id=task_id,
            year=year,
            month=month,
            day=day,
            week=week,
            day_of_week=day_of_week,
            hour=hour,
            minute=minute,
            second=second
        )

        self.logger.info(f"Cron task '{task_id}' added. Use new thread: {use_new_thread}")
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

    def add_once_task(self, func: Callable[..., Any], task_id: str, delay_seconds: int = 0,
                      args: list = None, kwargs: dict = None, use_new_thread: bool = False) -> str:
        """
        Adds a task that executes only once, either immediately or with a delay.

        Args:
            func: The function to be executed
            task_id: The unique identifier of the task
            delay_seconds: The number of seconds to delay execution. 0 indicates immediate execution
            args: Positional arguments to the function
            kwargs: Keyword arguments to the function
            use_new_thread: Whether to execute the task in a new thread

        Returns:
            The ID of the task
        """
        args = args or []
        kwargs = kwargs or {}
        kwargs['_use_new_thread'] = use_new_thread  # Store threading preference

        try:
            if delay_seconds > 0:
                return self._schedule_delayed_task(
                    func,
                    task_id,
                    delay_seconds,
                    *args,
                    **kwargs
                )
            else:
                # Immediate execution using common method
                self._execute_task_immediately(func, task_id, use_new_thread, *args, **kwargs)
                return f"immediate_{task_id}"

        except Exception as e:
            self.logger.error(f"Add once task '{task_id}' failed: {e}")
            raise

    def execute_task(self, task_id: str, delay_seconds: int = 0, reset_timer: bool = False) -> bool:
        """
        Manually execute the specified task with optional delay and timer reset.

        Args:
            task_id: Unique identifier of the task to execute
            delay_seconds: Delay in seconds before execution (0 for immediate)
            reset_timer: If True, reset the timer for periodic tasks (IntervalTrigger or CronTrigger)

        Returns:
            True if the task is found and successfully executed/trigger reset,
            False if the task is not found or operation fails.
        """
        job = self.scheduler.get_job(task_id)
        if not job:
            self.logger.error(f"Task '{task_id}' not found")
            return False

        try:
            # Extract execution parameters from original job
            job_args = list(job.args)
            job_kwargs = job.kwargs.copy()

            # Extract threading preference with default fallback
            use_new_thread = job_kwargs.pop('_use_new_thread', False)

            # Actual function is the first argument in our wrapper
            original_func = job.func

            if delay_seconds > 0:
                # Schedule new delayed execution
                self._schedule_delayed_task(
                    original_func,
                    task_id,
                    delay_seconds,
                    *job_args,
                    **job_kwargs
                )
            else:
                # Execute immediately
                self._execute_task_immediately(
                    original_func,
                    task_id,
                    use_new_thread,
                    *job_args,
                    **job_kwargs
                )

            # Handle periodic task reset
            if reset_timer:
                if isinstance(job.trigger, (IntervalTrigger, CronTrigger)):
                    job.reschedule(trigger=job.trigger)
                    self.logger.info(f"Reset timer for '{task_id}'. Next run: {job.next_run_time}")
                else:
                    self.logger.warning(f"Task '{task_id}' doesn't support timer reset")

            return True

        except Exception as e:
            self.logger.error(f"Execute task '{task_id}' failed: {e}", exc_info=True)
            return False

    def _execute_task_immediately(self, func: Callable[..., Any], task_id: str,
                                  use_new_thread: bool, *args, **kwargs) -> None:
        """
        Execute a task immediately in either current thread or new thread.

        Args:
            func: Function to execute
            task_id: Task identifier for logging
            use_new_thread: Whether to use new thread
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function
        """
        if use_new_thread:
            self.logger.info(f"Executing task '{task_id}' in new thread")
            # Create TaskWrapper and submit to thread pool
            task_wrapper = TaskWrapper(
                func=func,
                task_id=task_id,
                logger=self.logger,
                use_new_thread=use_new_thread,
                *args,
                **kwargs
            )
            self.thread_pool.submit(task_wrapper.run)
        else:
            self.logger.info(f"Executing task '{task_id}' in current thread")
            func(*args, **kwargs)
            self.logger.debug(f"Task '{task_id}' executed successfully")

    def _schedule_delayed_task(self, func: Callable[..., Any], task_id: str,
                               delay_seconds: int, *args, **kwargs) -> str:
        """
        Schedule a task with delay using unique task ID.

        Args:
            func: Function to schedule
            task_id: Base task ID (will be extended with uniqueness)
            delay_seconds: Delay duration in seconds
            args: Positional arguments for the function
            kwargs: Keyword arguments for the function

        Returns:
            Generated job ID
        """
        run_date = datetime.now() + timedelta(seconds=delay_seconds)
        task_id = task_id or f"{task_id}_delay_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"

        job = self.scheduler.add_job(
            func,
            trigger='date',
            run_date=run_date,
            id=task_id,
            args=args,
            kwargs=kwargs
        )
        self.logger.info(f"Scheduled task '{task_id}' with {delay_seconds}s delay (Job ID: {unique_id})")
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
