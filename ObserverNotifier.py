import threading
import weakref
from typing import Optional, Callable

import threading
import weakref
from typing import Optional, Callable


class ObserverNotifier:
    """
    A thread-safe observer pattern implementation for synchronous notifications
    with ordered observer registration.

    Design Intent:
    ==============
    Provides a dynamic notification system where:
    1. Notification methods (prefixed with 'notify_') are generated automatically
    2. Observer registration order is preserved
    3. Thread safety is guaranteed through locking mechanisms
    4. Memory safety is maintained via weak references
    5. Observer errors are contained with configurable error handling

    Key Changes:
    ============
    - Replaced unordered WeakSet with ordered list of weak references
    - Added dead reference cleanup mechanism
    - Preserved observer registration order

    Usage:
    ======
    - Observers implement handler methods named `on_[event]` (e.g., `on_login`)
    - Call corresponding `notify_[event]` methods to broadcast events

    Initialization:
    ----------------
    :param error_handler: Optional callable with signature (exception, observer, method_name)
                          Default: Prints errors to stdout

    Methods:
    --------
    add_observer(observer): Register observer instance
    remove_observer(observer): Unregister observer instance

    Dynamic Notification:
    ---------------------
    Any attribute access starting with 'notify_' (e.g., notify_login) generates
    an optimized event notification method that:
    1. Collects all current observers safely (in registration order)
    2. Invokes matching `on_[event]` methods synchronously
    3. Reports errors via configured handler

    Example:
    --------
    ```
    class LoginObserver:
        def on_login(self, user):
            print(f"Login: {user}")

        def on_logout(self, user):
            print(f"Logout: {user}")

    notifier = ObserverNotifier()
    notifier.add_observer(LoginObserver())

    notifier.notify_login(user="Alice")
    notifier.notify_logout(user="Bob")
    ```

    Important Notes:
    ---------------
    1. Observer methods must be named `on_[event]`
    2. Maintain strong references to observers during their lifecycle
    3. Method generation is cached for performance
    4. Operations are thread-safe
    5. Observer notification order matches registration order
    """

    def __init__(self, error_handler: Optional[Callable] = None):
        self.__lock = threading.Lock()
        # Use list of weak references instead of WeakSet for ordered storage
        self.__observers = []
        self.__cached_methods = {}
        self.error_handler = error_handler or self._default_error_handler

    def add_observer(self, observer):
        """Register an observer instance. Observer should implement
        handler methods prefixed with 'on_' corresponding to notification events.
        """
        with self.__lock:
            self._cleanup_dead_refs()
            # Check if observer already exists
            if any(ref() is observer for ref in self.__observers):
                return

            # Create weak reference and add to list (preserves order)
            ref = weakref.ref(observer)
            self.__observers.append(ref)

    def remove_observer(self, observer):
        """Unregister a previously added observer instance."""
        with self.__lock:
            self._cleanup_dead_refs()
            # Remove all references pointing to this observer
            self.__observers = [ref for ref in self.__observers
                                if ref() is not None and ref() != observer]

    def _cleanup_dead_refs(self):
        """Remove references to garbage-collected observers"""
        self.__observers = [ref for ref in self.__observers if ref() is not None]

    def __getattr__(self, name):
        cached_method = self.__cached_methods.get(name, None)
        if cached_method:
            return cached_method

        if name.startswith("notify_"):
            event_name = name[7:]

            def thread_safe_notify(*args, **kwargs):
                method_name = f"on_{event_name}"

                with self.__lock:
                    # Clean up dead references before notification
                    self._cleanup_dead_refs()
                    # Get active observers in registration order
                    observers = [ref() for ref in self.__observers]
                    # Filter out any remaining None references
                    observers = [obs for obs in observers if obs is not None]

                # Notify outside lock to minimize contention
                for observer in observers:
                    try:
                        method = getattr(observer, method_name, None)
                        if callable(method):
                            method(*args, **kwargs)
                    except Exception as e:
                        self.error_handler(e, observer, method_name)

            self.__cached_methods[name] = thread_safe_notify
            return thread_safe_notify

        raise AttributeError(f"Invalid method: {name}")

    def _default_error_handler(self, e, observer, method_name):
        print(f"Error in {observer}.{method_name}: {str(e)}")


class IExampleObserver:
    def on_req_saved(self, req_uri):
        print(f"Observer: Req saved {req_uri}")


def main():
    notifier = ObserverNotifier()

    observer = IExampleObserver()

    notifier.add_observer(observer)

    notifier.notify_req_saved("http://example.com")  # 正常调用观察者的 on_req_saved 方法
    notifier.notify_req_loaded("http://example.com")  # 如果没有实现 on_req_loaded，将打印警告


# ----------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print('Error =>', e)
        print('Error =>', traceback.format_exc())
        exit()
    finally:
        pass
