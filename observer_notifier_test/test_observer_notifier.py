import unittest
import threading
import time

from ObserverNotifier import ObserverNotifier


class TestObserver(unittest.TestCase):
    def setUp(self):
        self.notifier = ObserverNotifier()
        self.events = []
        self.errors = []

        self.notifier.error_handler = self.error_handler

    def error_handler(self, e, observer, method):
        self.errors.append((type(e).__name__, str(e), observer.id, method))

    class MockObserver:
        def __init__(self, id, events=None):
            self.id = id
            self.events = events or []

        def on_event(self, data):
            self.events.append(('event', data))

        def on_delayed_event(self, data):
            time.sleep(0.05)  # Simulate processing delay
            self.events.append(('delayed', data))

        def on_fail_event(self, data):
            raise ValueError("Simulated failure")

        def on_processing_event(self, data):
            # Simulate complex processing
            result = data * 2
            if result > 10:
                self.events.append(('high', result))
            else:
                self.events.append(('low', result))

    # 测试基础功能
    def test_basic_notification(self):
        observer = self.MockObserver(1)
        self.notifier.add_observer(observer)

        self.notifier.notify_event("test_data")
        self.assertEqual(len(observer.events), 1)
        self.assertEqual(observer.events[0], ('event', 'test_data'))

    # 新增：测试多次通知
    def test_multiple_notifications(self):
        observer = self.MockObserver(2)
        self.notifier.add_observer(observer)

        self.notifier.notify_event("first")
        self.notifier.notify_event("second")
        self.notifier.notify_event("third")

        self.assertEqual(len(observer.events), 3)
        self.assertEqual(observer.events[0], ('event', 'first'))
        self.assertEqual(observer.events[1], ('event', 'second'))
        self.assertEqual(observer.events[2], ('event', 'third'))

    # 新增：测试复杂处理逻辑
    def test_processing_logic(self):
        observer = self.MockObserver(3)
        self.notifier.add_observer(observer)

        self.notifier.notify_processing_event(4)
        self.notifier.notify_processing_event(6)
        self.notifier.notify_processing_event(7)

        self.assertEqual(len(observer.events), 3)
        self.assertEqual(observer.events[0], ('low', 8))
        self.assertEqual(observer.events[1], ('high', 12))
        self.assertEqual(observer.events[2], ('high', 14))

    # 新增：测试顺序处理
    def test_ordering(self):
        observer = self.MockObserver(4)
        self.notifier.add_observer(observer)

        for i in range(100):
            self.notifier.notify_processing_event(i)

        # Verify all events were processed in order
        for i, event in enumerate(observer.events):
            self.assertEqual(event[0], 'high' if i > 5 else 'low')
            self.assertEqual(event[1], i * 2)

    # 新增：测试处理延迟
    def test_handling_delay(self):
        observer = self.MockObserver(5)
        self.notifier.add_observer(observer)

        start = time.time()
        self.notifier.notify_delayed_event("delayed_data")
        end = time.time()

        self.assertGreaterEqual(end - start, 0.05)
        self.assertEqual(len(observer.events), 1)
        self.assertEqual(observer.events[0], ('delayed', 'delayed_data'))

    # 测试同步错误处理
    def test_sync_error_handling(self):
        observer1 = self.MockObserver(6)
        observer2 = self.MockObserver(7)

        self.notifier.add_observer(observer1)
        self.notifier.add_observer(observer2)

        self.notifier.notify_fail_event("sync_error_data")

        self.assertEqual(len(self.errors), 2)
        self.assertEqual(self.errors[0][0], 'ValueError')
        self.assertEqual(self.errors[0][1], 'Simulated failure')
        self.assertEqual(self.errors[0][2], 6)
        self.assertEqual(self.errors[0][3], 'on_fail_event')

    # 测试观察者移除
    def test_observer_removal(self):
        observer = self.MockObserver(8)
        self.notifier.add_observer(observer)

        self.notifier.notify_event("data1")
        self.notifier.remove_observer(observer)
        self.notifier.notify_event("data2")

        self.assertEqual(len(observer.events), 1)
        self.assertEqual(observer.events[0], ('event', 'data1'))

    # 测试弱引用回收
    def test_weak_reference(self):
        for i in range(100):
            observer = self.MockObserver(i)
            self.notifier.add_observer(observer)

        self.notifier.notify_event("before")
        # 删除引用，触发GC
        del observer
        import gc
        gc.collect()

        self.notifier.notify_event("after")
        self.assertEqual(len(self.errors), 0)  # 不应有错误
        with self.notifier._ObserverNotifier__lock:
            self.assertEqual(len(self.notifier._ObserverNotifier__observers), 0)

    # 测试方法缓存
    def test_method_caching(self):
        notify1 = self.notifier.notify_test
        notify2 = self.notifier.notify_test

        self.assertEqual(notify1, notify2)
        self.assertIn("notify_test", self.notifier._ObserverNotifier__cached_methods)

    # 测试线程安全
    def test_thread_safety(self):
        results = []
        lock = threading.Lock()

        class ThreadObserver:
            def __init__(self, id):
                self.id = id

            def on_thread_event(self, value):
                with lock:
                    results.append((self.id, value))

        # 创建多个观察者
        observers = [ThreadObserver(i) for i in range(10)]
        for obs in observers:
            self.notifier.add_observer(obs)

        # 并发通知
        threads = []
        for i in range(5):
            t = threading.Thread(
                target=self.notifier.notify_thread_event,
                args=(i,)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # 验证结果
        self.assertEqual(len(results), 50)  # 10观察者 * 5通知

    # 测试空通知
    def test_empty_notification(self):
        self.notifier.notify_event("no_observers")
        self.assertEqual(len(self.errors), 0)

    # 测试非法方法访问
    def test_invalid_method(self):
        with self.assertRaises(AttributeError):
            self.notifier.invalid_method()

        with self.assertRaises(AttributeError):
            self.notifier.notifyinvalid()  # 缺少下划线

    # 新增：测试多类型参数
    def test_various_parameters(self):
        observer = self.MockObserver(10)
        self.notifier.add_observer(observer)

        # 测试不同参数类型
        self.notifier.notify_event("string")
        self.notifier.notify_event(123)
        self.notifier.notify_event(3.14)
        self.notifier.notify_event(None)
        self.notifier.notify_event({"key": "value"})
        self.notifier.notify_event(["list", "of", "items"])

        self.assertEqual(len(observer.events), 6)
        self.assertEqual(observer.events[0], ('event', 'string'))
        self.assertEqual(observer.events[1], ('event', 123))
        self.assertEqual(observer.events[2], ('event', 3.14))
        self.assertEqual(observer.events[3], ('event', None))
        self.assertEqual(observer.events[4], ('event', {"key": "value"}))
        self.assertEqual(observer.events[5], ('event', ["list", "of", "items"]))

    # 新增：测试大量观察者
    def test_large_number_of_observers(self):
        ref_list = []

        # 创建并添加1000个观察者
        for i in range(1000):
            observer = self.MockObserver(i)
            ref_list.append(observer)
            self.notifier.add_observer(observer)

        # 触发通知
        self.notifier.notify_event("mass_notification")

        # 验证所有观察者都收到了通知
        with self.notifier._ObserverNotifier__lock:
            self.assertEqual(len(self.notifier._ObserverNotifier__observers), 1000)

        for ob in ref_list:
            self.assertEqual(ob.events[0], ("event", "mass_notification"))


if __name__ == '__main__':
    unittest.main()
