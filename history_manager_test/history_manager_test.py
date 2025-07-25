import time
import unittest

from HistoryManager import HistoryManager


class TestHistoryManager(unittest.TestCase):
    def setUp(self):
        self.history = HistoryManager(max_size=3)  # 限制容量便于测试边界

    # ----------------- 边界情况测试 -----------------

    def test_empty_history(self):
        """测试空历史记录的操作"""
        self.assertIsNone(self.history.back_trace())  # 空列表后退[9](@ref)
        self.assertIsNone(self.history.forward_trace())  # 空列表前进
        self.history.remove_trace("item")  # 空列表删除无异常

    def test_empty_and_single_item_edge(self):
        # 空列表删除不崩溃
        self.history.remove_trace("Invalid")

        # 单记录下反复删除/添加
        self.history.record_trace("Alone")
        self.history.remove_trace("Alone")
        self.history.record_trace("New_Alone")
        self.assertIsNone(self.history.back_trace())  # 无后退
        self.history.remove_trace("New_Alone")
        self.assertEqual(self.history._HistoryManager__current_index, -1)  # 重置为空

    def test_single_item_operations(self):
        """测试仅有一个记录时的边界"""
        self.history.record_trace("A")
        self.assertIsNone(self.history.back_trace())  # 无法后退
        self.assertIsNone(self.history.forward_trace())  # 无法前进
        self.history.remove_trace("A")  # 删除后恢复空状态
        self.assertEqual(self.history._HistoryManager__current_index, -1)

    def test_capacity_overflow(self):
        """测试容量超限时自动移除最早记录"""
        for item in ["A", "B", "C", "D"]:
            self.history.record_trace(item)
        # 验证最早记录A被移除
        self.assertEqual(self.history._HistoryManager__browse_trace, ["B", "C", "D"])
        self.assertEqual(self.history._HistoryManager__current_index, 2)  # 指针指向最新

    # ----------------- 删除操作测试 -----------------
    def test_remove_current_item(self):
        """删除当前指针所在位置的记录"""
        for item in ["A", "B", "C"]:
            self.history.record_trace(item)
        self.history.back_trace()  # 当前指针指向B
        self.history.remove_trace("B")
        # 验证指针调整到A（前一项）
        self.assertEqual(self.history._HistoryManager__current_index, 0)
        self.assertEqual(self.history._HistoryManager__browse_trace, ["A", "C"])

    def test_remove_multiple_duplicates(self):
        """删除重复记录并验证指针修正"""
        for item in ["A", "B", "A", "C"]:
            self.history.record_trace(item)
        self.history.back_trace()  # 当前指向第二个A
        self.history.remove_trace("A")  # 删除所有A
        # 验证指针调整到C（原位置前移2）
        self.assertEqual(self.history._HistoryManager__browse_trace, ["B", "C"])
        self.assertEqual(self.history._HistoryManager__current_index, 0)

    def test_remove_all_items(self):
        """删除所有记录后状态重置"""
        for item in ["A", "B"]:
            self.history.record_trace(item)
        self.history.remove_trace("A")
        self.history.remove_trace("B")
        self.assertEqual(self.history._HistoryManager__current_index, -1)  # 重置为空
        self.assertEqual(self.history._HistoryManager__browse_trace, [])

    # ----------------- 反复操作测试 -----------------
    def test_repeated_navigation(self):
        """反复前后移动指针的稳定性测试"""
        for item in ["A", "B", "C"]:
            self.history.record_trace(item)

        # 反复后退/前进2次
        for _ in range(2):
            self.history.back_trace()  # C→B→A
        self.assertEqual(self.history.forward_trace(), "B")  # A→B
        self.assertEqual(self.history.forward_trace(), "C")  # B→C
        self.assertIsNone(self.history.forward_trace())  # 无法继续前进

    def test_record_after_navigation(self):
        """在历史记录中间添加新记录"""
        for item in ["A", "B", "C"]:
            self.history.record_trace(item)
        self.history.back_trace()  # 指针指向B
        self.history.record_trace("D")  # 在B后插入D，清除C
        # 验证后续记录被清除
        self.assertEqual(self.history._HistoryManager__browse_trace, ["A", "B", "D"])
        self.assertEqual(self.history.forward_trace(), None)  # D后无记录

    def test_remove_and_restore(self):
        """删除后重新添加并导航"""
        self.history.record_trace("A")
        self.history.remove_trace("A")
        self.history.record_trace("B")
        # 验证新记录独立存在
        self.assertIsNone(self.history.back_trace())
        self.assertIsNone(self.history.forward_trace())

    def test_abnormal_operation_sequences(self):
        # 添加后立即删除当前项
        self.history.record_trace("A")
        self.history.record_trace("B")
        self.history.back_trace()  # 指向A
        self.history.remove_trace("A")  # 删除当前项
        self.assertEqual(self.history._HistoryManager__browse_trace, ["B"])
        self.assertEqual(self.history._HistoryManager__current_index, 0)

        self.history.set_capacity(100)

        # 连续删除后回退
        for i in range(5):
            self.history.record_trace(f"C_{i}")
        self.history.remove_trace("C_1")
        self.history.remove_trace("C_3")
        self.history.back_trace()  # 从C_4退到C_2
        self.history.back_trace()  # 退到C_0
        self.assertEqual(self.history.forward_trace(), "C_2")  # 前进跳过被删项

        # 混合插入与删除
        self.history.record_trace("D")  # 新分支
        self.history.remove_trace("C_0")
        self.history.remove_trace("C_2")
        self.assertEqual(self.history._HistoryManager__browse_trace, ["B", "D"])

    def test_performance_benchmark(self):
        start_time = time.time()
        for i in range(10000):
            self.history.record_trace(f"Perf_{i}")
            if i % 10 == 0:
                self.history.back_trace()
            if i % 20 == 0:
                self.history.remove_trace(f"Perf_{i-5}")
        elapsed = time.time() - start_time
        print(f"10000次操作耗时: {elapsed:.2f}s")
        self.assertLess(elapsed, 1.0)  # 性能阈值


if __name__ == "__main__":
    unittest.main(verbosity=2)  # 显示详细测试结果
