class HistoryManager:
    """
    Manages navigation history with forward/backward traversal, recording, and item removal.

    This class maintains a sequence of historical items with a current pointer, enabling 
    browser-like navigation. New recordings truncate future history to ensure linear continuity.

    Attributes:
        max_size (int): Maximum number of historical entries. Oldest items are purged when exceeded.

    Methods:
        back_trace(): 
            Navigates backward by one step and returns the item at the new position. 
            Returns `None` if at the beginning of history.

        forward_trace(): 
            Navigates forward by one step and returns the item at the new position. 
            Returns `None` if at the end of history.

        record_trace(item): 
            Records a new item into history, truncating all entries after the current pointer.
            Automatically enforces `max_size` by removing oldest entries when exceeded.

        remove_trace(item): 
            Removes *all* occurrences of `item` from history. Adjusts the current pointer 
            to the nearest valid position (prioritizing subsequent items if current item is removed).

    Example:
        manager = HistoryManager(max_size=3)
        manager.record_trace("Page1")
        manager.record_trace("Page2")
        manager.back_trace()  # Returns "Page1"
        manager.record_trace("Page3")  # Truncates "Page2"
        manager.remove_trace("Page1")  # Removes all "Page1" entries
    """
    def __init__(self, max_size: int = 1000):
        self.__max_size = max(max_size, 1)
        self.__browse_trace = []
        self.__current_index = -1

    def set_capacity(self, max_size):
        self.__max_size = max(max_size, 1)

    def clear_trace(self):
        self.__browse_trace = []
        self.__current_index = -1

    def back_trace(self) -> any or None:
        if self.__current_index > 0:
            self.__current_index -= 1
            return self.__browse_trace[self.__current_index]
        return None

    def forward_trace(self) -> any or None:
        if self.__current_index < len(self.__browse_trace) - 1:
            self.__current_index += 1
            return self.__browse_trace[self.__current_index]
        return None

    def record_trace(self, item: any):
        # Use current_index instead of len(browse_trace) avoiding
        #   deletion by mistake when current_index is not pointing to the tail.
        #
        # Exp: browse_trace = [A, B, C], max_size = 3, current_index = 1.
        #      When you adding D, judging len(browse_trace) >= max_size may cause wrong result [B, D]
        #      But the correct result should be [A, B, D]
        #
        #　You can use record_trace(None) to trigger deletion after you changing the max_size by set_capacity()
        #
        while self.__current_index >= self.__max_size - 1:
            self.__browse_trace.pop(0)
            self.__current_index -= 1
            
        if not item:
            return

        # Optimise:
        #   If the adding item is right the current item. Ignore.
        #   If the adding item is right the next item. Just forward.
        if self.__current_index >= 0:
            if self.__browse_trace[self.__current_index] == item:
                return
            elif self.__current_index + 1 < len(self.__browse_trace) and \
                    self.__browse_trace[self.__current_index + 1] == item:
                self.__current_index += 1

        self.__browse_trace = self.__browse_trace[:self.__current_index + 1]
        self.__browse_trace.append(item)
        self.__current_index = len(self.__browse_trace) - 1

    def remove_trace(self, item: any):
        if not self.__browse_trace:
            return

        # 单次遍历完成删除和索引计算
        new_trace = []
        removed_before = 0
        current_item_removed = False
        for i, n in enumerate(self.__browse_trace):
            if n == item:
                if i == self.__current_index:
                    current_item_removed = True
                if i <= self.__current_index:
                    removed_before += 1
            else:
                new_trace.append(n)

        # 无匹配项直接返回
        if removed_before == 0:
            return

        # 计算新索引
        if not new_trace:
            self.__browse_trace = []
            self.__current_index = -1
            return

        new_index = self.__current_index - removed_before
        if current_item_removed:
            new_index = max(0, min(new_index, len(new_trace) - 1))
        else:
            new_index = max(0, min(new_index, len(new_trace) - 1))

        self.__browse_trace = new_trace
        self.__current_index = new_index

