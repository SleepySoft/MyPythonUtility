import os
import sys
import shutil
import tempfile
import unittest
from datetime import datetime, timedelta

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from FileSqliteHyridDB import HybridDB


class TestHybridDB(unittest.TestCase):
    """
    A test suite for the HybridDB class.

    This suite creates a temporary directory for each test, ensuring that tests are
    isolated and do not leave artifacts on the filesystem.
    """

    def setUp(self):
        """
        Set up a temporary directory and a new HybridDB instance before each test.
        """
        # Create a temporary directory that will be automatically cleaned up
        self.test_dir = tempfile.mkdtemp()
        self.db = HybridDB(root_dir=self.test_dir)
        print(f"\nRunning test '{self._testMethodName}' in directory: {self.test_dir}")

    def tearDown(self):
        """
        Clean up the temporary directory after each test.
        """
        # del self.db  # Not strictly necessary, but good practice
        shutil.rmtree(self.test_dir)

    def test_01_initialization(self):
        """
        Test that the database and root directory are created on initialization.
        """
        self.assertTrue(os.path.isdir(self.test_dir))
        db_path = os.path.join(self.test_dir, 'index.sqlite')
        self.assertTrue(os.path.isfile(db_path))

    def test_02_add_and_get_text(self):
        """
        Test adding and retrieving a simple text record.
        """
        content = "Hello, world! This is a test. Special chars: 你好, éàç."
        index = self.db.add_text(content, category="notes", name="greeting")
        self.assertEqual(index, 1)

        # Retrieve metadata
        record = self.db.get_by_index(index)
        self.assertIsNotNone(record)
        self.assertEqual(record['id'], index)
        self.assertEqual(record['name'], "greeting")
        self.assertEqual(record['category'], "notes")
        self.assertEqual(record['content_type'], "text")

        # Verify path and file existence
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, record['path'])))

        # Retrieve content
        retrieved_content = self.db.get_content_by_index(index)
        self.assertEqual(content, retrieved_content)

    def test_03_add_and_get_binary(self):
        """
        Test adding and retrieving a simple binary record.
        """
        content = b'\x01\x02\x03\xDE\xAD\xBE\xEF'
        index = self.db.add_binary(content, category="data", name="firmware")
        self.assertEqual(index, 1)

        # Retrieve metadata
        record = self.db.get_by_index(index, absolute_path=True)
        self.assertIsNotNone(record)
        self.assertEqual(record['id'], index)
        self.assertEqual(record['name'], "firmware")
        self.assertEqual(record['category'], "data")
        self.assertEqual(record['content_type'], "binary")

        # Verify path and file existence
        self.assertTrue(os.path.exists(record['path']))

        # Retrieve content
        retrieved_content = self.db.get_content_by_index(index)
        self.assertEqual(content, retrieved_content)

    def test_04_default_and_sanitized_parameters(self):
        """
        Test default name/category and category sanitization logic.
        """
        # Test default parameters
        index_default = self.db.add_text("Default values.")
        record_default = self.db.get_by_index(index_default)
        self.assertEqual(record_default['category'], 'default')
        # Name should be a UUID4 hex string, which is 32 characters long
        self.assertEqual(len(record_default['name']), 32)

        # Test category sanitization
        illegal_category = "reports/2025\\final?*<:>"
        sanitized_category = "reports2025final"
        index_sanitized = self.db.add_text("Sanitized category.", category=illegal_category)
        record_sanitized = self.db.get_by_index(index_sanitized)

        self.assertEqual(record_sanitized['category'], sanitized_category)
        self.assertTrue(os.path.isdir(os.path.join(self.test_dir, sanitized_category)))

    def test_05_delete_record(self):
        """
        Test the deletion of a record and its associated file.
        """
        index = self.db.add_text("This will be deleted.", name="temp_file")
        record = self.db.get_by_index(index, absolute_path=True)
        self.assertIsNotNone(record)
        file_path = record['path']

        # File should exist before deletion
        self.assertTrue(os.path.exists(file_path))

        # Perform deletion
        delete_success = self.db.delete_by_index(index)
        self.assertTrue(delete_success)

        # File should not exist after deletion
        self.assertFalse(os.path.exists(file_path))

        # Record should not be in the database
        self.assertIsNone(self.db.get_by_index(index))

    def test_06_delete_non_existent_record(self):
        """
        Test that deleting a non-existent record returns False.
        """
        delete_success = self.db.delete_by_index(9999)
        self.assertFalse(delete_success)

    def test_07_find_queries(self):
        """
        Test the refactored 'find' method and the new workflow of getting records by indices.
        """
        time_now = datetime.now()
        # The returned indices will be 1, 2, 3, 4 in order of creation.
        id1 = self.db.add_text("Log entry 1", category="logs", name="app.log",
                               timestamp=time_now - timedelta(minutes=10))
        id2 = self.db.add_text("Log entry 2", category="logs", name="kernel.log",
                               timestamp=time_now - timedelta(minutes=5))
        id3 = self.db.add_binary(b"data1", category="data", name="dataset1.bin",
                                 timestamp=time_now - timedelta(minutes=2))
        id4 = self.db.add_text("Report for January", category="reports", name="jan_report",
                               timestamp=time_now - timedelta(days=30))

        # --- Find by category ---
        # Step 1: Find indices
        log_indices = self.db.find(category="logs")
        self.assertEqual(len(log_indices), 2)
        self.assertIn(id1, log_indices)
        self.assertIn(id2, log_indices)

        # Step 2: Get records
        log_records = self.db.get_records_by_indices(log_indices)
        self.assertEqual(len(log_records), 2)
        self.assertTrue(all(r['category'] == 'logs' for r in log_records))

        # --- Find by name (fuzzy search) ---
        log_name_indices = self.db.find(name_like="log")
        self.assertEqual(set(log_name_indices), {id1, id2})

        # --- Find by time range (start_time) ---
        # Should find records with id2 and id3. Note: find sorts by timestamp DESC.
        recent_indices = self.db.find(start_time=time_now - timedelta(minutes=6))
        self.assertEqual(len(recent_indices), 2)
        self.assertEqual(recent_indices[0], id3)  # Most recent
        self.assertEqual(recent_indices[1], id2)

        # Get records and check names
        recent_records = self.db.get_records_by_indices(recent_indices)
        self.assertEqual({r['name'] for r in recent_records}, {'kernel.log', 'dataset1.bin'})

        # --- Find with combined criteria ---
        specific_log_indices = self.db.find(category="logs", name_like="app")
        self.assertEqual(len(specific_log_indices), 1)
        self.assertEqual(specific_log_indices[0], id1)

        # --- Find with no results ---
        no_results_indices = self.db.find(category="nonexistent")
        self.assertEqual(len(no_results_indices), 0)

    def test_08_edge_cases(self):
        """
        Test various edge cases like empty content and missing files.
        """
        # Test adding empty content
        empty_text_id = self.db.add_text("", name="empty_text")
        empty_binary_id = self.db.add_binary(b"", name="empty_binary")

        self.assertEqual(self.db.get_content_by_index(empty_text_id), "")
        self.assertEqual(self.db.get_content_by_index(empty_binary_id), b"")

        # Test getting content when file is missing
        record_id = self.db.add_text("This file will disappear.", name="ghost")
        record = self.db.get_by_index(record_id, absolute_path=True)
        os.remove(record['path'])

        # Getting content should gracefully fail and return None
        self.assertIsNone(self.db.get_content_by_index(record_id))

        # Deleting a record whose file is already gone should still work
        self.assertTrue(self.db.delete_by_index(record_id))
        self.assertIsNone(self.db.get_by_index(record_id))

    def test_09_get_records_by_indices(self):
        """
        Test the new 'get_records_by_indices' method specifically.
        """
        # Add some records. IDs will be 1, 2, 3.
        id1 = self.db.add_text("First", name="file1")
        id2 = self.db.add_text("Second", name="file2")
        id3 = self.db.add_text("Third", name="file3")

        # Test getting multiple, specific records
        records = self.db.get_records_by_indices([id1, id3])
        self.assertEqual(len(records), 2)
        self.assertEqual({r['id'] for r in records}, {id1, id3})
        self.assertEqual({r['name'] for r in records}, {"file1", "file3"})

        # Test getting records that include non-existent indices
        records_with_invalid = self.db.get_records_by_indices([id2, 999, 1000])
        self.assertEqual(len(records_with_invalid), 1)
        self.assertEqual(records_with_invalid[0]['id'], id2)

        # Test getting records with an empty list
        empty_records = self.db.get_records_by_indices([])
        self.assertEqual(len(empty_records), 0)

        # Test the absolute_path flag
        # Relative path (default)
        relative_path_records = self.db.get_records_by_indices([id1])
        self.assertFalse(os.path.isabs(relative_path_records[0]['path']))
        self.assertTrue(relative_path_records[0]['path'].startswith("default"))

        # Absolute path
        absolute_path_records = self.db.get_records_by_indices([id1], absolute_path=True)
        self.assertTrue(os.path.isabs(absolute_path_records[0]['path']))
        self.assertTrue(absolute_path_records[0]['path'].startswith(self.test_dir))


if __name__ == '__main__':
    unittest.main(verbosity=2)
