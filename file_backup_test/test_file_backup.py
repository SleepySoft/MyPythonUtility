import errno
import io
import os
import sys
import time
import glob
import shutil
import unittest
import tempfile
from unittest.mock import patch
from datetime import datetime, timedelta

from file_backup import backup_file, backup_file_safe


class TestBackupFile(unittest.TestCase):
    def setUp(self):
        # 创建临时工作目录
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)
        self.test_file = "document.txt"

        # 创建测试文件
        with open(self.test_file, "w") as f:
            f.write("Original content")

    def remove_temp_dir(self):
        """安全的递归删除临时目录，处理文件锁定问题"""

        def handle_remove_readonly(func, path, exc):
            # 处理Windows上的只读文件错误
            excvalue = exc[1]
            if func in (os.rmdir, os.remove, os.unlink) and excvalue.errno == errno.EACCES:
                os.chmod(path, 0o700)  # 尝试修改权限
                func(path)
            else:
                raise

        for _ in range(3):  # 最多重试3次
            try:
                # 使用安全删除方法
                shutil.rmtree(self.test_dir, ignore_errors=False, onerror=handle_remove_readonly)
                return
            except (OSError, PermissionError) as e:
                # 如果是权限错误，等待后重试
                if e.errno in (errno.EACCES, errno.EPERM, errno.EBUSY):
                    time.sleep(0.1)
                else:
                    raise
        # 最终尝试
        try:
            shutil.rmtree(self.test_dir, ignore_errors=True)
        except Exception:
            pass  # 在测试失败时容忍错误

    def tearDown(self):
        # 清理临时目录
        self.remove_temp_dir()

    def test_basic_backup_creation(self):
        """测试基本备份创建功能"""
        backup_file(self.test_file, 3)

        # 检查备份目录是否存在
        backup_dir = os.path.join(self.test_dir, 'backup')
        self.assertTrue(os.path.exists(backup_dir), "Backup directory not created")

        # 检查备份文件是否存在
        backups = glob.glob(os.path.join(backup_dir, f"document_backup_*"))
        self.assertEqual(len(backups), 1, "Backup file not created")

        # 检查内容一致性
        with open(self.test_file, "r") as orig, open(backups[0], "r") as backup:
            self.assertEqual(orig.read(), backup.read(), "Backup content mismatch")

    def test_filename_format(self):
        """测试备份文件名格式是否正确"""
        with patch('datetime.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2023, 6, 15, 14, 30, 25, 123456)
            backup_file(self.test_file, 3)

        backups = glob.glob(os.path.join('backup', f"document_backup_*"))
        backup_name = os.path.basename(backups[0])

        # 检查时间戳格式
        self.assertRegex(
            backup_name,
            r"document_backup_20230615143025_123\.txt$",
            "Backup filename format incorrect"
        )

    def test_backup_limit_enforcement(self):
        """测试备份数量限制功能"""
        # 创建备份目录
        backup_dir = os.path.join(self.test_dir, 'backup')
        os.makedirs(backup_dir, exist_ok=True)

        # 获取文件名组件用于创建匹配的备份文件
        base_name = os.path.basename(self.test_file)
        base_no_ext, file_ext = os.path.splitext(base_name)

        # 创建不同时间的备份文件列表 (文件名, 时间偏移)
        timestamp_files = [
            (f"{base_no_ext}_backup_20230615120000_000{file_ext}", timedelta(days=5)),  # 最旧文件
            (f"{base_no_ext}_backup_20230615130000_000{file_ext}", timedelta(days=4)),
            (f"{base_no_ext}_backup_20230615140000_000{file_ext}", timedelta(days=3)),
            (f"{base_no_ext}_backup_20230615150000_000{file_ext}", timedelta(days=2)),
            (f"{base_no_ext}_backup_20230615160000_000{file_ext}", timedelta(days=1))  # 最新文件
        ]

        # 创建测试文件并设置时间戳
        base_time = datetime.now()
        for filename, time_offset in timestamp_files:
            file_path = os.path.join(backup_dir, filename)
            with open(file_path, "w") as f:
                f.write(f"Content for {filename}")

            # 设置文件修改时间为指定偏移
            file_time = base_time - time_offset
            timestamp = file_time.timestamp()
            os.utime(file_path, (timestamp, timestamp))

        # 执行备份操作（限制为保留3个最新备份）
        backup_file(self.test_file, 4)

        # 检查备份目录中的文件
        backup_files = os.listdir(backup_dir)

        # 验证数量：3个旧备份 + 1个新创建的备份
        self.assertEqual(len(backup_files), 4, "备份数量不正确")

        # 找出预创建的文件（排除新创建的备份）
        pre_created_files = [f for f in backup_files if "20230615" in f]

        # 应该保留3个时间较新的预创建备份
        self.assertEqual(len(pre_created_files), 3, "未保留正确数量的历史备份")

        # 验证保留的文件名
        retained_files = sorted(pre_created_files)
        expected_files = [
            f"{base_no_ext}_backup_20230615140000_000{file_ext}",
            f"{base_no_ext}_backup_20230615150000_000{file_ext}",
            f"{base_no_ext}_backup_20230615160000_000{file_ext}",
            # 最旧的文件应该被删除
        ]

        # 验证保留的正确性
        self.assertListEqual(
            retained_files,
            expected_files,
            "未保留正确的备份文件"
        )

        # 验证新的备份文件已创建
        new_backups = [f for f in backup_files if f not in timestamp_files]
        self.assertEqual(len(new_backups), 4, "未创建新的备份文件")
        self.assertTrue(
            new_backups[-1].startswith(f"{base_no_ext}_backup_") and new_backups[0].endswith(file_ext),
            "新备份文件名格式不正确"
        )

    def test_special_characters_in_filename(self):
        """测试特殊字符的文件名处理"""
        special_file = "file with spaces & special (chars).csv"
        with open(special_file, "w") as f:
            f.write("Special file content")

        backup_file(special_file, 2)

        backups = glob.glob(os.path.join('backup', f"file with spaces & special *"))
        self.assertEqual(len(backups), 1, "Backup not created for special filename")

    def test_file_without_extension(self):
        """测试无扩展名的文件备份"""
        no_ext_file = "datafile"
        with open(no_ext_file, "w") as f:
            f.write("No extension content")

        backup_file(no_ext_file, 2)

        backups = glob.glob(os.path.join('backup', f"datafile_backup_*"))
        self.assertEqual(len(backups), 1, "Backup not created for extensionless file")
        self.assertRegex(
            os.path.basename(backups[0]),
            r"^datafile_backup_\d{14}_\d{3}$",
            "Backup filename format incorrect for extensionless file"
        )

    def test_multi_dot_filename(self):
        """测试多扩展名的文件备份"""
        multi_dot_file = "archive.tar.gz"
        with open(multi_dot_file, "w") as f:
            f.write("Compressed data")

        backup_file(multi_dot_file, 2)

        backups = glob.glob(os.path.join('backup', f"archive_backup_*.tar.gz"))
        self.assertEqual(len(backups), 1, "Backup not created for multi-dot filename")
        self.assertRegex(
            os.path.basename(backups[0]),
            r"^archive_backup_\d{14}_\d{3}\.tar\.gz$",
            "Multi-dot filename handled incorrectly"
        )

    def test_backup_directory_creation(self):
        """测试备份目录自动创建"""
        # 确保目录不存在
        backup_dir = os.path.join(self.test_dir, 'backup')
        if os.path.exists(backup_dir):
            shutil.rmtree(backup_dir)

        backup_file(self.test_file, 1)
        self.assertTrue(os.path.exists(backup_dir), "Backup directory not created")

    def test_source_file_not_found(self):
        """测试源文件不存在的情况"""
        with self.assertRaises(FileNotFoundError):
            backup_file("non_existent_file.txt", 3)

    def test_invalid_backup_limit(self):
        """测试无效备份限制值"""
        with self.assertRaises(ValueError):
            backup_file(self.test_file, 0)

        with self.assertRaises(ValueError):
            backup_file(self.test_file, -5)

    def test_safe_backup_success(self):
        """测试安全备份函数成功场景"""
        result = backup_file_safe(self.test_file, 3)
        self.assertTrue(result, "Safe backup should return True on success")

    def test_safe_backup_failure(self):
        """测试安全备份函数失败场景"""
        # 重定向标准输出以捕获错误信息
        captured = io.StringIO()
        sys.stdout = captured

        result = backup_file_safe("invalid_file.txt", 3)

        # 恢复标准输出
        sys.stdout = sys.__stdout__

        output = captured.getvalue()
        self.assertFalse(result, "Safe backup should return False on failure")
        self.assertIn("[!] Backup failed for invalid_file.txt", output)
        self.assertIn("Source file not found", output)

    def test_metadata_preservation(self):
        """测试备份元数据是否正确保留"""
        # 设置原始文件修改时间
        orig_time = (datetime.now() - timedelta(days=1)).timestamp()
        os.utime(self.test_file, (orig_time, orig_time))

        backup_file(self.test_file, 1)

        backups = glob.glob(os.path.join('backup', f"document_backup_*"))
        backup_stat = os.stat(backups[0])

        # 验证元数据 (允许1秒误差)
        with open(self.test_file, "r") as f:  # 确保文件关闭
            orig_stat = os.stat(self.test_file)

        self.assertAlmostEqual(
            orig_stat.st_mtime,
            backup_stat.st_mtime,
            delta=1,
            msg="File modification time not preserved"
        )


if __name__ == '__main__':
    unittest.main()
