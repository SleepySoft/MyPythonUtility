import os
import glob
import shutil
import datetime


def get_base_and_full_extension(filename):
    base_name = os.path.basename(filename)
    first_dot_index = base_name.find('.')
    if first_dot_index == -1:
        return base_name, ""
    return base_name[:first_dot_index], base_name[first_dot_index:]


def backup_file(file_name: str, backup_limit: int) -> None:
    """
    Backs up a file with a timestamp and maintains a limited number of recent backups.

    Creates a versioned copy in a 'backup' subdirectory using the naming format:
    `{original_name}_backup_{timestamp}{extension}`. Automatically removes the oldest
    backups when exceeding the specified limit.

    Args:
        file_name: Path to the target file
        backup_limit: Maximum number of backups to retain (must be >= 1)

    Raises:
        FileNotFoundError: If source file doesn't exist
        OSError: For filesystem-related errors
        ValueError: If backup_limit < 1

    Example:
        backup_file("data/config.json", backup_limit=5)
        # Generates: backup/config_backup_20240620120530_123.json
        # Keeps 5 newest backups in 'backup' folder
    """
    # Validate backup limit
    if backup_limit < 1:
        raise ValueError("backup_limit must be at least 1")

    # Check source file existence
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"Source file not found: {file_name}")

    # Prepare paths
    file_path = os.path.abspath(file_name)
    file_dir = os.path.dirname(file_path)
    backup_dir = os.path.join(file_dir, 'backup')
    os.makedirs(backup_dir, exist_ok=True)

    # Extract filename components
    base_name = os.path.basename(file_name)
    base_part, full_extension = get_base_and_full_extension(base_name)

    timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S_%f')[:-3]

    if full_extension:
        backup_name = f"{base_part}_backup_{timestamp}{full_extension}"
    else:
        backup_name = f"{base_part}_backup_{timestamp}"

    dest_path = os.path.join(backup_dir, backup_name)

    # Create backup (preserve metadata)
    shutil.copy2(file_path, dest_path)

    # Find all backups matching pattern safely
    pattern = os.path.join(
        backup_dir,
        f"{glob.escape(base_part)}_backup_*{glob.escape(full_extension)}"
    )
    backups = glob.glob(pattern)

    # Delete the oldest backups when exceeding limit
    if len(backups) > backup_limit:
        # Sort by modification time (more reliable than create time)
        backups.sort(key=os.path.getmtime)
        # Delete only the oldest excess files
        for old_backup in backups[:len(backups) - backup_limit]:
            os.remove(old_backup)


def backup_file_safe(file_name: str, backup_limit: int) -> bool:
    """
    Safe wrapper for backup_file that suppresses exceptions.

    Returns True on success, False on failure while printing errors.

    Args:
        file_name: Path to the target file
        backup_limit: Maximum backups to retain

    Returns:
        bool: True if backup succeeded, False otherwise
    """
    try:
        backup_file(file_name, backup_limit)
        return True
    except Exception as e:
        print(f"[!] Backup failed for {file_name}")
        print(f"    Error: {str(e)}")
        return False
