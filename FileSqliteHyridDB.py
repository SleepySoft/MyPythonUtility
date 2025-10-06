import sqlite3
import os
import uuid
import re
from datetime import datetime
from typing import Optional, List, Union, Dict, Any


# Register adapters for datetime objects to be stored as ISO 8601 strings.
# This ensures timezone information is preserved and is easily parseable.
def adapt_datetime_iso(val):
    """Adapter to convert datetime to ISO 8601 string."""
    return val.isoformat()


def convert_datetime(val):
    """Converter to parse ISO 8601 string back to datetime object."""
    return datetime.fromisoformat(val.decode('utf-8'))


sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_converter("timestamp", convert_datetime)


class HybridDB:
    """
    A hybrid database system that uses SQLite for indexing and the filesystem for content storage.

    Design Intention:
    This class is designed to manage large pieces of data (like text files, images, or other binary blobs)
    efficiently. Instead of storing the large data directly in the database, which can lead to performance
    issues and database bloat, it stores the content in regular files. A lightweight SQLite database is
    used to store metadata and an index for this content, allowing for fast searches and queries.

    The filesystem organization is handled automatically. A root directory is specified during
    initialization. Inside this root, subdirectories are created for each 'category' of data. The
    actual content files are named using a combination of their given name (or a UUID) and a timestamp,
    ensuring uniqueness. The database stores a relative path to each file, making the entire root
    directory portable.

    Usage:
    >>> # 1. Initialize the database in a specific directory
    >>> db = HybridDB(root_dir="my_data_storage")
    >>>
    >>> # 2. Add content. The method returns the unique index (ID).
    >>> text_id = db.add_text("This is a document.", category="documents", name="sample_doc")
    >>> image_id = db.add_binary(b"...", category="images", name="profile_picture")
    >>>
    >>> # 3. Find a list of indices based on query parameters. This is a lightweight operation.
    >>> image_indices = db.find(category="images")
    >>> print(f"Found indices for images: {image_indices}")
    >>>
    >>> # 4. Fetch the full record details for those indices.
    >>> if image_indices:
    ...     image_records = db.get_records_by_indices(image_indices, absolute_path=True)
    ...     print(f"Image records: {image_records}")
    ...     # You can now iterate through image_records to get paths and other metadata.
    ...     for record in image_records:
    ...         print(f"Processing file at: {record['path']}")
    >>>
    >>> # 5. Retrieve the actual content for a single index
    >>> content = db.get_content_by_index(text_id)
    >>> print(f"Content for ID {text_id}: {content}")
    >>>
    >>> # 6. Delete a record and its corresponding file by index
    >>> db.delete_by_index(image_id)
    >>> print(f"Deleted record with ID: {image_id}")
    """

    def __init__(self, root_dir: str):
        """
        Initializes the HybridDB.

        Args:
            root_dir (str): The root directory where the SQLite database and content files will be stored.
                            It will be created if it doesn't exist.
        """
        self._root_dir = os.path.abspath(root_dir)
        self._db_path = os.path.join(self._root_dir, 'index.sqlite')
        os.makedirs(self._root_dir, exist_ok=True)
        self._initialize_db()

    def _get_connection(self) -> sqlite3.Connection:
        """
        Gets a new SQLite connection.
        Using a new connection for each operation is a simple way to ensure thread safety.
        """
        conn = sqlite3.connect(self._db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.row_factory = sqlite3.Row
        return conn

    def _initialize_db(self):
        """
        Creates the 'records' table in the database if it does not already exist.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    category TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    relative_path TEXT NOT NULL UNIQUE,
                    content_type TEXT NOT NULL CHECK(content_type IN ('text', 'binary'))
                );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_name ON records(name);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_category ON records(category);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON records(timestamp);")
            conn.commit()

    def _sanitize_category(self, category: str) -> str:
        """
        Removes characters that are illegal in directory names for Windows and Linux.
        """
        return re.sub(r'[<>:"/\\|?*\x00-\x1f]', '', category)

    def _add(self, content: Union[str, bytes], content_type: str, category: str, name: Optional[str],
             timestamp: Optional[datetime]) -> int:
        """Internal method to handle the logic of adding new content."""
        resolved_name = name if name else uuid.uuid4().hex
        resolved_time = timestamp if timestamp else datetime.now()
        sanitized_category = self._sanitize_category(category) if category else "default"

        category_dir = os.path.join(self._root_dir, sanitized_category)
        os.makedirs(category_dir, exist_ok=True)

        time_str = resolved_time.strftime('%Y%m%d%H%M%S%f')
        extension = ".txt" if content_type == 'text' else ".bin"
        filename = f"{resolved_name}_{time_str}{extension}"

        relative_path = os.path.join(sanitized_category, filename)
        absolute_path = os.path.join(self._root_dir, relative_path)

        try:
            mode = 'w' if content_type == 'text' else 'wb'
            encoding = 'utf-8' if content_type == 'text' else None
            with open(absolute_path, mode, encoding=encoding) as f:
                f.write(content)
        except IOError as e:
            raise IOError(f"Failed to write file at {absolute_path}: {e}") from e

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("""
                    INSERT INTO records (name, category, timestamp, relative_path, content_type)
                    VALUES (?, ?, ?, ?, ?)
                """, (resolved_name, sanitized_category, resolved_time, relative_path, content_type))
                record_id = cursor.lastrowid
                conn.commit()
                return record_id
            except sqlite3.IntegrityError as e:
                os.remove(absolute_path)
                raise ValueError(f"Failed to add record to database, likely a path conflict: {e}") from e

    def add_text(self, content: str, category: str = "default", name: Optional[str] = None,
                 timestamp: Optional[datetime] = None) -> int:
        """
        Adds text content to the database.

        Args:
            content (str): The string content to store.
            category (str, optional): A category to organize the content. Defaults to "default".
            name (Optional[str], optional): A name for the content. If None, a UUID4 hex is used. Defaults to None.
            timestamp (Optional[datetime], optional): The timestamp for the entry. If None, the current time is used. Defaults to None.

        Returns:
            int: The unique index (ID) of the stored content.
        """
        return self._add(content, 'text', category, name, timestamp)

    def add_binary(self, content: bytes, category: str = "default", name: Optional[str] = None,
                   timestamp: Optional[datetime] = None) -> int:
        """
        Adds binary content to the database.

        Args:
            content (bytes): The binary content to store.
            category (str, optional): A category to organize the content. Defaults to "default".
            name (Optional[str], optional): A name for the content. If None, a UUID4 hex is used. Defaults to None.
            timestamp (Optional[datetime], optional): The timestamp for the entry. If None, the current time is used. Defaults to None.

        Returns:
            int: The unique index (ID) of the stored content.
        """
        return self._add(content, 'binary', category, name, timestamp)

    def _format_row(self, row: sqlite3.Row, use_absolute_path: bool) -> Dict[str, Any]:
        """Formats a database row into a dictionary, handling path resolution."""
        record = dict(row)
        relative_path = record.pop('relative_path')
        record['path'] = os.path.join(self._root_dir, relative_path) if use_absolute_path else relative_path
        return record

    def find(self,
             name_like: Optional[str] = None,
             category: Optional[str] = None,
             start_time: Optional[datetime] = None,
             end_time: Optional[datetime] = None) -> List[int]:
        """
        Finds record indices based on specified criteria.

        This method performs a lightweight search and returns only the unique IDs of the
        matching records, which can then be used to fetch full metadata or content.

        Args:
            name_like (Optional[str], optional): A substring to search for in the record's name (case-insensitive). Defaults to None.
            category (Optional[str], optional): The exact category to filter by. Defaults to None.
            start_time (Optional[datetime], optional): The minimum timestamp for records to include. Defaults to None.
            end_time (Optional[datetime], optional): The maximum timestamp for records to include. Defaults to None.

        Returns:
            List[int]: A list of integer indices (IDs) for the matching records.
        """
        query = "SELECT id FROM records WHERE 1=1"
        params = []

        if name_like:
            query += " AND name LIKE ?"
            params.append(f"%{name_like}%")
        if category:
            query += " AND category = ?"
            params.append(category)
        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time)
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time)

        query += " ORDER BY timestamp DESC"

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            # Fetch all rows and extract the first element (the id) from each tuple
            return [row[0] for row in cursor.fetchall()]

    def get_records_by_indices(self, indices: List[int], absolute_path: bool = False) -> List[Dict[str, Any]]:
        """
        Retrieves full record metadata for a given list of indices.

        Args:
            indices (List[int]): A list of unique record IDs to fetch.
            absolute_path (bool, optional): If True, returns the full absolute path to the content file.
                                            If False, returns the path relative to the root directory. Defaults to False.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries, where each dictionary represents a record.
                                  The order of records is not guaranteed to match the input order.
        """
        if not indices:
            return []

        # Create a string of placeholders (?, ?, ?) for the IN clause
        placeholders = ', '.join('?' for _ in indices)
        query = f"SELECT * FROM records WHERE id IN ({placeholders})"

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, indices)
            rows = cursor.fetchall()
            return [self._format_row(row, absolute_path) for row in rows]

    def get_by_index(self, index: int, absolute_path: bool = False) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single record's metadata by its unique index (ID).

        This is a convenience method for fetching a single record.

        Args:
            index (int): The unique ID of the record.
            absolute_path (bool, optional): If True, returns the full absolute path. Defaults to False.

        Returns:
            Optional[Dict[str, Any]]: A dictionary of the record's metadata or None if not found.
        """
        records = self.get_records_by_indices([index], absolute_path=absolute_path)
        return records[0] if records else None

    def get_content_by_index(self, index: int) -> Optional[Union[str, bytes]]:
        """
        Retrieves the actual file content for a record by its index.
        """
        record = self.get_by_index(index, absolute_path=True)
        if not record:
            return None

        try:
            mode = 'r' if record['content_type'] == 'text' else 'rb'
            encoding = 'utf-8' if record['content_type'] == 'text' else None
            with open(record['path'], mode, encoding=encoding) as f:
                return f.read()
        except FileNotFoundError:
            return None

    def delete_by_index(self, index: int) -> bool:
        """
        Deletes a record from the database and its associated file from the filesystem.
        """
        record = self.get_by_index(index, absolute_path=True)
        if not record:
            return False

        try:
            if os.path.exists(record['path']):
                os.remove(record['path'])
        except OSError:
            pass  # Proceed to delete the DB record regardless of file deletion success

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM records WHERE id = ?", (index,))
            conn.commit()
            return cursor.rowcount > 0
