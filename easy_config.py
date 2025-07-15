import os
import sys
import json
import shutil
import logging
import tempfile
import traceback
from typing import Any, Optional
from collections import OrderedDict


class EasyConfig:
    """
    A hierarchical configuration manager for JSON-based configuration files.

    This class provides methods to load, modify, and save configuration settings
    using a nested key structure (e.g., `db.host`). It supports atomic writes to
    prevent file corruption and fallback loading strategies for robustness.

    Key Features:
    - Hierarchical key access using a custom delimiter (default: `.`).
    - Atomic file writes via temporary files.
    - Fallback loading from primary/alternate files or default dictionaries.
    - Environment variable support via `get_env()`.

    Args:
        config_file (str): Primary configuration file path (default: `config.json`).
        config_file_alter (str): Alternate configuration file if primary is missing (default: `config_example.json`).
        key_splitter (str): Delimiter for nested keys (default: `.`).
        encoding (str): File encoding (default: `utf-8`).
        auto_save (bool): Automatically save changes after `set()` calls (default: `True`).
        default_config (Optional[dict]): Fallback dictionary if no config files load (default: `None`).

    Example:
        > config = EasyConfig(default_config={"db": {"host": "localhost"}})
        > config.set("db.port", 5432)
        True
        > config.get("db.host")
        'localhost'
    """

    def __init__(self,
                 config_file: str = 'config.json',
                 config_file_alter: str = 'config_example.json',
                 key_splitter: str = '.',
                 encoding: str = 'utf-8',
                 auto_save: bool = True,
                 default_config: Optional[dict] = None):

        self.config_file = config_file
        self.key_splitter = key_splitter
        self.encoding = encoding
        self.auto_save = auto_save
        self.config_data = {}

        if os.path.isfile(config_file) and self.load():
            logging.info('Load config file successful.')
        elif config_file_alter and self._load_specified_file(config_file_alter):
            logging.warning(f'Alternative config loaded. '
                            f'You should make your {config_file} by reference to {config_file_alter}.')
        elif default_config:
            self.config_data = default_config.copy()
            logging.warning('Config file load fail. Use user specified default config.')
        else:
            logging.error('No config file can be loaded.')

    def set(self, key: str, value: Any) -> bool:
        """
        Sets a nested configuration value and optionally saves changes.

        Args:
            key (str): Nested key path (e.g., `server.port`).
            value (Any): Value to assign.

        Returns:
            bool: `True` if successful; `False` on error.

        Example:
            > config.set("server.port", "8080")
            True
        """
        try:
            keys = key.split(self.key_splitter)
            d = self.config_data
            for k in keys[:-1]:
                if k not in d:
                    d[k] = {}
                elif not isinstance(d[k], dict):
                    return False
                d = d[k]
            d[keys[-1]] = value
            return self.save() if self.auto_save else True
        except Exception as e:
            logging.error(f"Config set failed: {e}")
            return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a nested configuration value.

        Args:
            key (str): Nested key path (e.g., `db.credentials.user`).
            default (Any): Fallback value if key is missing (default: `None`).

        Returns:
            Any: Value if found; otherwise `default`.

        Example:
            > config.get("db.port", 8080)
            5432
        """
        try:
            keys = key.split(self.key_splitter)
            d = self.config_data
            for k in keys[:-1]:
                if not isinstance(d, dict) or k not in d:
                    return default
                d = d[k]
            if not isinstance(d, dict) or keys[-1] not in d:
                logging.error(f"No config for '{key}' , use default: {str(default)}")
                return default
            return d[keys[-1]]
        except Exception as e:
            logging.error(f"Config get failed: {e}")
            return None

    @staticmethod
    def get_env(key: str, default: Any = None) -> str:
        """
        Retrieves an environment variable with a fallback.

        Args:
            key (str): Environment variable name.
            default (Any): Fallback value if variable is unset (default: `None`).

        Returns:
            str: Environment value or `default`.

        Example:
            > EasyConfig.get_env("API_KEY", "default_key")
            "secret123"

            > config.get("API_KEY", EasyConfig.get_env("API_KEY", "default_key"))
            "default_key"
        """
        if env_val := os.getenv(key):
            return env_val
        else:
            return default

    def clear(self) -> bool:
        """
        Resets all configuration data and saves an empty file.

        Returns:
            bool: `True` if successful; `False` on error.
        """
        self.config_data = {}
        return self.save()

    def save(self) -> bool:
        """
        Saves the current configuration to the primary file atomically.

        Uses a temporary file to avoid corruption during write operations.

        Returns:
            bool: `True` if successful; `False` on error.
        """
        try:
            with tempfile.NamedTemporaryFile("w", delete=False, encoding=self.encoding) as tmp:
                json.dump(self.config_data, tmp, ensure_ascii=False, indent=4)
                tmp_path = tmp.name
            if sys.platform == "win32":
                shutil.move(tmp_path, self.config_file)
            else:
                os.replace(tmp_path, self.config_file)
            return True
        except Exception as e:
            self.config_data = {}
            logging.warning(f"Config save failed: {e}")
            return False

    def load(self) -> bool:
        """
        Loads the primary configuration file.

        Returns:
            bool: `True` if loaded; `False` on error.
        """
        return self._load_specified_file(self.config_file)

    def _load_specified_file(self, config_file: str) -> bool:
        """
        Internal method to load a specified JSON configuration file.

        Args:
            config_file (str): Path to the configuration file.

        Returns:
            bool: `True` if loaded; `False` on error.
        """
        try:
            with open(config_file, 'r', encoding=self.encoding) as f:
                self.config_data = json.load(f)
            return True
        except Exception as e:
            self.config_data = {}
            logging.warning(f"Config load failed: {e}")
            return False

    def dump_text(self) -> str:
        def flatten_dict(d, parent_key='', sep='.'):
            items = []
            for k, v in d.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                if isinstance(v, dict):
                    items.extend(flatten_dict(v, new_key, sep=sep).items())
                else:
                    items.append((new_key, v))
            return OrderedDict(items)

        if not self.config_data:
            return ""

        flat_dict = flatten_dict(self.config_data)
        max_key_len = max(len(key) for key in flat_dict.keys()) if flat_dict else 0

        lines = []
        for key, value in flat_dict.items():
            value_str = f'"{value}"' if isinstance(value, str) else json.dumps(value)
            lines.append(f"{key.ljust(max_key_len)} : {value_str}")

        return "\n".join(lines)


# ---------------------------------------------------------------------------------------------------------------------

def test_easy_config():
    config = EasyConfig('config_test.json')
    config.clear()
    assert config.set('a.b.c', 1)
    assert config.get('a.b.c') == 1
    assert config.get('a.b') == {'c': 1}
    assert config.set('a.b', 2)
    assert config.get('a.b') == 2
    assert config.get('a.b.d', 'default') == 'default'
    assert config.set('a.e.f.g', [1, 2, 3])
    assert config.get('a.e.f.g') == [1, 2, 3]

    # Test key not exist
    assert config.get('x.y.z', 'default') == 'default'

    # Test key target is not dict
    assert config.set('a.b.c.d', 2) == False

    print("All tests passed!")


def main():
    test_easy_config()


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
