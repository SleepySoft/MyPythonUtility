import os
import sys
import traceback
import importlib.util
import logging
from functools import partial
from types import ModuleType
from typing import List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_REQUIRED_FUNCTIONS = ['plugin_prob', 'plugin_capacities']

"""
Plugin Management System

Exposed Interfaces Documentation

CLASSES:

1. PluginManager
   Core class for managing plugin lifecycle operations

   Methods:
   - scan_path(dir_path: str) -> List[PluginWrapper]
     Scans a directory for Python files and loads valid plugins

   - add_plugin(file_path: str) -> None
     Loads a single plugin from specified file path

   - remove_plugin(name_or_path: str) -> None
     Unloads plugin by name or file path

   - get_plugin(name_or_path: str) -> Optional[PluginWrapper]
     Retrieves plugin instance by name or path

   - list_plugins() -> List[str]
     Returns names of all loaded plugins

   - invoke_one(plugin_name: str, function: str, *args, **kwargs) -> Any
     Executes specified function from a single plugin

   - invoke_all(function: str, *args, **kwargs) -> List[Any]
     Executes specified function from all loaded plugins

   Static Methods:
   - plugin_name(plugin_path: str) -> str
     Extracts plugin name from file path

   - safe_invoke(plugin_wrapper: PluginWrapper, function: str, *args, **kwargs) -> Any
     Safe execution wrapper with error handling

2. PluginWrapper
   Plugin instance container providing execution interface

   Methods:
   - invoke(_function: str, *args, **kwargs) -> Any
     Directly executes specified plugin function

   - has_function(function: str) -> bool
     Checks if plugin contains callable function

   - get_attribute(attribute: str) -> Any
     Retrieves attribute value from plugin module

   Magic Methods:
   - __getattr__(attr) -> partial
     Enables method-style calling: plugin.function_name(args)

PARAMETER SPECIFICATIONS:

- All path parameters accept both absolute and relative paths
- Function parameters support both positional and keyword arguments
- Return values preserve original plugin function return types
- Failed executions return None with logged errors

PLUGIN REQUIREMENTS:
- Must implement all functions specified in required_functions
- Should contain at least one callable function
- Must be valid Python module (.py file)
- Should not use reserved names (starting with '_' or '.')

ERROR HANDLING:
- Invalid plugins are skipped during loading
- Execution errors are logged with stack traces
- Missing functions/attributes return None silently
- Path errors raise standard OS exceptions
"""


class PluginWrapper:
    def __init__(self, plugin_manager, plugin_name: str, module_path: str, module_inst: ModuleType):
        self.plugin_manager = plugin_manager
        self.plugin_name = plugin_name
        self.module_path = module_path
        self.module_inst = module_inst
        self.user_data = {}

    def __getattr__(self, attr):
        return partial(self.invoke, attr)

    def invoke(self, function_name: str, *args, **kwargs) -> any:
        try:
            func = getattr(self.module_inst, function_name)
        except AttributeError:
            logger.warning(f"Function {function_name} not found")
            return None
        if not callable(func):
            logger.warning(f"Attribute {function_name} is not callable")
            return None
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Runtime error in {self.plugin_name}.{function_name}: {e}", exc_info=True)
            return None

    def has_function(self, function: str) -> bool:
        try:
            attr = getattr(self.module_inst, function)
            return callable(attr)
        except Exception:
            return False

    def get_attribute(self, attribute: str) -> any:
        try:
            return getattr(self.module_inst, attribute)
        except AttributeError:
            return None
        except Exception as e:
            logger.error(f"Error accessing {attribute}: {e}")
            return None


class PluginManager:
    def __init__(self, required_functions: Optional[List[str]] = None):
        self.required_functions = DEFAULT_REQUIRED_FUNCTIONS \
            if required_functions is None else required_functions
        self.plugins = {}

    def scan_path(self, dir_path: str) -> List[PluginWrapper]:
        plugin_list = []
        for py_file in self.list_py_files(dir_path):
            plugin = self.__add_plugin(py_file)
            if plugin:
                plugin_list.append(plugin)
        return plugin_list

    def add_plugin(self, file_path: str) -> PluginWrapper:
        return self.__add_plugin(file_path)

    def remove_plugin(self, name_or_path: str) -> None:
        self.__remove_plugin(name_or_path)

    def get_plugin(self, name_or_path: str) -> Optional[PluginWrapper]:
        return self.__get_plugin(name_or_path)

    def list_plugins(self) -> List[str]:
        return [plugin.plugin_name for plugin in self.plugins.values()]

    def invoke_one(self, plugin_name: str, function: str, *args, **kwargs) -> any:
        plugin_wrapper = self.__get_plugin_by_name(plugin_name)
        return self.safe_invoke(plugin_wrapper, function, *args, **kwargs)

    def invoke_all(self, function: str, *args, **kwargs) -> List[any]:
        return [self.safe_invoke(plugin, function, *args, **kwargs) for plugin in self.plugins.values()]

    @staticmethod
    def list_py_files(dir_path: str) -> List[str]:
        if not os.path.isdir(dir_path):
            return []
        return [
            os.path.join(dir_path, f)
            for f in os.listdir(dir_path)
            if f.endswith('.py') and not f.startswith(('_', '.'))
        ]

    @staticmethod
    def plugin_name(plugin_path: str) -> str:
        return os.path.splitext(os.path.basename(plugin_path))[0]

    # @staticmethod
    # def plugin_name(plugin_path: str) -> str:
    #     dir_name = os.path.basename(os.path.dirname(plugin_path))
    #     base_name = os.path.splitext(os.path.basename(plugin_path))[0]
    #     return f"{dir_name}_{base_name}"

    @staticmethod
    def safe_invoke(plugin_wrapper: Optional[PluginWrapper], function: str, *args, **kwargs) -> any:
        if plugin_wrapper is None:
            return None
        try:
            return plugin_wrapper.invoke(function, *args, **kwargs)
        except Exception as e:
            logger.error(f"Error invoking {function}: {e}", exc_info=True)
            return None

    # ----------------------------------- Private methods -----------------------------------

    def __add_plugin(self, file_path: str) -> PluginWrapper:
        abs_path = os.path.abspath(file_path)
        if abs_path in self.plugins:
            return self.plugins[abs_path]
        plugin = self.__load_plugin_file(abs_path)
        if plugin:
            self.plugins[abs_path] = plugin
        return plugin

    def __remove_plugin(self, name_or_path: str) -> None:
        abs_path = os.path.abspath(name_or_path)
        to_remove = None
        for k, v in self.plugins.items():
            if k == abs_path or v.plugin_name == name_or_path:
                to_remove = k
        if to_remove is not None:
            plugin = self.plugins.pop(to_remove)
            module_name = plugin.module_inst.__name__
            if module_name in sys.modules:
                del sys.modules[module_name]

    def __get_plugin(self, name_or_path: str) -> Optional[PluginWrapper]:
        abs_path = os.path.abspath(name_or_path)
        if abs_path in self.plugins:
            return self.plugins[abs_path]
        for plugin in self.plugins.values():
            if plugin.plugin_name == name_or_path:
                return plugin
        return None

    def __load_plugin_file(self, file_path: str) -> Optional[PluginWrapper]:
        plugin_name = self.plugin_name(file_path)
        try:
            # Generate unique module name to avoid conflicts
            module_name = f"plugin_{os.path.abspath(file_path).replace(os.sep, '_').replace('.', '_')}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if spec is None:
                logger.error(f"Failed to load spec for {file_path}")
                return None
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module

            try:
                spec.loader.exec_module(module)
            except SyntaxError as e:
                logger.error(f"Syntax error in {file_path}: {e}")
                raise
            except ImportError as e:
                logger.error(f"Import error in {file_path}: {e}")
                raise

            if not self.__check_required_functions(module):
                logger.warning(f"Plugin {plugin_name} lacks required functions")
                return None
            return PluginWrapper(self, plugin_name, file_path, module)
        except Exception as e:
            logger.warning(f"Error loading plugin {file_path}: {e}")
            return None

    def __check_required_functions(self, module) -> bool:
        return all(
            hasattr(module, pf) and callable(getattr(module, pf))
            for pf in self.required_functions
        )

    def __get_plugin_by_name(self, plugin_name: str) -> Optional[PluginWrapper]:
        for plugin in self.plugins.values():
            if plugin.plugin_name == plugin_name:
                return plugin
        return None


# ---------------------------------------------------------------------------------------------------------------------

def main():
    pm1 = PluginManager()
    pm1.scan_path('plugin_manager_test')

    print(pm1.plugins)

    assert pm1.invoke_one('plugin_with_prob', 'foo') is None
    assert pm1.invoke_one('plugin_without_prob', 'foo') is None

    assert pm1.invoke_one('plugin_with_prob', 'bar') == 'bar'
    assert pm1.invoke_one('plugin_without_prob', 'bar') is None

    print(pm1.invoke_all('foo'))
    print(pm1.invoke_all('bar'))

    print('-----------------------------------------------------------------------')

    pm2 = PluginManager([])
    pm2.scan_path('plugin_manager_test')

    print(pm2.plugins)

    assert pm2.invoke_one('plugin_with_prob', 'foo') is None
    assert pm2.invoke_one('plugin_without_prob', 'foo') == 'foo'

    assert pm2.invoke_one('plugin_with_prob', 'bar') == 'bar'
    assert pm2.invoke_one('plugin_without_prob', 'bar') is None

    print(pm2.invoke_all('foo'))
    print(pm2.invoke_all('bar'))

    print('Test passed.')


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
