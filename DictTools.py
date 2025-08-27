import sys
import json
import logging
import datetime
from typing import Any, Tuple, Dict, Optional, List
from pydantic import BaseModel, ValidationError

try:
    from colorama import init, Fore, Style
    HAS_COLORAMA = True
    init(autoreset=True)  # 自动重置颜色
except ImportError:
    HAS_COLORAMA = False


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def check_sanitize_dict(data: dict, verifier: BaseModel) -> Tuple[dict, str]:
    """
    Validates and sanitizes input dictionary using a Pydantic BaseModel schema.

    This function performs type conversion, data validation, and automatic cleanup
    via Pydantic's model_validate(). On success, returns the sanitized dict with
    excluded fields. On failure, returns structured error messages locating field-level issues.

    Key mechanisms:
    1. **Validation & Sanitization**:
       - Uses `verifier.model_validate(data)` for type coercion and validation.
       - Applies `model_dump(exclude_unset=True, exclude_none=True)` to:
         • Remove unset fields (exclude_unset)
         • Exclude None-valued fields (exclude_none)
    2. **Error Handling**:
       - Aggregates multiple validation errors into a single string.
       - Formats field paths using dot notation for nested errors (e.g., "user.address.zipcode").
       - Logs detailed errors via `logger.error`.

    Args:
        data (dict): Raw input dictionary to validate.
        verifier (BaseModel): Pydantic model defining data schema and constraints.

    Returns:
        Tuple[dict, str]:
          - On success: (sanitized_dict, empty string)
          - On failure: (empty dict, semicolon-delimited error messages)

    Raises:
        N/A (All exceptions are caught and returned as strings)

    Example usage:
    ```python
    from pydantic import BaseModel

    class UserSchema(BaseModel):
        name: str
        age: int
        email: str

    # Case 1: Valid input
    data = {"name": "Alice", "age": 30, "email": "alice@example.com"}
    result, err = check_sanitize_dict(data, UserSchema)
    print(result)  # Output: {'name': 'Alice', 'age': 30, 'email': 'alice@example.com'}

    # Case 2: Invalid input (type error)
    bad_data = {"name": "Bob", "age": "thirty", "email": "invalid"}
    result, err = check_sanitize_dict(bad_data, UserSchema)
    print(err)  # Output: "Field [age]: Input should be a valid integer...; Field [email]: value is not a valid email address..."

    # Case 3: Nested model validation
    class Address(BaseModel):
        city: str
        zipcode: int

    class Company(BaseModel):
        name: str
        address: Address

    data = {"name": "TechCo", "address": {"city": "Paris", "zipcode": "75000"}}
    result, err = check_sanitize_dict(data, Company)
    print(result)  # Output: {'name': 'TechCo', 'address': {'city': 'Paris', 'zipcode': 75000}}
    ```

    Notes:
    - Error format: `Field [path]: Error message (Type error: error_type)`
    - Unset/None fields are stripped from the output dict on success.
    - All errors are logged via `logger.error` before returning.
    """
    try:
        validated_data = verifier.model_validate(data).model_dump(exclude_unset=True, exclude_none=True)
        return validated_data, ''
    except ValidationError as e:
        error_details = []
        for error in e.errors():
            # 获取错误字段路径（如：field.sub_field）
            field_path = ".".join(map(str, error['loc']))
            error_msg = error['msg']
            error_type = error['type']
            error_details.append(f"Field [{field_path}]: {error_msg} (Type error: {error_type})")

        error_str = "; ".join(error_details)
        logger.error(f'Dict verification fail: {error_str}')
        return {}, error_str
    except Exception as e:
        logger.error(f'Dict verification got exception: {str(e)}')
        return {}, str(e)


# ----------------------------------------------------------------------------------------------------------------------

class DictPrinter:
    @classmethod
    def pretty_print(
        cls,
        data: Dict,
        indent: int = 2,
        sort_keys: bool = False,
        colorize: Optional[bool] = None,
        max_depth: int = 5,
        current_depth: int = 0
    ) -> str:
        """
        Use this function to print dictionary structure elegantly
        :param data: The dict to be printed.
        :param indent: number of spaces for indentation (default 2).
        :param sort_keys: sort_keys: whether to sort by key name (default False)
        :param colorize: colorize: whether to enable color (automatically detect terminal support).
        :param max_depth: maximum nesting depth (default 5 layers)
        :param current_depth: Parameters for recursive calls. Set to 0 when called by the user, or use the default value.
        :return: Formated dict output text.
        """
        if colorize is None:
            colorize = HAS_COLORAMA and sys.stdout.isatty()

        # 超出最大深度时返回简洁表示
        if current_depth >= max_depth:
            return f"{cls._color_wrap('...', 'dim', colorize)}"

        # 处理排序
        dict_items = data.items()
        if sort_keys:
            dict_items = sorted(dict_items)

        # 构建输出字符串
        output = []
        prefix = ' ' * indent * current_depth
        output.append(f"{prefix}{{")

        for key, value in dict_items:
            key_str = cls._format_key(key, colorize)
            value_str = cls._format_value(
                value,
                indent,
                sort_keys,
                colorize,
                max_depth,
                current_depth + 1
            )
            line = f"{prefix}  {key_str}: {value_str}"
            output.append(line)

        output.append(f"{prefix}}}")
        return '\n'.join(output)

    @classmethod
    def _color_wrap(cls, text: str, style: str, colorize: bool) -> str:
        """颜色包装"""
        if not colorize or not HAS_COLORAMA:
            return text

        styles = {
            'key': Fore.GREEN,
            'string': Fore.BLUE,
            'number': Fore.CYAN,
            'bool': Fore.MAGENTA,
            'null': Fore.YELLOW + Style.DIM,
            'type': Fore.WHITE + Style.DIM,
            'dim': Style.DIM
        }
        return f"{styles.get(style, '')}{text}{Style.RESET_ALL}"

    @classmethod
    def _format_key(cls, key: Any, colorize: bool) -> str:
        """格式化键"""
        key_str = json.dumps(str(key))  # 处理特殊字符
        return cls._color_wrap(key_str, 'key', colorize)

    @classmethod
    def _format_value(
        cls,
        value: Any,
        indent: int,
        sort_keys: bool,
        colorize: bool,
        max_depth: int,
        current_depth: int
    ) -> str:
        """格式化值"""
        if isinstance(value, dict):
            return cls.pretty_print(
                value,
                indent,
                sort_keys,
                colorize,
                max_depth,
                current_depth
            )
        elif isinstance(value, (list, tuple)):
            return cls._format_sequence(value, indent, sort_keys, colorize, max_depth, current_depth)
        else:
            return cls._format_simple_value(value, colorize)

    @classmethod
    def _format_sequence(
        cls,
        seq,
        indent: int,
        sort_keys: bool,
        colorize: bool,
        max_depth: int,
        current_depth: int
    ) -> str:
        """格式化列表/元组"""
        prefix = ' ' * indent * current_depth
        elements = []
        for i, item in enumerate(seq):
            if isinstance(item, dict):
                elem = cls.pretty_print(
                    item,
                    indent,
                    sort_keys,
                    colorize,
                    max_depth,
                    current_depth
                )
                elements.append(f"\n{prefix}  {i}: {elem}")
            else:
                elem = cls._format_simple_value(item, colorize)
                elements.append(elem)

        seq_type = 'list' if isinstance(seq, list) else 'tuple'
        return f"{cls._color_wrap(seq_type, 'type', colorize)} [{', '.join(elements)}]"

    @classmethod
    def _format_simple_value(cls, value: Any, colorize: bool) -> str:
        """格式化简单值"""
        if value is None:
            return cls._color_wrap("null", 'null', colorize)
        elif isinstance(value, bool):
            return cls._color_wrap(str(value).lower(), 'bool', colorize)
        elif isinstance(value, (int, float)):
            return cls._color_wrap(str(value), 'number', colorize)
        elif isinstance(value, str):
            quoted = json.dumps(value)
            return cls._color_wrap(quoted, 'string', colorize)
        elif isinstance(value, datetime.datetime):
            return cls._color_wrap(f"<DateTime {value.isoformat()}>", 'type', colorize)
        else:
            type_name = value.__class__.__name__
            return cls._color_wrap(f"<{type_name} object>", 'type', colorize)


def dict_list_to_markdown(data_list: List[Dict]):
    if not data_list:
        return ""

    headers = list(data_list[0].keys())
    header_row = "| " + " | ".join(headers) + " |"
    separator_row = "| " + " | ".join(["---"] * len(headers)) + " |"

    data_rows = []
    for item in data_list:
        row_values = [str(item.get(header, "")) if item.get(header) is not None else "" for header in headers]
        data_row = "| " + " | ".join(row_values) + " |"
        data_rows.append(data_row)
    markdown_table = "\n".join([header_row, separator_row] + data_rows)

    return markdown_table
