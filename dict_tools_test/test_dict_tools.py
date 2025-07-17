import os
import sys
import datetime
from pydantic import BaseModel

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from DictTools import *


def test_dict_printer():
    sample_data = {
        "name": "Alice",
        "age": 30,
        "active": True,
        "scores": [85.5, 92.0, 78.3],
        "metadata": {
            "created_at": datetime.datetime.now(),
            "tags": ["admin", "user"],
            "config": {
                "theme": "dark",
                "notifications": {
                    "email": True,
                    "sms": False
                }
            }
        },
        "none_field": None
    }

    print("优雅打印示例：")
    print(DictPrinter.pretty_print(
        sample_data,
        indent=2,
        sort_keys=True,
        colorize=True,
        max_depth=4
    ))


class ExampleData(BaseModel):
    id: int
    token: str
    args: list
    kwargs: dict | None = None


def test_check_sanitize_dict():
    dict1 = {
        'id': 36,
        'token': '2aa4bbfe-c1ed-43af-afcc-f11049e16fc5',
        'args': ['This', 'is', 'args'],
        'kwargs': {
            'key1': 'Value1',
            'key2': 2,
            'key3': datetime.datetime.now(),
        },
        'extra_fields': 'This is an extra fields.'
    }
    validated_data, error_text = check_sanitize_dict(dict1, ExampleData)

    assert not error_text
    assert 'extra_fields' not in validated_data

    # --------------------------------------------------------------------

    dict2 = {
        'id': 36,
        'token': '2aa4bbfe-c1ed-43af-afcc-f11049e16fc5',
        'args': ['This', 'is', 'args']
    }
    validated_data, error_text = check_sanitize_dict(dict2, ExampleData)

    assert not error_text
    assert 'kwargs' not in validated_data

    # --------------------------------------------------------------------

    dict3 = {
        'id': [36],
        'token': '2aa4bbfe-c1ed-43af-afcc-f11049e16fc5',
        'args': ['This', 'is', 'args']
    }
    validated_data, error_text = check_sanitize_dict(dict3, ExampleData)

    assert 'id' in error_text
    assert 'int_type' in error_text
    assert not validated_data

    # --------------------------------------------------------------------

    dict4 = {
        'id': 36,
        'args': ['This', 'is', 'args']
    }
    validated_data, error_text = check_sanitize_dict(dict4, ExampleData)

    assert 'token' in error_text
    assert 'missing' in error_text
    assert not validated_data


def main():
    test_dict_printer()
    test_check_sanitize_dict()


if __name__ == "__main__":
    main()