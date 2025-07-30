"""

"""

import os
import sys
import time
import datetime
import traceback
from functools import partial

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root_path)

from ArbitraryRPC import RPCProxy, RPCService, FlaskRPCServer, requests_sender


class ServiceProvider:
    def __init__(self):
        self.error_list = []

    def add(self, x, y) -> any:
        return x + y

    def ping(self):
        return "pong"

    def echo(self, text: str) -> str:
        return text

    def get_each_types(self):
        return {
            "int": 42,
            "float": 3.14159,
            "str": "hello world",
            "bool": True,
            "none": None,
            "bytes": b"binary data".decode('latin1')  # 注意序列化限制
        }

    def get_nested_structure(self):
        """返回嵌套数据结构"""
        return {
            "list": [1, "two", 3.0, False],
            "tuple": (1, 2, 3),
            "dict": {
                "a": [{"b": 5}, {"c": "deep"}],
                "d": {"e": 7}
            },
            "set": {1, 2, 3, 2, 1},  # 注意：集合可能会转换为列表
            "date": datetime.datetime.now().isoformat()
        }

    def get_large_data(self):
        """返回大尺寸数据"""
        return {
            "large_string": "A" * 5000,
            "large_list": list(range(1000))
        }

    def run_error_test(self):
        raise ValueError("Intentional error for testing")

    def run_heavy_task(self):
        time.sleep(1)
        return "done"

    # ----------------------------------------------------------------

    def get_error(self) -> list:
        return self.error_list

    def clean_error(self):
        self.error_list.clear()

    # ----------------------------------------------------------------

    def check_token(self, token: str) -> bool:
        return token == SERVICE_TOKEN

    def handle_error(self, error: str):
        self.error_list.append(error)
        print(f'Handle error in ServiceProvider: {error}')


SERVICE_TOKEN = 'SleepySoft'


def build_repeater_service(
        repeater_name: str,
        listen_ip: str,
        listen_port: int,
        downstream_host: str,
        downstream_port: int):

    repeater_proxy = RPCProxy(
        sender=partial(requests_sender, f'http://{downstream_host}:{downstream_port}/api'),
        timeout = 10,        # If you want to debug, make this timeout longer.
        token = SERVICE_TOKEN
    )

    repeater_server = FlaskRPCServer(
        inst_name = repeater_name,
        listen_ip = listen_ip,
        listen_port = listen_port,

        rpc_stub=repeater_proxy,
        token_checker=None,
        error_handler=None
    )

    return repeater_server


def launch_service(listen_ip: str, listen_port: int):
    service_provider = ServiceProvider()

    rpc_server = FlaskRPCServer(
        inst_name = 'Main',
        listen_ip = listen_ip,
        listen_port = listen_port,

        rpc_stub=service_provider,
        token_checker=service_provider.check_token,
        error_handler=service_provider.handle_error
    )

    rpc_server.run_service_in_thread()


def launch_client(host: str, port: int):
    rpc_proxy = RPCProxy(
        sender=partial(requests_sender, f'http://{host}:{port}/api'),
        timeout = 10,        # If you want to debug, make this timeout longer.
        token = SERVICE_TOKEN
    )

    print(f'=> ping()')
    result = rpc_proxy.ping()
    print('<= ' + str(result))

    print(f"=> echo('Hello from client and echo back.')")
    result = rpc_proxy.echo('Hello from client and echo back.')
    print('<= ' + str(result))

    print(f'=> add(1, 2)')
    result = rpc_proxy.add(1, 2)
    print('<= ' + str(result))

    print(f"=> add('Sleepy', 'Soft')")
    result = rpc_proxy.add('Sleepy', 'Soft')
    print('<= ' + str(result))

    print(f'=> get_each_types()')
    result = rpc_proxy.get_each_types()
    print('<= ' + str(result))

    print(f'=> get_nested_structure()')
    result = rpc_proxy.get_nested_structure()
    print('<= ' + str(result))

    print(f'=> get_large_data()')
    result = rpc_proxy.get_large_data()
    print('<= ' + str(result))

    print(f'=> run_error_test()')
    result = rpc_proxy.run_error_test()
    print('<= ' + str(result))

    print(f'=> run_heavy_task()')
    result = rpc_proxy.run_heavy_task()
    print('<= ' + str(result))


def main():
    launch_service('0.0.0.0', 10800)

    repeater1 = build_repeater_service('repeater1', '0.0.0.0', 10700, 'localhost', 10800)
    repeater2 = build_repeater_service('repeater2', '0.0.0.0', 10600, 'localhost', 10700)
    repeater1.run_service_in_thread()
    repeater2.run_service_in_thread()

    launch_client('localhost', 10600)

    # client -> repeater2 -> repeater1 -> server


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error =>', e)
        print('Error =>', traceback.format_exc())
        exit()
    finally:
        pass