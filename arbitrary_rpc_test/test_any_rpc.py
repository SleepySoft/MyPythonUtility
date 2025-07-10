import time
import datetime
import traceback
from ArbitraryRPC import RPCProxy, RPCService, FlaskRPCServer


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


def launch_service():
    service_provider = ServiceProvider()

    rpc_service = RPCService(
        rpc_stub=service_provider,
        token_checker=service_provider.check_token,
        error_handler=service_provider.handle_error
    )

    rpc_server = FlaskRPCServer(
        listen_ip = '0.0.0.0',
        listen_port = 10800,
        rpc_service = rpc_service
    )

    rpc_server.run_service_in_thread()


def launch_client():
    rpc_proxy = RPCProxy(
        api_url = 'http://localhost:10800/api',
        timeout = 5,        # If you want to debug, make this timeout longer.
        token = SERVICE_TOKEN
    )

    print(f'=> ping()')
    result = rpc_proxy.ping()
    print('<= ' + str(result))

    print(f'=> ping()')
    result = rpc_proxy.echo("Hello from client and echo back.")
    print('<= ' + str(result))

    print(f'=> ping()')
    result = rpc_proxy.add(1, 2)
    print('<= ' + str(result))

    print(f'=> add()')
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
    launch_service()
    launch_client()


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error =>', e)
        print('Error =>', traceback.format_exc())
        exit()
    finally:
        pass