import json
import time
import logging
import datetime
import requests
import threading
import traceback
from functools import partial
from typing import Optional, Callable
from JsonSerializer import serialize, deserialize


REQUEST_KEY_API = 'api'
REQUEST_KEY_TOKEN = 'token'
REQUEST_KEY_ARGS = 'args'
REQUEST_KEY_KWARGS = 'kwargs'

REQUEST_TOKEN = 'SleepySoft'


class RPCProxy:
    def __init__(self,
                 api_url: str = 'http://localhost:8000/api',
                 timeout: int = 0,
                 token: Optional[str] = None,
                 header: Optional[dict] = None):
        self.token = token or ''
        self.header = header or {}
        self.timeout = timeout or 0xFFFFFFFF
        self.api_url = api_url

    # ------------------------------------------------------------------------------------

    def __getattr__(self, attr):
        return partial(self.rpc_interface_proxy, attr)

    def rpc_interface_proxy(self, api: str, *args, **kwargs) -> any:
        """
        Cooperate with WebApiInterface.rest_interface_stub
        :param api: The function name of interface that you want to call
        :param args: The list args (which will be ignored in server side)
        :param kwargs: The key-value args
        :return: The response of server
        """
        payload = {
            REQUEST_KEY_API: api,
            REQUEST_KEY_TOKEN: self.token,
            REQUEST_KEY_ARGS: serialize(args),
            REQUEST_KEY_KWARGS: serialize(kwargs),
        }

        try:
            resp = requests.post(self.api_url, json=payload, headers=self.header, timeout=self.timeout)
            resp_text = resp.text
            if not resp_text:
                return None
            try:
                return deserialize(resp_text)
            except Exception as e:
                print(f'Cannot parse RPC result to json: {str(e)}')
                # print(traceback.format_exc())
                return resp_text
        except Exception as e:
            print(f'Parse RPC result fail: {str(e)}')
            # print(traceback.format_exc())
        finally:
            pass


class RPCService:
    class DefaultRPCStub:
        def __getattr__(self, api: str):
            def print_rpc_call(*args, **kwargs) -> str:
                result = f'RPC Call: {api}\n  args:\n{args}\n  kwargs:\n{kwargs}'
                print( result)
                return result
            return print_rpc_call

    def __init__(self,
                 rpc_stub: object,
                 token_checker: Callable[[str], bool],
                 error_handler: Callable[[str], None]):
        self.rpc_stub = rpc_stub or RPCService.DefaultRPCStub()
        self.token_checker = token_checker or (lambda _: True)
        self.error_handler = error_handler or (lambda error: print(error))

    def handle_flash_request(self, flask_request):
        from flask import request
        flask_request: request

        req_data = flask_request.data
        req_dict = json.loads(req_data)

        return self.handle_request_dict(req_dict)

    def handle_request_dict(self, req_data: dict) -> str:
        api = req_data.get(REQUEST_KEY_API, '')
        token = req_data.get(REQUEST_KEY_TOKEN, '')
        args_json = req_data.get(REQUEST_KEY_ARGS, '')
        kwargs_json = req_data.get(REQUEST_KEY_KWARGS, '')

        if RPCService.check_request(api, token, args_json, kwargs_json):
            success, args, kwargs = RPCService.parse_request(args_json, kwargs_json)
            resp_serialized = self.dispatch_request(api, token, *args, **kwargs)
            return resp_serialized
        return ''

    # ----------------------------------------------------------------------------

    @staticmethod
    def check_request(api: str, token: str, args_json: str, kwargs_json: str) -> bool:
        return isinstance(api, str) and api != '' and \
               isinstance(token, str) and token != '' and \
               isinstance(args_json, str) and isinstance(kwargs_json, str)

    @staticmethod
    def parse_request(args_json: str, kwargs_json: str) -> (bool, list, dict):
        try:
            args = deserialize(args_json)
            kwargs = deserialize(kwargs_json)
            return isinstance(args, list) and isinstance(kwargs, dict), args, kwargs
        except Exception as e:
            print(str(e))
            return False, [], {}

    def dispatch_request(self, api: str, token: str, *args, **kwargs) -> any:
        try:
            func = getattr(self.rpc_stub, api, None)
            if callable(func):
                resp = func(*args, **kwargs)
                resp_serialized = serialize(resp)
                return resp_serialized
            raise TypeError(f'{func} is not callable.')
        except Exception as e:
            self.error_handler(str(e))
            return f'Exception: {str(e)}'


try:
    from flask import Flask, request
    flaskApp = Flask(__name__)
    flaskApp.logger.setLevel(logging.ERROR)
except Exception as e:
    flaskApp = None
    print(str(e))


class FlaskRPCServer:
    def __init__(self, listen_ip: str, listen_port: int, rpc_service: RPCService):
        self.listen_ip = listen_ip
        self.listen_port = listen_port
        self.rpc_service = rpc_service
        self.service_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.init_service()

    def init_service(self):
        if flaskApp is not None:
            @flaskApp.route('/api', methods=['POST'])
            def webapi_entry():
                try:
                    response = self.rpc_service.handle_flash_request(request)
                except Exception as e:
                    print('/api Error', e)
                    print(traceback.format_exc())
                    response = ''
                finally:
                    pass
                return response

    # ------------------------------------------------------------------------------------------------------------------

    def run_service_blocking(self, debug: bool):
        if flaskApp is not None:
            print(f'Starting service: host = {self.listen_ip}, port = {self.listen_port}, debug = {debug}.')

            # https://stackoverflow.com/a/9476701/12929244
            flaskApp.run(
                host=self.listen_ip,
                port=self.listen_port,
                debug=debug,
                use_reloader=False          # MUST set use_reloader=False in non-main thread.
            )
        else:
            print('Error: Flask application not initialized!')

    def run_service_in_thread(self, debug: bool = False) -> threading.Thread:
        if self.service_thread and self.service_thread.is_alive():
            print('Service is already running!')
            return self.service_thread

        self.stop_event.clear()
        print(f'Starting service in background thread: port = {self.listen_port}, debug = {debug}.')

        self.service_thread = threading.Thread(
            target=self._run_thread,
            args=(debug,),
            daemon=True  # 设置为守护线程，确保主进程退出时自动终止
        )
        self.service_thread.start()
        return self.service_thread

    def stop_service(self) -> None:
        """停止运行的服务线程"""
        if self.service_thread and self.service_thread.is_alive():
            print('Stopping Flask service...')
            # Flask 没有提供原生停止方法，我们需要强制中断
            # 但在实际应用中，您应该使用成熟的WSGI服务器来处理生产环境中的优雅关闭

            # 设置停止事件（如果服务线程检查它）
            self.stop_event.set()

            # 尝试优雅等待线程结束
            self.service_thread.join(timeout=5.0)

            if self.service_thread.is_alive():
                # 如果线程没有响应，强制终止（不推荐，但作为后备方案）
                print('Warning: Forcibly terminating service thread...')
                # 注意：在真实系统中应避免强制终止，可能导致资源泄漏

    def is_service_running(self) -> bool:
        return self.service_thread is not None and self.service_thread.is_alive()

    # ------------------------------------------------------------------------------------------------------------------

    def _run_thread(self, debug: bool) -> None:
        """线程执行函数"""
        try:
            # 添加自定义逻辑（如果需要）
            # self.pre_run_setup()

            # 调用阻塞模式运行
            self.run_service_blocking(debug=debug)

            # 添加清理逻辑（如果需要）
            # self.post_run_cleanup()
        except Exception as e:
            print(f'Service thread failed: {str(e)}')


# ----------------------------------------------------------------------------------------------------------------------

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


