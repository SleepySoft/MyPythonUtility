"""JSON-based RPC framework for remote procedure calls over HTTP with Flask backend."""

import os
import sys
import json
import logging
import requests
import threading
import traceback
from functools import partial
from typing import Optional, Callable

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)

from JsonSerializer import serialize, deserialize


REQUEST_KEY_API = 'api'
REQUEST_KEY_TOKEN = 'token'
REQUEST_KEY_ARGS = 'args'
REQUEST_KEY_KWARGS = 'kwargs'


class RPCProxy:
    """Client-side proxy for making remote procedure calls to a JSON-RPC server.

    Args:
        api_url (str): Endpoint URL of the RPC server (default 'http://localhost:8000/api')
        timeout (int): Request timeout in seconds (0 = no timeout)
        token (str, optional): Authentication token for server requests
        header (dict, optional): Custom HTTP headers for POST requests

    Attributes:
        token (str): Authentication token
        header (dict): HTTP headers
        timeout (int): Request timeout
        api_url (str): Server endpoint URL
    """
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
        """Dynamically creates partial function for remote method call.
        """
        return partial(self.rpc_interface_proxy, attr)

    def rpc_interface_proxy(self, api: str, *args, **kwargs) -> any:
        """Executes RPC call to specified remote method.

        Args:
            api: Remote method name to call
            *args: Positional arguments (will be serialized)
            **kwargs: Keyword arguments (will be serialized)

        Returns:
            Deserialized response from server or raw text if deserialization fails
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
    """Server-side RPC request handler with authentication and error handling."""

    class DefaultRPCStub:
        """Placeholder implementation for demo/testing purposes."""

        def __getattr__(self, api: str):
            def print_rpc_call(*args, **kwargs) -> str:
                result = f'RPC Call: {api}\n  args:\n{args}\n  kwargs:\n{kwargs}'
                print( result)
                return result
            return print_rpc_call

    def __init__(self,
                 rpc_stub: object,
                 token_checker: Callable[[str], bool] = None,
                 error_handler: Callable[[str], None] = None):
        """Initializes RPC service core.

        Args:
            rpc_stub: Object containing registered RPC methods
            token_checker: Callback function for token validation
            error_handler: Callback for processing runtime exceptions
        """
        self.rpc_stub = rpc_stub or RPCService.DefaultRPCStub()
        self.token_checker = token_checker or (lambda _: True)
        self.error_handler = error_handler or (lambda error: print(error))

    def handle_flask_request(self, flask_request):
        """Handles Flask request object directly.

        Args:
            flask_request: Raw Flask request object

        Returns:
            Serialized response data
        """
        from flask import request
        flask_request: request

        req_data = flask_request.data
        req_dict = json.loads(req_data)

        return self.handle_request_dict(req_dict)

    def handle_request_dict(self, req_data: dict) -> str:
        """Processes request data in dictionary format.

        Args:
            req_data: Deserialized JSON-RPC request

        Returns:
            Serialized response data
        """
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
        """Validates basic request parameters format.

        Returns:
            True if all parameters meet type requirements
        """
        return isinstance(api, str) and api != '' and \
               isinstance(token, str) and token != '' and \
               isinstance(args_json, str) and isinstance(kwargs_json, str)

    @staticmethod
    def parse_request(args_json: str, kwargs_json: str) -> (bool, list, dict):
        """Deserializes arguments from JSON strings.

        Returns:
            Tuple: (success status, deserialized args, deserialized kwargs)
        """
        try:
            args = deserialize(args_json)
            kwargs = deserialize(kwargs_json)
            return isinstance(args, list) and isinstance(kwargs, dict), args, kwargs
        except Exception as e:
            print(str(e))
            return False, [], {}

    def dispatch_request(self, api: str, token: str, *args, **kwargs) -> any:
        """Executes requested RPC method after validation.

        Args:
            api: Method name to invoke
            token: Authentication token
            *args: Deserialized positional arguments
            **kwargs: Deserialized keyword arguments

        Returns:
            Serialized result of method execution
        """
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


class FlaskRPCServer:
    """Flask-based HTTP server for hosting JSON-RPC services."""

    def __init__(self, inst_name: str,
                 listen_ip: str,
                 listen_port: int,
                 rpc_stub: object,
                 token_checker: Callable[[str], bool] = None,
                 error_handler: Callable[[str], None] = None):
        """Configures RPC server instance.

        Args:
            listen_ip: Network interface binding address
            listen_port: TCP port to listen on
            rpc_service: Configured RPCService handler
        """
        self.inst_name = inst_name
        self.listen_ip = listen_ip
        self.listen_port = listen_port

        self.rpc_service = RPCService(
            rpc_stub=rpc_stub,
            token_checker=token_checker,
            error_handler=error_handler
        )

        self.app = None
        self.blue_print = None
        self.service_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self._init_flask()

    def _init_flask(self):
        try:
            from flask import Flask, request, Blueprint

            self.app = Flask(__name__)
            self.app.logger.setLevel(logging.ERROR)
            self.blue_print = Blueprint(f'bp_{self.inst_name}', __name__)

            @self.blue_print.route('/api', methods=['POST'])
            def webapi_entry():
                try:
                    response = self.rpc_service.handle_flask_request(request)
                except Exception as e:
                    print('/api Error', e)
                    print(traceback.format_exc())
                    response = ''
                return response

            self.app.register_blueprint(self.blue_print)
        except Exception as e:
            self.app = None
            self.blue_print = None
            print(f"Flask init fail: {str(e)}")

    # ------------------------------------------------------------------------------------------------------------------

    def run_service_blocking(self, debug: bool):
        """Starts Flask server in blocking mode.

        Args:
            debug: Enable Flask debug mode
        """
        if self.app is not None:
            print(f'Starting service: host = {self.listen_ip}, port = {self.listen_port}, debug = {debug}.')

            # https://stackoverflow.com/a/9476701/12929244
            self.app.run(
                host=self.listen_ip,
                port=self.listen_port,
                debug=debug,
                use_reloader=False          # MUST set use_reloader=False in non-main thread.
            )
        else:
            print('Error: Flask application not initialized!')

    def run_service_in_thread(self, debug: bool = False) -> threading.Thread:
        """Launches service in background thread.

        Args:
            debug: Enable Flask debug mode

        Returns:
            Reference to running thread
        """
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
        """Attempts graceful termination of background service thread.
        (Not completely implemented.)
        """
        if self.service_thread and self.service_thread.is_alive():
            print('Stopping Flask service...')
            # Flask does not provide a native stop method, we need to force a shutdown
            # But in real applications, you should use a mature WSGI server to handle graceful shutdown in production environments

            # Set the stop event (if the service thread checks it)
            self.stop_event.set()

            # Try to wait gracefully for the thread to end
            self.service_thread.join(timeout=5.0)

            if self.service_thread.is_alive():
                # If the thread is not responding, force it to terminate (not recommended, but as a fallback)
                print('Warning: Forcibly terminating service thread...')
                # Note: Avoid forced termination in real systems, as it may cause resource leaks

    def is_service_running(self) -> bool:
        """Checks service thread status.

        Returns:
            True if service thread is active
        """
        return self.service_thread is not None and self.service_thread.is_alive()

    # ------------------------------------------------------------------------------------------------------------------

    def _run_thread(self, debug: bool) -> None:
        """Internal threading target that executes blocking service."""
        try:
            # self.pre_run_setup()
            self.run_service_blocking(debug=debug)
            # self.post_run_cleanup()
        except Exception as e:
            print(f'Service thread failed: {str(e)}')
