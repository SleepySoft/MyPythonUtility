"""JSON-based RPC framework for remote procedure calls over HTTP with Flask backend."""

import os
import sys
import json
import logging
import threading
import traceback
import uuid
from enum import Enum
from functools import partial

from pydantic import BaseModel, ValidationError
from typing import Optional, Callable, List

try:
    import requests
except Exception as e:
    requests = None

root_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(root_path)

from JsonSerializer import serialize, deserialize


RPC_KEY_ENTRY = 'entry'
RPC_KEY_TOKEN = 'token'
RPC_KEY_SESSION = 'session'
RPC_KEY_MSG_TYPE = 'msg_type'
RPC_KEY_MSG_PAYLOAD = 'msg_payload'

RPC_PAYLOAD_KEY_ARGS = 'args'
RPC_PAYLOAD_KEY_KWARGS = 'kwargs'
RPC_PAYLOAD_KEY_RAWDATA = 'rawdata'


class MsgTypeEnum(str, Enum):
    NA = ''
    ACK = 'ack'
    REQUEST = 'req'
    RESPONSE = 'rep'


class RpcPayload(BaseModel):
    args: str | None = None
    kwargs: str | None = None
    rawdata: str | None = None


class RpcMessage(BaseModel):
    entry: str
    token: str | None = ''
    session: str | None = ''
    msg_type: MsgTypeEnum = MsgTypeEnum.NA
    msg_payload: RpcPayload | None


def requests_sender(url: str, payload: dict, headers: dict, timeout: int) -> str:
    if requests:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        return resp.text if resp else ''
    raise ValueError('No request lib.')


def web_socket_sender(ws, payload: dict, headers: dict, timeout: int) -> str:
    try:
        if ws.closed:
            return ''

        json_payload = serialize(payload)
        ws.send(json_payload)

        response = ws.recv(timeout=timeout)
        return response
    except ConnectionResetError:
        return ''
    except TimeoutError as e:
        return serialize({f"WS timeout error: {str(e)}"})
    except Exception as e:
        print(f"WS send error: {str(e)}")
        return ''


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
                 sender: Callable[[dict, dict, int], str] = partial(requests_sender, 'http://localhost:8000/api'),
                 timeout: int = 0,
                 token: Optional[str] = None,
                 header: Optional[dict] = None):
        self.token = token or ''
        self.sender = sender
        self.header = header or {}
        self.timeout = timeout or 0xFFFFFFFF

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
            RPC_KEY_ENTRY: api,
            RPC_KEY_TOKEN: self.token,
            RPC_KEY_SESSION: str(uuid.uuid4()),
            RPC_KEY_MSG_TYPE: MsgTypeEnum.REQUEST,

            RPC_KEY_MSG_PAYLOAD: RpcPayload(
                args = serialize(args),
                kwargs = serialize(kwargs)
            ).model_dump()
        }

        try:
            rpc_text = self.sender(payload, self.header, self.timeout)
            rpc_data = json.loads(rpc_text)

            try:
                if not self.verify_sync_response(payload, rpc_data):
                    return ''

                resp_text = rpc_data.get(RPC_KEY_MSG_PAYLOAD, {}).get(RPC_PAYLOAD_KEY_RAWDATA, '')
                resp_data = deserialize(resp_text)

                return resp_data

            except Exception as e:
                print(f'Cannot parse RPC result to json: {str(e)}')
                # print(traceback.format_exc())
                return rpc_text

        except Exception as e:
            print(f'Parse RPC result fail: {str(e)}')
            # print(traceback.format_exc())
            return ''
        finally:
            pass

    @staticmethod
    def verify_sync_response(original_data: dict, response_data: dict) -> bool:
        try:
            validated_data = RpcMessage.model_validate(response_data).model_dump()
            if validated_data[RPC_KEY_MSG_TYPE] not in [MsgTypeEnum.NA, MsgTypeEnum.ACK, MsgTypeEnum.RESPONSE]:
                return False
            if validated_data[RPC_KEY_ENTRY] and validated_data[RPC_KEY_ENTRY] != original_data[RPC_KEY_ENTRY]:
                return False
            if validated_data[RPC_KEY_SESSION] and validated_data[RPC_KEY_SESSION] != original_data[RPC_KEY_SESSION]:
                return False
            return True
        except ValidationError:
            return False
        except Exception as e:
            print(f'RPC Response check fail: {str(e)}')
            return False


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

    def handle_ws_request(self, ws_request) -> dict:
        try:
            req_dict = json.loads(ws_request)
            response = self.handle_request_text(req_dict)
            return response
        except json.JSONDecodeError:
            return {"error": "Invalid JSON"}
        except Exception as e:
            return {"error": str(e)}

    def handle_flask_request(self, flask_request) -> dict:
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
        response = self.handle_request_text(req_dict)

        return response

    def handle_request_text(self, rpc_data: str) -> dict:
        """Processes request data in dictionary format.

        Args:
            rpc_data: RPC serialized string.

        Returns:
            Serialized response data
        """
        try:
            validated_data = RpcMessage.model_validate(rpc_data).model_dump(exclude_unset=False, exclude_none=False)

            msg_type = validated_data.get(RPC_KEY_MSG_TYPE, '')
            if msg_type not in [MsgTypeEnum.NA, MsgTypeEnum.REQUEST]:
                raise TypeError('Not a request message.')

            api = validated_data.get(RPC_KEY_ENTRY, '')
            token = validated_data.get(RPC_KEY_TOKEN, '')
            session = validated_data.get(RPC_KEY_SESSION, '')
            payload = validated_data.get(RPC_KEY_MSG_PAYLOAD, { })

            args_json = payload.get(RPC_PAYLOAD_KEY_ARGS, '')
            kwargs_json = payload.get(RPC_PAYLOAD_KEY_KWARGS, '')

            success, args, kwargs = self.parse_request(args_json, kwargs_json)

            # args = payload.get(RPC_PAYLOAD_KEY_ARGS, [])
            # kwargs = payload.get(RPC_PAYLOAD_KEY_KWARGS, {})
            result = self.dispatch_request(api, token, *args, **kwargs)

            response = RpcMessage(
                entry=api,
                token='',
                session=session,
                msg_type=MsgTypeEnum.RESPONSE,
                msg_payload=RpcPayload(
                    rawdata=result
                )
            ).model_dump()

            return response

        except ValidationError as e:
            return { 'error': str(e) }
        except Exception as e:
            return { 'error': str(e) }

    # ----------------------------------------------------------------------------

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

    class WsClient:
        def __init__(self, address: str, connection, reversed_proxy: RPCProxy):
            self.address = address
            self.connection = connection
            self.reversed_proxy = reversed_proxy

        def close(self):
            if not self.connection.closed:
                self.connection.close()
            self.reversed_proxy = None

    def __init__(self,
                 inst_name: str,
                 listen_ip: str,
                 listen_port: int,
                 rpc_stub: object,
                 ws_support: bool = False,
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
        self.ws_support = ws_support

        self.rpc_service = RPCService(
            rpc_stub=rpc_stub,
            token_checker=token_checker,
            error_handler=error_handler
        )

        self.app = None
        self.sock = None
        self.blue_print = None
        self.ws_connections = {}
        self.lock = threading.Lock()
        self.service_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        self._init_flask()

    def _init_flask(self):
        try:
            from flask import Flask

            self.app = Flask(__name__)
            self.app.logger.setLevel(logging.ERROR)

            self._init_http(self.app)

            if self.ws_support:
                self._init_websocket(self.app)
        except Exception as e:
            self.app = None
            print(f"Flask init fail: {str(e)}")

    def _init_http(self, flask_app):
        try:
            from flask import request, Blueprint

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

            flask_app.register_blueprint(self.blue_print)
        except Exception as e:
            self.blue_print = None
            print(f"Blue print init fail: {str(e)}")

    def _init_websocket(self, flask_app):
        try:
            from flask_sock import Sock

            self.sock = Sock(flask_app)

            @self.sock.route('/reverse-rpc')
            def handle_reverse_connection(ws):
                client_id = f"{ws.remote_addr}:{id(ws)}"

                try:
                    with self.lock:
                        self.ws_connections[client_id] = FlaskRPCServer.WsClient(
                            client_id,
                            ws,
                            RPCProxy(sender=partial(web_socket_sender, ws))
                        )

                    while not ws.closed:
                        try:
                            data = ws.receive(timeout=5)
                            if data is None:
                                continue
                            if isinstance(data, bytes) and data == b"PING":
                                ws.send(b"PONG")
                                continue
                            response = self.rpc_service.handle_ws_request(data)
                            ws.send(response)
                        except TimeoutError:
                            ws.ping()
                        except ConnectionError:
                            break

                except Exception as e:
                    error_txt = f"Server error: {str(e)}"
                    error_msg = serialize({"error": error_txt})

                    try:
                        ws.send(error_msg)
                    except:
                        pass

                    print(error_txt)
                finally:
                    with self.lock:
                        client = self.ws_connections.pop(client_id, None)
                        if client:
                            client.close()
        except Exception as e:
            print(f"Web socket init fail: {str(e)}")

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

    def get_ws_clients(self) -> List[str]:
        return list(self.ws_connections.keys())

    def get_ws_client_proxy(self, client_id):
        return self.ws_connections.get(client_id, None)

    # ------------------------------------------------------------------------------------------------------------------

    def _run_thread(self, debug: bool) -> None:
        """Internal threading target that executes blocking service."""
        try:
            # self.pre_run_setup()
            self.run_service_blocking(debug=debug)
            # self.post_run_cleanup()
        except Exception as e:
            print(f'Service thread failed: {str(e)}')
