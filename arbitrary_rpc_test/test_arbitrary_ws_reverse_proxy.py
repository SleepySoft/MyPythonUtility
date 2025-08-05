import unittest
import json
import time
import threading
from functools import partial

import websocket
from unittest.mock import MagicMock, patch

from ArbitraryRPC import FlaskRPCServer, RPCProxy, RPCService, web_socket_sender, serialize, deserialize


# 测试存根实现
class TestServerStub:
    def add(self, a, b):
        return a + b

    def get_server_info(self):
        return {"name": "TestServer", "status": "running"}


class TestClientStub:
    def multiply(self, a, b):
        return a * b

    def get_client_info(self):
        return {"id": "client_123", "active": True}


# WebSocket 客户端线程
class WebSocketClientThread(threading.Thread):
    def __init__(self, url, stub, ready_event):
        super().__init__(daemon=True)
        self.url = url
        self.stub = stub
        self.ready_event = ready_event
        self.ws = None
        self.connected = False
        self.response = None

    def run(self):
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_message=self.on_message
        )
        self.ws.run_forever()

    def on_open(self, ws):
        self.connected = True
        self.ready_event.set()

    def on_message(self, ws, message):
        # 模拟客户端处理 RPC 请求
        req_data = json.loads(message)
        api = req_data["api"]
        args = deserialize(req_data["args"])
        kwargs = deserialize(req_data["kwargs"])

        func = getattr(self.stub, api, None)
        if callable(func):
            result = func(*args, **kwargs)
            self.response = serialize(result)
            ws.send(self.response)


# 测试主类
class TestWebSocketRPC(unittest.TestCase):
    PORT = 5678  # 使用独立测试端口
    WS_URL = f"ws://localhost:{PORT}/reverse-rpc"

    def setUp(self):
        # 创建同步事件
        self.client_ready = threading.Event()
        self.server_ready = threading.Event()

        # 初始化服务端
        self.server_stub = TestServerStub()
        self.server = FlaskRPCServer(
            inst_name="test_server",
            listen_ip="localhost",
            listen_port=self.PORT,
            rpc_stub=self.server_stub,
            ws_support=True
        )

        # 启动服务线程
        self.server_thread = threading.Thread(
            target=self.server.run_service_blocking,
            kwargs={"debug": False},
            daemon=True
        )
        self.server_thread.start()
        time.sleep(1)  # 等待服务器启动

        # 初始化客户端
        self.client_stub = TestClientStub()
        self.client_thread = WebSocketClientThread(
            self.WS_URL,
            self.client_stub,
            self.client_ready
        )
        self.client_thread.start()

        # 等待客户端连接
        self.assertTrue(
            self.client_ready.wait(timeout=5),
            "Client failed to connect within 5 seconds"
        )

        # 等待服务端注册客户端
        self._wait_for_client_registration()

    def tearDown(self):
        if self.client_thread.ws:
            self.client_thread.ws.close()
        self.server.stop_event.set()
        self.server_thread.join(timeout=2)

    def _wait_for_client_registration(self):
        """等待服务端成功注册客户端连接"""
        for _ in range(10):
            if self.server.ws_connections:
                self.server_ready.set()
                return
            time.sleep(0.5)
        self.fail("Server failed to register client within 5 seconds")

    def test_server_to_client_rpc(self):
        """测试服务端通过WS调用客户端方法"""
        # 获取客户端代理
        client_id = list(self.server.ws_connections.keys())[0]
        client_proxy = self.server.ws_connections[client_id].reversed_proxy

        # 远程调用客户端方法
        result = client_proxy.multiply(4, 5)
        self.assertEqual(result, 20)

        # 测试带返回值的调用
        info = client_proxy.get_client_info()
        self.assertEqual(info["id"], "client_123")
        self.assertTrue(info["active"])

    def test_client_to_server_rpc(self):
        """测试客户端通过WS调用服务端方法"""
        # 创建客户端代理
        client_proxy = RPCProxy(
            sender=partial(web_socket_sender, self.client_thread.ws),
            timeout=5
        )

        # 远程调用服务端方法
        result = client_proxy.add(100, 25)
        self.assertEqual(result, 125)

        # 测试复杂数据类型
        info = client_proxy.get_server_info()
        self.assertEqual(info["name"], "TestServer")
        self.assertEqual(info["status"], "running")

    def test_concurrent_rpc_calls(self):
        """测试双向同时RPC调用"""
        # 获取双方代理
        server_client_id = list(self.server.ws_connections.keys())[0]
        server_proxy = self.server.ws_connections[server_client_id].reversed_proxy
        client_proxy = RPCProxy(
            sender=partial(web_socket_sender, self.client_thread.ws),
            timeout=5
        )

        # 创建并发任务
        def server_task():
            return server_proxy.multiply(8, 9)

        def client_task():
            return client_proxy.add(30, 12)

        # 并行执行
        t1 = threading.Thread(target=server_task)
        t2 = threading.Thread(target=client_task)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        # 验证结果
        self.assertEqual(server_task(), 72)
        self.assertEqual(client_task(), 42)

    @patch("requests.post")
    def test_http_fallback(self, mock_post):
        """测试HTTP回退机制"""
        # 模拟失败返回空
        mock_post.return_value = None

        # 创建HTTP代理
        http_proxy = RPCProxy(
            api_url=f"http://localhost:{self.PORT}/api"
        )

        with self.assertLogs(level='ERROR') as logs:
            result = http_proxy.add(3, 4)
            self.assertIsNone(result)
            self.assertTrue("Parse RPC result fail" in logs.output[0])


# 运行测试
if __name__ == "__main__":
    unittest.main()
