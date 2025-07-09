import time
import random
import threading
import numpy as np
from datetime import datetime
from AnyRPC import RPCProxy, RPCService, FlaskRPCService


# ====================== 服务端实现 ======================
class TestService:
    """测试用的服务端业务逻辑"""

    def ping(self):
        """基本连通性测试"""
        return "pong"

    def get_types(self):
        """返回各种基础数据类型"""
        return {
            "int": 42,
            "float": 3.14159,
            "str": "hello world",
            "bool": True,
            "none": None,
            "bytes": b"binary data".decode('latin1')  # 注意序列化限制
        }

    def nested_structure(self):
        """返回嵌套数据结构"""
        return {
            "list": [1, "two", 3.0, False],
            "tuple": (1, 2, 3),
            "dict": {
                "a": [{"b": 5}, {"c": "deep"}],
                "d": {"e": 7}
            },
            "set": {1, 2, 3, 2, 1},  # 注意：集合可能会转换为列表
            "date": datetime.now().isoformat()  # 时间对象转换为字符串
        }

    def large_data(self):
        """返回大尺寸数据"""
        return {
            "large_string": "A" * 5000,
            "large_list": list(range(1000))
        }

    def process_data(self, *args, **kwargs):
        """处理传入的各种数据类型"""
        return {
            "args": args,
            "kwargs": kwargs
        }

    def error_test(self):
        """抛出异常测试"""
        raise ValueError("Intentional error for testing")

    def heavy_task(self):
        """模拟耗时操作"""
        time.sleep(0.1)
        return "done"


# 创建RPC服务实例
def token_validator(token):
    """简单的token验证器"""
    return token == "TEST_TOKEN"


def error_handler(err_msg):
    """自定义错误处理器"""
    print(f"服务端错误: {err_msg}")


test_service = TestService()
rpc_service = RPCService(
    rpc_stub=test_service,
    token_checker=token_validator,
    error_handler=error_handler
)

# 启动服务端（后台线程）
flask_service = FlaskRPCService(
    listen_ip='127.0.0.1',
    listen_port=9000,
    rpc_service=rpc_service
)
server_thread = flask_service.run_service_in_thread(debug=False)

# 等待服务器启动
time.sleep(1)

# ====================== 客户端测试程序 ======================
proxy = RPCProxy(
    api_url='http://127.0.0.1:9000/api',
    token="TEST_TOKEN",
    timeout=5  # 5秒超时
)

# 数据类型测试用例
TEST_CASES = [
    # (名称, 调用方式, 验证函数)
    ("基础类型", lambda: proxy.get_types(),
     lambda r: all(key in r for key in ["int", "float", "str", "bool", "none"])),

    ("嵌套结构", lambda: proxy.nested_structure(),
     lambda r: "deep" in r["dict"]["a"][1]["c"]),

    ("大尺寸数据", lambda: proxy.large_data(),
     lambda r: len(r["large_list"]) == 1000 and len(r["large_string"]) == 5000),

    ("参数传递-位置参数", lambda: proxy.process_data(1, 2.0, "three", False),
     lambda r: r["args"] == [1, 2.0, "three", False]),

    ("参数传递-关键字参数", lambda: proxy.process_data(a=1, b="test", c=None),
     lambda r: r["kwargs"] == {"a": 1, "b": "test", "c": None}),

    ("参数传递-混合参数",
     lambda: proxy.process_data(1, "two", [3, 4], {"a": 5}, k1="v1", k2=9.99),
     lambda r: (
             r["args"] == [1, "two", [3, 4], {"a": 5}] and
             r["kwargs"] == {"k1": "v1", "k2": 9.99}
     )),

    ("异常处理", lambda: proxy.error_test(),
     lambda r: isinstance(r, dict) and "error" in str(r)),

    ("空返回", lambda: proxy.process_data(),
     lambda r: r["args"] == [] and r["kwargs"] == {}),

    ("性能测试", proxy.heavy_task,
     lambda r: r == "done"),
]


# 性能统计结构
class Statistics:
    def __init__(self):
        self.total_count = 0
        self.success_count = 0
        self.error_count = 0
        self.latencies = []

    def add_result(self, success, latency):
        self.total_count += 1
        if success:
            self.success_count += 1
        else:
            self.error_count += 1
        self.latencies.append(latency)

    def summary(self):
        if not self.latencies:
            return "No data collected"

        sorted_lat = sorted(self.latencies)
        return {
            "total": self.total_count,
            "success_rate": f"{self.success_count / self.total_count * 100:.1f}%",
            "min_latency": f"{min(self.latencies) * 1000:.1f}ms",
            "max_latency": f"{max(self.latencies) * 1000:.1f}ms",
            "p50": f"{np.percentile(self.latencies, 50) * 1000:.1f}ms",
            "p95": f"{np.percentile(self.latencies, 95) * 1000:.1f}ms",
            "p99": f"{np.percentile(self.latencies, 99) * 1000:.1f}ms",
            "avg_latency": f"{sum(self.latencies) / len(self.latencies) * 1000:.1f}ms"
        }


# 全局统计信息
overall_stats = Statistics()
case_stats = {name: Statistics() for name, _, _ in TEST_CASES}
case_stats["性能测试"] = Statistics()  # 单独性能统计


def run_test_case(name, func, validator):
    """执行单个测试用例并记录结果"""
    global case_stats

    try:
        start_time = time.perf_counter()
        result = func()
        latency = time.perf_counter() - start_time

        success = validator(result)
        case_stats[name].add_result(success, latency)
        overall_stats.add_result(success, latency)

        return success, result, latency
    except Exception as e:
        print(f"{name} 测试失败: {str(e)}")
        case_stats[name].add_result(False, 0)
        overall_stats.add_result(False, 0)
        return False, None, 0


# ====================== 主测试循环 ======================
try:
    print("=" * 50)
    print("开始RPC框架综合测试")
    print("=" * 50)
    print("正在运行数据类型兼容性测试...")

    # 首先运行一次所有测试用例展示结果
    for name, func, validator in TEST_CASES:
        success, result, latency = run_test_case(name, func, validator)
        status = "成功" if success else "失败"
        print(f"- [{status}] {name}: 耗时{latency * 1000:.1f}ms")
        if not success and result:
            print(f"  错误响应: {str(result)[:100]}...")

    print("\n" + "=" * 50)
    print("开始压力测试循环（1000次混合调用）...")

    # 压力测试循环
    total_requests = 1000
    for i in range(total_requests):
        # 随机选择一个测试用例
        name, func, validator = random.choice(TEST_CASES)

        # 特殊处理性能测试用例
        if name == "性能测试":
            _, _, latency = run_test_case(name, func, validator)
            case_stats["性能测试"].add_result(True, latency)
        else:
            run_test_case(name, func, validator)

        # 进度显示
        if (i + 1) % 100 == 0:
            print(f"已完成 {i + 1}/{total_requests} 次请求")

    # 打印最终统计结果
    print("\n" + "=" * 50)
    print("RPC框架测试结果摘要")
    print("=" * 50)

    # 按类别统计
    print("\n[按测试类别统计]:")
    for name, stats in case_stats.items():
        if stats.total_count > 0:
            summary = stats.summary()
            print(f"- {name}:")
            print(f"  总请求: {summary['total']}, 成功率: {summary['success_rate']}")
            if stats.latencies:
                print(
                    f"  延迟: {summary['avg_latency']} (min: {summary['min_latency']}, max: {summary['max_latency']})")

    # 总体统计
    print("\n[整体性能统计]:")
    overall_summary = overall_stats.summary()
    for k, v in overall_summary.items():
        if k == "total":
            print(f"- {k}: {v}")
        else:
            print(f"- {k}: {v}")

    print("\n测试完成!")

finally:
    # 清理服务
    flask_service.stop_service()
    server_thread.join(timeout=2)
