# data-qd-223 · HTTP 上游健康路由

网关可以用路由器向配置好的上游发送 GET，按轮转方式选择上游，遇到连接故障或 5xx 后切换到其他上游。故障上游冷却后可再次参与；上游 4xx 原样返回。当前是进程内路由库，处理小型响应；连接超时按套接字操作计，不是端到端截止时间。未提供主动探测或每请求重试预算。

Python 3.9+，仅标准库。演示在回环地址启动两个临时真实上游，测试不访问外网。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

运行可见 `failover: (200, b'secondary')`、`cooldown: (200, b'secondary')` 及 `recovery: (200, b'primary')`。
