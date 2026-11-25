# data-qd-223 · HTTP 上游健康路由

网关可以用路由器向配置好的上游发送 GET，按轮转方式选择上游，遇到连接故障或 5xx 后切换到其他上游。故障上游冷却后可再次参与；上游 4xx 原样返回。当前是进程内路由库，处理小型响应。

默认模式（`deadline_mode=False`）保持原有行为：连接超时按套接字操作计，不是端到端截止时间；`get(path)` 返回 `(status, body)`，不支持每请求预算或主动探测。

## 端到端预算模式

```python
from router import Router, DeadlineExceeded, Unavailable

router = Router(
    [("10.0.0.11", 8080), ("10.0.0.12", 8080)],  # 1–16 个不同的 IPv4 字面地址:端口
    clock=time.monotonic,                        # 注入的单调时钟，单位秒
    cooldown=5.0,                                # 失败后的冷却秒数
    timeout=0.5,                                 # 每次上游尝试的绝对时限（秒）
    deadline_mode=True,
)
try:
    status, body = router.get("/healthz", budget=0.45)  # 一次 get 的绝对预算
except DeadlineExceeded:
    ...  # 预算耗尽（可能发生在选择/连接/发送/读取/切换途中）
except Unavailable as exc:
    ...  # 预算尚在但所有候选都不可用或已尝试过
```

启用后：

- 一次 `get` 只有一份绝对预算，覆盖轮转选择、连接、发送、读完整个响应及切换上游；若未在预算内得到可交付结果，抛 `DeadlineExceeded`，不返回局部数据，也不再拨下一站。
- 每次真正获准尝试上游时，另有 `timeout` 秒的绝对尝试时限，不因收到字节而重置。两个截止中先到者生效；到达时立即停止网络尝试并关闭连接。两者相等时归请求截止（`DeadlineExceeded`），不冷却该上游。
- 非 5xx 响应必须在两个截止前完整读完才原样返回；4xx 不重试。收到可判定的 5xx 响应头即可判定失败，无需等待（可能被阻塞的）响应体。
- 每个调用沿轮转顺序至多尝试每个上游一次；冷却中或正在被占用的上游直接跳过，不排队等槽。每个上游同一时刻至多一个实际网络尝试，各上游互不阻塞。
- 槽从拨号前一直占到此次尝试不再进行网络操作且本地套接字已关闭，之后才可再次获槽。
- 5xx、传输/协议失败、以及先到的尝试截止会让该上游从失败确定时起进入 `cooldown`。请求预算先耗尽则不新增、不延长冷却。
- 已结束尝试的迟到事件（迟到的失败/清理）不能改写健康状态、释放后来尝试的槽或触发重试。
- 冷却到点且槽空闲时，该上游即重新参与轮转，无需主动探测。
- 支持同一实例最多 32 个并发调用；超出时快速返回 `Unavailable`。
- `budget`、`timeout`、`cooldown` 必须为有限正数；`budget` 缺失或非法、本地路径非法（不以 `/` 开头、以 `//` 开头、含非 ASCII 或控制字符）时，在访问上游之前抛 `ValueError`。

协议范围：仅明文 HTTP、IPv4 字面地址；响应头不超过 16 KiB、必须有有效的 `Content-Length`、响应体不超过 64 KiB。不支持 DNS、TLS、分块传输。提前断开、无效响应定界（缺失/冲突/非数字 `Content-Length`、chunked）、声明超限均按上游失败处理。

回归测试覆盖：预算超时停止慢速滴流并关闭连接且不进入冷却；并发下占用中的上游不被再次拨号；尝试超时后在预算内切换、冷却、冷却后的完整 404；受控时钟下的截止相等与迟到事件隔离；5xx 响应头阻塞时仍能切换；各类无效响应。

## 单可用区故障接管

`outage_router.OutageRouter(router, sites)` 在已按 `deadline_mode=True` 构造的
`Router` 之上按发布侧核准的容量计划发送请求。原 `router.get(path, budget=...)`
保持不变，可与新入口并存。

```python
from router import Router
from outage_router import OutageRouter, NoFeasiblePlan

router = Router([("10.0.0.11", 8080), ("10.0.0.12", 8080),
                 ("10.0.0.21", 8080), ("10.0.0.22", 8080)],
                clock=time.monotonic, cooldown=5.0, timeout=0.5,
                deadline_mode=True)
gateway = OutageRouter(router, {
    "east-1":  ("east",  ("10.0.0.11", 8080)),   # 2–16 个站点，至少两个区
    "east-2":  ("east",  ("10.0.0.12", 8080)),
    "north-1": ("north", ("10.0.0.21", 8080)),
    "west-1":  ("west",  ("10.0.0.22", 8080)),
})

revision, assignments = gateway.install(
    {                                  # 1–4096 个桶，键 0–4095
        0: ("east-1", ["north-1", "west-1"]),  # 主站 + 至多 15 个合格站
        1: ("east-2", ["north-1"]),
    },
    {"east-1": 0, "east-2": 0, "north-1": 1, "west-1": 1},  # 备用桶数
)
status, body = gateway.get(0, "/content", budget=0.45,
                           outage_zone="east")   # 0→west-1，1→north-1
```

- `install` 整体替换部署方案：结构不合法抛 `ValueError`；配置合法但某个区的
  全部主站桶无法由区外合格站按 `spare` 完整接住时抛 `NoFeasiblePlan`。失败不
  消耗版本、不切换任何桶，此前版本继续服务。版本从 1 起逐次递增，成功时返回
  `(revision, assignments)`（桶号→备用站 ID）。
- 候选列表可以包含主站自己或同区站点（结构合法），但主站所在区故障时这些站点
  不能用于接管。不同故障区是互斥场景：同一备用容量可在各区的独立分配中重复使用。
  分配由最大流精确判定，不会因顺序不当误报容量不足，也不会只比较总容量。
- 相同的逻辑配置（与映射插入顺序、候选顺序无关）得到相同的分配；返回值是副本，
  改动输入或返回值不影响已核准计划。
- `get(bucket, path, *, budget, outage_zone=None)`：无故障、或桶的主站不在声明
  的故障区时只向主站发 GET；否则只向该版本为其分配的备用站发 GET。没有计划时
  抛 `router.Unavailable`；有计划但桶未知、或故障区不在站点目录中时，在发任何
  网络请求之前抛 `ValueError`。选定站失败按单候选语义处理（不转向同区、未分配
  站或新版本），合法 404 原样返回。
- 每次 `get` 在开始时就固定使用一份完整计划；`install` 不等待在途 HTTP 请求，
  安装返回后发起的 GET 使用新版本，已开始的 GET 仍使用旧版本选定的站点。
- `install` 由单个发布者调用，可与最多 32 个并发 GET 同时进行。

## 运行

Python 3.9+，仅标准库。演示在回环地址启动两个临时真实上游，测试不访问外网。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

运行 demo 可见 `failover: (200, b'secondary')`、`cooldown: (200, b'secondary')` 及 `recovery: (200, b'primary')`。
