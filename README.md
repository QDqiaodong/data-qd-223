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

## 可共享正文缓存与同批回源

`OutageRouter.get_cached(bucket, path, *, budget, language="zh", outage_zone=None)`
返回与旧入口相同的 `(status, body)`，但会在实例内缓存符合本服务约定的公开
200，并合并同一身份的并发回源：

```python
status, body = gateway.get_cached(
    0, "/public/hot?lang=zh",
    budget=0.45,
    language="zh",          # 仅 "zh" 或 "en"，发送对应 Accept-Language
    outage_zone="east",
)
```

- 缓存与在途请求的完整身份是
  `(bucket, 实际选定的 endpoint, 原样 path（含查询串）, language)`。
  换站、换语言、换桶或改路径都不会借用另一身份的正文、ETag 或在途结果；
  只改计划版本但实际目标 endpoint 不变时仍属于同一身份。
- `path` 沿用原入口的本地 ASCII 路径校验，另限至多 2048 个字符且不改写查询串；
  `budget` 必须是有限正数。无计划仍抛 `Unavailable`，非法桶、故障区、路径、
  语言或预算抛 `ValueError`，且不会发包。
- 实例内最多驻留 128 个条目（LRU 替换），不做磁盘或跨进程共享。新鲜命中不回源、
  不占 endpoint 槽；新鲜期使用注入给 `Router` 的同一单调时钟，从完整响应确认时
  起算，`now >= expires_at` 即视为过期。
- 只有同时满足以下条件的 200 才可共享：正文完整且不超过 64 KiB；恰好一条
  `Cache-Control: max-age=1..3600` 且 `max-age` 是唯一指令；一条带双引号的强
  ETag（标签字符为 `A-Za-z0-9._-`，长度 1–64）；无 `Set-Cookie`；`Vary` 缺失或
  仅含 `Accept-Language`。字段名/指令名大小写不敏感。
- 过期条目存在时发送 `If-None-Match: <原 ETag>`。合法 304 必须带同一强标签、
  有效 `max-age` 以及相同的 `Vary`/`Set-Cookie` 约束；304 无正文，可无
  `Content-Length`，或只有一条等于原正文长度的 `Content-Length`（网关不会等待
  该正文）。成功 304 续用原正文并更新新鲜期。冷请求收到 304、标签不符或约定
  不满足均按单站协议失败处理。
- `no-store` 等不可共享但可交付的 200，以及 404 等其他最终非 5xx，只返回给实际
  完成该次回源的调用方，删除该身份旧条目，不缓存也不广播；同批其他仍有预算的
  调用会重新竞争一次接续回源。
- 同一身份同一时刻只有一次真实 HTTP 交换；跟随者用各自剩余预算等待。已确认的
  上游失败可通知同批未过期调用方；单个等待者超时不影响在途请求。若发起者预算
  先到期，Router 会按原有“请求截止优先、不记冷却”的规则关闭该次尝试；其他调用
  可在旧 endpoint token/套接字清理后接续。所有回源仍经过 `Router` 的真实 HTTP
  传输、尝试时限、每 endpoint 单尝试、32 调用上限和冷却规则；未另起后台刷新任务。

## 调用归属观测

`OutageRouter(router, sites, observe=sink)` 可附加一个被动观测回调；不传或传
`None` 时完全保留原入口行为。回调由执行该次前台调用的线程同步调用，因此接收器
需要自己保证线程安全。每次 `get` 或 `get_cached` 的有效调用恰好得到一条独立
字典，且只有以下五个键：

```python
{
    "revision": 2,          # 安排这次调用的已核准版本
    "site": "east-1",       # 与 revision 来自同一份不可变计划
    "completed_ms": 12345,  # floor(router.clock() * 1000)
    "latency_ms": 42,       # 选站后到结果确定；向下取整，上限 1000
    "outcome": "delivered", # delivered | deadline | unavailable
}
```

- 任何合法 HTTP 响应（包括 404）都是 `delivered`；`DeadlineExceeded` 是
  `deadline`；使用了计划但抛出 `Unavailable` 是 `unavailable`。
- 无计划，或桶、故障区、语言、路径、预算等输入校验拒绝时，不产生记录。
- 新鲜缓存命中也产生一条；多个前台调用共用一次回源时，仍按各自的最终结果各
  产生一条。记录不包含路径、请求头或正文。
- 版本和站点在调用开始时随本地计划快照固定；之后安装新版本不会改写在途调用
  的归属。
- 时间戳使用注入 `Router` 的同一个单调时钟。回调在结果确定、HTTP/socket/
  endpoint 槽和 `OutageRouter` 调用槽均释放后执行。
- 回调抛出普通 `Exception` 会被被动层吞掉，不能改变调用的返回值或原异常，也
  不会触发重试。回调内若需长期工作，应自行快速入队，避免占用前台调用线程。

## 事故重算：区分服务退化与采集断档

`gateway_watch.evaluate(observations, *, origin_ms, duration_s, scopes, coverage)`
可直接导入，仅使用标准库。它不会发网络通知、不会改转发决策，只返回
`(windows, actions)` 两个列表。

`origin_ms` 是本次单调时间轴上的绝对毫秒起点；`scopes`、`coverage`、窗口中的
`end_ms` 和动作中的 `end_ms` 均使用相对该起点的毫秒坐标。例如
`completed_ms=origin_ms+60500` 对应相对时刻 `60500`。

```python
from gateway_watch import evaluate

windows, actions = evaluate(
    observations,  # 一次性可迭代的归属字典；最多 2,000,000 条
    origin_ms=1_732_000_000_000,
    duration_s=600,                 # 60..3600，且为 10 的倍数
    scopes={
        (1, "east-1"): (0, 600_000),  # 端点为 10000 的倍数，长度 >=60000
    },
    coverage={
        (1, "east-1"): [(0, 240_000), (240_000, 600_000)],  # 相接即完整
    },
)
```

窗口元组为：

```python
(revision, site, end_ms, count, failures, p95_ms, complete, verdict)
```

- 窗口是 `[end_ms-60000, end_ms)`，尾点从值班区间起点后的 `60000` 开始，每
  `10000` 毫秒一个，直到值班区间右端点（含该尾点；右端样本不纳入）。
- `count` 计入三种结果；`failures` 是 `deadline + unavailable`。
- `p95_ms` 只统计 `delivered`，使用整数延迟上的精确最近秩
  `ceil(0.95 * n)`；没有交付样本时为 `None`。
- `coverage` 只能由采集方提供，不能从样本量推断。相接区间视为连续覆盖；只差
  一毫秒也会使窗口 `complete=False`，判定为 `gap`。
- 判定优先级是：不完整 → `gap`；完整但 `count < 20` → `low`；否则
  `failures * 20 >= count`（失败率 ≥5%）或 `p95_ms >= 200` → `bad`；其余为
  `good`。
- 动作元组为 `(revision, site, end_ms, kind)`。连续两个 `bad` 发一次
  `open_incident`，事故中连续两个 `good` 才发一次 `resolve_incident`；`low`
  和 `gap` 都打断连续性，且 `gap/low` 不解除已开事故。第一个 `gap` 窗立即发
  `open_gap`，之后第一个非 `gap` 窗发 `resolve_gap`（即使该窗是 `low`）。区间
  结束时仍未结的事故或采集缺口不补发解除动作。
- 窗口和动作均按 `(end_ms, revision, site)` 排序。聚合不依赖记录到达顺序。

实现不保存全部原始记录：延迟只有 0–1000 的整数，因此使用紧凑的 32 位直方图和
计数数组；最大 24 条序列、每条最长 3600 秒时，直方图约占 34 MB（约 33 MiB），
可在 96 MiB `RLIMIT_AS` 子进程中流式处理 200 万条不可二次遍历的乱序生成记录。
当前不需要临时文件；若调用方自行使用磁盘，应在成功或拒绝后清理自建文件。

## 运行

Python 3.9+，仅标准库。演示在回环地址启动两个临时真实上游，测试不访问外网。
`test_cache_gateway.py` 使用可控时钟和事件门闩覆盖缓存命中、条件 304、同批合并、
私有 `no-store`、失败清理、128 条上限和截止时间接续。
`test_observation_hook.py` 用真实回环、门闩和线程接收器核对跨版本迟完成归属、
缓存样本、截止/不可用和回调异常；`test_gateway_watch.py` 覆盖手算窗口与值班状态，
`test_gateway_watch_memory.py` 在 96 MiB 地址空间子进程中处理 200 万条记录。

```sh
python3 -B -m unittest -v
python3 -B demo.py
```

运行 demo 可见 `failover: (200, b'secondary')`、`cooldown: (200, b'secondary')` 及 `recovery: (200, b'primary')`。
