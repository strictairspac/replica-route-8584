# 站点读流量路由演练

栖原站点运维组（11人）在交接班前，用节点位点与会话时序复核读写路由，明确何时读副本、何时回主库、何时必须拒绝读取。

## 场景与模块

原先值班员在配置表和聊天记录中核对副本延迟，以“延迟很低”推断写后读安全。痛点：低延迟不等于已应用会话写位点、故障时回退策略不清、决策无逐请求证据。上游输入为人工确认的节点状态和请求轨迹，本站承接路由规则演练与证据留档，下游为站点值班与变更复核人员。

三个核心实体：复制节点 Node（主副角色/在线状态/日志谱系/位点/延迟），会话请求集 RequestSet（带时序的读写、复制应用、故障恢复事件），路由试验 Trial（冻结输入、策略参数与结果）。4业务页+登录页，另有管理员审计。

**这是受控复制路由模型，不连接真实主从集群。** MySQL真实保存节点、请求、任务及证据；Python真实逐事件计算策略。日志位点采用命名谱系中的单调整数，未实现完整GTID集合/真实复制协议。

## 快速启动

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```

前端 http://localhost:18247 ，后端 http://localhost:19247/docs ，MySQL localhost:17747。

|角色|账号|密码|权限|
|---|---|---|---|
|admin|admin@qiyuan.test|Demo2026!|全部业务写入、运行与审计|
|operator|operator@qiyuan.test|Demo2026!|节点/请求编辑、运行、导出|
|viewer|viewer@qiyuan.test|Demo2026!|只读和导出，写API403|

## 一天的工作剧本

09:00 operator登录；09:05查看harbor-primary位点105、north-replica位点104/150ms、south-replica位点105/20ms；09:10打开 `QY-1117-A` 请求集；09:15确认八事件时序；09:20以100ms延迟门槛和500ms粘滞执行 `shift-20261117`；09:25检查：写入主库位点106、100ms读仍走主库、600ms南站追平106、700ms读走南站、800ms南站故障、900ms回主库、1000ms主库故障、1100ms拒绝读取；09:30导出route-1.json；09:40 viewer核验拒绝理由；10:00 admin看审计。

另有 `QY-1117-B` 高延迟副本回退主库、`QY-1117-C` 主库故障且副本未追平的拒绝案例。所有试验使用冻结快照，不会把模型故障写回节点台账。

## 真实计算规则

输入上限12节点、100事件；时间0–86400000ms，事件按at_ms非递减，时间相同按输入顺序执行。必须有且仅一个主节点，同谱系副本初始位点不能超过主库。事件字段：

```json
[
  {"at_ms":0,"kind":"write","session":"lab-A"},
  {"at_ms":100,"kind":"read","session":"lab-A"},
  {"at_ms":600,"kind":"replicate","node":"south-replica","position":106,"delay_ms":20},
  {"at_ms":700,"kind":"read","session":"lab-A"}
]
```

- write：主库在线才成功；主库位点+1，记录该会话required_position和last_write_ms。失败写入不推进会话位点。
- read：若距最近成功写入 `< sticky_ms`，必须主库在线、同谱系且位点满足；否则拒绝。到达边界即结束粘滞。
- 粘滞期外：副本必须在线、同谱系、position≥会话required_position、delay_ms≤max_delay_ms，四条件同时成立。多个候选按延迟升序、位点降序、名称字典序选择。无副本达标则回主库；主库也失效则拒绝。
- replicate：只允许在线同谱系副本；位点不能回退、不能超过主库已提交位点。node_down/node_up修改模型状态。未知节点或非法复制在入队前422。
- 每个会话独立维护位点；从未成功写入的会话required=0，允许最终一致副本读取，不承诺全局强一致。低延迟绝不覆盖落后位点。

输出每事件目标/拒绝、会话所需位点、实际位点、各副本四项资格、最终节点、会话状态、汇总和SHA256。相同输入产生相同摘要；不依赖随机数或外部网络。拒绝不安全读是正常业务结果，不算任务失败。快照与结果保存在MySQL，Redis只传递任务ID。

## 订阅投递演练（主库故障下的键值变更供数）

独立于读路由的第二套演练，回答「主库故障时订阅方还能收到哪些键值变更」。复用同一套节点台账（主/副、谱系、位点、在线状态），新增订阅场景与订阅演练两类实体，原读路由模块与路由规则不受影响。

订阅事件类型（与读路由事件分开维护）：

```json
[
  {"at_ms":0,"kind":"write","key":"signal:1","value":"OPEN"},
  {"at_ms":20,"kind":"subscribe","subscriber":"watchtower"},
  {"at_ms":80,"kind":"node_down","node":"harbor-primary"},
  {"at_ms":300,"kind":"replicate","node":"south-replica","position":107,"delay_ms":20},
  {"at_ms":500,"kind":"ack","subscriber":"watchtower","seq":1,"lost":true}
]
```

- write：主库在线才成功并生成单调整数变更序号 seq；主库下线时写入失败，不占序号、不供数。
- 供数选择：主库在线由主库供数；主库下线后，在线、同谱系、已应用位点覆盖该变更的副本接力供数（按延迟/位点/名称排序）；没有任何节点持有该位点则订阅方进入「缺位点等待」，直到副本 replicate 追平后再投递。
- subscribe 建立订阅，可用 from_position_seq 指定起点；sub_down/sub_up 模拟订阅连接中断与恢复。
- ack 回执：`lost:true` 表示回执在网络中丢失；`out_of_order:true` 表示乱序回执。订阅进度只按连续回执推进，乱序或丢失的回执不能越过未确认变更，缺口补齐后暂存回执连续补位。
- 中断恢复后从未确认位点重试，订阅方按 seq 幂等去重，重复投递不会重复入账（applied 只记一次）。
- 丢回执策略二选一，演示「重复收件 vs 追平速度」的取舍：
  - `resend`：超时即重发，追平快，但订阅方会收到重复件（靠 seq 幂等去重，重复件触发自动补回执）。
  - `reconcile`：超时先发起查清（probe）往返，确认订阅方已收位点后只补回执、零重复收件；未持有才补投，代价是多一次往返、追平更慢。
- 在途窗口 inflight_window 限制未确认变更数量；自动重发有上限（默认8次），超限保留未确认状态，保证确定性重放收敛。

输出含已提交变更清单、各订阅方 acked/delivered/追平耗时/重复收件/暂存乱序回执、写入/复制/故障/投递/回执五条泳道的完整时序、汇总与SHA256。引擎为纯标准库实现，可用 `python3 -I tests/test_subscription.py` 离线运行17条单元测试。

## 工程说明

|服务|技术|职责|
|---|---|---|
|frontend|原生HTML/CSS/JS + Nginx|读路由4页+订阅3页、编辑表单、共享组件、时序泳道、/api代理|
|backend|Python3.11/FastAPI/SQLAlchemy/PyJWT|校验、权限、审计、快照与导出|
|worker|Python3.11|读路由算法与订阅投递算法，串行消费两个Redis队列|
|db|MySQL8.0|真实持久化业务和JSON证据（含sub_scenarios/sub_trials）|
|redis|Redis7|持久队列（replicaroute:queue 与 subdelivery:queue）|

各实体贯穿 `backend/src/{models,schemas,services,routes}` 和 `frontend/src/{api,stores,pages}`；订阅模块对应 `sub_scenarios/sub_trials/sub_evidence` 与 `sub_*` 后端文件。其余强制目录含controllers/middlewares/config/constants/utils、components/common/hooks/types/router。三组件table/badge/frame共用；hooks/session/feedback和utils/forms/html共用；request_id/errors双中间件。

共享枚举角色admin/operator/viewer与任务queued/running/completed/failed位于 `backend/src/constants/enums.py` / `frontend/src/constants/enums.js`，security/trials、frame/badge消费。节点角色primary/replica和状态healthy/down由node schema及前端表单定义，事件类型由request_set schema及路由算法定义。

环境变量见.env.example：COMPOSE_PROJECT_NAME、三端口、DB_PASSWORD、JWT_SECRET、SEED_DEMO。默认密码仅本地演示、JWT8小时；seed自然键仅补缺失，不覆盖修改。schema由metadata.create_all初始建表，无在线升级引擎。5服务均健康检查，backend/worker依赖健康db/redis，frontend依赖backend；数据库、Redis、worker产物使用命名卷。

本地开发：Python3.11安装backend/requirements.txt、配置DATABASE_URL/REDIS_URL，在backend运行 `uvicorn src.main:app --reload` 和 `python -m src.worker`，前端经Nginx同源代理。停止清空：`docker compose down -v --remove-orphans`。

限制：单worker串行、无生产任务重试/崩溃续跑；不模拟网络随机抖动、自动选主或完整GTID集合。Google Fonts离线回退，业务不依赖字体网络。

## License

MIT。源码外 `../../test-artifacts/gb-4108/` 含真实测试、截图与结果。
