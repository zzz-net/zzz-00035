# 冷链到货温控复盘看板

基于 Streamlit 的冷链温控异常事件管理系统，支持温度数据导入、异常事件检测、复核分派、审计追溯和结果导出。

## 快速开始

```bash
pip install streamlit pandas pyyaml
python -m streamlit run app.py --server.port 8501
```

浏览器访问 http://localhost:8501

## 目录结构

```
cold-chain-dashboard/
├── app.py                  # Streamlit 主程序（页面、导出）
├── config.yaml             # 阈值、验证、默认班次配置
├── core/
│   ├── models.py           # 数据模型（事件 / 证据 / 审计日志 / 优先级 / 状态枚举）
│   ├── persistence.py      # 持久化 & 数据迁移 & 版本冲突检测
│   ├── analyzer.py         # CSV 解析 & 异常事件生成
│   └── evidence.py         # 证据关联（收货备注 / 承运商告警）
├── store/                  # JSON 持久化目录（应用重启后自动恢复）
└── test_regression.py      # 回归测试（48 个用例，pytest / unittest）
```

---

## 导出格式规范

系统支持 **CSV** 和 **JSON** 两种导出格式，字段来源、筛选范围、日志归属完全一致。
导出入口：左侧菜单「导出」，支持按事件 `status` 筛选，筛选后 **事件主表和审计日志跟随同范围**。

### 编码

| 格式 | 编码 | 说明 |
|------|------|------|
| CSV | UTF-8 **带 BOM**（utf-8-sig） | 便于 Excel 双击直接打开中文；`config.yaml > export.default_encoding` 可改 |
| JSON | UTF-8 无 BOM | `ensure_ascii=False` 直接输出中文 |

---

### CSV 格式（核心：**row_type 区分事件行 / 操作记录行**）

CSV **不是纯事件清单**，而是把**异常事件 + 操作流水（审计日志）放在同一张表里**，用首列 `row_type` 标识。
接入方解析时必须先按 `row_type` 分流，否则会漏读日志行、追责字段拿不全。

#### 行类型一览

| row_type | 含义 | 有效列 |
|----------|------|--------|
| `事件` | 异常事件主记录（1 条事件 = 1 行） | `event_id, box_id, status, priority, assignee, deadline, start_time, end_time, max_temperature, duration_minutes, handler, handler_remark, close_time, last_updated_at, version, created_at, is_overdue, overdue_status, batch_id, evidence_ids...` |
| `审计日志` | 一条操作流水（分派/改期/状态变更 = 1 行） | `event_id, box_id, log_id, action, field_changed, old_value, new_value, operator, log_timestamp, remark` |

两类行都带 `event_id` 和 `box_id`，可以直接 JOIN / 透视。
不在本行类型有效列范围内的列，导出为**空字符串**（不是 `NaN` / 也不是 `"null"`）。

#### 字段顺序（稳定，下游可按位置或列名读取）

```
row_type | event_id | box_id | status | priority | assignee | deadline |
is_overdue | overdue_status | start_time | end_time | max_temperature |
duration_minutes | handler | handler_remark | close_time | last_updated_at |
version | created_at | log_id | action | field_changed | old_value | new_value |
operator | log_timestamp | remark | batch_id | raw_data_hash | config_signature |
event_signature | evidence_ids
```

#### 事件行字段说明（row_type = "事件"）

| 列名 | 类型 | 含义 |
|------|------|------|
| `event_id` | string | 事件唯一 ID（12 位 hex），跨重启、跨导出稳定 |
| `box_id` | string | 箱号（温控数据主键） |
| `status` | string | 事件状态：`待处理 / 已确认 / 误报 / 已关闭` |
| `priority` | string | 优先级：`低 / 中 / 高 / 紧急`，默认 `中` |
| `assignee` | string | 责任人（分派后非空） |
| `deadline` | string | 截止时间，格式 `YYYY-MM-DD HH:MM:SS`；未设置则空 |
| `is_overdue` | bool | 是否已逾期（基于 `deadline` 与导出时间对比） |
| `overdue_status` | string | `已逾期 / 正常`，中文可读版 |
| `start_time / end_time` | string | 超温起止时间 |
| `max_temperature` | float | 期间最高温度 (°C) |
| `duration_minutes` | int | 持续时长 (分钟) |
| `handler` | string | 复核处理人（状态变更时填写） |
| `handler_remark` | string | 复核备注 |
| `close_time` | string | 关闭时间，仅 `status = 已关闭` 非空 |
| `last_updated_at` | string | 最后一次变更时间 |
| `version` | int | 乐观锁版本号，每次更新 +1；并发修改时用于冲突检测 |
| `created_at` | string | 事件生成时间 |
| `evidence_ids` | string | Python list 字符串形式（可 split / ast.literal_eval 解析），关联证据 ID |

#### 操作记录行字段说明（row_type = "审计日志"）

**追责关键字段都在这里**。每次 `update_event` 或 `update_event_assignment` 时，**每个变更字段都会生成一条独立审计行**（例如一次分派同时改了 assignee + deadline + priority → 3 条日志行）。

| 列名 | 类型 | 含义 |
|------|------|------|
| `log_id` | string | 日志唯一 ID（12 位 hex） |
| `event_id` | string | 所属事件 ID，与事件行 JOIN |
| `box_id` | string | 所属箱号，冗余字段便于直接筛选 |
| `action` | string | 操作描述（中文），如 `责任人分派: 未设置 -> 早班A`、`状态变更: 待处理 -> 已确认` |
| `field_changed` | string | 变更字段名：`assignee / deadline / priority / status / handler / handler_remark` |
| `old_value` | string | 变更前的值；首次设置时通常为空字符串 |
| `new_value` | string | 变更后的值 |
| `operator` | string | 操作人（分派时填"操作人"输入框，状态变更时填"处理人"输入框） |
| `log_timestamp` | string | 操作时间，`YYYY-MM-DD HH:MM:SS` |
| `remark` | string | 操作备注（分派备注 / 复核备注） |

#### 定位一次变更

1. 按 `row_type = 审计日志` 过滤操作记录行
2. 用 `event_id` 找到对应事件行
3. 结合 `field_changed + old_value + new_value` 看**改了什么**
4. 用 `operator + log_timestamp` 看**谁在什么时候改的**
5. `action` 列是人类可读摘要（同时出现在 `field_changed` 行），做 BI 报表时选一种即可

#### 常见 field_changed → action / old / new 对照表

| field_changed | action 样例 | old_value | new_value |
|---------------|-------------|-----------|-----------|
| `assignee` | `责任人分派: 未设置 -> 早班A` | `""` / 旧责任人 | 新责任人 |
| `deadline` | `截止时间变更: 未设置 -> 2025-06-11 18:00:00` | `""` / 旧截止 | 新截止 |
| `priority` | `优先级变更: 中 -> 高` | 旧优先级 | 新优先级 |
| `status` | `状态变更: 待处理 -> 已确认` | 旧状态 | 新状态 |
| `handler` | `处理人变更: 未设置 -> 测试员` | `""` / 旧处理人 | 新处理人 |
| `handler_remark` | `处理备注更新` | 旧备注 | 新备注 |

#### 筛选后的日志归属

导出页按 `status` 筛选事件时：

- 先把 `events` 过滤成 `filtered`（只有 status 命中的事件）
- `build_csv_export(filtered)` / `build_json_export(filtered)` 遍历 `filtered` 中的**每条事件**，查询其完整审计日志并附加
- 所以：**只要事件本身被筛选命中，它的全部历史操作记录都会跟着导出**（不会只导出状态为 X 期间的日志）

这样设计的目的是保留追责完整链路——例如一个"已关闭"事件，导出时也能看到它从"待处理→已确认→已关闭"的全过程。

---

### JSON 格式

JSON 导出分成 **3 个独立数组**，适合做数据仓库 / ETL：

```json
{
  "events":      [ { ...event fields + is_overdue/overdue_status... } ],
  "evidence":    [ { ...evidence fields (来源温度记录 / 备注 / 告警) } ],
  "audit_logs":  [ { ...audit fields } ],
  "export_time": "2026-06-13 10:00:00",
  "export_metadata": {
    "total_events":       10,
    "total_evidence":     40,
    "total_audit_logs":   35,
    "filter_status":      ["待处理", "已确认", "误报", "已关闭"]
  }
}
```

**字段与 CSV 同源**：`events` 和 `audit_logs` 中的每条记录字段，和 CSV 对应行的字段名 / 取值完全一致（只是 JSON 结构里没有 `row_type` 字段——因为数组本身就是分区）。

#### events / audit_logs 与 CSV 的对应关系

| JSON 数组 | CSV row_type | 关联键 |
|-----------|--------------|--------|
| `events[i]` | `事件` | `event_id` |
| `audit_logs[j]` | `审计日志` | `event_id` + `log_id` |

---

## 承运商告警影响分析

### 匹配规则

承运商告警 JSON 导入后，系统会按配置的时间窗口自动匹配超温事件：

- **匹配条件**：同一箱号（`box_id`）下，告警时间落在 `[事件开始时间 - pre_window_minutes, 事件结束时间 + post_window_minutes]` 区间内
- **时间窗口**：在「阈值配置」页可分别设置事件前窗口（pre）和事件后窗口（post），单位为分钟，默认各 30 分钟
- **窗口变更生效**：修改后写回 `config.yaml`，重启后依然生效；同一批温度数据重分析时会用新窗口重新计算告警匹配字段

### 派生字段

| 字段 | 含义 | 筛选口径 |
|------|------|----------|
| `carrier_alert_count` | 匹配到的承运商告警数量 | `> 0` 视为"有承运商告警"，`= 0` 视为"无告警" |
| `nearest_alert_time` | 距离事件开始时间最近的一条告警时间 | 用于辅助判断告警是否恰好在超温前后触发 |
| `carrier` | 所有匹配告警的承运商名称，逗号分隔、去重排序 | 可按承运商做影响面分析 |
| `alert_types` | 所有匹配告警的告警类型，逗号分隔、去重排序 | 如 `equipment_fault,temperature_exceeded` |
| `has_carrier_alert` | 布尔派生字段（仅导出），等价于 `carrier_alert_count > 0` | 便于 BI 报表直接筛选 |

### 看板和事件详情筛选

- **异常事件看板**：顶部新增「按承运商告警筛选」下拉框（全部 / 有承运商告警 / 无承运商告警），并显示两类事件数量指标
- **事件复核**：同样支持按承运商告警筛选，便于优先处理有承运商侧告警的事件
- **事件详情**：展示承运商告警数量、最近告警时间、承运商、告警类型摘要

### 重分析注意事项

同一批温度数据用新窗口重分析时：
- ✅ 保留复核状态、处理人、处理备注、关闭时间
- ✅ 保留责任人分派、截止时间、优先级
- ✅ 保留全部审计日志
- ✅ 保留非温度证据（收货备注、承运商告警证据本身不重复生成）
- 🔄 **承运商告警派生字段会用新窗口重新计算**

---

## 数据模型（持久化）

详见 `core/models.py`：

| 模型 | 关键字段 |
|------|----------|
| `AnomalyEvent` | 见上文事件行字段（含承运商告警派生字段） |
| `AuditLog` | `log_id, event_id, action, operator, remark, field_changed, old_value, new_value, timestamp` |
| `Evidence` | `evidence_id, event_id, evidence_type (温度记录/收货备注/承运商告警), detail, source_file` |
| `Priority` (enum) | `低 / 中 / 高 / 紧急` |
| `EventStatus` (enum) | `待处理 / 已确认 / 误报 / 已关闭` |

**旧数据迁移**：应用启动时 `load_events / load_audit_logs` 会自动给缺失字段补默认值（`assignee="" / deadline="" / priority="中" / version=1 / last_updated_at=created_at / field_changed="" / old_value="" / new_value="" / carrier_alert_count=0 / nearest_alert_time="" / carrier="" / alert_types=""`），不破坏已有事件、证据和复核记录。

---

## 并发与版本控制

详见 `core/persistence.py`：

- `update_event()` / `update_event_assignment()` 支持可选 `expected_version` 参数
- 页面端会在加载事件时把 `version` 存进 `st.session_state.current_event_version`，提交时回传
- 若版本不匹配 → 抛 `VersionConflictError`，页面弹出醒目提示，不允许悄悄覆盖
- 每次成功更新 `version + 1`、`last_updated_at = now()`，并为**每个变更字段**生成一条独立审计日志

---

## 测试

```bash
cd cold-chain-dashboard
python -m pytest test_regression.py -v
```

覆盖范围：
- 默认配置、旧 store 数据迁移、缺失字段补默认
- 责任人分派、截止时间、优先级更新 + 审计日志
- 版本冲突检测（多人改同一事件）
- 重分析保留分派字段、跨重启数据恢复
- **CSV / JSON 导出字段一致性**（审计日志可追溯 + 编码 + 筛选后日志跟随）
- 状态筛选后导出、事件行完整性、空值填充、README 文档字段一致性
