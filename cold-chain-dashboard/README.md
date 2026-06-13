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

---

## 交接温差核对模块

### 功能概述

通过门岗或司机上传的交接 CSV，追踪箱子在**离仓**和**到仓**时的温度，自动关联已有的异常事件，实现全链路温差核对。

- 左侧导航新增「**交接温差核对**」菜单（三个 Tab：导入交接 / 交接记录 / 撤销导入）
- 异常事件看板和事件复核页的「事件详情」新增「关联交接记录」和「同箱最近交接记录」
- 导入规则可通过 `config.yaml > handover_check` 灵活配置
- 备注修改具备乐观锁版本冲突检测，所有变更写审计日志
- 所有记录（交接记录、批次、跳过行、撤销记录、审计日志）持久化到 `store/*.json`，**应用重启后自动恢复**
- CSV / JSON 导出补全交接摘要、交接记录、跳过行、冲突状态、审计日志

---

### 交接 CSV 样例

```csv
箱号,交接时间,交接点,交接温度,交接人,备注
BX-001,2025-06-10 07:30:00,冷藏仓A,-20.5,司机王师傅,离仓温度正常
BX-001,2025-06-10 09:00:00,门岗B,-18.2,门岗李,到仓签收录入
BX-002,2025-06-10 08:45:00,冷藏仓A,-21.0,司机赵,离仓
BX-002,2025-06-10 10:15:00,门岗B,-19.5,门岗李,外观完好
BX-004,2025-06-10 10:30:00,冷藏仓A,-13.0,司机孙,离仓时温度偏高
BX-004,2025-06-10 12:00:00,门岗B,-8.5,门岗张,温度计显示异常高
```

---

### 字段含义

| 列名（中文） | 字段名（代码） | 类型 | 必填 | 说明 |
|--------------|----------------|------|------|------|
| **箱号** | `box_id` | string | ✅ | 箱子唯一编号，用于关联异常事件 |
| **交接时间** | `handover_time` | string | ✅ | 格式 `YYYY-MM-DD HH:MM:SS`（可配置），离仓或到仓的实际时间 |
| **交接点** | `handover_point` | string | ✅ | 交接发生的物理位置，如 `冷藏仓A`、`门岗B`、`配送点C` 等 |
| **交接温度** | `handover_temperature` | float | ✅ | 交接时读取的箱内温度 (°C)，超过配置上下限将被跳过 |
| **交接人** | `handover_person` | string | ❌ | 执行交接的人员姓名/工号，如司机、门岗等 |
| **备注** | `remark` | string | ❌ | 自由文本备注，支持后续人工修改 |

---

### config.yaml 导入规则配置

```yaml
handover_check:
  # 必填列（缺失时导入报错）
  required_columns:
    - 箱号
    - 交接时间
    - 交接点
    - 交接温度
    - 交接人
    - 备注

  # 时间列和格式
  time_column: "交接时间"
  time_format: "%Y-%m-%d %H:%M:%S"

  # 温度上下限 (°C)，超出范围的行被跳过
  temperature_lower_limit: -40.0   # 低于此温度视为无效
  temperature_upper_limit: 10.0    # 高于此温度视为无效

  # 与异常事件匹配的时间窗口（分钟）
  # 交接时间落在 [事件开始 - window, 事件结束 + window] 区间内自动关联
  match_window_minutes: 120
```

---

### 校验规则与跳过行

导入时**逐行校验**，不满足的行写入 `store/handover_skipped_rows.json`，每条跳过行都带明确原因：

| 跳过原因 | 触发条件 |
|----------|----------|
| `缺箱号` | 箱号为空 |
| `缺交接时间` | 交接时间为空 |
| `缺交接点` | 交接点为空 |
| `缺交接温度` | 交接温度为空 |
| `时间格式错误: {raw}，期望: {format}` | 交接时间无法按 `time_format` 解析 |
| `温度超出范围: {raw}，范围: [{lower}, {upper}]` | 温度低于下限或高于上限 |
| `温度值无法解析: {raw}` | 温度列不是合法数字 |
| `同箱同交接点同时间重复: 箱号={bid}, 时间={t}, 交接点={p}` | 同一 (箱号, 交接时间, 交接点) 组合已存在（跨批次去重） |

---

### 页面功能

#### 📥 Tab1：导入交接
- **CSV 上传** + 操作人填写
- 点击「开始导入交接数据」后：
  - 校验缺列 → 缺必填列直接报错并终止
  - 逐行校验 → 跳过行带原因落盘
  - 去重 → 同箱同交接点同时间的重复记录被跳过
  - 事件关联 → 交接时间在异常事件 ±match_window 内的自动关联
  - 关联成功的写入证据（`EvidenceType.HANDOVER`）和审计日志
- 导入结果展示：有效记录数 / 关联事件数 / 证据数 / 跳过行数，支持展开查看跳过行

#### 📊 Tab2：交接记录
- **顶部指标**：总记录数、关联事件数、未关联数、导入批次数
- **三维筛选**（支持组合）：
  - 按**导入批次**筛选
  - 按**交接点**筛选
  - 按**是否命中现有异常**筛选（全部 / 已关联异常 / 未关联异常）
- **列表展示**：箱号、交接时间、交接点、温度、交接人、备注、关联事件、上传人、版本等
- **备注编辑**：输入交接 ID → 加载当前值和版本 → 修改 → 提交
  - 提交时带 `expected_version` 做乐观锁校验
  - 若版本冲突 → 弹出 `HandoverVersionConflictError`，提示当前版本和期望版本
  - 成功后 `version += 1`，写入审计日志（`field_changed = "handover_remark"`）

#### ↩️ Tab3：撤销导入
- 可撤销**最近一次**交接导入批次（删除对应交接记录、交接证据、跳过行、批次记录）
- 写入「撤销交接导入」审计日志
- 展示完整「撤销历史」（撤销ID、批次ID、时间、操作人、移除记录数）

#### 📋 异常事件详情 - 新增交接区块
在「关联质检抽检」之后新增：
1. **关联交接记录**（bullet 列表）：直接关联到该事件的交接记录
2. **同箱最近交接记录**（表格 DataFrame）：按时间倒序的最近 10 条同箱交接，含温度、交接点、关联状态

---

### 导出字段补全（CSV / JSON）

#### CSV 新增行类型

| row_type | 说明 | 有效列 |
|----------|------|--------|
| `交接记录` | 每条交接 = 1 行（关联事件的跟着事件走，未关联的在末尾） | `handover_id, handover_batch_id, handover_time, handover_point, handover_temperature, handover_person, handover_remark, ho_operator, ho_version, ho_last_updated_at` |
| `交接摘要` | 全局统计汇总 = 1 行 | `ho_total_handovers, ho_linked_to_events, ho_unlinked, ho_handover_point_counts, ho_total_batches, ho_total_skipped_rows, ho_total_undo_records` |
| `交接跳过行` | 每条跳过记录 = 1 行 | `handover_batch_id, row_number, box_id, handover_time_raw, handover_point_raw, skip_reason` |
| `交接撤销记录` | 每次撤销 = 1 行 | `ho_undo_id, handover_batch_id, ho_undone_at, ho_undo_operator, handover_count` |

#### CSV 事件行新增交接字段

| 新增列 | 类型 | 含义 |
|--------|------|------|
| `handover_count` | int | 关联到此事件的交接记录数量 |
| `handover_summary` | string | 摘要字符串：`{箱号}@{时间}: 交接点={p}, 温度={t}°C, 交接人={name}; ...` |
| `handover_remark_conflict` | bool | 是否存在交接备注变更（用于检测冲突） |
| `handover_audit_logs` | string | 交接备注变更的审计日志摘要：`{时间}: {旧值} -> {新值} (by {操作人}); ...` |

#### JSON 新增顶层键

```jsonc
{
  "events": [
    {
      // ...原有事件字段...
      "handover_count": 2,
      "handovers": [ /* 关联的 HandoverRecord 数组 */ ],
      "handover_remark_conflict": true,
      "handover_conflict_logs": [ /* 交接备注变更的审计日志数组 */ ]
    }
  ],
  // ...原有导出字段...
  "handover_summary": {
    "total_handovers": 50,
    "linked_to_events": 32,
    "unlinked": 18,
    "handover_point_counts": {"冷藏仓A": 25, "门岗B": 25},
    "total_batches": 3,
    "total_skipped_rows": 5,
    "total_undo_records": 1
  },
  "handover_records":       [ /* 全部 HandoverRecord */ ],
  "handover_skipped_rows":  [ /* 全部 HandoverSkippedRowLog */ ],
  "handover_undo_records":  [ /* 全部 HandoverUndoRecord */ ],
  "handover_import_batches":[ /* 全部 HandoverImportBatch */ ]
}
```

---

### 验证方法（自测 / 联调）

1. **CSV 格式验证**：用 README 样例 CSV 导入，观察跳过行
2. **缺列测试**：删掉「交接点」列重导入，预期报错「缺少必填列」
3. **温度超限测试**：改温度为 `-50`（超下限）或 `20`（超上限），预期跳过并带「温度超出范围」原因
4. **重复记录测试**：相同箱号/时间/交接点导入两次，第二次预期跳过
5. **关联测试**：温度 CSV 生成异常事件 → 导入交接 CSV（交接时间在事件窗口内）→ 查看事件详情确认关联
6. **冲突测试**：打开两个浏览器会话改同一条备注 → 第二个提交预期触发 `HandoverVersionConflictError`
7. **重启恢复测试**：导入后关掉 streamlit 再启动 → 交接记录、跳过行、撤销记录、审计日志仍然存在
8. **导出验证**：CSV 用 Excel 打开确认 UTF-8-BOM 中文正常；JSON 检查 `handover_summary`、`handover_records`、事件内 `handovers` 都非空
9. **筛选测试**：在「交接记录」Tab 按批次、交接点、是否关联异常分别筛选，确认列表正确过滤

---

### 数据模型一览（persistence store）

| store 文件 | 模型类 | 主键 / 索引 |
|------------|--------|-------------|
| `handover_records.json` | `HandoverRecord` | `handover_id`（12位 hex）；去重键 `(box_id, handover_time, handover_point)` |
| `handover_import_batches.json` | `HandoverImportBatch` | `handover_batch_id` |
| `handover_skipped_rows.json` | `HandoverSkippedRowLog` | `log_id`；关联 `handover_batch_id` |
| `handover_undo_records.json` | `HandoverUndoRecord` | `undo_id`；关联 `handover_batch_id` |

以上文件全部在 `store/` 目录下，应用重启时自动从 JSON 恢复（读取 → 迁移默认值 → 内存对象）。

---

## 包装破损理赔初筛模块

### 功能概述

通过仓库拍照登记后的破损 CSV 记录，自动初筛哪些温控异常需要走理赔流程，帮助复核人员快速定位。

- 左侧导航新增「**包装破损理赔**」菜单（三个 Tab：导入破损 / 破损记录 / 撤销导入）
- 异常事件看板和事件复核页的「事件详情」新增「关联包装破损记录」和「同箱最近包装破损记录」
- 导入规则可通过 `config.yaml > damage_claim` 灵活配置
- 备注修改具备乐观锁版本冲突检测，所有变更写审计日志
- 所有记录（破损记录、批次、跳过行、撤销记录、审计日志）持久化到 `store/*.json`，**应用重启后自动恢复**
- CSV / JSON 导出补全理赔摘要、破损记录、跳过行、撤销记录、备注冲突日志

---

### 破损 CSV 样例

```csv
箱号,登记时间,破损类型,破损等级,照片编号,登记人,备注
BX-001,2025-06-10 08:15:00,外包装破损,严重,PHT-20250610-001,仓管员A,箱体一角明显压坏
BX-001,2025-06-10 08:16:00,内包装破损,中度,PHT-20250610-002,仓管员A,内盒有轻微变形
BX-002,2025-06-10 09:20:00,封条破损,轻微,PHT-20250610-003,仓管员B,封条有撬动痕迹
BX-003,2025-06-10 10:05:00,箱体变形,中度,PHT-20250610-004,仓管员B,箱体轻微凹陷
BX-004,2025-06-10 11:10:00,渗漏,严重,PHT-20250610-005,仓管员A,箱底有液体渗出
BX-004,2025-06-10 11:12:00,标签缺失,轻微,PHT-20250610-006,仓管员A,温度标签脱落
```

---

### 字段含义

| 列名（中文） | 字段名（代码） | 类型 | 必填 | 说明 |
|--------------|----------------|------|------|------|
| **箱号** | `box_id` | string | ✅ | 箱子唯一编号，用于关联异常事件 |
| **登记时间** | `registration_time` | string | ✅ | 格式 `YYYY-MM-DD HH:MM:SS`（可配置），破损拍照登记时间 |
| **破损类型** | `damage_type` | string | ✅ | 破损类型，必须在 `acceptable_damage_types` 枚举范围内 |
| **破损等级** | `damage_level` | string | ✅ | 破损等级，必须在 `damage_levels` 枚举范围内 |
| **照片编号** | `photo_number` | string | ✅ | 照片唯一编号，用于去重（同箱同照片编号视为重复） |
| **登记人** | `registrar` | string | ✅ | 执行拍照登记的人员姓名/工号 |
| **备注** | `remark` | string | ✅ | 自由文本备注，支持后续人工修改 |

---

### config.yaml 导入规则配置

```yaml
damage_claim:
  # 必填列（缺失时导入报错）
  required_columns:
    - 箱号
    - 登记时间
    - 破损类型
    - 破损等级
    - 照片编号
    - 登记人
    - 备注

  # 时间列和格式
  time_column: 登记时间
  time_format: "%Y-%m-%d %H:%M:%S"

  # 破损等级枚举（不在列表中的行被跳过）
  damage_levels:
    - 轻微
    - 中度
    - 严重
    - 拒收

  # 可受理破损类型（不在列表中的行被跳过）
  acceptable_damage_types:
    - 外包装破损
    - 内包装破损
    - 封条破损
    - 箱体变形
    - 标签缺失
    - 渗漏
    - 虫害
    - 其他

  # 与异常事件匹配的时间窗口（分钟）
  # 登记时间落在 [事件开始 - window, 事件结束 + window] 区间内自动关联
  match_window_minutes: 180
```

---

### 校验规则与跳过行

导入时**逐行校验**，不满足的行写入 `store/damage_claim_skipped_rows.json`，每条跳过行都带明确原因：

| 跳过原因 | 触发条件 |
|----------|----------|
| `缺箱号` | 箱号为空 |
| `缺登记时间` | 登记时间为空 |
| `缺破损类型` | 破损类型为空 |
| `缺破损等级` | 破损等级为空 |
| `缺照片编号` | 照片编号为空 |
| `缺登记人` | 登记人为空 |
| `时间格式错误: {raw}，期望: {format}` | 登记时间无法按 `time_format` 解析 |
| `破损等级无效: {raw}，允许值: [...]` | 破损等级不在 `damage_levels` 枚举内 |
| `破损类型不受理: {raw}，允许类型: [...]` | 破损类型不在 `acceptable_damage_types` 枚举内 |
| `同箱同照片编号重复记录: 箱号={bid}, 照片编号={p}` | 同一 (箱号, 照片编号) 组合已存在（跨批次去重） |

---

### 页面功能

#### 📥 Tab1：导入破损
- **CSV 上传** + 操作人填写
- 点击「开始导入破损数据」后：
  - 校验缺列 → 缺必填列直接报错并终止
  - 逐行校验 → 跳过行带原因落盘
  - 去重 → 同箱同照片编号的重复记录被跳过
  - 事件关联 → 登记时间在异常事件 ±match_window 内的自动关联
  - 关联成功的写入证据（`EvidenceType.DAMAGE_CLAIM`）和审计日志
- 导入结果展示：有效记录数 / 关联事件数 / 证据数 / 跳过行数，支持展开查看跳过行

#### 📊 Tab2：破损记录
- **顶部指标**：总破损记录、关联事件数、未关联数、导入批次数
- **三维筛选**（支持组合）：
  - 按**导入批次**筛选
  - 按**破损等级**筛选
  - 按**是否命中现有异常**筛选（全部 / 已关联异常 / 未关联异常）
- **列表展示**：箱号、登记时间、破损类型、破损等级、照片编号、登记人、备注、关联事件、上传人、版本等
- **备注编辑**：输入破损 ID → 加载当前值和版本 → 修改 → 提交
  - 提交时带 `expected_version` 做乐观锁校验
  - 若版本冲突 → 弹出 `DamageClaimVersionConflictError`，提示当前版本和期望版本
  - 成功后 `version += 1`，写入审计日志（`field_changed = "damage_claim_remark"`）

#### ↩️ Tab3：撤销导入
- 可撤销**最近一次**破损导入批次（删除对应破损记录、破损证据、跳过行、批次记录）
- 写入「撤销破损导入」审计日志
- 展示完整「撤销历史」（撤销ID、批次ID、时间、操作人、移除记录数）

#### 📋 异常事件详情 - 新增破损区块
在「关联交接记录」之后新增：
1. **关联包装破损记录**（bullet 列表）：直接关联到该事件的破损记录
2. **同箱最近包装破损记录**（表格 DataFrame）：按时间倒序的最近 10 条同箱破损，含类型、等级、照片编号、关联状态

---

### 导出字段补全（CSV / JSON）

#### CSV 新增行类型

| row_type | 说明 | 有效列 |
|----------|------|--------|
| `包装破损` | 每条破损 = 1 行（关联事件的跟着事件走，未关联的在末尾） | `damage_claim_id, damage_claim_batch_id, registration_time, damage_type, damage_level, photo_number, registrar, damage_claim_remark, dc_operator, dc_version, dc_last_updated_at` |
| `理赔摘要` | 全局统计汇总 = 1 行 | `dc_total_damage_claims, dc_linked_to_events, dc_unlinked, dc_damage_level_counts, dc_total_batches, dc_total_skipped_rows, dc_total_undo_records` |
| `理赔跳过行` | 每条跳过记录 = 1 行 | `damage_claim_batch_id, row_number, box_id, registration_time_raw, damage_type_raw, damage_level_raw, photo_number_raw, skip_reason` |
| `理赔撤销记录` | 每次撤销 = 1 行 | `dc_undo_id, damage_claim_batch_id, dc_undone_at, dc_undo_operator, damage_claim_count` |

#### CSV 事件行新增破损字段

| 新增列 | 类型 | 含义 |
|--------|------|------|
| `damage_claim_count` | int | 关联到此事件的破损记录数量 |
| `damage_claim_summary` | string | 摘要字符串：`{箱号}@{时间}: 类型={t}, 等级={l}, 照片={p}, 登记人={name}; ...` |
| `damage_claim_remark_conflict` | bool | 是否存在破损备注变更（用于检测冲突） |
| `damage_claim_audit_logs` | string | 破损备注变更的审计日志摘要：`{时间}: {旧值} -> {新值} (by {操作人}); ...` |

#### JSON 新增顶层键

```jsonc
{
  "events": [
    {
      // ...原有事件字段...
      "damage_claim_count": 2,
      "damage_claims": [ /* 关联的 DamageClaimRecord 数组 */ ],
      "damage_claim_remark_conflict": true,
      "damage_claim_conflict_logs": [ /* 破损备注变更的审计日志数组 */ ]
    }
  ],
  // ...原有导出字段...
  "damage_claim_summary": {
    "total_damage_claims": 50,
    "linked_to_events": 32,
    "unlinked": 18,
    "damage_level_counts": {"轻微": 20, "中度": 15, "严重": 10, "拒收": 5},
    "total_batches": 3,
    "total_skipped_rows": 5,
    "total_undo_records": 1
  },
  "damage_claim_records":       [ /* 全部 DamageClaimRecord */ ],
  "damage_claim_skipped_rows":  [ /* 全部 DamageClaimSkippedRowLog */ ],
  "damage_claim_undo_records": [ /* 全部 DamageClaimUndoRecord */ ],
  "damage_claim_import_batches":[ /* 全部 DamageClaimImportBatch */ ]
}
```

---

### 验证方法（自测 / 联调）

1. **CSV 格式验证**：用 `data/sample_damage_claims.csv` 导入，观察跳过行
2. **缺列测试**：删掉「破损类型」列重导入，预期报错「缺少必填列」
3. **等级无效测试**：改破损等级为 `unknown-level`，预期跳过并带「破损等级无效」原因
4. **类型不受理测试**：改破损类型为 `虚构类型`，预期跳过并带「破损类型不受理」原因
5. **重复记录测试**：相同箱号+照片编号导入两次，第二次预期跳过
6. **关联测试**：温度 CSV 生成异常事件 → 导入破损 CSV（登记时间在事件窗口内）→ 查看事件详情确认关联
7. **冲突测试**：打开两个浏览器会话改同一条备注 → 第二个提交预期触发 `DamageClaimVersionConflictError`
8. **重启恢复测试**：导入后关掉 streamlit 再启动 → 破损记录、跳过行、撤销记录、审计日志仍然存在
9. **导出验证**：CSV 用 Excel 打开确认 UTF-8-BOM 中文正常；JSON 检查 `damage_claim_summary`、`damage_claim_records`、事件内 `damage_claims` 都非空
10. **筛选测试**：在「破损记录」Tab 按批次、破损等级、是否关联异常分别筛选，确认列表正确过滤
11. **撤销测试**：导入一批后撤销 → 确认记录清空、撤销历史增加一条

---

### 数据模型一览（persistence store）

| store 文件 | 模型类 | 主键 / 索引 |
|------------|--------|-------------|
| `damage_claim_records.json` | `DamageClaimRecord` | `damage_claim_id`（12位 hex）；去重键 `(box_id, photo_number)` |
| `damage_claim_import_batches.json` | `DamageClaimImportBatch` | `damage_claim_batch_id` |
| `damage_claim_skipped_rows.json` | `DamageClaimSkippedRowLog` | `log_id`；关联 `damage_claim_batch_id` |
| `damage_claim_undo_records.json` | `DamageClaimUndoRecord` | `undo_id`；关联 `damage_claim_batch_id` |

以上文件全部在 `store/` 目录下，应用重启时自动从 JSON 恢复（读取 → 迁移默认值 → 内存对象）。
