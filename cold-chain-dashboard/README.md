# 冷链到货温控复盘看板

本地冷链到货温度异常检测与复核看板，基于 Streamlit 构建，带样例数据可直接启动。

## 快速启动

```bash
cd cold-chain-dashboard
pip install -r requirements.txt
streamlit run app.py
```

浏览器打开后即可使用。

## 项目结构

```
cold-chain-dashboard/
├── app.py                    # Streamlit 主应用
├── config.yaml               # 阈值与校验配置
├── requirements.txt          # Python 依赖
├── test_regression.py        # 回归测试套件（13个测试）
├── core/
│   ├── models.py             # 数据模型（事件、证据、审计日志、导入批次、跳过行）
│   ├── analyzer.py           # 温度解析、事件生成、证据关联
│   └── persistence.py        # JSON 文件持久化（线程安全、重分析逻辑）
├── data/
│   ├── sample_temperature.csv
│   ├── sample_receipt_notes.csv
│   └── sample_carrier_alerts.json
└── store/                    # 运行时自动生成的持久化数据
    ├── events.json
    ├── evidence.json
    ├── audit_log.json
    ├── batches.json
    └── skipped_rows.json     # 跳过行日志（新增）
```

## 配置说明 (config.yaml)

| 参数 | 含义 | 默认值 |
|------|------|--------|
| `temperature_upper_limit` | 温度上限（°C），超过即视为超温 | `-15.0` |
| `continuous_over_temp_minutes` | 连续超温最短时长（分钟），低于此值不生成事件 | `5` |
| `breakpoint_interval_minutes` | 断点间隔（分钟），两次超温记录间隔超过此值视为断开 | `10` |
| `merge_window_minutes` | 合并窗口（分钟），断开的两段超温间隔在此窗口内则合并 | `30` |
| `allow_missing_box_id` | 是否允许缺箱号（缺箱号行标记为 UNKNOWN） | `true` |
| `skip_invalid_timestamp_rows` | 是否跳过时间解析失败行 | `true` |
| `reject_duplicate_batch` | 是否拒绝重复导入同一文件 | `true` |

## 完整工作流程

### 1. 导入数据

1. 在侧边栏选择 **数据导入**
2. 上传 **温度 CSV**（必需），格式：`box_id, timestamp, temperature_c`
3. 可选上传 **收货备注 CSV**，格式：`box_id, arrival_time, receiver, remark`
4. 可选上传 **承运商告警 JSON**，数组格式，每项需含 `box_id` 和 `alert_time`
5. 点击 **开始导入与分析**

系统会：
- 校验文件格式，缺少必需列时报错
- 对温度数据逐行校验（缺箱号、时间解析失败、温度无效的行会被跳过并计数）
- 对同一文件基于 SHA-256 哈希去重，重复导入会被拒绝
- 按当前阈值配置生成异常事件
- 将收货备注和承运商告警作为证据关联到对应箱号的事件
- 所有数据持久化到 `store/` 目录

### 2. 阈值分析

事件生成逻辑：
1. 按 `box_id` 分组，按时间排序
2. 筛选温度超过 `temperature_upper_limit` 的记录
3. 按 `breakpoint_interval_minutes` 将相邻超温记录分段
4. 按 `merge_window_minutes` 将间隔较小的段合并
5. 持续时间 ≥ `continuous_over_temp_minutes` 的段生成异常事件（单条记录也生成）
6. 每条超温记录保留为来源证据

可在 **阈值配置** 页面调整参数，修改后对下次导入生效，不影响已生成事件。

### 3. 复核关闭

1. 在侧边栏选择 **事件复核**
2. 从下拉框选择待复核事件（待处理/已确认状态）
3. 查看该事件的所有来源证据（温度记录、收货备注、承运商告警）
4. 选择目标状态：待处理 → 已确认 → 误报 / 已关闭
5. 填写处理人和备注，点击提交

每次状态变更都会写入审计日志，记录操作人、变更内容、备注和时间。
关闭事件时自动记录关闭时间。

### 4. 导出

1. 在侧边栏选择 **导出**
2. 选择导出格式：CSV 或 JSON
3. 可按状态筛选导出范围
4. 点击下载

- **CSV 导出**：事件主表（含所有字段），编码 UTF-8-BOM
- **JSON 导出**：完整数据包，包含事件、关联证据和审计日志

## 边界情况处理

| 场景 | 处理方式 |
|------|----------|
| 缺箱号 | 配置 `allow_missing_box_id=true` 时标记为 UNKNOWN 并保留；`false` 时跳过 |
| 时间解析失败 | 跳过该行，计入 skipped 计数，页面显示警告 |
| 温度字段无效 | 跳过该行 |
| 阈值配置错误 | 保存前校验数值有效性，无效值拒绝保存 |
| 重复导入同一批数据 | 基于文件内容 SHA-256 哈希检测，重复文件直接拒绝 |
| 必需列缺失 | 上传时立即报错，不生成任何事件 |

## 数据持久化与重启一致性

所有运行时数据保存在 `store/` 目录下的 JSON 文件中：
- `events.json` — 异常事件（含状态、处理人、备注、关闭时间）
- `evidence.json` — 来源证据
- `audit_log.json` — 处理日志
- `batches.json` — 导入批次记录（含文件哈希）

重启 Streamlit 后，所有事件状态、证据链和处理日志自动从文件恢复，保持完全一致。
