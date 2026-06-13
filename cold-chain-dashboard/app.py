import io
import json
import os
import tempfile
from datetime import datetime
from typing import Dict

import pandas as pd
import streamlit as st
import yaml

from core.analyzer import (
    compute_config_hash,
    compute_file_hash,
    compute_raw_data_hash,
    generate_events,
    InvalidTimestampError,
    link_alert_evidence,
    link_receipt_evidence,
    parse_carrier_alerts,
    parse_receipt_csv,
    parse_temperature_csv,
    validate_temperature_rows,
)
from core.models import AnomalyEvent, AuditLog, EventStatus, ImportBatch, Priority, SkippedRowLog
from core.persistence import (
    add_events,
    add_evidence_only,
    find_batch_by_raw_data_hash,
    get_audit_logs_for_event,
    get_evidence_for_event,
    get_event_by_id,
    get_events_by_raw_data_hash,
    get_skipped_logs_for_batch,
    is_exact_duplicate_batch,
    is_duplicate_batch,
    load_audit_logs,
    load_batches,
    load_events,
    load_snapshots,
    load_diffs,
    update_event,
    update_event_assignment,
    update_events_for_reanalysis,
    VersionConflictError,
    ReanalysisConflictError,
    get_diffs_by_batch_id,
    get_diffs_by_event_id,
    get_diffs_by_change_type,
    get_diff_summary,
    get_snapshots_by_raw_data_hash,
    get_latest_snapshot_for_raw_data,
    rollback_last_reanalysis,
    ChangeType,
    import_qc_csv,
    save_qc_import,
    load_qc_inspections,
    load_qc_batches,
    load_qc_skipped_logs,
    load_qc_undo_records,
    get_qc_inspections_for_event,
    get_qc_skipped_logs_for_batch,
    update_qc_inspection,
    undo_last_qc_import,
    filter_qc_inspections,
    get_qc_summary,
    QCVersionConflictError,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.yaml")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_config(cfg):
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)


st.set_page_config(page_title="冷链到货温控复盘看板", layout="wide")
st.title("🧊 冷链到货温控复盘看板")

menu = st.sidebar.radio("导航", ["数据导入", "到货质检抽检", "异常事件看板", "事件复核", "重分析追踪", "导出", "阈值配置", "导入历史"])

cfg = load_config()


def _status_color(s):
    m = {
        "待处理": "🔵",
        "已确认": "🟡",
        "误报": "🟢",
        "已关闭": "⚫",
    }
    return m.get(s, "")


def _priority_color(p):
    m = {
        "低": "⚪",
        "中": "🔵",
        "高": "🟠",
        "紧急": "🔴",
    }
    return m.get(p, "⚪")


def _is_overdue(deadline: str) -> bool:
    if not deadline:
        return False
    try:
        deadline_dt = datetime.strptime(deadline, "%Y-%m-%d %H:%M:%S")
        return datetime.now() > deadline_dt
    except (ValueError, TypeError):
        return False


def build_csv_export(events: list) -> str:
    csv_rows = []
    event_ids = [e.event_id for e in events]
    all_diffs = load_diffs() if events else []
    relevant_diffs = [
        d for d in all_diffs
        if d.old_event_id in event_ids or d.new_event_id in event_ids
    ]

    diff_by_event: Dict[str, list] = {}
    for d in relevant_diffs:
        if d.old_event_id:
            diff_by_event.setdefault(d.old_event_id, []).append(d)
        if d.new_event_id and d.new_event_id != d.old_event_id:
            diff_by_event.setdefault(d.new_event_id, []).append(d)

    total_diff_summary = get_diff_summary() if events else {}

    all_qc_inspections = load_qc_inspections()
    qc_by_event: Dict[str, list] = {}
    for qi in all_qc_inspections:
        if qi.event_id:
            qc_by_event.setdefault(qi.event_id, []).append(qi)

    for e in events:
        event_diffs = diff_by_event.get(e.event_id, [])
        has_conflict = any(d.has_conflict for d in event_diffs)
        conflict_reason = "; ".join([d.conflict_reason for d in event_diffs if d.has_conflict])
        change_types = sorted(set(d.change_type for d in event_diffs))

        event_qc = qc_by_event.get(e.event_id, [])
        qc_summary_str = "; ".join([
            f"{qi.box_id}@{qi.inspection_time}: 外观={qi.appearance_result}, 温度计={qi.thermometer_reading}, 处置={qi.disposal_suggestion}"
            for qi in event_qc
        ])

        event_dict = e.to_dict()
        event_dict["is_overdue"] = _is_overdue(e.deadline)
        event_dict["overdue_status"] = "已逾期" if _is_overdue(e.deadline) else "正常"
        event_dict["row_type"] = "事件"
        event_dict["has_carrier_alert"] = e.carrier_alert_count > 0
        event_dict["has_conflict"] = has_conflict
        event_dict["conflict_reason"] = conflict_reason
        event_dict["change_types"] = ", ".join(change_types)
        event_dict["reanalysis_count"] = len(event_diffs)
        event_dict["duration_minutes"] = str(int(e.duration_minutes))
        event_dict["version"] = str(int(e.version))
        event_dict["carrier_alert_count"] = str(int(e.carrier_alert_count))
        event_dict["qc_inspection_count"] = len(event_qc)
        event_dict["qc_inspection_summary"] = qc_summary_str
        csv_rows.append(event_dict)

        for log in get_audit_logs_for_event(e.event_id):
            csv_rows.append({
                "row_type": "审计日志",
                "event_id": e.event_id,
                "box_id": e.box_id,
                "log_id": log.log_id,
                "action": log.action,
                "field_changed": log.field_changed,
                "old_value": log.old_value,
                "new_value": log.new_value,
                "operator": log.operator,
                "log_timestamp": log.timestamp,
                "remark": log.remark,
            })

        for qi in event_qc:
            csv_rows.append({
                "row_type": "质检抽检",
                "event_id": e.event_id,
                "box_id": qi.box_id,
                "inspection_id": qi.inspection_id,
                "qc_batch_id": qi.qc_batch_id,
                "inspection_time": qi.inspection_time,
                "appearance_result": qi.appearance_result,
                "thermometer_reading": qi.thermometer_reading,
                "disposal_suggestion": qi.disposal_suggestion,
                "qc_operator": qi.operator,
                "qc_version": qi.version,
                "qc_last_updated_at": qi.last_updated_at,
            })

        for d in event_diffs:
            field_changes = "; ".join([
                f"{fd.field_name}: {fd.old_value} → {fd.new_value}"
                for fd in d.field_diffs
            ])
            evidence_changes = "; ".join([
                f"{ed.change_type}-{ed.evidence_type}: {ed.new_detail or ed.old_detail}"
                for ed in d.evidence_diffs
            ])
            csv_rows.append({
                "row_type": "重分析差异",
                "event_id": e.event_id,
                "box_id": e.box_id,
                "diff_id": d.diff_id,
                "snapshot_id": d.snapshot_id,
                "batch_id": d.batch_id,
                "change_type": d.change_type,
                "event_signature": d.event_signature,
                "alert_count_old": d.alert_count_old,
                "alert_count_new": d.alert_count_new,
                "has_conflict": d.has_conflict,
                "conflict_reason": d.conflict_reason,
                "field_changes": field_changes,
                "evidence_changes": evidence_changes,
                "diff_timestamp": d.created_at,
            })

    if total_diff_summary:
        csv_rows.append({
            "row_type": "差异摘要",
            "total_diffs": total_diff_summary["total_diffs"],
            "added": total_diff_summary["added"],
            "removed": total_diff_summary["removed"],
            "field_changed": total_diff_summary["field_changed"],
            "evidence_changed": total_diff_summary["evidence_changed"],
            "alert_changed": total_diff_summary["alert_changed"],
            "conflicts": total_diff_summary["conflicts"],
        })

    qc_summary = get_qc_summary()
    if qc_summary and qc_summary["total_inspections"] > 0:
        csv_rows.append({
            "row_type": "质检摘要",
            "qc_total_inspections": qc_summary["total_inspections"],
            "qc_linked_to_events": qc_summary["linked_to_events"],
            "qc_unlinked": qc_summary["unlinked"],
            "qc_appearance_result_counts": json.dumps(qc_summary["appearance_result_counts"], ensure_ascii=False),
            "qc_total_skipped_rows": qc_summary["total_skipped_rows"],
            "qc_total_undo_records": qc_summary["total_undo_records"],
        })

    all_qc_skipped = load_qc_skipped_logs()
    for sl in all_qc_skipped:
        csv_rows.append({
            "row_type": "质检跳过行",
            "qc_batch_id": sl.qc_batch_id,
            "row_number": sl.row_number,
            "box_id": sl.box_id,
            "inspection_time_raw": sl.inspection_time_raw,
            "skip_reason": sl.reason,
        })

    all_qc_undo = load_qc_undo_records()
    for ur in all_qc_undo:
        csv_rows.append({
            "row_type": "质检撤销记录",
            "undo_id": ur.undo_id,
            "qc_batch_id": ur.qc_batch_id,
            "undone_at": ur.undone_at,
            "undo_operator": ur.operator,
            "inspection_count": ur.inspection_count,
        })

    df = pd.DataFrame(csv_rows)
    column_order = [
        "row_type", "event_id", "box_id",
        "status", "priority", "assignee", "deadline",
        "is_overdue", "overdue_status", "start_time", "end_time",
        "max_temperature", "duration_minutes",
        "carrier_alert_count", "nearest_alert_time", "carrier", "alert_types", "has_carrier_alert",
        "handler", "handler_remark",
        "close_time", "last_updated_at", "version", "created_at",
        "log_id", "action", "field_changed", "old_value", "new_value",
        "operator", "log_timestamp", "remark",
        "batch_id", "raw_data_hash", "config_signature", "event_signature",
        "evidence_ids",
        "has_conflict", "conflict_reason", "change_types", "reanalysis_count",
        "qc_inspection_count", "qc_inspection_summary",
        "inspection_id", "qc_batch_id", "inspection_time",
        "appearance_result", "thermometer_reading", "disposal_suggestion",
        "qc_operator", "qc_version", "qc_last_updated_at",
        "diff_id", "snapshot_id", "change_type",
        "alert_count_old", "alert_count_new",
        "field_changes", "evidence_changes", "diff_timestamp",
        "total_diffs", "added", "removed", "field_changed",
        "evidence_changed", "alert_changed", "conflicts",
        "qc_total_inspections", "qc_linked_to_events", "qc_unlinked",
        "qc_appearance_result_counts", "qc_total_skipped_rows", "qc_total_undo_records",
        "row_number", "inspection_time_raw", "skip_reason",
        "undo_id", "undone_at", "undo_operator", "inspection_count",
    ]
    available_cols = [c for c in column_order if c in df.columns]
    df = df[available_cols]
    buf = io.StringIO()
    df.to_csv(buf, index=False, na_rep="")
    return buf.getvalue()


def build_json_export(events: list) -> dict:
    export_events = []
    event_ids = [e.event_id for e in events]
    all_diffs = load_diffs() if events else []
    relevant_diffs = [
        d for d in all_diffs
        if d.old_event_id in event_ids or d.new_event_id in event_ids
    ]

    diff_by_event: Dict[str, list] = {}
    for d in relevant_diffs:
        if d.old_event_id:
            diff_by_event.setdefault(d.old_event_id, []).append(d)
        if d.new_event_id and d.new_event_id != d.old_event_id:
            diff_by_event.setdefault(d.new_event_id, []).append(d)

    all_qc_inspections = load_qc_inspections()
    qc_by_event: Dict[str, list] = {}
    for qi in all_qc_inspections:
        if qi.event_id:
            qc_by_event.setdefault(qi.event_id, []).append(qi)

    for e in events:
        event_diffs = diff_by_event.get(e.event_id, [])
        has_conflict = any(d.has_conflict for d in event_diffs)
        conflict_reason = "; ".join([d.conflict_reason for d in event_diffs if d.has_conflict])
        change_types = sorted(set(d.change_type for d in event_diffs))

        event_qc = qc_by_event.get(e.event_id, [])

        event_dict = e.to_dict()
        event_dict["is_overdue"] = _is_overdue(e.deadline)
        event_dict["overdue_status"] = "已逾期" if _is_overdue(e.deadline) else "正常"
        event_dict["has_carrier_alert"] = e.carrier_alert_count > 0
        event_dict["has_conflict"] = has_conflict
        event_dict["conflict_reason"] = conflict_reason
        event_dict["change_types"] = change_types
        event_dict["reanalysis_count"] = len(event_diffs)
        event_dict["reanalysis_diffs"] = [d.to_dict() for d in event_diffs]
        event_dict["qc_inspection_count"] = len(event_qc)
        event_dict["qc_inspections"] = [qi.to_dict() for qi in event_qc]
        export_events.append(event_dict)

    evidence_data = []
    for e in events:
        evidence_data.extend([ev.to_dict() for ev in get_evidence_for_event(e.event_id)])
    audit_data = []
    for e in events:
        for l in get_audit_logs_for_event(e.event_id):
            log_dict = l.to_dict()
            log_dict["log_timestamp"] = log_dict.pop("timestamp")
            audit_data.append(log_dict)

    snapshots = load_snapshots() if events else []
    relevant_snapshot_ids = set()
    for d in relevant_diffs:
        relevant_snapshot_ids.add(d.snapshot_id)
    relevant_snapshots = [
        s.to_dict() for s in snapshots if s.snapshot_id in relevant_snapshot_ids
    ]

    total_diff_summary = get_diff_summary() if events else {}
    qc_summary = get_qc_summary()
    qc_skipped_logs = [sl.to_dict() for sl in load_qc_skipped_logs()]
    qc_undo_records = [ur.to_dict() for ur in load_qc_undo_records()]
    qc_batches = [b.to_dict() for b in load_qc_batches()]

    return {
        "events": export_events,
        "evidence": evidence_data,
        "audit_logs": audit_data,
        "reanalysis_diffs": [d.to_dict() for d in relevant_diffs],
        "reanalysis_snapshots": relevant_snapshots,
        "diff_summary": total_diff_summary,
        "qc_summary": qc_summary,
        "qc_inspections": [qi.to_dict() for qi in all_qc_inspections],
        "qc_skipped_rows": qc_skipped_logs,
        "qc_undo_records": qc_undo_records,
        "qc_import_batches": qc_batches,
        "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


if menu == "数据导入":
    st.header("数据导入")
    st.markdown("依次上传温度 CSV、收货备注 CSV 和承运商告警 JSON，系统将按阈值自动生成异常事件。")

    col1, col2, col3 = st.columns(3)
    with col1:
        temp_file = st.file_uploader("温度 CSV", type=["csv"], key="temp_csv")
    with col2:
        receipt_file = st.file_uploader("收货备注 CSV", type=["csv"], key="receipt_csv")
    with col3:
        alert_file = st.file_uploader("承运商告警 JSON", type=["json"], key="alert_json")

    if st.button("开始导入与分析", type="primary"):
        if not temp_file:
            st.error("请至少上传温度 CSV 文件")
        else:
            try:
                temp_content = temp_file.read()
                file_hash = compute_file_hash(temp_content)

                temp_df = parse_temperature_csv(temp_content)
                batch_id = ImportBatch().batch_id

                try:
                    valid_rows, skipped_logs = validate_temperature_rows(
                        temp_df, cfg, batch_id=batch_id
                    )
                except InvalidTimestampError as e:
                    st.error(f"❌ 导入失败: {e}")
                    if e.details:
                        with st.expander("查看错误详情", expanded=True):
                            st.json(e.details)
                    st.stop()

                skipped = len(skipped_logs)
                raw_data_hash = compute_raw_data_hash(valid_rows)
                config_signature = compute_config_hash(cfg)

                if is_exact_duplicate_batch(raw_data_hash, config_signature):
                    st.error(
                        "⚠️ 该文件已在当前阈值配置下导入过，完全重复的导入被拒绝。"
                        "如需重新分析，请调整阈值后再次导入。"
                    )
                    st.stop()

                existing_batch = find_batch_by_raw_data_hash(raw_data_hash)
                is_reanalysis = existing_batch is not None

                batch = ImportBatch(
                    batch_id=batch_id,
                    file_name=temp_file.name,
                    file_hash=file_hash,
                    raw_data_hash=raw_data_hash,
                    config_signature=config_signature,
                    row_count=len(temp_df),
                    skipped_rows=skipped,
                    status="成功" if valid_rows else "无有效数据",
                    is_reanalysis=is_reanalysis,
                )

                events, evidences = generate_events(
                    valid_rows, cfg, batch_id, temp_file.name,
                    raw_data_hash=raw_data_hash, config_signature=config_signature
                )
                receipt_evidence = []
                alert_evidence = []

                if receipt_file and not is_reanalysis:
                    try:
                        receipt_content = receipt_file.read()
                        receipt_df = parse_receipt_csv(receipt_content)
                        receipt_evidence = link_receipt_evidence(
                            events, receipt_df, batch_id, receipt_file.name
                        )
                    except Exception as e:
                        st.warning(f"收货备注解析失败: {e}")

                if alert_file:
                    try:
                        alert_content = alert_file.read()
                        alerts = parse_carrier_alerts(alert_content)
                        alert_evidence = link_alert_evidence(
                            events, alerts, batch_id, alert_file.name, cfg
                        )
                    except Exception as e:
                        st.warning(f"承运商告警解析失败: {e}")

                if is_reanalysis:
                    try:
                        updated, new_count, removed_count, conflict_count, diffs, snapshot_id = update_events_for_reanalysis(
                            events, evidences, batch, cfg, skipped_logs
                        )
                        diff_summary = get_diff_summary(batch_id=batch.batch_id)
                        st.success(
                            f"🔄 重新分析完成: 更新 {updated} 个事件, 新增 {new_count} 个事件, "
                            f"消失 {removed_count} 个事件, 冲突 {conflict_count} 个事件。\n\n"
                            f"差异统计: 新增 {diff_summary['added']}, 消失 {diff_summary['removed']}, "
                            f"字段变更 {diff_summary['field_changed']}, 证据变更 {diff_summary['evidence_changed']}, "
                            f"告警变更 {diff_summary['alert_changed']}。\n\n"
                            f"原始复核状态、处理人、处理备注、关闭时间和审计日志已全部保留。\n\n"
                            f"快照ID: {snapshot_id}"
                        )
                        if conflict_count > 0:
                            st.warning(
                                f"⚠️ 检测到 {conflict_count} 个事件存在用户操作记录。"
                                f"请前往「重分析追踪」页面查看详情并决定是否覆盖。"
                            )
                    except ReanalysisConflictError as e:
                        st.error(f"❌ {str(e)}")
                        with st.expander("查看所有冲突事件", expanded=True):
                            for conflict in e.conflicting_events:
                                st.markdown(
                                    f"- **事件ID**: {conflict['event_id']} | "
                                    f"**箱号**: {conflict['box_id']} | "
                                    f"**原因**: {conflict['reason']}"
                                )
                        if st.button("强制重新分析（覆盖用户操作记录）", type="secondary"):
                            updated, new_count, removed_count, conflict_count, diffs, snapshot_id = update_events_for_reanalysis(
                                events, evidences, batch, cfg, skipped_logs,
                                force=True, operator="user_override"
                            )
                            diff_summary = get_diff_summary(batch_id=batch.batch_id)
                            st.success(
                                f"🔄 强制重新分析完成: 更新 {updated} 个事件, 新增 {new_count} 个事件, "
                                f"消失 {removed_count} 个事件。\n\n"
                                f"已覆盖 {conflict_count} 个冲突事件。\n\n"
                                f"快照ID: {snapshot_id}"
                            )
                            st.rerun()
                else:
                    if events:
                        add_events(events, evidences + receipt_evidence + alert_evidence, batch, skipped_logs)
                        st.success(
                            f"✅ 导入完成: 生成 {len(events)} 条异常事件, "
                            f"{len(evidences) + len(receipt_evidence) + len(alert_evidence)} 条证据, "
                            f"跳过 {skipped} 行无效数据"
                        )
                    else:
                        add_events([], [], batch, skipped_logs)
                        st.info(f"✅ 导入完成但未发现异常事件 (跳过 {skipped} 行无效数据)")

                if skipped > 0:
                    with st.expander(f"⚠️ 查看 {skipped} 条跳过行的详细记录", expanded=False):
                        skip_df = pd.DataFrame([
                            {
                                "行号": l.row_number,
                                "箱号": l.box_id,
                                "原始时间戳": l.timestamp_raw,
                                "原始温度": l.temperature_raw,
                                "跳过原因": l.reason,
                            }
                            for l in skipped_logs
                        ])
                        st.dataframe(skip_df, use_container_width=True, hide_index=True)

            except Exception as e:
                st.error(f"❌ 导入失败: {e}")


elif menu == "到货质检抽检":
    st.header("🔍 到货质检抽检")
    st.markdown("上传质检员抽检 CSV，系统将自动关联到现有异常事件和批次记录。")

    tab_import, tab_list, tab_undo = st.tabs(["📋 导入抽检", "📊 抽检记录", "↩️ 撤销导入"])

    with tab_import:
        qc_file = st.file_uploader("抽检 CSV", type=["csv"], key="qc_csv")
        operator = st.text_input("质检员/操作人")

        with st.expander("CSV 格式要求"):
            st.markdown("""
            必填列: 箱号、抽检时间、外观结果、温度计读数、处置建议

            抽检时间格式: YYYY-MM-DD HH:MM:SS

            同箱号+同抽检时间的重复记录将被跳过。
            """)

        if st.button("开始导入抽检数据", type="primary"):
            if not qc_file:
                st.error("请上传抽检 CSV 文件")
            else:
                try:
                    content = qc_file.read()
                    inspections, evidences, qc_batch, skipped_logs = import_qc_csv(
                        content, cfg, operator=operator.strip() or "system"
                    )

                    qc_batch.file_name = qc_file.name
                    save_qc_import(inspections, evidences, qc_batch, skipped_logs)

                    linked = sum(1 for i in inspections if i.event_id)
                    st.success(
                        f"✅ 导入完成: {len(inspections)} 条抽检记录, "
                        f"{linked} 条关联到异常事件, "
                        f"{len(evidences)} 条质检证据, "
                        f"跳过 {len(skipped_logs)} 行"
                    )

                    if skipped_logs:
                        with st.expander(f"⚠️ 查看 {len(skipped_logs)} 条跳过行", expanded=False):
                            skip_df = pd.DataFrame([
                                {
                                    "行号": l.row_number,
                                    "箱号": l.box_id,
                                    "抽检时间": l.inspection_time_raw,
                                    "跳过原因": l.reason,
                                }
                                for l in skipped_logs
                            ])
                            st.dataframe(skip_df, use_container_width=True, hide_index=True)

                except ValueError as e:
                    st.error(f"❌ 导入失败: {e}")
                except Exception as e:
                    st.error(f"❌ 导入失败: {e}")

    with tab_list:
        all_inspections = load_qc_inspections()
        all_qc_batches = load_qc_batches()

        if not all_inspections:
            st.info("暂无抽检记录")
        else:
            summary = get_qc_summary()
            col_s1, col_s2, col_s3, col_s4 = st.columns(4)
            col_s1.metric("总抽检记录", summary["total_inspections"])
            col_s2.metric("关联事件数", summary["linked_to_events"])
            col_s3.metric("未关联数", summary["unlinked"])
            col_s4.metric("导入批次", summary["total_batches"])

            st.markdown("---")

            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                batch_options = ["全部"] + [b.qc_batch_id for b in all_qc_batches]
                selected_batch = st.selectbox("按导入批次筛选", batch_options, index=0)
            with col_f2:
                alert_filter_qc = st.selectbox(
                    "按承运商告警筛选",
                    ["全部", "有承运商告警", "无承运商告警"],
                    index=0,
                )
            with col_f3:
                appearance_options = ["全部"] + sorted(set(i.appearance_result for i in all_inspections if i.appearance_result))
                selected_appearance = st.selectbox("按外观结果筛选", appearance_options, index=0)

            filtered = filter_qc_inspections(
                batch_id="" if selected_batch == "全部" else selected_batch,
                has_carrier_alert=True if alert_filter_qc == "有承运商告警" else (False if alert_filter_qc == "无承运商告警" else None),
                appearance_result="" if selected_appearance == "全部" else selected_appearance,
            )

            if not filtered:
                st.info("无匹配记录")
            else:
                st.markdown(f"**共 {len(filtered)} 条抽检记录**")
                rows = []
                for i in filtered:
                    event_info = ""
                    if i.event_id:
                        ev = get_event_by_id(i.event_id)
                        if ev:
                            event_info = f"{ev.event_id} ({ev.box_id})"
                    rows.append({
                        "箱号": i.box_id,
                        "抽检时间": i.inspection_time,
                        "外观结果": i.appearance_result,
                        "温度计读数": i.thermometer_reading,
                        "处置建议": i.disposal_suggestion,
                        "关联事件": event_info,
                        "质检员": i.operator,
                        "版本": i.version,
                        "最后更新": i.last_updated_at,
                    })
                df = pd.DataFrame(rows)
                st.dataframe(df, use_container_width=True, hide_index=True)

            st.markdown("---")
            selected_insp_id = st.text_input("输入抽检记录ID修改结论")
            if selected_insp_id:
                all_insp = load_qc_inspections()
                target_insp = next((i for i in all_insp if i.inspection_id == selected_insp_id), None)
                if target_insp:
                    st.markdown(f"**箱号:** {target_insp.box_id} | **抽检时间:** {target_insp.inspection_time} | **当前版本:** {target_insp.version}")
                    new_appearance = st.text_input("外观结果", value=target_insp.appearance_result, key="edit_appearance")
                    new_thermo = st.text_input("温度计读数", value=target_insp.thermometer_reading, key="edit_thermo")
                    new_disposal = st.text_input("处置建议", value=target_insp.disposal_suggestion, key="edit_disposal")
                    edit_operator = st.text_input("操作人", key="edit_qc_operator")

                    if st.button("提交修改", type="primary", key="submit_qc_edit"):
                        if not edit_operator.strip():
                            st.error("请填写操作人")
                        else:
                            try:
                                success, updated = update_qc_inspection(
                                    selected_insp_id,
                                    appearance_result=new_appearance,
                                    thermometer_reading=new_thermo,
                                    disposal_suggestion=new_disposal,
                                    operator=edit_operator.strip(),
                                    expected_version=target_insp.version,
                                )
                                if success:
                                    st.success(f"抽检记录 {selected_insp_id} 已更新，版本: {updated.version}")
                                    st.rerun()
                            except QCVersionConflictError as e:
                                st.error(f"❌ {str(e)}")
                else:
                    st.warning("未找到该抽检记录")

    with tab_undo:
        qc_batches = load_qc_batches()
        undo_records = load_qc_undo_records()

        if not qc_batches:
            st.info("暂无导入记录可撤销")
        else:
            last_batch = qc_batches[-1]
            st.warning(
                f"⚠️ 将撤销最近一次质检导入（批次: {last_batch.qc_batch_id}，"
                f"时间: {last_batch.import_time}，有效记录: {last_batch.valid_count}条）。"
                f"此操作不可逆。"
            )
            undo_operator = st.text_input("操作人", value="admin", key="qc_undo_operator")
            if st.button("撤销最近一次质检导入", type="primary"):
                if not undo_operator.strip():
                    st.error("请填写操作人")
                else:
                    success, count, batch_id = undo_last_qc_import(operator=undo_operator.strip())
                    if success:
                        st.success(f"✅ 撤销成功！已移除 {count} 条抽检记录（批次: {batch_id}）")
                        st.rerun()
                    else:
                        st.error("❌ 撤销失败")

        if undo_records:
            st.markdown("---")
            st.subheader("撤销历史")
            undo_rows = []
            for r in undo_records:
                undo_rows.append({
                    "撤销ID": r.undo_id,
                    "批次ID": r.qc_batch_id,
                    "撤销时间": r.undone_at,
                    "操作人": r.operator,
                    "移除记录数": r.inspection_count,
                })
            st.dataframe(pd.DataFrame(undo_rows), use_container_width=True, hide_index=True)


elif menu == "异常事件看板":
    st.header("异常事件看板")
    events = load_events()

    if not events:
        st.info("暂无异常事件，请先导入数据")
    else:
        status_filter = st.multiselect(
            "按状态筛选",
            [s.value for s in EventStatus],
            default=[s.value for s in EventStatus],
        )
        alert_filter = st.selectbox(
            "按承运商告警筛选",
            ["全部", "有承运商告警", "无承运商告警"],
            index=0,
        )
        filtered = [e for e in events if e.status in status_filter]
        if alert_filter == "有承运商告警":
            filtered = [e for e in filtered if e.carrier_alert_count > 0]
        elif alert_filter == "无承运商告警":
            filtered = [e for e in filtered if e.carrier_alert_count == 0]

        col_a, col_b, col_c, col_d = st.columns(4)
        counts = {s.value: 0 for s in EventStatus}
        for e in events:
            counts[e.status] = counts.get(e.status, 0) + 1
        col_a.metric("待处理", counts.get("待处理", 0))
        col_b.metric("已确认", counts.get("已确认", 0))
        col_c.metric("误报", counts.get("误报", 0))
        col_d.metric("已关闭", counts.get("已关闭", 0))

        col_e, col_f = st.columns(2)
        with_alerts = sum(1 for e in events if e.carrier_alert_count > 0)
        without_alerts = sum(1 for e in events if e.carrier_alert_count == 0)
        col_e.metric("有承运商告警", with_alerts)
        col_f.metric("无承运商告警", without_alerts)

        st.markdown("---")

        if not filtered:
            st.info("无匹配事件")
        else:
            rows = []
            for e in filtered:
                overdue = _is_overdue(e.deadline)
                has_alert = "⚠️ 有告警" if e.carrier_alert_count > 0 else "✅ 无告警"
                rows.append({
                    "状态": _status_color(e.status),
                    "优先级": _priority_color(e.priority),
                    "承运商告警": has_alert,
                    "告警数量": e.carrier_alert_count,
                    "最近告警时间": e.nearest_alert_time,
                    "承运商": e.carrier,
                    "告警类型": e.alert_types,
                    "事件ID": e.event_id,
                    "箱号": e.box_id,
                    "开始时间": e.start_time,
                    "最高温度(°C)": e.max_temperature,
                    "持续(分钟)": e.duration_minutes,
                    "状态值": e.status,
                    "优先级值": e.priority,
                    "责任人": e.assignee or "未分派",
                    "截止时间": e.deadline or "未设置",
                    "是否逾期": "⚠️ 已逾期" if overdue else "✅ 正常",
                    "处理人": e.handler,
                    "最后更新": e.last_updated_at,
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True, column_order=[
                "状态", "优先级", "承运商告警", "告警数量", "最近告警时间",
                "承运商", "告警类型", "事件ID", "箱号", "开始时间",
                "最高温度(°C)", "持续(分钟)", "状态值", "优先级值",
                "责任人", "截止时间", "是否逾期", "处理人", "最后更新"
            ])

        selected = st.text_input("输入事件ID查看详情和证据")
        if selected:
            ev = next((e for e in events if e.event_id == selected), None)
            if ev:
                st.subheader(f"事件 {ev.event_id} 详情")
                col1, col2 = st.columns(2)
                with col1:
                    st.markdown(f"**承运商告警数量:** {ev.carrier_alert_count}")
                    st.markdown(f"**最近告警时间:** {ev.nearest_alert_time or '无'}")
                with col2:
                    st.markdown(f"**承运商:** {ev.carrier or '无'}")
                    st.markdown(f"**告警类型:** {ev.alert_types or '无'}")
                st.json(ev.to_dict())
                ev_list = get_evidence_for_event(ev.event_id)
                if ev_list:
                    st.markdown("**来源证据:**")
                    for e in ev_list:
                        st.markdown(f"- [{e.evidence_type}] {e.detail} (来源: {e.source_file}, 时间: {e.timestamp})")

                qc_inspections = get_qc_inspections_for_event(ev.event_id)
                if qc_inspections:
                    st.markdown("**关联质检抽检:**")
                    for qi in qc_inspections:
                        st.markdown(
                            f"- 🔍 箱号: {qi.box_id} | 抽检时间: {qi.inspection_time} | "
                            f"外观: {qi.appearance_result} | 温度计: {qi.thermometer_reading} | "
                            f"处置: {qi.disposal_suggestion} | 质检员: {qi.operator} | 版本: {qi.version}"
                        )

                log_list = get_audit_logs_for_event(ev.event_id)
                if log_list:
                    st.markdown("**处理日志:**")
                    for l in log_list:
                        field_info = f" | 字段: {l.field_changed}" if l.field_changed else ""
                        value_info = f" | {l.old_value} → {l.new_value}" if l.old_value or l.new_value else ""
                        st.markdown(f"- [{l.timestamp}] {l.action} | 操作人: {l.operator}{field_info}{value_info} | 备注: {l.remark}")

                st.markdown("---")
                st.subheader("重分析差异追踪")
                event_diffs = get_diffs_by_event_id(ev.event_id)
                if event_diffs:
                    st.markdown(f"共发现 **{len(event_diffs)}** 条重分析差异记录")
                    for diff in event_diffs:
                        conflict_badge = "⚠️ **冲突**" if diff.has_conflict else "✅ 无冲突"
                        st.markdown(f"### [{diff.change_type}] 批次 {diff.batch_id} {conflict_badge}")
                        st.markdown(f"- **快照ID**: {diff.snapshot_id}")
                        st.markdown(f"- **事件签名**: {diff.event_signature}")
                        st.markdown(f"- **告警数量变化**: {diff.alert_count_old} → {diff.alert_count_new}")
                        if diff.has_conflict:
                            st.warning(f"**冲突原因**: {diff.conflict_reason}")
                        if diff.field_diffs:
                            st.markdown("**字段变更:**")
                            for fd in diff.field_diffs:
                                st.markdown(f"  - `{fd.field_name}`: `{fd.old_value}` → `{fd.new_value}`")
                        if diff.evidence_diffs:
                            st.markdown("**证据变更:**")
                            for ed in diff.evidence_diffs:
                                if ed.change_type == "新增":
                                    st.markdown(f"  - ➕ [{ed.evidence_type}] 新增: {ed.new_detail}")
                                else:
                                    st.markdown(f"  - ➖ [{ed.evidence_type}] 删除: {ed.old_detail}")
                        st.markdown(f"- **创建时间**: {diff.created_at}")
                else:
                    st.info("该事件暂无重分析差异记录")
            else:
                st.warning("未找到该事件")


elif menu == "事件复核":
    st.header("事件复核")
    events = load_events()
    pending_or_confirmed = [e for e in events if e.status in ("待处理", "已确认")]

    if not pending_or_confirmed:
        st.info("没有待复核的事件")
    else:
        all_assignees = sorted(set([e.assignee for e in pending_or_confirmed if e.assignee]))
        all_statuses = sorted(set([e.status for e in pending_or_confirmed]))

        col_filter1, col_filter2, col_filter3, col_filter4 = st.columns(4)
        with col_filter1:
            filter_assignee = st.multiselect(
                "按责任人筛选",
                all_assignees,
                default=all_assignees,
            )
        with col_filter2:
            filter_status = st.multiselect(
                "按状态筛选",
                all_statuses,
                default=all_statuses,
            )
        with col_filter3:
            filter_overdue = st.selectbox(
                "按逾期筛选",
                ["全部", "已逾期", "未逾期"],
                index=0,
            )
        with col_filter4:
            filter_alert = st.selectbox(
                "按承运商告警筛选",
                ["全部", "有承运商告警", "无承运商告警"],
                index=0,
            )

        filtered = pending_or_confirmed
        if filter_assignee:
            filtered = [e for e in filtered if e.assignee in filter_assignee]
        if filter_status:
            filtered = [e for e in filtered if e.status in filter_status]
        if filter_overdue == "已逾期":
            filtered = [e for e in filtered if _is_overdue(e.deadline)]
        elif filter_overdue == "未逾期":
            filtered = [e for e in filtered if not _is_overdue(e.deadline)]
        if filter_alert == "有承运商告警":
            filtered = [e for e in filtered if e.carrier_alert_count > 0]
        elif filter_alert == "无承运商告警":
            filtered = [e for e in filtered if e.carrier_alert_count == 0]

        st.markdown("---")
        if not filtered:
            st.info("无匹配事件")
        else:
            st.markdown(f"**共 {len(filtered)} 条事件**")
            rows = []
            for e in filtered:
                overdue = _is_overdue(e.deadline)
                has_alert = "⚠️ 有告警" if e.carrier_alert_count > 0 else "✅ 无告警"
                rows.append({
                    "状态": _status_color(e.status),
                    "优先级": _priority_color(e.priority),
                    "承运商告警": has_alert,
                    "告警数量": e.carrier_alert_count,
                    "事件ID": e.event_id,
                    "箱号": e.box_id,
                    "开始时间": e.start_time,
                    "结束时间": e.end_time,
                    "最高温度(°C)": e.max_temperature,
                    "持续(分钟)": e.duration_minutes,
                    "责任人": e.assignee or "未分派",
                    "截止时间": e.deadline or "未设置",
                    "是否逾期": "⚠️ 已逾期" if overdue else "✅ 正常",
                    "优先级值": e.priority,
                    "最后更新": e.last_updated_at,
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True, column_order=[
                "状态", "优先级", "承运商告警", "告警数量", "事件ID", "箱号",
                "开始时间", "最高温度(°C)", "持续(分钟)", "责任人",
                "截止时间", "是否逾期", "最后更新"
            ])

        st.markdown("---")
        options = [f"{e.event_id} | {e.box_id} | {e.status} | {e.start_time}" for e in filtered]
        selected_idx = st.selectbox("选择事件进行处理", range(len(options)), format_func=lambda i: options[i])
        ev = filtered[selected_idx]

        if "current_event_version" not in st.session_state or st.session_state.get("current_event_id") != ev.event_id:
            st.session_state.current_event_id = ev.event_id
            st.session_state.current_event_version = ev.version

        if ev.version != st.session_state.current_event_version:
            st.warning(
                f"⚠️ 该事件已被其他用户更新！当前版本: {ev.version}, 您加载的版本: {st.session_state.current_event_version}。"
                "请刷新页面查看最新数据后再操作。"
            )
            st.session_state.current_event_version = ev.version

        st.markdown(f"**箱号:** {ev.box_id}  |  **时间:** {ev.start_time} ~ {ev.end_time}  |  **最高温度:** {ev.max_temperature}°C  |  **持续:** {ev.duration_minutes}分钟")
        st.markdown(f"**当前状态:** {_status_color(ev.status)} {ev.status}  |  **优先级:** {_priority_color(ev.priority)} {ev.priority}  |  **责任人:** {ev.assignee or '未分派'}  |  **截止时间:** {ev.deadline or '未设置'}")
        st.markdown(f"**版本:** {ev.version}  |  **最后更新:** {ev.last_updated_at}")

        ev_list = get_evidence_for_event(ev.event_id)
        if ev_list:
            with st.expander("查看来源证据", expanded=True):
                for e in ev_list:
                    st.markdown(f"- **{e.evidence_type}** | {e.detail} | 来源: {e.source_file}")

        log_list = get_audit_logs_for_event(ev.event_id)
        if log_list:
            with st.expander("历史处理日志"):
                for l in log_list:
                    field_info = f" | 字段: {l.field_changed}" if l.field_changed else ""
                    value_info = f" | {l.old_value} → {l.new_value}" if l.old_value or l.new_value else ""
                    st.markdown(f"- [{l.timestamp}] {l.action} | 操作人: {l.operator}{field_info}{value_info} | 备注: {l.remark}")

        tab1, tab2 = st.tabs(["📋 状态变更", "👤 班次交接/责任人分派"])

        with tab1:
            new_status = st.selectbox("变更状态为", [s.value for s in EventStatus], index=list(EventStatus).index(EventStatus(ev.status)))
            handler = st.text_input("处理人", value=ev.handler)
            remark = st.text_area("处理备注", value=ev.handler_remark)

            if st.button("提交复核", type="primary", key="submit_review"):
                if not handler.strip():
                    st.error("请填写处理人")
                else:
                    try:
                        success, updated_ev = update_event(
                            ev.event_id, new_status, handler.strip(), remark.strip(),
                            expected_version=st.session_state.current_event_version
                        )
                        if success:
                            st.success(f"事件 {ev.event_id} 已更新为: {new_status}")
                            st.session_state.current_event_version = updated_ev.version
                            st.rerun()
                    except VersionConflictError as e:
                        st.error(f"❌ {str(e)}")
                        st.session_state.current_event_version = e.current_version

        with tab2:
            assignee = st.text_input("责任人", value=ev.assignee)
            col_d1, col_d2 = st.columns(2)
            with col_d1:
                deadline_date = st.date_input(
                    "截止日期",
                    value=datetime.strptime(ev.deadline, "%Y-%m-%d %H:%M:%S").date() if ev.deadline else datetime.now().date()
                )
            with col_d2:
                deadline_time = st.time_input(
                    "截止时间",
                    value=datetime.strptime(ev.deadline, "%Y-%m-%d %H:%M:%S").time() if ev.deadline else datetime.now().time().replace(minute=0, second=0)
                )
            deadline_str = f"{deadline_date.strftime('%Y-%m-%d')} {deadline_time.strftime('%H:%M:%S')}"
            priority = st.selectbox(
                "优先级",
                [p.value for p in Priority],
                index=list(Priority).index(Priority(ev.priority))
            )
            operator = st.text_input("操作人", key="assign_operator")
            assign_remark = st.text_area("分派备注", key="assign_remark")

            if st.button("提交分派", type="primary", key="submit_assignment"):
                if not operator.strip():
                    st.error("请填写操作人")
                else:
                    try:
                        success, updated_ev = update_event_assignment(
                            ev.event_id, assignee.strip(), deadline_str, priority,
                            operator.strip(), assign_remark.strip(),
                            expected_version=st.session_state.current_event_version
                        )
                        if success:
                            st.success(f"事件 {ev.event_id} 已分派给: {assignee or '未设置'}")
                            st.session_state.current_event_version = updated_ev.version
                            st.rerun()
                    except VersionConflictError as e:
                        st.error(f"❌ {str(e)}")
                        st.session_state.current_event_version = e.current_version


elif menu == "重分析追踪":
    st.header("🔄 重分析差异追踪")

    batches = load_batches()
    reanalysis_batches = [b for b in batches if b.is_reanalysis]

    if not reanalysis_batches:
        st.info("暂无重分析记录")
    else:
        st.markdown(f"共 **{len(reanalysis_batches)}** 次重分析记录")

        batch_options = [
            f"{b.import_time} | {b.batch_id} | {b.file_name} | 配置签名: {b.config_signature[:8]}"
            for b in reanalysis_batches
        ]
        selected_idx = st.selectbox(
            "选择重分析批次",
            range(len(batch_options)),
            format_func=lambda i: batch_options[i],
        )
        selected_batch = reanalysis_batches[selected_idx]

        col_info1, col_info2, col_info3, col_info4 = st.columns(4)
        diff_summary = get_diff_summary(batch_id=selected_batch.batch_id)
        col_info1.metric("总差异数", diff_summary["total_diffs"])
        col_info2.metric("新增事件", diff_summary["added"])
        col_info3.metric("消失事件", diff_summary["removed"])
        col_info4.metric("冲突事件", diff_summary["conflicts"])

        col_info5, col_info6, col_info7, col_info8 = st.columns(4)
        col_info5.metric("字段变更", diff_summary["field_changed"])
        col_info6.metric("证据变更", diff_summary["evidence_changed"])
        col_info7.metric("告警变更", diff_summary["alert_changed"])
        col_info8.metric("配置签名", selected_batch.config_signature[:8])

        st.markdown("---")
        st.subheader("配置快照")
        snapshots = get_snapshots_by_raw_data_hash(selected_batch.raw_data_hash)
        batch_snapshot = next((s for s in snapshots if s.batch_id == selected_batch.batch_id), None)
        if batch_snapshot:
            st.markdown(f"**快照ID**: {batch_snapshot.snapshot_id}")
            st.markdown(f"**父快照**: {batch_snapshot.parent_snapshot_id or '无'}")
            st.markdown(f"**操作人**: {batch_snapshot.operator}")
            st.markdown(f"**创建时间**: {batch_snapshot.created_at}")
            with st.expander("查看完整配置快照", expanded=False):
                st.json(batch_snapshot.config_snapshot)

        st.markdown("---")
        st.subheader("差异筛选")
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            change_type_filter = st.multiselect(
                "按变更类型筛选",
                [ct.value for ct in ChangeType],
                default=[ct.value for ct in ChangeType],
            )
        with col_f2:
            conflict_filter = st.selectbox(
                "按冲突状态筛选",
                ["全部", "仅显示冲突", "仅显示无冲突"],
                index=0,
            )

        include_conflicts_only = conflict_filter == "仅显示冲突"
        filtered_diffs = get_diffs_by_change_type(
            batch_id=selected_batch.batch_id,
            change_types=change_type_filter if change_type_filter else None,
            include_conflicts_only=include_conflicts_only,
        )

        if not filtered_diffs:
            st.info("无匹配的差异记录")
        else:
            st.markdown(f"显示 **{len(filtered_diffs)}** 条差异记录")

            for diff in filtered_diffs:
                conflict_icon = "⚠️" if diff.has_conflict else "✅"
                st.markdown("---")
                st.markdown(f"### {conflict_icon} [{diff.change_type}] 事件 {diff.event_signature[:8]}")
                st.markdown(f"- **批次ID**: {diff.batch_id}")
                st.markdown(f"- **快照ID**: {diff.snapshot_id}")
                if diff.old_event_id:
                    st.markdown(f"- **原事件ID**: {diff.old_event_id}")
                if diff.new_event_id:
                    st.markdown(f"- **新事件ID**: {diff.new_event_id}")
                st.markdown(f"- **告警数量**: {diff.alert_count_old} → {diff.alert_count_new}")
                if diff.has_conflict:
                    st.error(f"**冲突**: {diff.conflict_reason}")
                if diff.field_diffs:
                    st.markdown("**字段变更:**")
                    for fd in diff.field_diffs:
                        st.markdown(f"  - `{fd.field_name}`: `{fd.old_value}` → `{fd.new_value}`")
                if diff.evidence_diffs:
                    st.markdown("**证据变更:**")
                    for ed in diff.evidence_diffs:
                        if ed.change_type == "新增":
                            st.markdown(f"  - ➕ [{ed.evidence_type}] 新增: {ed.new_detail}")
                        else:
                            st.markdown(f"  - ➖ [{ed.evidence_type}] 删除: {ed.old_detail}")
                st.markdown(f"- **记录时间**: {diff.created_at}")

        st.markdown("---")
        st.subheader("撤销重分析")
        latest_snapshot = get_latest_snapshot_for_raw_data(selected_batch.raw_data_hash)
        if latest_snapshot and latest_snapshot.batch_id == selected_batch.batch_id:
            st.warning(
                f"⚠️ 将撤销最近一次重分析（批次: {selected_batch.batch_id}），"
                f"回滚到重分析前的状态。用户操作日志将被保留。"
            )
            rollback_operator = st.text_input("操作人", value="admin", key="rollback_operator")
            if st.button("撤销本次重分析", type="primary"):
                if not rollback_operator.strip():
                    st.error("请填写操作人")
                else:
                    success, rolled_back_count, snapshot_id = rollback_last_reanalysis(
                        selected_batch.raw_data_hash,
                        operator=rollback_operator.strip()
                    )
                    if success:
                        st.success(
                            f"✅ 撤销成功！已回滚 {rolled_back_count} 个事件。"
                            f"快照 {snapshot_id} 已删除。"
                        )
                        st.rerun()
                    else:
                        st.error("❌ 撤销失败，没有找到可回滚的重分析记录")
        else:
            st.info("本次重分析不是最新的，无法撤销。只能撤销最近一次重分析。")


elif menu == "导出":
    st.header("导出")
    events = load_events()

    if not events:
        st.info("暂无事件可导出")
    else:
        fmt = st.radio("导出格式", ["CSV", "JSON"])
        status_filter = st.multiselect(
            "按状态筛选导出",
            [s.value for s in EventStatus],
            default=[s.value for s in EventStatus],
        )
        filtered = [e for e in events if e.status in status_filter]

        if not filtered:
            st.warning("无匹配事件")
        else:
            if fmt == "CSV":
                csv_str = build_csv_export(filtered)
                csv_bytes = csv_str.encode(cfg.get("export", {}).get("default_encoding", "utf-8-sig"))
                st.download_button(
                    "下载 CSV",
                    data=csv_bytes,
                    file_name=f"cold_chain_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
            else:
                payload = build_json_export(filtered)
                payload["export_metadata"] = {
                    "total_events": len(payload["events"]),
                    "total_evidence": len(payload["evidence"]),
                    "total_audit_logs": len(payload["audit_logs"]),
                    "filter_status": status_filter,
                }
                st.download_button(
                    "下载 JSON",
                    data=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
                    file_name=f"cold_chain_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                )


elif menu == "阈值配置":
    st.header("阈值配置")
    st.markdown("修改后即时生效，影响下次导入分析。已生成的事件不受影响。")

    thresholds = cfg.get("thresholds", {})
    new_limit = st.number_input("温度上限 (°C)", value=float(thresholds.get("temperature_upper_limit", -15.0)), step=0.5)
    new_cont = st.number_input("连续超温时长 (分钟)", value=int(thresholds.get("continuous_over_temp_minutes", 5)), min_value=1)
    new_bp = st.number_input("断点间隔 (分钟)", value=int(thresholds.get("breakpoint_interval_minutes", 10)), min_value=1)
    new_merge = st.number_input("合并窗口 (分钟)", value=int(thresholds.get("merge_window_minutes", 30)), min_value=1)

    st.markdown("---")
    st.subheader("承运商告警匹配窗口")
    st.markdown("用于匹配承运商告警与超温事件的时间窗口设置。")
    carrier_alert = cfg.get("carrier_alert", {})
    new_pre_window = st.number_input("事件前窗口 (分钟)", value=int(carrier_alert.get("pre_window_minutes", 30)), min_value=0, help="超温事件开始前多少分钟内的告警计入匹配")
    new_post_window = st.number_input("事件后窗口 (分钟)", value=int(carrier_alert.get("post_window_minutes", 30)), min_value=0, help="超温事件结束后多少分钟内的告警计入匹配")

    st.markdown("---")
    validation = cfg.get("validation", {})
    new_allow_missing = st.checkbox("允许缺箱号", value=validation.get("allow_missing_box_id", True))
    new_skip_bad_ts = st.checkbox("跳过时间解析失败行", value=validation.get("skip_invalid_timestamp_rows", True))
    new_reject_dup = st.checkbox("拒绝重复导入", value=validation.get("reject_duplicate_batch", True))

    if st.button("保存配置", type="primary"):
        try:
            float(new_limit)
            int(new_cont)
            int(new_bp)
            int(new_merge)
            int(new_pre_window)
            int(new_post_window)
        except (ValueError, TypeError):
            st.error("⚠️ 阈值配置错误: 请确保所有数值有效")
        else:
            cfg["thresholds"] = {
                "temperature_upper_limit": new_limit,
                "continuous_over_temp_minutes": int(new_cont),
                "breakpoint_interval_minutes": int(new_bp),
                "merge_window_minutes": int(new_merge),
            }
            cfg["carrier_alert"] = {
                "pre_window_minutes": int(new_pre_window),
                "post_window_minutes": int(new_post_window),
            }
            cfg["validation"] = {
                "allow_missing_box_id": new_allow_missing,
                "skip_invalid_timestamp_rows": new_skip_bad_ts,
                "reject_duplicate_batch": new_reject_dup,
            }
            save_config(cfg)
            st.success("配置已保存")
            st.rerun()

    st.markdown("---")
    st.markdown("**当前配置:**")
    st.json(cfg)


elif menu == "导入历史":
    st.header("导入历史")
    batches = load_batches()
    if not batches:
        st.info("暂无导入记录")
    else:
        rows = []
        for b in batches:
            rows.append({
                "导入时间": b.import_time,
                "批次ID": b.batch_id,
                "文件名": b.file_name,
                "总行数": b.row_count,
                "跳过行数": b.skipped_rows,
                "状态": b.status,
                "是否重分析": "是" if b.is_reanalysis else "否",
                "配置签名": b.config_signature[:8] if b.config_signature else "",
            })
        df = pd.DataFrame(rows)
        st.dataframe(df, use_container_width=True, hide_index=True)

        selected_batch = st.selectbox(
            "选择批次查看跳过行日志",
            options=[""] + [b.batch_id for b in batches if b.skipped_rows > 0],
            format_func=lambda x: "请选择" if not x else x,
        )
        if selected_batch:
            skipped = get_skipped_logs_for_batch(selected_batch)
            if skipped:
                skip_df = pd.DataFrame([
                    {
                        "行号": l.row_number,
                        "箱号": l.box_id,
                        "原始时间戳": l.timestamp_raw,
                        "原始温度": l.temperature_raw,
                        "跳过原因": l.reason,
                    }
                    for l in skipped
                ])
                st.dataframe(skip_df, use_container_width=True, hide_index=True)
            else:
                st.info("该批次无跳过行记录")
