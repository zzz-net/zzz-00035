import io
import json
import os
import tempfile
from datetime import datetime

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
    get_skipped_logs_for_batch,
    is_exact_duplicate_batch,
    is_duplicate_batch,
    load_audit_logs,
    load_batches,
    load_events,
    update_event,
    update_event_assignment,
    update_events_for_reanalysis,
    VersionConflictError,
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

menu = st.sidebar.radio("导航", ["数据导入", "异常事件看板", "事件复核", "导出", "阈值配置", "导入历史"])

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


def build_csv_export(events: list, encoding: str = "utf-8-sig") -> str:
    csv_rows = []
    for e in events:
        event_dict = e.to_dict()
        event_dict["is_overdue"] = _is_overdue(e.deadline)
        event_dict["overdue_status"] = "已逾期" if _is_overdue(e.deadline) else "正常"
        event_dict["row_type"] = "事件"
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

    df = pd.DataFrame(csv_rows)
    column_order = [
        "row_type", "event_id", "box_id",
        "status", "priority", "assignee", "deadline",
        "is_overdue", "overdue_status", "start_time", "end_time",
        "max_temperature", "duration_minutes", "handler", "handler_remark",
        "close_time", "last_updated_at", "version", "created_at",
        "log_id", "action", "field_changed", "old_value", "new_value",
        "operator", "log_timestamp", "remark",
        "batch_id", "raw_data_hash", "config_signature", "event_signature",
        "evidence_ids",
    ]
    available_cols = [c for c in column_order if c in df.columns]
    df = df[available_cols]
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding=encoding)
    return buf.getvalue()


def build_json_export(events: list) -> dict:
    export_events = []
    for e in events:
        event_dict = e.to_dict()
        event_dict["is_overdue"] = _is_overdue(e.deadline)
        event_dict["overdue_status"] = "已逾期" if _is_overdue(e.deadline) else "正常"
        export_events.append(event_dict)

    evidence_data = []
    for e in events:
        evidence_data.extend([ev.to_dict() for ev in get_evidence_for_event(e.event_id)])
    audit_data = []
    for e in events:
        audit_data.extend([l.to_dict() for l in get_audit_logs_for_event(e.event_id)])
    return {
        "events": export_events,
        "evidence": evidence_data,
        "audit_logs": audit_data,
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

                if alert_file and not is_reanalysis:
                    try:
                        alert_content = alert_file.read()
                        alerts = parse_carrier_alerts(alert_content)
                        alert_evidence = link_alert_evidence(
                            events, alerts, batch_id, alert_file.name
                        )
                    except Exception as e:
                        st.warning(f"承运商告警解析失败: {e}")

                if is_reanalysis:
                    updated, new_count, unchanged = update_events_for_reanalysis(
                        events, evidences, batch, skipped_logs
                    )
                    st.success(
                        f"🔄 重新分析完成: 更新 {updated} 个事件, 新增 {new_count} 个事件, "
                        f"其他 {unchanged} 个事件保持不变。\n\n"
                        f"原始复核状态、处理人、处理备注、关闭时间和审计日志已全部保留。"
                    )
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
        filtered = [e for e in events if e.status in status_filter]

        col_a, col_b, col_c, col_d = st.columns(4)
        counts = {s.value: 0 for s in EventStatus}
        for e in events:
            counts[e.status] = counts.get(e.status, 0) + 1
        col_a.metric("待处理", counts.get("待处理", 0))
        col_b.metric("已确认", counts.get("已确认", 0))
        col_c.metric("误报", counts.get("误报", 0))
        col_d.metric("已关闭", counts.get("已关闭", 0))

        st.markdown("---")

        if not filtered:
            st.info("无匹配事件")
        else:
            rows = []
            for e in filtered:
                overdue = _is_overdue(e.deadline)
                rows.append({
                    "状态": _status_color(e.status),
                    "优先级": _priority_color(e.priority),
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
                "状态", "优先级", "事件ID", "箱号", "开始时间", "最高温度(°C)",
                "持续(分钟)", "状态值", "优先级值", "责任人", "截止时间",
                "是否逾期", "处理人", "最后更新"
            ])

        selected = st.text_input("输入事件ID查看详情和证据")
        if selected:
            ev = next((e for e in events if e.event_id == selected), None)
            if ev:
                st.subheader(f"事件 {ev.event_id} 详情")
                st.json(ev.to_dict())
                ev_list = get_evidence_for_event(ev.event_id)
                if ev_list:
                    st.markdown("**来源证据:**")
                    for e in ev_list:
                        st.markdown(f"- [{e.evidence_type}] {e.detail} (来源: {e.source_file}, 时间: {e.timestamp})")
                log_list = get_audit_logs_for_event(ev.event_id)
                if log_list:
                    st.markdown("**处理日志:**")
                    for l in log_list:
                        field_info = f" | 字段: {l.field_changed}" if l.field_changed else ""
                        value_info = f" | {l.old_value} → {l.new_value}" if l.old_value or l.new_value else ""
                        st.markdown(f"- [{l.timestamp}] {l.action} | 操作人: {l.operator}{field_info}{value_info} | 备注: {l.remark}")
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

        col_filter1, col_filter2, col_filter3 = st.columns(3)
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

        filtered = pending_or_confirmed
        if filter_assignee:
            filtered = [e for e in filtered if e.assignee in filter_assignee]
        if filter_status:
            filtered = [e for e in filtered if e.status in filter_status]
        if filter_overdue == "已逾期":
            filtered = [e for e in filtered if _is_overdue(e.deadline)]
        elif filter_overdue == "未逾期":
            filtered = [e for e in filtered if not _is_overdue(e.deadline)]

        st.markdown("---")
        if not filtered:
            st.info("无匹配事件")
        else:
            st.markdown(f"**共 {len(filtered)} 条事件**")
            rows = []
            for e in filtered:
                overdue = _is_overdue(e.deadline)
                rows.append({
                    "状态": _status_color(e.status),
                    "优先级": _priority_color(e.priority),
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
                "状态", "优先级", "事件ID", "箱号", "开始时间", "最高温度(°C)",
                "持续(分钟)", "责任人", "截止时间", "是否逾期", "最后更新"
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
                csv_content = build_csv_export(filtered, cfg.get("export", {}).get("default_encoding", "utf-8-sig"))
                st.download_button(
                    "下载 CSV",
                    data=csv_content.encode("utf-8-sig"),
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
        except (ValueError, TypeError):
            st.error("⚠️ 阈值配置错误: 请确保所有数值有效")
        else:
            cfg["thresholds"] = {
                "temperature_upper_limit": new_limit,
                "continuous_over_temp_minutes": int(new_cont),
                "breakpoint_interval_minutes": int(new_bp),
                "merge_window_minutes": int(new_merge),
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
