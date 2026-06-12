import io
import json
import os
import tempfile
from datetime import datetime

import pandas as pd
import streamlit as st
import yaml

from core.analyzer import (
    compute_file_hash,
    generate_events,
    link_alert_evidence,
    link_receipt_evidence,
    parse_carrier_alerts,
    parse_receipt_csv,
    parse_temperature_csv,
    validate_temperature_rows,
)
from core.models import AnomalyEvent, AuditLog, EventStatus, ImportBatch
from core.persistence import (
    add_events,
    add_evidence_only,
    get_audit_logs_for_event,
    get_evidence_for_event,
    is_duplicate_batch,
    load_audit_logs,
    load_batches,
    load_events,
    update_event,
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
                if is_duplicate_batch(file_hash):
                    st.error("⚠️ 该文件已导入过，重复导入同一批数据会被拒绝以保护旧数据。")
                    st.stop()

                temp_df = parse_temperature_csv(temp_content)
                valid_rows, skipped = validate_temperature_rows(temp_df, cfg)
                batch_id = ImportBatch().batch_id
                batch = ImportBatch(
                    batch_id=batch_id,
                    file_name=temp_file.name,
                    file_hash=file_hash,
                    row_count=len(temp_df),
                    skipped_rows=skipped,
                    status="成功" if valid_rows else "无有效数据",
                )

                events, evidences = generate_events(valid_rows, cfg, batch_id, temp_file.name)
                receipt_evidence = []
                alert_evidence = []

                if receipt_file:
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
                            events, alerts, batch_id, alert_file.name
                        )
                    except Exception as e:
                        st.warning(f"承运商告警解析失败: {e}")

                if events:
                    add_events(events, evidences + receipt_evidence + alert_evidence, batch)
                    st.success(
                        f"导入完成: 生成 {len(events)} 条异常事件, "
                        f"{len(evidences) + len(receipt_evidence) + len(alert_evidence)} 条证据, "
                        f"跳过 {skipped} 行无效数据"
                    )
                else:
                    add_events([], [], batch)
                    st.info(f"导入完成但未发现异常事件 (跳过 {skipped} 行无效数据)")

                if skipped > 0:
                    st.warning(f"⚠️ 跳过了 {skipped} 行无效数据（缺箱号/时间解析失败/温度无效）")

            except Exception as e:
                st.error(f"导入失败: {e}")


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
                rows.append({
                    "图标": _status_color(e.status),
                    "事件ID": e.event_id,
                    "箱号": e.box_id,
                    "开始时间": e.start_time,
                    "结束时间": e.end_time,
                    "最高温度(°C)": e.max_temperature,
                    "持续(分钟)": e.duration_minutes,
                    "状态": e.status,
                    "处理人": e.handler,
                })
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True, hide_index=True)

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
                        st.markdown(f"- [{l.timestamp}] {l.action} | 操作人: {l.operator} | 备注: {l.remark}")
            else:
                st.warning("未找到该事件")


elif menu == "事件复核":
    st.header("事件复核")
    events = load_events()
    pending_or_confirmed = [e for e in events if e.status in ("待处理", "已确认")]

    if not pending_or_confirmed:
        st.info("没有待复核的事件")
    else:
        options = [f"{e.event_id} | {e.box_id} | {e.status} | {e.start_time}" for e in pending_or_confirmed]
        selected_idx = st.selectbox("选择事件", range(len(options)), format_func=lambda i: options[i])
        ev = pending_or_confirmed[selected_idx]

        st.markdown(f"**箱号:** {ev.box_id}  |  **时间:** {ev.start_time} ~ {ev.end_time}  |  **最高温度:** {ev.max_temperature}°C  |  **持续:** {ev.duration_minutes}分钟")

        ev_list = get_evidence_for_event(ev.event_id)
        if ev_list:
            with st.expander("查看来源证据", expanded=True):
                for e in ev_list:
                    st.markdown(f"- **{e.evidence_type}** | {e.detail} | 来源: {e.source_file}")

        log_list = get_audit_logs_for_event(ev.event_id)
        if log_list:
            with st.expander("历史处理日志"):
                for l in log_list:
                    st.markdown(f"- [{l.timestamp}] {l.action} | 操作人: {l.operator} | 备注: {l.remark}")

        new_status = st.selectbox("变更状态为", [s.value for s in EventStatus], index=0)
        handler = st.text_input("处理人")
        remark = st.text_area("处理备注")

        if st.button("提交复核", type="primary"):
            if not handler.strip():
                st.error("请填写处理人")
            else:
                update_event(ev.event_id, new_status, handler.strip(), remark.strip())
                st.success(f"事件 {ev.event_id} 已更新为: {new_status}")
                st.rerun()


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
                df = pd.DataFrame([e.to_dict() for e in filtered])
                buf = io.StringIO()
                df.to_csv(buf, index=False, encoding=cfg.get("export", {}).get("default_encoding", "utf-8-sig"))
                st.download_button(
                    "下载 CSV",
                    data=buf.getvalue().encode("utf-8-sig"),
                    file_name=f"cold_chain_events_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                )
            else:
                data = [e.to_dict() for e in filtered]
                evidence_data = []
                for e in filtered:
                    evidence_data.extend([ev.to_dict() for ev in get_evidence_for_event(e.event_id)])
                audit_data = []
                for e in filtered:
                    audit_data.extend([l.to_dict() for l in get_audit_logs_for_event(e.event_id)])
                payload = {
                    "events": data,
                    "evidence": evidence_data,
                    "audit_logs": audit_data,
                    "export_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
        df = pd.DataFrame([b.to_dict() for b in batches])
        st.dataframe(df, use_container_width=True, hide_index=True)
