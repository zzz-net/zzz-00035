import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from .models import (
    AnomalyEvent, Evidence, EvidenceType, ImportBatch, EventStatus,
    SkippedRowLog, HandoverRecord, HandoverImportBatch, HandoverSkippedRowLog,
)


def compute_file_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def compute_config_hash(config: dict) -> str:
    thresholds = config.get("thresholds", {})
    carrier_alert = config.get("carrier_alert", {})
    key = (
        float(thresholds.get("temperature_upper_limit", 0)),
        int(thresholds.get("continuous_over_temp_minutes", 0)),
        int(thresholds.get("breakpoint_interval_minutes", 0)),
        int(thresholds.get("merge_window_minutes", 0)),
        int(carrier_alert.get("pre_window_minutes", 0)),
        int(carrier_alert.get("post_window_minutes", 0)),
    )
    return hashlib.sha256(repr(key).encode()).hexdigest()[:16]


def compute_raw_data_hash(valid_rows: list) -> str:
    keys = []
    for r in sorted(valid_rows, key=lambda x: (x["box_id"], x["timestamp"].isoformat())):
        keys.append(f"{r['box_id']}|{r['timestamp'].isoformat()}|{r['temperature_c']}")
    return hashlib.sha256("\n".join(keys).encode()).hexdigest()


def compute_event_signature(box_id: str, start_time: str, end_time: str, temp_limit: float) -> str:
    key = f"{box_id}|{start_time}|{end_time}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def parse_temperature_csv(content: bytes) -> pd.DataFrame:
    df = pd.read_csv(pd.io.common.BytesIO(content), dtype=str)
    df.columns = df.columns.str.strip()
    required = {"box_id", "timestamp", "temperature_c"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"温度 CSV 缺少必需列: {required - set(df.columns)}")
    return df


def parse_receipt_csv(content: bytes) -> pd.DataFrame:
    df = pd.read_csv(pd.io.common.BytesIO(content), dtype=str)
    df.columns = df.columns.str.strip()
    required = {"box_id", "arrival_time", "receiver", "remark"}
    if not required.issubset(set(df.columns)):
        raise ValueError(f"收货备注 CSV 缺少必需列: {required - set(df.columns)}")
    return df


def parse_carrier_alerts(content: bytes) -> list:
    data = json.loads(content)
    if not isinstance(data, list):
        raise ValueError("承运商告警 JSON 必须是数组")
    for item in data:
        if "box_id" not in item or "alert_time" not in item:
            raise ValueError("承运商告警条目缺少 box_id 或 alert_time")
    return data


class InvalidTimestampError(Exception):
    def __init__(self, message: str, details: dict = None):
        super().__init__(message)
        self.details = details or {}


def validate_temperature_rows(df: pd.DataFrame, config: dict, batch_id: str = "") -> tuple:
    valid_rows = []
    skipped_logs: list[SkippedRowLog] = []
    allow_missing = config.get("validation", {}).get("allow_missing_box_id", True)
    skip_bad_ts = config.get("validation", {}).get("skip_invalid_timestamp_rows", True)

    for idx, row in df.iterrows():
        row_number = int(idx) + 2
        box_id = str(row.get("box_id", "")).strip()
        ts_raw = str(row.get("timestamp", "")).strip()
        temp_raw = str(row.get("temperature_c", "")).strip()

        if not box_id or box_id == "nan":
            if not allow_missing:
                skipped_logs.append(SkippedRowLog(
                    batch_id=batch_id,
                    row_number=row_number,
                    reason="缺箱号",
                    box_id="",
                    timestamp_raw=ts_raw,
                    temperature_raw=temp_raw,
                ))
                continue

        if not temp_raw or temp_raw == "nan":
            skipped_logs.append(SkippedRowLog(
                batch_id=batch_id,
                row_number=row_number,
                reason="温度值为空",
                box_id=box_id,
                timestamp_raw=ts_raw,
                temperature_raw=temp_raw,
            ))
            continue

        try:
            temperature = float(temp_raw)
        except ValueError:
            skipped_logs.append(SkippedRowLog(
                batch_id=batch_id,
                row_number=row_number,
                reason=f"温度值无法解析: {temp_raw}",
                box_id=box_id,
                timestamp_raw=ts_raw,
                temperature_raw=temp_raw,
            ))
            continue

        ts = None
        try:
            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            if not skip_bad_ts:
                raise InvalidTimestampError(
                    f"第 {row_number} 行时间戳解析失败: '{ts_raw}'，格式应为 YYYY-MM-DD HH:MM:SS。"
                    f"请修正数据或将 skip_invalid_timestamp_rows 设为 true 以跳过该行。",
                    details={
                        "row_number": row_number,
                        "box_id": box_id,
                        "timestamp_raw": ts_raw,
                        "temperature_raw": temp_raw,
                    }
                )
            else:
                skipped_logs.append(SkippedRowLog(
                    batch_id=batch_id,
                    row_number=row_number,
                    reason=f"时间戳解析失败: {ts_raw}",
                    box_id=box_id,
                    timestamp_raw=ts_raw,
                    temperature_raw=temp_raw,
                ))
                continue

        valid_rows.append({
            "box_id": box_id if box_id and box_id != "nan" else "UNKNOWN",
            "timestamp": ts,
            "temperature_c": temperature,
            "timestamp_raw": ts_raw,
        })

    return valid_rows, skipped_logs


def generate_events(
    valid_rows: list,
    config: dict,
    batch_id: str,
    source_file: str,
    raw_data_hash: str = "",
    config_signature: str = "",
) -> tuple:
    temp_limit = config["thresholds"]["temperature_upper_limit"]
    cont_min = config["thresholds"]["continuous_over_temp_minutes"]
    bp_min = config["thresholds"]["breakpoint_interval_minutes"]
    merge_min = config["thresholds"]["merge_window_minutes"]

    if not raw_data_hash:
        raw_data_hash = compute_raw_data_hash(valid_rows)
    if not config_signature:
        config_signature = compute_config_hash(config)

    by_box: dict[str, list] = {}
    for r in valid_rows:
        by_box.setdefault(r["box_id"], []).append(r)
    for box_id in by_box:
        by_box[box_id].sort(key=lambda x: x["timestamp"])

    events: list[AnomalyEvent] = []
    evidences: list[Evidence] = []

    for box_id, rows in by_box.items():
        over_rows = [r for r in rows if r["temperature_c"] > temp_limit]
        if not over_rows:
            continue

        segments: list[list] = []
        current: list = [over_rows[0]]
        for i in range(1, len(over_rows)):
            gap = (over_rows[i]["timestamp"] - over_rows[i - 1]["timestamp"]).total_seconds() / 60
            if gap > bp_min:
                segments.append(current)
                current = [over_rows[i]]
            else:
                current.append(over_rows[i])
        segments.append(current)

        merged: list[list] = [segments[0]]
        for i in range(1, len(segments)):
            gap = (segments[i][0]["timestamp"] - merged[-1][-1]["timestamp"]).total_seconds() / 60
            if gap <= merge_min:
                merged[-1].extend(segments[i])
            else:
                merged.append(segments[i])

        for seg in merged:
            duration = (seg[-1]["timestamp"] - seg[0]["timestamp"]).total_seconds() / 60
            if duration >= cont_min or len(seg) == 1:
                if len(seg) == 1:
                    duration = 0
                max_temp = max(r["temperature_c"] for r in seg)
                start_time = seg[0]["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                end_time = seg[-1]["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
                event_sig = compute_event_signature(box_id, start_time, end_time, temp_limit)
                ev = AnomalyEvent(
                    box_id=box_id,
                    start_time=start_time,
                    end_time=end_time,
                    max_temperature=round(max_temp, 2),
                    duration_minutes=int(duration),
                    batch_id=batch_id,
                    raw_data_hash=raw_data_hash,
                    config_signature=config_signature,
                    event_signature=event_sig,
                )
                ev_ids = []
                for r in seg:
                    e = Evidence(
                        event_id=ev.event_id,
                        evidence_type=EvidenceType.TEMPERATURE_RECORD.value,
                        source_file=source_file,
                        box_id=r["box_id"],
                        timestamp=r["timestamp_raw"],
                        detail=f"温度 {r['temperature_c']}°C 超过上限 {temp_limit}°C",
                    )
                    evidences.append(e)
                    ev_ids.append(e.evidence_id)
                ev.evidence_ids = ev_ids
                events.append(ev)

    return events, evidences


def link_receipt_evidence(
    events: list[AnomalyEvent],
    receipt_df: pd.DataFrame,
    batch_id: str,
    source_file: str,
) -> list:
    evidences = []
    receipt_map: dict[str, list] = {}
    for _, row in receipt_df.iterrows():
        bid = str(row.get("box_id", "")).strip()
        if bid:
            receipt_map.setdefault(bid, []).append(row)

    for ev in events:
        rows = receipt_map.get(ev.box_id, [])
        for r in rows:
            e = Evidence(
                event_id=ev.event_id,
                evidence_type=EvidenceType.RECEIPT_NOTE.value,
                source_file=source_file,
                box_id=ev.box_id,
                timestamp=str(r.get("arrival_time", "")),
                detail=f"收货人: {r.get('receiver', '')}, 备注: {r.get('remark', '')}",
            )
            evidences.append(e)
            ev.evidence_ids.append(e.evidence_id)
    return evidences


def _parse_ts(ts_str: str):
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def link_alert_evidence(
    events: list[AnomalyEvent],
    alerts: list,
    batch_id: str,
    source_file: str,
    config: dict,
) -> list:
    evidences = []
    alert_map: dict[str, list] = {}
    for a in alerts:
        bid = str(a.get("box_id", "")).strip()
        if bid:
            alert_map.setdefault(bid, []).append(a)

    carrier_config = config.get("carrier_alert", {})
    pre_window = timedelta(minutes=int(carrier_config.get("pre_window_minutes", 30)))
    post_window = timedelta(minutes=int(carrier_config.get("post_window_minutes", 30)))

    for ev in events:
        event_start = _parse_ts(ev.start_time)
        event_end = _parse_ts(ev.end_time)
        if not event_start or not event_end:
            continue

        window_start = event_start - pre_window
        window_end = event_end + post_window

        matched_alerts = []
        als = alert_map.get(ev.box_id, [])
        for a in als:
            alert_time = _parse_ts(str(a.get("alert_time", "")))
            if alert_time and window_start <= alert_time <= window_end:
                matched_alerts.append(a)

        if matched_alerts:
            ev.carrier_alert_count = len(matched_alerts)

            nearest_alert = None
            min_diff = None
            carriers = set()
            alert_types_set = set()

            for a in matched_alerts:
                alert_time = _parse_ts(str(a.get("alert_time", "")))
                carriers.add(str(a.get("carrier", "")))
                alert_types_set.add(str(a.get("alert_type", "")))

                diff = abs((alert_time - event_start).total_seconds())
                if min_diff is None or diff < min_diff:
                    min_diff = diff
                    nearest_alert = a

            if nearest_alert:
                ev.nearest_alert_time = str(nearest_alert.get("alert_time", ""))

            ev.carrier = ",".join(sorted(c for c in carriers if c))
            ev.alert_types = ",".join(sorted(t for t in alert_types_set if t))

        for a in matched_alerts:
            e = Evidence(
                event_id=ev.event_id,
                evidence_type=EvidenceType.CARRIER_ALERT.value,
                source_file=source_file,
                box_id=ev.box_id,
                timestamp=str(a.get("alert_time", "")),
                detail=f"承运商: {a.get('carrier', '')}, 类型: {a.get('alert_type', '')}, 信息: {a.get('message', '')}",
            )
            evidences.append(e)
            ev.evidence_ids.append(e.evidence_id)
    return evidences


def parse_handover_csv(content: bytes) -> pd.DataFrame:
    df = pd.read_csv(pd.io.common.BytesIO(content), dtype=str)
    df.columns = df.columns.str.strip()
    return df


def validate_handover_rows(
    df: pd.DataFrame,
    config: dict,
    handover_batch_id: str = "",
) -> tuple:
    handover_config = config.get("handover_check", {})
    required_columns = handover_config.get(
        "required_columns",
        ["箱号", "交接时间", "交接点", "交接温度", "交接人", "备注"]
    )
    time_column = handover_config.get("time_column", "交接时间")
    time_format = handover_config.get("time_format", "%Y-%m-%d %H:%M:%S")
    temp_lower = float(handover_config.get("temperature_lower_limit", -40.0))
    temp_upper = float(handover_config.get("temperature_upper_limit", 10.0))

    column_mapping = {
        "箱号": "box_id",
        "交接时间": "handover_time",
        "交接点": "handover_point",
        "交接温度": "handover_temperature",
        "交接人": "handover_person",
        "备注": "remark",
    }

    valid_records = []
    skipped_logs = []

    for idx, row in df.iterrows():
        row_number = int(idx) + 2
        raw_values = {}
        for cn_col, model_field in column_mapping.items():
            raw_values[model_field] = str(row.get(cn_col, "")).strip() if cn_col in df.columns else ""

        box_id = raw_values["box_id"]
        handover_time_raw = raw_values["handover_time"]
        handover_point = raw_values["handover_point"]
        handover_temp_raw = raw_values["handover_temperature"]
        handover_person = raw_values["handover_person"]
        remark = raw_values["remark"]

        skip_reason = ""

        if not box_id or box_id == "nan":
            skip_reason = "缺箱号"
        elif not handover_time_raw or handover_time_raw == "nan":
            skip_reason = "缺交接时间"
        elif not handover_point or handover_point == "nan":
            skip_reason = "缺交接点"
        elif not handover_temp_raw or handover_temp_raw == "nan":
            skip_reason = "缺交接温度"
        else:
            try:
                datetime.strptime(handover_time_raw, time_format)
            except (ValueError, TypeError):
                skip_reason = f"时间格式错误: {handover_time_raw}，期望: {time_format}"

        if not skip_reason:
            try:
                handover_temp = float(handover_temp_raw)
                if handover_temp < temp_lower or handover_temp > temp_upper:
                    skip_reason = f"温度超出范围: {handover_temp_raw}，范围: [{temp_lower}, {temp_upper}]"
            except ValueError:
                skip_reason = f"温度值无法解析: {handover_temp_raw}"

        if skip_reason:
            skipped_logs.append(HandoverSkippedRowLog(
                handover_batch_id=handover_batch_id,
                row_number=row_number,
                reason=skip_reason,
                box_id=box_id if box_id != "nan" else "",
                handover_time_raw=handover_time_raw if handover_time_raw != "nan" else "",
                handover_point_raw=handover_point if handover_point != "nan" else "",
                handover_temperature_raw=handover_temp_raw if handover_temp_raw != "nan" else "",
                handover_person_raw=handover_person if handover_person != "nan" else "",
                remark_raw=remark if remark != "nan" else "",
            ))
            continue

        valid_records.append({
            "box_id": box_id,
            "handover_time": handover_time_raw,
            "handover_point": handover_point,
            "handover_temperature": float(handover_temp_raw),
            "handover_person": handover_person if handover_person != "nan" else "",
            "remark": remark if remark != "nan" else "",
        })

    return valid_records, skipped_logs
