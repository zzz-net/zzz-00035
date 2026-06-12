import hashlib
import json
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from .models import (
    AnomalyEvent, Evidence, EvidenceType, ImportBatch, EventStatus,
)


def compute_file_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


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


def validate_temperature_rows(df: pd.DataFrame, config: dict) -> tuple:
    valid_rows = []
    skipped = 0
    allow_missing = config.get("validation", {}).get("allow_missing_box_id", True)
    skip_bad_ts = config.get("validation", {}).get("skip_invalid_timestamp_rows", True)

    for _, row in df.iterrows():
        box_id = str(row.get("box_id", "")).strip()
        ts_raw = str(row.get("timestamp", "")).strip()
        temp_raw = str(row.get("temperature_c", "")).strip()

        if not box_id or box_id == "nan":
            if not allow_missing:
                skipped += 1
                continue
        if not temp_raw or temp_raw == "nan":
            skipped += 1
            continue
        try:
            temperature = float(temp_raw)
        except ValueError:
            skipped += 1
            continue
        try:
            ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            if skip_bad_ts:
                skipped += 1
                continue
            else:
                skipped += 1
                continue

        valid_rows.append({
            "box_id": box_id if box_id and box_id != "nan" else "UNKNOWN",
            "timestamp": ts,
            "temperature_c": temperature,
            "timestamp_raw": ts_raw,
        })

    return valid_rows, skipped


def generate_events(
    valid_rows: list,
    config: dict,
    batch_id: str,
    source_file: str,
) -> tuple:
    temp_limit = config["thresholds"]["temperature_upper_limit"]
    cont_min = config["thresholds"]["continuous_over_temp_minutes"]
    bp_min = config["thresholds"]["breakpoint_interval_minutes"]
    merge_min = config["thresholds"]["merge_window_minutes"]

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
                ev = AnomalyEvent(
                    box_id=box_id,
                    start_time=seg[0]["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                    end_time=seg[-1]["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                    max_temperature=round(max_temp, 2),
                    duration_minutes=int(duration),
                    batch_id=batch_id,
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


def link_alert_evidence(
    events: list[AnomalyEvent],
    alerts: list,
    batch_id: str,
    source_file: str,
) -> list:
    evidences = []
    alert_map: dict[str, list] = {}
    for a in alerts:
        bid = str(a.get("box_id", "")).strip()
        if bid:
            alert_map.setdefault(bid, []).append(a)

    for ev in events:
        als = alert_map.get(ev.box_id, [])
        for a in als:
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
