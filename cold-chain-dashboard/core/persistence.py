import json
import os
import threading
from dataclasses import fields as dataclass_fields
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

from .models import (
    AnomalyEvent, AuditLog, Evidence, ImportBatch, Priority, SkippedRowLog,
    ReanalysisSnapshot, EventDiffRecord, FieldDiff, EvidenceDiff, ChangeType,
)

_BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "store")
_EVENTS_FILE = os.path.join(_BASE_DIR, "events.json")
_EVIDENCE_FILE = os.path.join(_BASE_DIR, "evidence.json")
_AUDIT_FILE = os.path.join(_BASE_DIR, "audit_log.json")
_BATCHES_FILE = os.path.join(_BASE_DIR, "batches.json")
_SKIPPED_FILE = os.path.join(_BASE_DIR, "skipped_rows.json")
_SNAPSHOTS_FILE = os.path.join(_BASE_DIR, "reanalysis_snapshots.json")
_DIFFS_FILE = os.path.join(_BASE_DIR, "event_diffs.json")

_lock = threading.Lock()


def _ensure_dir():
    os.makedirs(_BASE_DIR, exist_ok=True)


def _read_json(path: str) -> list:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return []


def _write_json(path: str, data):
    _ensure_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _migrate_event_data(event_dict: dict) -> dict:
    defaults = {
        "assignee": "",
        "deadline": "",
        "priority": Priority.MEDIUM.value,
        "last_updated_at": event_dict.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        "version": 1,
        "carrier_alert_count": 0,
        "nearest_alert_time": "",
        "carrier": "",
        "alert_types": "",
    }
    for key, default_value in defaults.items():
        if key not in event_dict:
            event_dict[key] = default_value
    return event_dict


def _migrate_audit_log_data(log_dict: dict) -> dict:
    defaults = {
        "field_changed": "",
        "old_value": "",
        "new_value": "",
    }
    for key, default_value in defaults.items():
        if key not in log_dict:
            log_dict[key] = default_value
    return log_dict


def load_events() -> list[AnomalyEvent]:
    data = _read_json(_EVENTS_FILE)
    migrated_data = [_migrate_event_data(d) for d in data]
    return [AnomalyEvent(**d) for d in migrated_data]


def save_events(events: list[AnomalyEvent]):
    _write_json(_EVENTS_FILE, [e.to_dict() for e in events])


def load_evidence() -> list[Evidence]:
    data = _read_json(_EVIDENCE_FILE)
    return [Evidence(**d) for d in data]


def save_evidence(evidences: list[Evidence]):
    _write_json(_EVIDENCE_FILE, [e.to_dict() for e in evidences])


def load_audit_logs() -> list[AuditLog]:
    data = _read_json(_AUDIT_FILE)
    migrated_data = [_migrate_audit_log_data(d) for d in data]
    return [AuditLog(**d) for d in migrated_data]


def save_audit_logs(logs: list[AuditLog]):
    _write_json(_AUDIT_FILE, [l.to_dict() for l in logs])


def load_batches() -> list[ImportBatch]:
    data = _read_json(_BATCHES_FILE)
    return [ImportBatch(**d) for d in data]


def save_batches(batches: list[ImportBatch]):
    _write_json(_BATCHES_FILE, [b.to_dict() for b in batches])


def load_skipped_logs() -> list[SkippedRowLog]:
    data = _read_json(_SKIPPED_FILE)
    return [SkippedRowLog(**d) for d in data]


def save_skipped_logs(logs: list[SkippedRowLog]):
    _write_json(_SKIPPED_FILE, [l.to_dict() for l in logs])


def add_events(
    new_events: list[AnomalyEvent],
    new_evidence: list[Evidence],
    batch: ImportBatch,
    skipped_logs: list[SkippedRowLog] = None,
):
    with _lock:
        existing_events = load_events()
        existing_evidence = load_evidence()
        existing_batches = load_batches()
        existing_skipped = load_skipped_logs()

        existing_events.extend(new_events)
        existing_evidence.extend(new_evidence)
        existing_batches.append(batch)
        if skipped_logs:
            existing_skipped.extend(skipped_logs)

        save_events(existing_events)
        save_evidence(existing_evidence)
        save_batches(existing_batches)
        save_skipped_logs(existing_skipped)


def add_skipped_logs(skipped_logs: list[SkippedRowLog]):
    with _lock:
        existing = load_skipped_logs()
        existing.extend(skipped_logs)
        save_skipped_logs(existing)


def add_evidence_only(new_evidence: list[Evidence]):
    with _lock:
        existing_evidence = load_evidence()
        existing_evidence.extend(new_evidence)
        save_evidence(existing_evidence)


class VersionConflictError(Exception):
    def __init__(self, event_id: str, current_version: int, expected_version: int):
        self.event_id = event_id
        self.current_version = current_version
        self.expected_version = expected_version
        super().__init__(f"事件 {event_id} 已被更新（当前版本: {current_version}, 期望版本: {expected_version}）")


def _append_audit_log(
    audit_logs: list[AuditLog],
    event_id: str,
    action: str,
    operator: str,
    remark: str = "",
    field_changed: str = "",
    old_value: str = "",
    new_value: str = "",
):
    log = AuditLog(
        event_id=event_id,
        action=action,
        operator=operator,
        remark=remark,
        field_changed=field_changed,
        old_value=str(old_value),
        new_value=str(new_value),
    )
    audit_logs.append(log)


def update_event(event_id: str, status: str, handler: str, remark: str, close_time: str = "", expected_version: int = None) -> Tuple[bool, AnomalyEvent]:
    with _lock:
        events = load_events()
        audit_logs = load_audit_logs()
        event = None
        for ev in events:
            if ev.event_id == event_id:
                event = ev
                break

        if not event:
            return False, None

        if expected_version is not None and event.version != expected_version:
            raise VersionConflictError(event_id, event.version, expected_version)

        old_status = event.status
        old_handler = event.handler
        old_remark = event.handler_remark

        event.status = status
        event.handler = handler
        event.handler_remark = remark
        event.version += 1
        event.last_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if status == "已关闭" and not event.close_time:
            event.close_time = close_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if old_status != status:
            _append_audit_log(
                audit_logs, event_id,
                action=f"状态变更: {old_status} -> {status}",
                operator=handler,
                remark=remark,
                field_changed="status",
                old_value=old_status,
                new_value=status,
            )

        if old_handler != handler:
            _append_audit_log(
                audit_logs, event_id,
                action=f"处理人变更: {old_handler or '未设置'} -> {handler}",
                operator=handler,
                remark=remark,
                field_changed="handler",
                old_value=old_handler,
                new_value=handler,
            )

        if old_remark != remark:
            _append_audit_log(
                audit_logs, event_id,
                action="处理备注更新",
                operator=handler,
                remark=remark,
                field_changed="handler_remark",
                old_value=old_remark,
                new_value=remark,
            )

        save_events(events)
        save_audit_logs(audit_logs)
        return True, event


def update_event_assignment(
    event_id: str,
    assignee: str,
    deadline: str,
    priority: str,
    operator: str,
    remark: str = "",
    expected_version: int = None,
) -> Tuple[bool, AnomalyEvent]:
    with _lock:
        events = load_events()
        audit_logs = load_audit_logs()
        event = None
        for ev in events:
            if ev.event_id == event_id:
                event = ev
                break

        if not event:
            return False, None

        if expected_version is not None and event.version != expected_version:
            raise VersionConflictError(event_id, event.version, expected_version)

        old_assignee = event.assignee
        old_deadline = event.deadline
        old_priority = event.priority

        event.assignee = assignee
        event.deadline = deadline
        event.priority = priority
        event.version += 1
        event.last_updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if old_assignee != assignee:
            _append_audit_log(
                audit_logs, event_id,
                action=f"责任人分派: {old_assignee or '未设置'} -> {assignee}",
                operator=operator,
                remark=remark,
                field_changed="assignee",
                old_value=old_assignee,
                new_value=assignee,
            )

        if old_deadline != deadline:
            _append_audit_log(
                audit_logs, event_id,
                action=f"截止时间变更: {old_deadline or '未设置'} -> {deadline}",
                operator=operator,
                remark=remark,
                field_changed="deadline",
                old_value=old_deadline,
                new_value=deadline,
            )

        if old_priority != priority:
            _append_audit_log(
                audit_logs, event_id,
                action=f"优先级变更: {old_priority} -> {priority}",
                operator=operator,
                remark=remark,
                field_changed="priority",
                old_value=old_priority,
                new_value=priority,
            )

        save_events(events)
        save_audit_logs(audit_logs)
        return True, event


def get_event_by_id(event_id: str) -> Optional[AnomalyEvent]:
    events = load_events()
    for ev in events:
        if ev.event_id == event_id:
            return ev
    return None


def is_duplicate_batch(file_hash: str) -> bool:
    batches = load_batches()
    return any(b.file_hash == file_hash for b in batches)


def is_exact_duplicate_batch(raw_data_hash: str, config_signature: str) -> bool:
    batches = load_batches()
    return any(
        b.raw_data_hash == raw_data_hash and b.config_signature == config_signature
        for b in batches
    )


def find_batch_by_raw_data_hash(raw_data_hash: str) -> Optional[ImportBatch]:
    batches = load_batches()
    for b in batches:
        if b.raw_data_hash == raw_data_hash:
            return b
    return None


def get_events_by_raw_data_hash(raw_data_hash: str) -> list[AnomalyEvent]:
    events = load_events()
    return [e for e in events if e.raw_data_hash == raw_data_hash]


def _migrate_snapshot_data(snapshot_dict: dict) -> dict:
    defaults = {
        "parent_snapshot_id": "",
        "operator": "system",
        "event_ids": [],
        "evidence_ids": [],
        "pre_events": [],
        "pre_evidence": [],
    }
    for key, default_value in defaults.items():
        if key not in snapshot_dict:
            snapshot_dict[key] = default_value
    return snapshot_dict


def _migrate_diff_data(diff_dict: dict) -> dict:
    defaults = {
        "field_diffs": [],
        "evidence_diffs": [],
        "alert_count_old": 0,
        "alert_count_new": 0,
        "has_conflict": False,
        "conflict_reason": "",
    }
    for key, default_value in defaults.items():
        if key not in diff_dict:
            diff_dict[key] = default_value
    if "field_diffs" in diff_dict:
        diff_dict["field_diffs"] = [
            fd if isinstance(fd, FieldDiff) else FieldDiff(**fd)
            for fd in diff_dict["field_diffs"]
        ]
    if "evidence_diffs" in diff_dict:
        diff_dict["evidence_diffs"] = [
            ed if isinstance(ed, EvidenceDiff) else EvidenceDiff(**ed)
            for ed in diff_dict["evidence_diffs"]
        ]
    return diff_dict


def load_snapshots() -> list[ReanalysisSnapshot]:
    data = _read_json(_SNAPSHOTS_FILE)
    migrated_data = [_migrate_snapshot_data(d) for d in data]
    return [ReanalysisSnapshot(**d) for d in migrated_data]


def save_snapshots(snapshots: list[ReanalysisSnapshot]):
    _write_json(_SNAPSHOTS_FILE, [s.to_dict() for s in snapshots])


def load_diffs() -> list[EventDiffRecord]:
    data = _read_json(_DIFFS_FILE)
    migrated_data = [_migrate_diff_data(d) for d in data]
    return [EventDiffRecord(**d) for d in migrated_data]


def save_diffs(diffs: list[EventDiffRecord]):
    _write_json(_DIFFS_FILE, [d.to_dict() for d in diffs])


def get_snapshots_by_raw_data_hash(raw_data_hash: str) -> list[ReanalysisSnapshot]:
    snapshots = load_snapshots()
    return [s for s in snapshots if s.raw_data_hash == raw_data_hash]


def get_snapshot_by_id(snapshot_id: str) -> Optional[ReanalysisSnapshot]:
    snapshots = load_snapshots()
    for s in snapshots:
        if s.snapshot_id == snapshot_id:
            return s
    return None


def get_latest_snapshot_for_raw_data(raw_data_hash: str) -> Optional[ReanalysisSnapshot]:
    all_snapshots = load_snapshots()
    filtered = [s for s in all_snapshots if s.raw_data_hash == raw_data_hash]
    if not filtered:
        return None
    return filtered[-1]


def get_diffs_by_snapshot_id(snapshot_id: str) -> list[EventDiffRecord]:
    diffs = load_diffs()
    return [d for d in diffs if d.snapshot_id == snapshot_id]


def get_diffs_by_batch_id(batch_id: str) -> list[EventDiffRecord]:
    diffs = load_diffs()
    return [d for d in diffs if d.batch_id == batch_id]


def get_diffs_by_event_id(event_id: str) -> list[EventDiffRecord]:
    diffs = load_diffs()
    return [d for d in diffs if d.old_event_id == event_id or d.new_event_id == event_id]


def check_event_has_user_operation(event: AnomalyEvent, audit_logs: Optional[list[AuditLog]] = None) -> tuple[bool, str]:
    if audit_logs is None:
        audit_logs = get_audit_logs_for_event(event.event_id)
    has_user_ops = any(
        log.action.startswith("状态变更") or
        log.action.startswith("处理人变更") or
        log.action.startswith("责任人分派") or
        log.action.startswith("截止时间变更") or
        log.action.startswith("优先级变更") or
        log.action.startswith("处理备注更新")
        for log in audit_logs
    )
    reasons = []
    if event.status != "待处理":
        reasons.append(f"状态为{event.status}")
    if event.handler:
        reasons.append("已有处理人")
    if event.handler_remark:
        reasons.append("已有处理备注")
    if event.close_time:
        reasons.append("已有关闭时间")
    if event.assignee:
        reasons.append("已有责任人分派")
    if has_user_ops:
        reasons.append("存在用户操作记录")
    return len(reasons) > 0, "、".join(reasons) if reasons else ""


def compute_field_diffs(old_event: AnomalyEvent, new_event: AnomalyEvent) -> list[FieldDiff]:
    diff_fields = [
        "start_time", "end_time", "max_temperature",
        "duration_minutes", "carrier_alert_count",
        "nearest_alert_time", "carrier", "alert_types",
        "config_signature", "batch_id"
    ]
    field_diffs = []
    for field in diff_fields:
        old_val = str(getattr(old_event, field, ""))
        new_val = str(getattr(new_event, field, ""))
        if old_val != new_val:
            field_diffs.append(FieldDiff(
                field_name=field, old_value=old_val, new_value=new_val
            ))
    return field_diffs


def compute_evidence_diffs(
    old_evidence: list[Evidence],
    new_evidence: list[Evidence],
) -> list[EvidenceDiff]:
    old_by_id = {e.evidence_id: e for e in old_evidence}
    new_by_id = {e.evidence_id: e for e in new_evidence}
    old_temp_ids = {e.evidence_id for e in old_evidence if e.evidence_type == "温度记录"}
    new_temp_ids = {e.evidence_id for e in new_evidence if e.evidence_type == "温度记录"}
    evidence_diffs = []
    for eid in new_temp_ids - old_temp_ids:
        e = new_by_id[eid]
        evidence_diffs.append(EvidenceDiff(
            evidence_id=eid,
            change_type="新增",
            evidence_type=e.evidence_type,
            new_detail=e.detail,
        ))
    for eid in old_temp_ids - new_temp_ids:
        e = old_by_id[eid]
        evidence_diffs.append(EvidenceDiff(
            evidence_id=eid,
            change_type="删除",
            evidence_type=e.evidence_type,
            old_detail=e.detail,
        ))
    return evidence_diffs


def compute_reanalysis_diffs(
    old_events: list[AnomalyEvent],
    new_events: list[AnomalyEvent],
    old_evidence_map: Dict[str, list[Evidence]],
    new_evidence_map: Dict[str, list[Evidence]],
    snapshot_id: str,
    batch_id: str,
    event_signature_map: Dict[str, AnomalyEvent],
    matched_old_ids: set,
) -> list[EventDiffRecord]:
    diffs = []
    old_events_by_id = {e.event_id: e for e in old_events}
    new_events_by_id = {e.event_id: e for e in new_events}

    matched_ids = set(old_events_by_id.keys()) & set(new_events_by_id.keys())

    for eid in matched_ids:
        old_ev = old_events_by_id[eid]
        new_ev = new_events_by_id[eid]
        has_conflict, conflict_reason = check_event_has_user_operation(old_ev)
        field_diffs = compute_field_diffs(old_ev, new_ev)
        old_ev_list = old_evidence_map.get(old_ev.event_id, [])
        new_ev_list = new_evidence_map.get(new_ev.event_id, [])
        evidence_diffs = compute_evidence_diffs(old_ev_list, new_ev_list)
        alert_changed = old_ev.carrier_alert_count != new_ev.carrier_alert_count
        change_type = None
        if field_diffs:
            change_type = ChangeType.FIELD_CHANGED.value
        elif evidence_diffs:
            change_type = ChangeType.EVIDENCE_CHANGED.value
        elif alert_changed:
            change_type = ChangeType.ALERT_CHANGED.value
        if change_type or has_conflict:
            if not change_type:
                change_type = ChangeType.FIELD_CHANGED.value
            diff = EventDiffRecord(
                snapshot_id=snapshot_id,
                batch_id=batch_id,
                event_signature=new_ev.event_signature,
                old_event_id=old_ev.event_id,
                new_event_id=new_ev.event_id,
                change_type=change_type,
                field_diffs=field_diffs,
                evidence_diffs=evidence_diffs,
                alert_count_old=old_ev.carrier_alert_count,
                alert_count_new=new_ev.carrier_alert_count,
                has_conflict=has_conflict,
                conflict_reason=conflict_reason,
            )
            diffs.append(diff)

    old_ids = set(old_events_by_id.keys())
    new_ids = set(new_events_by_id.keys())

    for eid in new_ids - old_ids:
        new_ev = new_events_by_id[eid]
        diffs.append(EventDiffRecord(
            snapshot_id=snapshot_id,
            batch_id=batch_id,
            event_signature=new_ev.event_signature,
            new_event_id=new_ev.event_id,
            change_type=ChangeType.ADDED.value,
            alert_count_new=new_ev.carrier_alert_count,
        ))

    for eid in old_ids - new_ids:
        old_ev = old_events_by_id[eid]
        if old_ev.event_id in matched_old_ids:
            continue
        has_conflict, conflict_reason = check_event_has_user_operation(old_ev)
        diffs.append(EventDiffRecord(
            snapshot_id=snapshot_id,
            batch_id=batch_id,
            event_signature=old_ev.event_signature,
            old_event_id=old_ev.event_id,
            change_type=ChangeType.REMOVED.value,
            alert_count_old=old_ev.carrier_alert_count,
            has_conflict=has_conflict,
            conflict_reason=conflict_reason,
        ))
    return diffs


def create_reanalysis_snapshot(
    batch: ImportBatch,
    config: dict,
    event_ids: list[str],
    evidence_ids: list[str],
    pre_events: list[dict] = None,
    pre_evidence: list[dict] = None,
    parent_snapshot_id: str = "",
    operator: str = "system",
) -> ReanalysisSnapshot:
    snapshot = ReanalysisSnapshot(
        batch_id=batch.batch_id,
        raw_data_hash=batch.raw_data_hash,
        config_signature=batch.config_signature,
        config_snapshot=config,
        event_ids=event_ids,
        evidence_ids=evidence_ids,
        pre_events=pre_events or [],
        pre_evidence=pre_evidence or [],
        parent_snapshot_id=parent_snapshot_id,
        operator=operator,
    )
    snapshots = load_snapshots()
    snapshots.append(snapshot)
    save_snapshots(snapshots)
    return snapshot


def _parse_ts(ts_str: str):
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _overlap_minutes(start1: str, end1: str, start2: str, end2: str) -> float:
    s1, e1 = _parse_ts(start1), _parse_ts(end1)
    s2, e2 = _parse_ts(start2), _parse_ts(end2)
    if not all([s1, e1, s2, e2]):
        return 0
    overlap_start = max(s1, s2)
    overlap_end = min(e1, e2)
    if overlap_end > overlap_start:
        return (overlap_end - overlap_start).total_seconds() / 60
    return 0


class ReanalysisConflictError(Exception):
    def __init__(self, conflicting_events: list[dict]):
        self.conflicting_events = conflicting_events
        event_list = ", ".join([f"{e['event_id']}({e['reason']})" for e in conflicting_events[:3]])
        super().__init__(
            f"发现 {len(conflicting_events)} 个事件存在用户操作记录，无法静默覆盖。"
            f"冲突事件: {event_list}{'...' if len(conflicting_events) > 3 else ''}"
        )


def update_events_for_reanalysis(
    new_events: list[AnomalyEvent],
    new_temperature_evidence: list[Evidence],
    batch: ImportBatch,
    config: dict,
    skipped_logs: list[SkippedRowLog] = None,
    force: bool = False,
    operator: str = "system",
) -> Tuple[int, int, int, int, list, str]:
    """
    Re-analyze existing raw data with new thresholds/config.
    - Preserves user review status, handler info, close time, and audit logs
    - Updates derived event fields only (time range, max temperature, duration, etc.)
    - Keeps original non-temperature evidence (receipt notes, carrier alerts) untouched
    - New events (not seen before) are created fresh
    - Detects conflicts with events that have user operations
    - Creates snapshot and diff records for tracking
    - Returns (updated_count, new_count, removed_count, conflict_count, diffs, snapshot_id)
    """
    with _lock:
        existing_events = load_events()
        existing_evidence = load_evidence()
        existing_batches = load_batches()
        existing_skipped = load_skipped_logs()
        existing_audit = load_audit_logs()

        raw_data_hash = batch.raw_data_hash

        old_events_same_raw = [e for e in existing_events if e.raw_data_hash == raw_data_hash]
        old_event_ids = {e.event_id for e in old_events_same_raw}
        old_temp_evidence_ids = {
            e.evidence_id for e in existing_evidence
            if e.evidence_type == "温度记录" and e.event_id in old_event_ids
        }

        old_non_temp_evidence = [
            e for e in existing_evidence
            if e.evidence_type != "温度记录" and e.event_id in old_event_ids
        ]

        old_evidence_by_event: Dict[str, list[Evidence]] = {}
        for e in existing_evidence:
            if e.event_id in old_event_ids:
                old_evidence_by_event.setdefault(e.event_id, []).append(e)

        old_events_by_sig: dict[str, AnomalyEvent] = {}
        old_events_by_box: dict[str, list[AnomalyEvent]] = {}
        for e in old_events_same_raw:
            if e.event_signature:
                old_events_by_sig[e.event_signature] = e
            old_events_by_box.setdefault(e.box_id, []).append(e)

        updated = 0
        new_count = 0
        removed_count = 0
        conflict_count = 0
        conflicts: list[dict] = []

        new_event_ids_map: dict[str, str] = {}
        matched_old_event_ids: set[str] = set()
        event_signature_map: dict[str, AnomalyEvent] = {}

        for new_ev in new_events:
            sig = new_ev.event_signature
            matched_old = None

            if sig in old_events_by_sig:
                matched_old = old_events_by_sig[sig]

            if matched_old is None and new_ev.box_id in old_events_by_box:
                best_overlap = 0
                best_old = None
                for old_ev in old_events_by_box[new_ev.box_id]:
                    if old_ev.event_id in matched_old_event_ids:
                        continue
                    overlap = _overlap_minutes(
                        new_ev.start_time, new_ev.end_time,
                        old_ev.start_time, old_ev.end_time
                    )
                    if overlap > best_overlap:
                        best_overlap = overlap
                        best_old = old_ev
                if best_old and best_overlap > 0:
                    matched_old = best_old

            if matched_old:
                event_audit_logs = [l for l in existing_audit if l.event_id == matched_old.event_id]
                has_conflict, conflict_reason = check_event_has_user_operation(matched_old, event_audit_logs)
                if has_conflict:
                    conflict_count += 1
                    conflicts.append({
                        "event_id": matched_old.event_id,
                        "event_signature": matched_old.event_signature,
                        "box_id": matched_old.box_id,
                        "reason": conflict_reason,
                    })
                event_signature_map[sig] = matched_old

                carrier_alert_count = new_ev.carrier_alert_count
                nearest_alert_time = new_ev.nearest_alert_time
                carrier = new_ev.carrier
                alert_types = new_ev.alert_types

                new_ev.event_id = matched_old.event_id
                new_ev.status = matched_old.status
                new_ev.handler = matched_old.handler
                new_ev.handler_remark = matched_old.handler_remark
                new_ev.close_time = matched_old.close_time
                new_ev.created_at = matched_old.created_at
                new_ev.assignee = matched_old.assignee
                new_ev.deadline = matched_old.deadline
                new_ev.priority = matched_old.priority
                new_ev.last_updated_at = matched_old.last_updated_at
                new_ev.version = matched_old.version

                new_ev.carrier_alert_count = carrier_alert_count
                new_ev.nearest_alert_time = nearest_alert_time
                new_ev.carrier = carrier
                new_ev.alert_types = alert_types

                new_event_ids_map[sig] = matched_old.event_id
                matched_old_event_ids.add(matched_old.event_id)
                updated += 1
            else:
                new_count += 1

        for sig, old_ev in old_events_by_sig.items():
            if old_ev.event_id not in matched_old_event_ids:
                removed_count += 1
                event_audit_logs = [l for l in existing_audit if l.event_id == old_ev.event_id]
                has_conflict, conflict_reason = check_event_has_user_operation(old_ev, event_audit_logs)
                if has_conflict:
                    conflict_count += 1
                    conflicts.append({
                        "event_id": old_ev.event_id,
                        "event_signature": old_ev.event_signature,
                        "box_id": old_ev.box_id,
                        "reason": conflict_reason,
                    })

        if conflicts and not force:
            raise ReanalysisConflictError(conflicts)

        for e in new_temperature_evidence:
            sig = None
            for ne in new_events:
                if ne.event_id == e.event_id:
                    sig = ne.event_signature
                    break
            if sig and sig in new_event_ids_map:
                e.event_id = new_event_ids_map[sig]

        final_evidence = [
            e for e in existing_evidence
            if e.evidence_id not in old_temp_evidence_ids
        ]
        final_evidence.extend(new_temperature_evidence)

        for e in new_events:
            temp_ids = [
                evid.evidence_id for evid in new_temperature_evidence
                if evid.event_id == e.event_id
            ]
            non_temp_ids = [
                evid.evidence_id for evid in old_non_temp_evidence
                if evid.event_id == e.event_id
            ]
            e.evidence_ids = temp_ids + non_temp_ids

        remaining_old_events = [
            e for e in existing_events if e.raw_data_hash != raw_data_hash
        ]
        unchanged = len(remaining_old_events)
        final_events = remaining_old_events + new_events

        existing_batches.append(batch)
        if skipped_logs:
            existing_skipped.extend(skipped_logs)

        old_snapshot = get_latest_snapshot_for_raw_data(raw_data_hash)
        parent_snapshot_id = old_snapshot.snapshot_id if old_snapshot else ""

        old_event_ids_list = [e.event_id for e in old_events_same_raw]
        old_evidence_ids_list = list(old_temp_evidence_ids)

        new_event_ids_list = [e.event_id for e in new_events]
        new_evidence_ids_list = [e.evidence_id for e in new_temperature_evidence]

        save_events(final_events)
        save_evidence(final_evidence)
        save_batches(existing_batches)
        save_skipped_logs(existing_skipped)

        pre_events_dicts = [e.to_dict() for e in old_events_same_raw]
        pre_temp_evidence = [
            e for e in existing_evidence
            if e.evidence_id in old_temp_evidence_ids
        ]
        pre_evidence_dicts = [e.to_dict() for e in pre_temp_evidence]

        snapshot = create_reanalysis_snapshot(
            batch=batch,
            config=config,
            event_ids=new_event_ids_list + old_event_ids_list,
            evidence_ids=new_evidence_ids_list + old_evidence_ids_list,
            pre_events=pre_events_dicts,
            pre_evidence=pre_evidence_dicts,
            parent_snapshot_id=parent_snapshot_id,
            operator=operator,
        )

        new_evidence_by_event: Dict[str, list[Evidence]] = {}
        for e in new_temperature_evidence:
            new_evidence_by_event.setdefault(e.event_id, []).append(e)

        diffs = compute_reanalysis_diffs(
            old_events=old_events_same_raw,
            new_events=new_events,
            old_evidence_map=old_evidence_by_event,
            new_evidence_map=new_evidence_by_event,
            snapshot_id=snapshot.snapshot_id,
            batch_id=batch.batch_id,
            event_signature_map=event_signature_map,
            matched_old_ids=matched_old_event_ids,
        )

        existing_diffs = load_diffs()
        existing_diffs.extend(diffs)
        save_diffs(existing_diffs)

        for conflict in conflicts:
            _append_audit_log(
                existing_audit,
                event_id=conflict["event_id"],
                action="重分析冲突检测",
                operator=operator,
                remark=f"重分析批次 {batch.batch_id} 检测到冲突: {conflict['reason']}",
            )
        save_audit_logs(existing_audit)

        return updated, new_count, removed_count, conflict_count, diffs, snapshot.snapshot_id


def rollback_last_reanalysis(raw_data_hash: str, operator: str = "system") -> Tuple[bool, int, str]:
    """
    Rollback the most recent reanalysis for a given raw_data_hash.
    - Restores events and evidence to the state before the last reanalysis
    - Preserves user operation audit logs
    - Returns (success, rolled_back_event_count, snapshot_id)
    """
    with _lock:
        existing_events = load_events()
        existing_evidence = load_evidence()
        existing_batches = load_batches()
        existing_snapshots = load_snapshots()
        existing_diffs = load_diffs()
        existing_audit = load_audit_logs()

        snapshots_for_hash = [
            s for s in existing_snapshots if s.raw_data_hash == raw_data_hash
        ]
        if not snapshots_for_hash:
            return False, 0, ""

        latest_snapshot = snapshots_for_hash[-1]

        pre_events_dicts = latest_snapshot.pre_events
        pre_evidence_dicts = latest_snapshot.pre_evidence

        if not pre_events_dicts:
            return False, 0, latest_snapshot.snapshot_id

        pre_event_ids = {e["event_id"] for e in pre_events_dicts}

        other_events = [e for e in existing_events if e.raw_data_hash != raw_data_hash]
        pre_events = []
        for ed in pre_events_dicts:
            ev = AnomalyEvent()
            for f in dataclass_fields(AnomalyEvent):
                if f.name in ed:
                    setattr(ev, f.name, ed[f.name])
            pre_events.append(ev)

        final_events = other_events + pre_events
        rolled_back_count = len(pre_events)

        other_evidence = [
            e for e in existing_evidence
            if e.event_id not in pre_event_ids or e.evidence_type != "温度记录"
        ]
        pre_evidence = []
        for ed in pre_evidence_dicts:
            ev = Evidence()
            for f in dataclass_fields(Evidence):
                if f.name in ed:
                    setattr(ev, f.name, ed[f.name])
            pre_evidence.append(ev)

        final_evidence = other_evidence + pre_evidence

        batch_for_snapshot = next(
            (b for b in existing_batches if b.batch_id == latest_snapshot.batch_id),
            None
        )
        if batch_for_snapshot:
            existing_batches = [b for b in existing_batches if b.batch_id != latest_snapshot.batch_id]

        diffs_for_snapshot = [
            d for d in existing_diffs if d.snapshot_id == latest_snapshot.snapshot_id
        ]

        for d in diffs_for_snapshot:
            event_id = d.old_event_id or d.new_event_id
            if event_id:
                _append_audit_log(
                    existing_audit,
                    event_id=event_id,
                    action="重分析回滚",
                    operator=operator,
                    remark=f"回滚重分析快照 {latest_snapshot.snapshot_id}，变更类型: {d.change_type}",
                )

        existing_snapshots = [
            s for s in existing_snapshots if s.snapshot_id != latest_snapshot.snapshot_id
        ]
        existing_diffs = [
            d for d in existing_diffs if d.snapshot_id != latest_snapshot.snapshot_id
        ]

        save_events(final_events)
        save_evidence(final_evidence)
        save_batches(existing_batches)
        save_snapshots(existing_snapshots)
        save_diffs(existing_diffs)
        save_audit_logs(existing_audit)

        return True, rolled_back_count, latest_snapshot.snapshot_id


def get_diff_summary(batch_id: str = "", snapshot_id: str = "") -> Dict[str, Any]:
    """
    Get summary of diffs for a batch or snapshot.
    Returns counts by change type and conflict information.
    """
    if snapshot_id:
        diffs = get_diffs_by_snapshot_id(snapshot_id)
    elif batch_id:
        diffs = get_diffs_by_batch_id(batch_id)
    else:
        diffs = load_diffs()

    summary = {
        "total_diffs": len(diffs),
        "added": sum(1 for d in diffs if d.change_type == ChangeType.ADDED.value),
        "removed": sum(1 for d in diffs if d.change_type == ChangeType.REMOVED.value),
        "field_changed": sum(1 for d in diffs if d.change_type == ChangeType.FIELD_CHANGED.value),
        "evidence_changed": sum(1 for d in diffs if d.change_type == ChangeType.EVIDENCE_CHANGED.value),
        "alert_changed": sum(1 for d in diffs if d.change_type == ChangeType.ALERT_CHANGED.value),
        "conflicts": sum(1 for d in diffs if d.has_conflict),
    }
    return summary


def get_evidence_for_event(event_id: str) -> list[Evidence]:
    all_ev = load_evidence()
    return [e for e in all_ev if e.event_id == event_id]


def get_audit_logs_for_event(event_id: str) -> list[AuditLog]:
    all_logs = load_audit_logs()
    return [l for l in all_logs if l.event_id == event_id]


def get_skipped_logs_for_batch(batch_id: str) -> list[SkippedRowLog]:
    all_logs = load_skipped_logs()
    return [l for l in all_logs if l.batch_id == batch_id]


def get_diffs_by_change_type(
    batch_id: str = "",
    snapshot_id: str = "",
    change_types: Optional[list[str]] = None,
    include_conflicts_only: bool = False,
) -> list[EventDiffRecord]:
    """
    Get diffs filtered by change type and conflict status.
    """
    if snapshot_id:
        diffs = get_diffs_by_snapshot_id(snapshot_id)
    elif batch_id:
        diffs = get_diffs_by_batch_id(batch_id)
    else:
        diffs = load_diffs()

    if change_types:
        diffs = [d for d in diffs if d.change_type in change_types]
    if include_conflicts_only:
        diffs = [d for d in diffs if d.has_conflict]
    return diffs


def clear_all_for_test():
    """Only for testing purposes."""
    with _lock:
        for path in [
            _EVENTS_FILE, _EVIDENCE_FILE, _AUDIT_FILE, _BATCHES_FILE,
            _SKIPPED_FILE, _SNAPSHOTS_FILE, _DIFFS_FILE
        ]:
            if os.path.exists(path):
                os.remove(path)
