import json
import os
import threading
from datetime import datetime
from typing import Optional, Tuple

from .models import AnomalyEvent, AuditLog, Evidence, ImportBatch, SkippedRowLog

_BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "store")
_EVENTS_FILE = os.path.join(_BASE_DIR, "events.json")
_EVIDENCE_FILE = os.path.join(_BASE_DIR, "evidence.json")
_AUDIT_FILE = os.path.join(_BASE_DIR, "audit_log.json")
_BATCHES_FILE = os.path.join(_BASE_DIR, "batches.json")
_SKIPPED_FILE = os.path.join(_BASE_DIR, "skipped_rows.json")

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


def load_events() -> list[AnomalyEvent]:
    data = _read_json(_EVENTS_FILE)
    return [AnomalyEvent(**d) for d in data]


def save_events(events: list[AnomalyEvent]):
    _write_json(_EVENTS_FILE, [e.to_dict() for e in events])


def load_evidence() -> list[Evidence]:
    data = _read_json(_EVIDENCE_FILE)
    return [Evidence(**d) for d in data]


def save_evidence(evidences: list[Evidence]):
    _write_json(_EVIDENCE_FILE, [e.to_dict() for e in evidences])


def load_audit_logs() -> list[AuditLog]:
    data = _read_json(_AUDIT_FILE)
    return [AuditLog(**d) for d in data]


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


def update_event(event_id: str, status: str, handler: str, remark: str, close_time: str = ""):
    with _lock:
        events = load_events()
        audit_logs = load_audit_logs()
        for ev in events:
            if ev.event_id == event_id:
                old_status = ev.status
                ev.status = status
                ev.handler = handler
                ev.handler_remark = remark
                if status == "已关闭" and not ev.close_time:
                    from datetime import datetime
                    ev.close_time = close_time or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log = AuditLog(
                    event_id=event_id,
                    action=f"状态变更: {old_status} -> {status}",
                    operator=handler,
                    remark=remark,
                )
                audit_logs.append(log)
                break
        save_events(events)
        save_audit_logs(audit_logs)


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


def update_events_for_reanalysis(
    new_events: list[AnomalyEvent],
    new_temperature_evidence: list[Evidence],
    batch: ImportBatch,
    skipped_logs: list[SkippedRowLog] = None,
) -> Tuple[int, int, int]:
    """
    Re-analyze existing raw data with new thresholds/config.
    - Preserves user review status, handler info, close time, and audit logs
    - Updates derived event fields only (time range, max temperature, duration, etc.)
    - Keeps original non-temperature evidence (receipt notes, carrier alerts) untouched
    - New events (not seen before) are created fresh
    - Returns (updated_count, new_count, unchanged_count)
    """
    with _lock:
        existing_events = load_events()
        existing_evidence = load_evidence()
        existing_batches = load_batches()
        existing_skipped = load_skipped_logs()

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

        old_events_by_sig: dict[str, AnomalyEvent] = {}
        old_events_by_box: dict[str, list[AnomalyEvent]] = {}
        for e in old_events_same_raw:
            if e.event_signature:
                old_events_by_sig[e.event_signature] = e
            old_events_by_box.setdefault(e.box_id, []).append(e)

        updated = 0
        new_count = 0
        unchanged = 0

        new_event_ids_map: dict[str, str] = {}
        matched_old_event_ids: set[str] = set()

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
                new_ev.event_id = matched_old.event_id
                new_ev.status = matched_old.status
                new_ev.handler = matched_old.handler
                new_ev.handler_remark = matched_old.handler_remark
                new_ev.close_time = matched_old.close_time
                new_ev.created_at = matched_old.created_at
                new_event_ids_map[sig] = matched_old.event_id
                matched_old_event_ids.add(matched_old.event_id)
                updated += 1
            else:
                new_count += 1

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

        save_events(final_events)
        save_evidence(final_evidence)
        save_batches(existing_batches)
        save_skipped_logs(existing_skipped)

        return updated, new_count, unchanged


def get_evidence_for_event(event_id: str) -> list[Evidence]:
    all_ev = load_evidence()
    return [e for e in all_ev if e.event_id == event_id]


def get_audit_logs_for_event(event_id: str) -> list[AuditLog]:
    all_logs = load_audit_logs()
    return [l for l in all_logs if l.event_id == event_id]


def get_skipped_logs_for_batch(batch_id: str) -> list[SkippedRowLog]:
    all_logs = load_skipped_logs()
    return [l for l in all_logs if l.batch_id == batch_id]


def clear_all_for_test():
    """Only for testing purposes."""
    with _lock:
        for path in [_EVENTS_FILE, _EVIDENCE_FILE, _AUDIT_FILE, _BATCHES_FILE, _SKIPPED_FILE]:
            if os.path.exists(path):
                os.remove(path)
