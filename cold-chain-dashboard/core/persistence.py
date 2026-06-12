import json
import os
import threading
from typing import Optional

from .models import AnomalyEvent, AuditLog, Evidence, ImportBatch

_BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "store")
_EVENTS_FILE = os.path.join(_BASE_DIR, "events.json")
_EVIDENCE_FILE = os.path.join(_BASE_DIR, "evidence.json")
_AUDIT_FILE = os.path.join(_BASE_DIR, "audit_log.json")
_BATCHES_FILE = os.path.join(_BASE_DIR, "batches.json")

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


def add_events(new_events: list[AnomalyEvent], new_evidence: list[Evidence], batch: ImportBatch):
    with _lock:
        existing_events = load_events()
        existing_evidence = load_evidence()
        existing_batches = load_batches()

        existing_events.extend(new_events)
        existing_evidence.extend(new_evidence)
        existing_batches.append(batch)

        save_events(existing_events)
        save_evidence(existing_evidence)
        save_batches(existing_batches)


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


def get_evidence_for_event(event_id: str) -> list[Evidence]:
    all_ev = load_evidence()
    return [e for e in all_ev if e.event_id == event_id]


def get_audit_logs_for_event(event_id: str) -> list[AuditLog]:
    all_logs = load_audit_logs()
    return [l for l in all_logs if l.event_id == event_id]
