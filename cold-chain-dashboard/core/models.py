from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional, Any
import uuid


class EventStatus(str, Enum):
    PENDING = "待处理"
    CONFIRMED = "已确认"
    FALSE_ALARM = "误报"
    CLOSED = "已关闭"


class Priority(str, Enum):
    LOW = "低"
    MEDIUM = "中"
    HIGH = "高"
    URGENT = "紧急"


class EvidenceType(str, Enum):
    TEMPERATURE_RECORD = "温度记录"
    RECEIPT_NOTE = "收货备注"
    CARRIER_ALERT = "承运商告警"
    QC_INSPECTION = "到货质检"
    HANDOVER = "交接记录"


class ChangeType(str, Enum):
    ADDED = "新增"
    REMOVED = "消失"
    FIELD_CHANGED = "字段变更"
    EVIDENCE_CHANGED = "证据变更"
    ALERT_CHANGED = "告警匹配变更"


@dataclass
class ReanalysisSnapshot:
    snapshot_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    batch_id: str = ""
    raw_data_hash: str = ""
    config_signature: str = ""
    config_snapshot: dict = field(default_factory=dict)
    event_ids: list = field(default_factory=list)
    evidence_ids: list = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    parent_snapshot_id: str = ""
    operator: str = "system"
    pre_events: list = field(default_factory=list)
    pre_evidence: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


@dataclass
class FieldDiff:
    field_name: str = ""
    old_value: str = ""
    new_value: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class EvidenceDiff:
    evidence_id: str = ""
    change_type: str = ""
    evidence_type: str = ""
    old_detail: str = ""
    new_detail: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class EventDiffRecord:
    diff_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    snapshot_id: str = ""
    batch_id: str = ""
    event_signature: str = ""
    old_event_id: str = ""
    new_event_id: str = ""
    change_type: str = ""
    field_diffs: list = field(default_factory=list)
    evidence_diffs: list = field(default_factory=list)
    alert_count_old: int = 0
    alert_count_new: int = 0
    has_conflict: bool = False
    conflict_reason: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return {
            **asdict(self),
            "field_diffs": [fd.to_dict() for fd in self.field_diffs],
            "evidence_diffs": [ed.to_dict() for ed in self.evidence_diffs],
        }


@dataclass
class Evidence:
    evidence_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    event_id: str = ""
    evidence_type: str = ""
    source_file: str = ""
    box_id: str = ""
    timestamp: str = ""
    detail: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class AuditLog:
    log_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    event_id: str = ""
    action: str = ""
    operator: str = ""
    remark: str = ""
    field_changed: str = ""
    old_value: str = ""
    new_value: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return asdict(self)


@dataclass
class AnomalyEvent:
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    box_id: str = ""
    start_time: str = ""
    end_time: str = ""
    max_temperature: float = 0.0
    duration_minutes: int = 0
    status: str = EventStatus.PENDING.value
    handler: str = ""
    handler_remark: str = ""
    close_time: str = ""
    assignee: str = ""
    deadline: str = ""
    priority: str = Priority.MEDIUM.value
    last_updated_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    version: int = 1
    batch_id: str = ""
    raw_data_hash: str = ""
    config_signature: str = ""
    event_signature: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    evidence_ids: list = field(default_factory=list)
    carrier_alert_count: int = 0
    nearest_alert_time: str = ""
    carrier: str = ""
    alert_types: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class ImportBatch:
    batch_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    import_time: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    file_name: str = ""
    file_hash: str = ""
    raw_data_hash: str = ""
    config_signature: str = ""
    row_count: int = 0
    skipped_rows: int = 0
    status: str = ""
    is_reanalysis: bool = False

    def to_dict(self):
        return asdict(self)


@dataclass
class SkippedRowLog:
    log_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    batch_id: str = ""
    row_number: int = 0
    reason: str = ""
    box_id: str = ""
    timestamp_raw: str = ""
    temperature_raw: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return asdict(self)


@dataclass
class QCInspection:
    inspection_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    box_id: str = ""
    inspection_time: str = ""
    appearance_result: str = ""
    thermometer_reading: str = ""
    disposal_suggestion: str = ""
    event_id: str = ""
    qc_batch_id: str = ""
    operator: str = ""
    version: int = 1
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    last_updated_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return asdict(self)


@dataclass
class QCImportBatch:
    qc_batch_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    import_time: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    file_name: str = ""
    file_hash: str = ""
    row_count: int = 0
    valid_count: int = 0
    skipped_rows: int = 0
    status: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class QCSkippedRowLog:
    log_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    qc_batch_id: str = ""
    row_number: int = 0
    reason: str = ""
    box_id: str = ""
    inspection_time_raw: str = ""
    appearance_result_raw: str = ""
    thermometer_reading_raw: str = ""
    disposal_suggestion_raw: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return asdict(self)


@dataclass
class QCUndoRecord:
    undo_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    qc_batch_id: str = ""
    undone_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    operator: str = ""
    inspection_count: int = 0
    pre_inspections: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)


@dataclass
class HandoverRecord:
    handover_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    box_id: str = ""
    handover_time: str = ""
    handover_point: str = ""
    handover_temperature: float = 0.0
    handover_person: str = ""
    remark: str = ""
    event_id: str = ""
    handover_batch_id: str = ""
    operator: str = ""
    version: int = 1
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    last_updated_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return asdict(self)


@dataclass
class HandoverImportBatch:
    handover_batch_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    import_time: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    file_name: str = ""
    file_hash: str = ""
    row_count: int = 0
    valid_count: int = 0
    skipped_rows: int = 0
    status: str = ""

    def to_dict(self):
        return asdict(self)


@dataclass
class HandoverSkippedRowLog:
    log_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    handover_batch_id: str = ""
    row_number: int = 0
    reason: str = ""
    box_id: str = ""
    handover_time_raw: str = ""
    handover_point_raw: str = ""
    handover_temperature_raw: str = ""
    handover_person_raw: str = ""
    remark_raw: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def to_dict(self):
        return asdict(self)


@dataclass
class HandoverUndoRecord:
    undo_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    handover_batch_id: str = ""
    undone_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    operator: str = ""
    handover_count: int = 0
    pre_handovers: list = field(default_factory=list)

    def to_dict(self):
        return asdict(self)
