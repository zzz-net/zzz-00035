from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Optional
import uuid


class EventStatus(str, Enum):
    PENDING = "待处理"
    CONFIRMED = "已确认"
    FALSE_ALARM = "误报"
    CLOSED = "已关闭"


class EvidenceType(str, Enum):
    TEMPERATURE_RECORD = "温度记录"
    RECEIPT_NOTE = "收货备注"
    CARRIER_ALERT = "承运商告警"


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
    batch_id: str = ""
    raw_data_hash: str = ""
    config_signature: str = ""
    event_signature: str = ""
    created_at: str = field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    evidence_ids: list = field(default_factory=list)

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
