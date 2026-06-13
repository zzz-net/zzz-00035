import io
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime

import pandas as pd
import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

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
    clear_all_for_test,
    get_audit_logs_for_event,
    get_evidence_for_event,
    get_event_by_id,
    get_events_by_raw_data_hash,
    get_skipped_logs_for_batch,
    is_exact_duplicate_batch,
    load_audit_logs,
    load_batches,
    load_events,
    load_evidence,
    load_skipped_logs,
    save_audit_logs,
    save_batches,
    save_events,
    update_event,
    update_event_assignment,
    update_events_for_reanalysis,
    VersionConflictError,
)


class TestBase(unittest.TestCase):
    def setUp(self):
        clear_all_for_test()
        self.config = {
            "thresholds": {
                "temperature_upper_limit": -15.0,
                "continuous_over_temp_minutes": 5,
                "breakpoint_interval_minutes": 10,
                "merge_window_minutes": 30,
            },
            "validation": {
                "allow_missing_box_id": True,
                "skip_invalid_timestamp_rows": True,
                "reject_duplicate_batch": True,
            },
            "export": {"default_encoding": "utf-8-sig"},
        }

        self.temp_csv_content = """box_id,timestamp,temperature_c
BX-001,2025-06-10 08:00:00,-18.5
BX-001,2025-06-10 08:05:00,-17.2
BX-001,2025-06-10 08:10:00,-14.0
BX-001,2025-06-10 08:15:00,-12.3
BX-001,2025-06-10 08:20:00,-11.0
BX-001,2025-06-10 08:25:00,-10.5
BX-001,2025-06-10 08:30:00,-18.0
BX-002,2025-06-10 09:00:00,-17.8
BX-002,2025-06-10 09:05:00,-16.5
BX-002,2025-06-10 09:10:00,-14.8
BX-002,2025-06-10 09:15:00,-13.1
BX-002,2025-06-10 09:20:00,-14.5
BX-002,2025-06-10 09:25:00,-17.0
,2025-06-10 10:00:00,-19.0
bad-row,bad-timestamp,-17.0
BX-005,2025-06-10 10:10:00,bad-temp
BX-004,2025-06-10 11:00:00,-14.0
BX-004,2025-06-10 11:05:00,-12.0
BX-004,2025-06-10 11:10:00,-10.0
BX-004,2025-06-10 11:15:00,-8.0
BX-004,2025-06-10 11:20:00,-6.0
BX-004,2025-06-10 11:25:00,-5.0
BX-004,2025-06-10 11:30:00,-13.0
"""

        self.receipt_csv_content = """box_id,arrival_time,receiver,remark
BX-001,2025-06-10 08:02:00,张三,外观正常
BX-002,2025-06-10 09:03:00,李四,封条完好
BX-004,2025-06-10 11:02:00,赵六,温度计显示偏高
"""

        self.alert_json_content = """[
  {"carrier": "顺丰冷链", "alert_time": "2025-06-10 08:18:00", "box_id": "BX-001", "alert_type": "temperature_exceeded", "message": "BX-001 温度超过阈值"},
  {"carrier": "京东冷链", "alert_time": "2025-06-10 11:12:00", "box_id": "BX-004", "alert_type": "temperature_exceeded", "message": "BX-004 持续升温"}
]"""

    def tearDown(self):
        clear_all_for_test()


class TestBadTimestampHandling(TestBase):
    """Test skip_invalid_timestamp_rows configuration works correctly."""

    def test_skip_bad_ts_true_skips_and_logs(self):
        """When skip_invalid_timestamp_rows=true, bad rows should be skipped and logged."""
        self.config["validation"]["skip_invalid_timestamp_rows"] = True
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="test-batch-1")

        self.assertEqual(len(valid_rows), 21)
        self.assertEqual(len(skipped_logs), 2)

        reasons = [l.reason for l in skipped_logs]
        self.assertTrue(any("时间戳解析失败: bad-timestamp" in r for r in reasons))
        self.assertTrue(any("温度值无法解析: bad-temp" in r for r in reasons))

        row_numbers = [l.row_number for l in skipped_logs]
        self.assertIn(16, row_numbers)
        self.assertIn(17, row_numbers)

        unknown_rows = [r for r in valid_rows if r["box_id"] == "UNKNOWN"]
        self.assertEqual(len(unknown_rows), 1)

        for log in skipped_logs:
            self.assertEqual(log.batch_id, "test-batch-1")
            self.assertIsNotNone(log.log_id)

    def test_skip_bad_ts_false_raises_clear_error(self):
        """When skip_invalid_timestamp_rows=false, bad timestamp should block import with clear error."""
        self.config["validation"]["skip_invalid_timestamp_rows"] = False
        df = parse_temperature_csv(self.temp_csv_content.encode())

        with self.assertRaises(InvalidTimestampError) as ctx:
            validate_temperature_rows(df, self.config, batch_id="test-batch-2")

        error_msg = str(ctx.exception)
        self.assertIn("第 16 行", error_msg)
        self.assertIn("bad-timestamp", error_msg)
        self.assertIn("YYYY-MM-DD HH:MM:SS", error_msg)
        self.assertIn("skip_invalid_timestamp_rows", error_msg)

        self.assertIsNotNone(ctx.exception.details)
        self.assertEqual(ctx.exception.details["row_number"], 16)
        self.assertEqual(ctx.exception.details["box_id"], "bad-row")
        self.assertEqual(ctx.exception.details["timestamp_raw"], "bad-timestamp")
        self.assertEqual(ctx.exception.details["temperature_raw"], "-17.0")

    def test_skip_bad_ts_true_persists_logs(self):
        """Skipped row logs should be persisted and retrievable."""
        self.config["validation"]["skip_invalid_timestamp_rows"] = True
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="test-batch-3")

        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig = compute_config_hash(self.config)
        events, evidences = generate_events(
            valid_rows, self.config, "test-batch-3", "test.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig,
        )
        batch = ImportBatch(
            batch_id="test-batch-3",
            file_name="test.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
        )
        add_events(events, evidences, batch, skipped_logs)

        retrieved = get_skipped_logs_for_batch("test-batch-3")
        self.assertEqual(len(retrieved), len(skipped_logs))
        self.assertEqual(retrieved[0].batch_id, "test-batch-3")

        all_skipped = load_skipped_logs()
        self.assertEqual(len(all_skipped), len(skipped_logs))


class TestReimportAndReanalysis(TestBase):
    """Test deduplication and re-analysis after threshold changes."""

    def _import_first_time(self):
        """Helper: import data for the first time."""
        df = parse_temperature_csv(self.temp_csv_content.encode())
        receipt_df = parse_receipt_csv(self.receipt_csv_content.encode())
        alerts = parse_carrier_alerts(self.alert_json_content.encode())

        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="batch-v1")
        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig = compute_config_hash(self.config)

        events, temp_evidence = generate_events(
            valid_rows, self.config, "batch-v1", "temp.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig,
        )
        receipt_evidence = link_receipt_evidence(events, receipt_df, "batch-v1", "receipt.csv")
        alert_evidence = link_alert_evidence(events, alerts, "batch-v1", "alerts.json")

        batch = ImportBatch(
            batch_id="batch-v1",
            file_name="temp.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
        )
        all_evidence = temp_evidence + receipt_evidence + alert_evidence
        add_events(events, all_evidence, batch, skipped_logs)

        reloaded_events = load_events()
        events_map = {e.event_id: e for e in reloaded_events}
        events_with_evidence = []
        for e in events:
            if e.event_id in events_map:
                events_with_evidence.append(events_map[e.event_id])
            else:
                events_with_evidence.append(e)

        return events_with_evidence, all_evidence, raw_data_hash, config_sig, valid_rows

    def test_exact_duplicate_rejected(self):
        """Same data + same config should be rejected as exact duplicate."""
        self._import_first_time()

        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, _ = validate_temperature_rows(df, self.config, batch_id="batch-v2")
        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig = compute_config_hash(self.config)

        self.assertTrue(is_exact_duplicate_batch(raw_data_hash, config_sig))

    def test_threshold_change_allows_reanalysis(self):
        """Same data + different config should allow re-analysis."""
        events, all_evidence, raw_data_hash, old_config_sig, valid_rows = self._import_first_time()

        self.config["thresholds"]["temperature_upper_limit"] = -18.0
        new_config_sig = compute_config_hash(self.config)

        self.assertNotEqual(old_config_sig, new_config_sig)
        self.assertFalse(is_exact_duplicate_batch(raw_data_hash, new_config_sig))

    def test_reanalysis_preserves_review_status(self):
        """Re-analysis should preserve status, handler, close_time, and audit logs."""
        events, all_evidence, raw_data_hash, old_config_sig, valid_rows = self._import_first_time()

        event_to_review = events[0]
        update_event(
            event_id=event_to_review.event_id,
            status=EventStatus.CONFIRMED.value,
            handler="测试员",
            remark="确认超温",
        )
        event_2 = events[1] if len(events) > 1 else events[0]
        update_event(
            event_id=event_2.event_id,
            status=EventStatus.CLOSED.value,
            handler="主管",
            remark="已处理完毕",
            close_time="2025-06-11 10:00:00",
        )

        old_audit = get_audit_logs_for_event(event_to_review.event_id)
        self.assertGreaterEqual(len(old_audit), 2)
        operators = [l.operator for l in old_audit]
        self.assertIn("测试员", operators)

        old_evidence_before = get_evidence_for_event(event_to_review.event_id)
        old_evidence_count = len(load_evidence())
        old_temp_evidence_ids = [
            e.evidence_id for e in old_evidence_before
            if e.evidence_type == "温度记录"
        ]
        old_non_temp_ids = [
            e.evidence_id for e in old_evidence_before
            if e.evidence_type != "温度记录"
        ]
        self.assertTrue(len(old_non_temp_ids) > 0)

        self.config["thresholds"]["temperature_upper_limit"] = -18.0
        new_config_sig = compute_config_hash(self.config)

        new_events, new_temp_evidence = generate_events(
            valid_rows, self.config, "batch-v2", "temp.csv",
            raw_data_hash=raw_data_hash, config_signature=new_config_sig,
        )

        batch_v2 = ImportBatch(
            batch_id="batch-v2",
            file_name="temp.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=new_config_sig,
            row_count=23,
            skipped_rows=2,
            status="成功",
            is_reanalysis=True,
        )

        updated, new_count, unchanged = update_events_for_reanalysis(
            new_events, new_temp_evidence, batch_v2, []
        )

        self.assertGreater(updated, 0)

        final_events = load_events()
        final_event_ids = {e.event_id for e in final_events}
        self.assertIn(event_to_review.event_id, final_event_ids)

        updated_event = next(e for e in final_events if e.event_id == event_to_review.event_id)
        self.assertEqual(updated_event.status, EventStatus.CONFIRMED.value)
        self.assertEqual(updated_event.handler, "测试员")
        self.assertEqual(updated_event.handler_remark, "确认超温")
        self.assertEqual(updated_event.config_signature, new_config_sig)
        self.assertEqual(updated_event.raw_data_hash, raw_data_hash)

        if event_2.event_id != event_to_review.event_id:
            updated_event_2 = next(e for e in final_events if e.event_id == event_2.event_id)
            self.assertEqual(updated_event_2.status, EventStatus.CLOSED.value)
            self.assertEqual(updated_event_2.handler, "主管")
            self.assertEqual(updated_event_2.close_time, "2025-06-11 10:00:00")

        audit_after = get_audit_logs_for_event(event_to_review.event_id)
        self.assertEqual(len(audit_after), len(old_audit))
        operators_after = [l.operator for l in audit_after]
        self.assertIn("测试员", operators_after)
        remarks_after = [l.remark for l in audit_after]
        self.assertIn("确认超温", remarks_after)

        evidence_after = get_evidence_for_event(event_to_review.event_id)
        after_non_temp_ids = [
            e.evidence_id for e in evidence_after
            if e.evidence_type != "温度记录"
        ]
        self.assertEqual(sorted(after_non_temp_ids), sorted(old_non_temp_ids))

        new_temp_evidence_ids = [
            e.evidence_id for e in evidence_after
            if e.evidence_type == "温度记录"
        ]
        for old_id in old_temp_evidence_ids:
            self.assertNotIn(old_id, new_temp_evidence_ids)

        old_audit_count = len(load_audit_logs())
        self.assertEqual(old_audit_count, len(audit_after) + len(get_audit_logs_for_event(event_2.event_id)))

    def test_reanalysis_does_not_duplicate_evidence(self):
        """Re-analysis should not duplicate non-temperature evidence."""
        events, all_evidence, raw_data_hash, old_config_sig, valid_rows = self._import_first_time()

        non_temp_count_before = sum(
            1 for e in load_evidence() if e.evidence_type != "温度记录"
        )
        self.assertGreater(non_temp_count_before, 0)

        self.config["thresholds"]["merge_window_minutes"] = 60
        new_config_sig = compute_config_hash(self.config)
        new_events, new_temp_evidence = generate_events(
            valid_rows, self.config, "batch-v2", "temp.csv",
            raw_data_hash=raw_data_hash, config_signature=new_config_sig,
        )
        batch_v2 = ImportBatch(
            batch_id="batch-v2",
            file_name="temp.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=new_config_sig,
            row_count=23,
            skipped_rows=2,
            status="成功",
            is_reanalysis=True,
        )
        update_events_for_reanalysis(new_events, new_temp_evidence, batch_v2, [])

        non_temp_count_after = sum(
            1 for e in load_evidence() if e.evidence_type != "温度记录"
        )
        self.assertEqual(non_temp_count_before, non_temp_count_after)


class TestRestartConsistency(TestBase):
    """Test that data survives restart and export is consistent."""

    def test_restart_preserves_events_evidence_logs(self):
        """After restart (reloading from JSON), all data should be identical."""
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="batch-restart")
        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig = compute_config_hash(self.config)

        events, temp_evidence = generate_events(
            valid_rows, self.config, "batch-restart", "test.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig,
        )

        batch = ImportBatch(
            batch_id="batch-restart",
            file_name="test.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
        )
        add_events(events, temp_evidence, batch, skipped_logs)

        reloaded_events = load_events()
        ev = reloaded_events[0]
        update_event(ev.event_id, EventStatus.CONFIRMED.value, "测试员", "测试备注")

        events_before = load_events()
        evidence_before = load_evidence()
        audit_before = load_audit_logs()
        batches_before = load_batches()
        skipped_before = load_skipped_logs()

        ev_before = next(e for e in events_before if e.event_id == ev.event_id)
        self.assertEqual(ev_before.status, EventStatus.CONFIRMED.value)
        self.assertEqual(ev_before.handler, "测试员")
        self.assertEqual(ev_before.handler_remark, "测试备注")

        events_after_reload = load_events()
        evidence_after_reload = load_evidence()
        audit_after_reload = load_audit_logs()
        batches_after_reload = load_batches()
        skipped_after_reload = load_skipped_logs()

        self.assertEqual(len(events_before), len(events_after_reload))
        self.assertEqual(len(evidence_before), len(evidence_after_reload))
        self.assertEqual(len(audit_before), len(audit_after_reload))
        self.assertEqual(len(batches_before), len(batches_after_reload))
        self.assertEqual(len(skipped_before), len(skipped_after_reload))

        ev_after = next(e for e in events_after_reload if e.event_id == ev.event_id)
        self.assertEqual(ev_after.status, ev_before.status)
        self.assertEqual(ev_after.handler, ev_before.handler)
        self.assertEqual(ev_after.handler_remark, ev_before.handler_remark)
        self.assertEqual(ev_after.close_time, ev_before.close_time)
        self.assertEqual(ev_after.raw_data_hash, ev_before.raw_data_hash)
        self.assertEqual(ev_after.config_signature, ev_before.config_signature)

    def test_csv_json_export_consistency_after_restart(self):
        """Exported CSV/JSON should have same event status, evidence, and logs after restart."""
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="batch-export")
        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig = compute_config_hash(self.config)
        events, temp_evidence = generate_events(
            valid_rows, self.config, "batch-export", "test.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig,
        )

        batch = ImportBatch(
            batch_id="batch-export",
            file_name="test.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
        )
        add_events(events, temp_evidence, batch, skipped_logs)

        reloaded_events = load_events()
        for i, ev in enumerate(reloaded_events[:2]):
            status = EventStatus.CONFIRMED.value if i == 0 else EventStatus.CLOSED.value
            update_event(ev.event_id, status, f"处理人{i}", f"备注{i}",
                        close_time="2025-06-11 12:00:00" if status == EventStatus.CLOSED.value else "")

        events_before = load_events()
        evidence_before = load_evidence()
        audit_before = load_audit_logs()

        csv_buf = io.StringIO()
        df_events = pd.DataFrame([e.to_dict() for e in events_before])
        df_events.to_csv(csv_buf, index=False)
        csv_content = csv_buf.getvalue()

        json_payload = {
            "events": [e.to_dict() for e in events_before],
            "evidence": [e.to_dict() for e in evidence_before],
            "audit_logs": [a.to_dict() for a in audit_before],
        }
        json_content = json.dumps(json_payload, ensure_ascii=False)

        events_reload = load_events()
        evidence_reload = load_evidence()
        audit_reload = load_audit_logs()

        csv_buf2 = io.StringIO()
        df_events2 = pd.DataFrame([e.to_dict() for e in events_reload])
        df_events2.to_csv(csv_buf2, index=False)
        csv_content_after = csv_buf2.getvalue()

        json_payload2 = {
            "events": [e.to_dict() for e in events_reload],
            "evidence": [e.to_dict() for e in evidence_reload],
            "audit_logs": [a.to_dict() for a in audit_reload],
        }
        json_content_after = json.dumps(json_payload2, ensure_ascii=False)

        self.assertEqual(csv_content, csv_content_after)
        self.assertEqual(json_content, json_content_after)

        exported_events = json.loads(json_content)["events"]
        for exp_ev in exported_events[:2]:
            self.assertIn("status", exp_ev)
            self.assertIn("handler", exp_ev)
            self.assertIn("handler_remark", exp_ev)
            self.assertIn("close_time", exp_ev)
            self.assertIn("evidence_ids", exp_ev)
            self.assertTrue(len(exp_ev["evidence_ids"]) > 0)

        exported_evidence = json.loads(json_content)["evidence"]
        self.assertTrue(len(exported_evidence) > 0)

        exported_audit = json.loads(json_content)["audit_logs"]
        self.assertTrue(len(exported_audit) >= 2)


class TestThresholdConfigValidation(TestBase):
    """Test threshold configuration error handling."""

    def test_config_hash_changes_with_thresholds(self):
        """Config hash should change when any threshold changes."""
        h1 = compute_config_hash(self.config)

        self.config["thresholds"]["temperature_upper_limit"] = -20.0
        h2 = compute_config_hash(self.config)
        self.assertNotEqual(h1, h2)

        self.config["thresholds"]["continuous_over_temp_minutes"] = 10
        h3 = compute_config_hash(self.config)
        self.assertNotEqual(h2, h3)

        self.config["thresholds"]["breakpoint_interval_minutes"] = 20
        h4 = compute_config_hash(self.config)
        self.assertNotEqual(h3, h4)

        self.config["thresholds"]["merge_window_minutes"] = 60
        h5 = compute_config_hash(self.config)
        self.assertNotEqual(h4, h5)

        self.config["thresholds"]["temperature_upper_limit"] = -15.0
        self.config["thresholds"]["continuous_over_temp_minutes"] = 5
        self.config["thresholds"]["breakpoint_interval_minutes"] = 10
        self.config["thresholds"]["merge_window_minutes"] = 30
        h6 = compute_config_hash(self.config)
        self.assertEqual(h1, h6)

    def test_config_hash_not_affected_by_validation(self):
        """Config hash should only depend on thresholds, not validation settings."""
        h1 = compute_config_hash(self.config)
        self.config["validation"]["skip_invalid_timestamp_rows"] = False
        h2 = compute_config_hash(self.config)
        self.assertEqual(h1, h2)


class TestMissingBoxId(TestBase):
    """Test missing box_id handling."""

    def test_allow_missing_box_id_true(self):
        """When allow_missing_box_id=true, rows with empty box_id should use UNKNOWN."""
        self.config["validation"]["allow_missing_box_id"] = True
        self.config["validation"]["skip_invalid_timestamp_rows"] = True
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, _ = validate_temperature_rows(df, self.config, batch_id="test-missing")

        unknown_rows = [r for r in valid_rows if r["box_id"] == "UNKNOWN"]
        self.assertEqual(len(unknown_rows), 1)
        self.assertEqual(unknown_rows[0]["temperature_c"], -19.0)

    def test_allow_missing_box_id_false(self):
        """When allow_missing_box_id=false, rows with empty box_id should be skipped."""
        self.config["validation"]["allow_missing_box_id"] = False
        self.config["validation"]["skip_invalid_timestamp_rows"] = True
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="test-missing2")

        unknown_rows = [r for r in valid_rows if r["box_id"] == "UNKNOWN"]
        self.assertEqual(len(unknown_rows), 0)

        skipped_reasons = [l.reason for l in skipped_logs]
        self.assertTrue(any("缺箱号" in r for r in skipped_reasons))


class TestDefaultConfigAndMigration(TestBase):
    """Test default configuration and data migration for old stores."""

    def test_new_event_has_default_values(self):
        """Newly created events should have correct default values for new fields."""
        event = AnomalyEvent(box_id="BX-TEST", start_time="2025-06-10 08:00:00", end_time="2025-06-10 09:00:00")
        self.assertEqual(event.assignee, "")
        self.assertEqual(event.deadline, "")
        self.assertEqual(event.priority, Priority.MEDIUM.value)
        self.assertEqual(event.version, 1)
        self.assertIsNotNone(event.last_updated_at)

    def test_old_event_data_migration(self):
        """Old event data without new fields should be migrated with default values."""
        old_event_data = {
            "event_id": "test-old-001",
            "box_id": "BX-OLD",
            "start_time": "2025-06-10 08:00:00",
            "end_time": "2025-06-10 09:00:00",
            "max_temperature": -10.0,
            "duration_minutes": 60,
            "status": EventStatus.PENDING.value,
            "handler": "",
            "handler_remark": "",
            "close_time": "",
            "batch_id": "batch-old",
            "raw_data_hash": "oldhash",
            "config_signature": "oldsig",
            "event_signature": "oldeventsig",
            "created_at": "2025-06-10 10:00:00",
            "evidence_ids": [],
        }

        from core.persistence import _migrate_event_data
        migrated = _migrate_event_data(old_event_data.copy())

        self.assertEqual(migrated["assignee"], "")
        self.assertEqual(migrated["deadline"], "")
        self.assertEqual(migrated["priority"], Priority.MEDIUM.value)
        self.assertEqual(migrated["version"], 1)
        self.assertEqual(migrated["last_updated_at"], "2025-06-10 10:00:00")

        for key in old_event_data:
            self.assertEqual(migrated[key], old_event_data[key])

    def test_old_audit_log_migration(self):
        """Old audit log data without new fields should be migrated with default values."""
        old_log_data = {
            "log_id": "log-old-001",
            "event_id": "test-old-001",
            "action": "状态变更: 待处理 -> 已确认",
            "operator": "测试员",
            "remark": "确认超温",
            "timestamp": "2025-06-10 11:00:00",
        }

        from core.persistence import _migrate_audit_log_data
        migrated = _migrate_audit_log_data(old_log_data.copy())

        self.assertEqual(migrated["field_changed"], "")
        self.assertEqual(migrated["old_value"], "")
        self.assertEqual(migrated["new_value"], "")

        for key in old_log_data:
            self.assertEqual(migrated[key], old_log_data[key])

    def test_migrated_data_persists_and_reloads(self):
        """Migrated data should be saved correctly and reload with new fields intact."""
        old_event_data = {
            "event_id": "test-migrate-001",
            "box_id": "BX-MIG",
            "start_time": "2025-06-10 08:00:00",
            "end_time": "2025-06-10 09:00:00",
            "max_temperature": -10.0,
            "duration_minutes": 60,
            "status": EventStatus.PENDING.value,
            "handler": "",
            "handler_remark": "",
            "close_time": "",
            "batch_id": "batch-mig",
            "raw_data_hash": "mighash",
            "config_signature": "migsig",
            "event_signature": "migeventsig",
            "created_at": "2025-06-10 10:00:00",
            "evidence_ids": [],
        }

        import json
        from core.persistence import _EVENTS_FILE, _ensure_dir
        _ensure_dir()
        with open(_EVENTS_FILE, "w", encoding="utf-8") as f:
            json.dump([old_event_data], f, ensure_ascii=False, indent=2)

        loaded = load_events()
        self.assertEqual(len(loaded), 1)
        ev = loaded[0]
        self.assertEqual(ev.event_id, "test-migrate-001")
        self.assertEqual(ev.assignee, "")
        self.assertEqual(ev.deadline, "")
        self.assertEqual(ev.priority, Priority.MEDIUM.value)
        self.assertEqual(ev.version, 1)
        self.assertEqual(ev.last_updated_at, "2025-06-10 10:00:00")

        save_events(loaded)

        reloaded = load_events()
        self.assertEqual(len(reloaded), 1)
        ev2 = reloaded[0]
        self.assertEqual(ev2.event_id, "test-migrate-001")
        self.assertEqual(ev2.assignee, "")
        self.assertEqual(ev2.priority, Priority.MEDIUM.value)

    def test_config_has_default_assignees(self):
        """Config should have default assignees and priority settings."""
        import yaml
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

        self.assertIn("assignment", cfg)
        self.assertEqual(cfg["assignment"]["default_priority"], "中")
        self.assertGreater(len(cfg["assignment"]["default_assignees"]), 0)
        self.assertIn("早班A", cfg["assignment"]["default_assignees"])


class TestAssignmentAndAuditLog(TestBase):
    """Test event assignment, audit logging, and version tracking."""

    def _create_test_event(self):
        """Helper to create and save a test event."""
        event = AnomalyEvent(
            box_id="BX-TEST-001",
            start_time="2025-06-10 08:00:00",
            end_time="2025-06-10 09:00:00",
            max_temperature=-10.0,
            duration_minutes=60,
        )
        save_events([event])
        return event

    def test_update_event_assignment(self):
        """Event assignment (assignee, deadline, priority) should be saved correctly."""
        event = self._create_test_event()
        deadline = "2025-06-11 18:00:00"

        success, updated = update_event_assignment(
            event.event_id,
            assignee="早班A",
            deadline=deadline,
            priority=Priority.HIGH.value,
            operator="主管",
            remark="紧急处理",
        )

        self.assertTrue(success)
        self.assertEqual(updated.assignee, "早班A")
        self.assertEqual(updated.deadline, deadline)
        self.assertEqual(updated.priority, Priority.HIGH.value)
        self.assertEqual(updated.version, 2)

        reloaded = get_event_by_id(event.event_id)
        self.assertEqual(reloaded.assignee, "早班A")
        self.assertEqual(reloaded.deadline, deadline)
        self.assertEqual(reloaded.priority, Priority.HIGH.value)
        self.assertEqual(reloaded.version, 2)

    def test_assignment_audit_logs(self):
        """Assignment changes should create detailed audit logs with old/new values."""
        event = self._create_test_event()

        update_event_assignment(
            event.event_id,
            assignee="早班A",
            deadline="2025-06-11 18:00:00",
            priority=Priority.HIGH.value,
            operator="主管",
            remark="紧急处理",
        )

        logs = get_audit_logs_for_event(event.event_id)
        self.assertGreaterEqual(len(logs), 3)

        log_actions = [l.action for l in logs]
        self.assertTrue(any("责任人分派" in a for a in log_actions))
        self.assertTrue(any("截止时间变更" in a for a in log_actions))
        self.assertTrue(any("优先级变更" in a for a in log_actions))

        assignee_log = next(l for l in logs if l.field_changed == "assignee")
        self.assertEqual(assignee_log.old_value, "")
        self.assertEqual(assignee_log.new_value, "早班A")
        self.assertEqual(assignee_log.operator, "主管")
        self.assertEqual(assignee_log.remark, "紧急处理")

        priority_log = next(l for l in logs if l.field_changed == "priority")
        self.assertEqual(priority_log.old_value, Priority.MEDIUM.value)
        self.assertEqual(priority_log.new_value, Priority.HIGH.value)

    def test_update_event_audit_logs(self):
        """Status and handler changes should create detailed audit logs."""
        event = self._create_test_event()

        success, updated = update_event(
            event.event_id,
            status=EventStatus.CONFIRMED.value,
            handler="测试员",
            remark="确认超温",
        )

        self.assertTrue(success)
        logs = get_audit_logs_for_event(event.event_id)
        self.assertGreaterEqual(len(logs), 2)

        status_log = next(l for l in logs if l.field_changed == "status")
        self.assertEqual(status_log.old_value, EventStatus.PENDING.value)
        self.assertEqual(status_log.new_value, EventStatus.CONFIRMED.value)

        handler_log = next(l for l in logs if l.field_changed == "handler")
        self.assertEqual(handler_log.old_value, "")
        self.assertEqual(handler_log.new_value, "测试员")

    def test_get_event_by_id(self):
        """get_event_by_id should return correct event or None."""
        event = self._create_test_event()

        found = get_event_by_id(event.event_id)
        self.assertIsNotNone(found)
        self.assertEqual(found.event_id, event.event_id)

        not_found = get_event_by_id("nonexistent-id")
        self.assertIsNone(not_found)


class TestVersionConflict(TestBase):
    """Test version conflict detection for concurrent edits."""

    def _create_test_event(self):
        event = AnomalyEvent(
            box_id="BX-TEST-CONF",
            start_time="2025-06-10 08:00:00",
            end_time="2025-06-10 09:00:00",
            max_temperature=-10.0,
            duration_minutes=60,
        )
        save_events([event])
        return event

    def test_version_increments_on_update(self):
        """Version should increment on each update."""
        event = self._create_test_event()
        self.assertEqual(event.version, 1)

        _, updated1 = update_event(
            event.event_id, EventStatus.CONFIRMED.value, "测试员", "第一次更新"
        )
        self.assertEqual(updated1.version, 2)

        _, updated2 = update_event_assignment(
            event.event_id, "早班A", "2025-06-11 18:00:00", Priority.HIGH.value, "主管"
        )
        self.assertEqual(updated2.version, 3)

        reloaded = get_event_by_id(event.event_id)
        self.assertEqual(reloaded.version, 3)

    def test_version_conflict_raises_error(self):
        """Updating with wrong expected version should raise VersionConflictError."""
        event = self._create_test_event()

        update_event(
            event.event_id, EventStatus.CONFIRMED.value, "测试员", "第一次更新"
        )

        with self.assertRaises(VersionConflictError) as ctx:
            update_event(
                event.event_id, EventStatus.CLOSED.value, "测试员2", "第二次更新",
                expected_version=1
            )

        self.assertEqual(ctx.exception.event_id, event.event_id)
        self.assertEqual(ctx.exception.current_version, 2)
        self.assertEqual(ctx.exception.expected_version, 1)

    def test_version_conflict_for_assignment(self):
        """Assignment update with wrong version should also raise error."""
        event = self._create_test_event()

        update_event_assignment(
            event.event_id, "早班A", "2025-06-11 18:00:00", Priority.HIGH.value, "主管"
        )

        with self.assertRaises(VersionConflictError) as ctx:
            update_event_assignment(
                event.event_id, "早班B", "2025-06-12 18:00:00", Priority.MEDIUM.value, "主管2",
                expected_version=1
            )

        self.assertEqual(ctx.exception.current_version, 2)
        self.assertEqual(ctx.exception.expected_version, 1)

    def test_no_version_check_when_expected_version_none(self):
        """When expected_version is None, no version check should be performed."""
        event = self._create_test_event()

        update_event(
            event.event_id, EventStatus.CONFIRMED.value, "测试员", "第一次更新"
        )

        success, updated = update_event(
            event.event_id, EventStatus.CLOSED.value, "测试员2", "第二次更新",
            expected_version=None
        )

        self.assertTrue(success)
        self.assertEqual(updated.version, 3)

    def test_last_updated_at_changes_on_update(self):
        """last_updated_at should be updated on each change."""
        event = self._create_test_event()
        original_time = event.last_updated_at

        import time
        time.sleep(1)

        _, updated = update_event(
            event.event_id, EventStatus.CONFIRMED.value, "测试员", "更新"
        )

        self.assertNotEqual(updated.last_updated_at, original_time)


class TestRestartConsistencyNewFields(TestBase):
    """Test that new fields survive restart and are consistent."""

    def test_new_fields_survive_restart(self):
        """All new fields should be preserved after save/load cycles (simulating restart)."""
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="batch-newfields")
        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig = compute_config_hash(self.config)

        events, temp_evidence = generate_events(
            valid_rows, self.config, "batch-newfields", "test.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig,
        )

        batch = ImportBatch(
            batch_id="batch-newfields",
            file_name="test.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
        )
        add_events(events, temp_evidence, batch, skipped_logs)

        reloaded = load_events()
        ev = reloaded[0]
        self.assertEqual(ev.assignee, "")
        self.assertEqual(ev.deadline, "")
        self.assertEqual(ev.priority, Priority.MEDIUM.value)
        self.assertEqual(ev.version, 1)
        self.assertIsNotNone(ev.last_updated_at)

        update_event_assignment(
            ev.event_id, "早班A", "2025-06-11 18:00:00", Priority.URGENT.value, "主管", "紧急处理"
        )

        events_before = load_events()
        audits_before = load_audit_logs()

        events_after = load_events()
        audits_after = load_audit_logs()

        self.assertEqual(len(events_before), len(events_after))
        self.assertEqual(len(audits_before), len(audits_after))

        ev_after = next(e for e in events_after if e.event_id == ev.event_id)
        self.assertEqual(ev_after.assignee, "早班A")
        self.assertEqual(ev_after.deadline, "2025-06-11 18:00:00")
        self.assertEqual(ev_after.priority, Priority.URGENT.value)
        self.assertEqual(ev_after.version, 2)

        audit_fields = [(l.field_changed, l.old_value, l.new_value) for l in audits_after if l.event_id == ev.event_id]
        self.assertTrue(any(f == ("assignee", "", "早班A") for f in audit_fields))
        self.assertTrue(any(f == ("priority", Priority.MEDIUM.value, Priority.URGENT.value) for f in audit_fields))

    def test_reanalysis_preserves_new_fields(self):
        """Re-analysis should preserve assignee, deadline, priority, version, and last_updated_at."""
        df = parse_temperature_csv(self.temp_csv_content.encode())
        valid_rows, skipped_logs = validate_temperature_rows(df, self.config, batch_id="batch-reanalysis-v1")
        raw_data_hash = compute_raw_data_hash(valid_rows)
        config_sig_v1 = compute_config_hash(self.config)

        events, temp_evidence = generate_events(
            valid_rows, self.config, "batch-reanalysis-v1", "test.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig_v1,
        )

        batch_v1 = ImportBatch(
            batch_id="batch-reanalysis-v1",
            file_name="test.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig_v1,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
        )
        add_events(events, temp_evidence, batch_v1, skipped_logs)

        reloaded = load_events()
        ev = reloaded[0]
        update_event_assignment(
            ev.event_id, "中班B", "2025-06-12 12:00:00", Priority.HIGH.value, "主管", "分派任务"
        )

        ev_before = get_event_by_id(ev.event_id)
        self.assertEqual(ev_before.assignee, "中班B")
        self.assertEqual(ev_before.deadline, "2025-06-12 12:00:00")
        self.assertEqual(ev_before.priority, Priority.HIGH.value)
        self.assertEqual(ev_before.version, 2)

        self.config["thresholds"]["temperature_upper_limit"] = -18.0
        config_sig_v2 = compute_config_hash(self.config)

        new_events, new_temp_evidence = generate_events(
            valid_rows, self.config, "batch-reanalysis-v2", "test.csv",
            raw_data_hash=raw_data_hash, config_signature=config_sig_v2,
        )

        batch_v2 = ImportBatch(
            batch_id="batch-reanalysis-v2",
            file_name="test.csv",
            file_hash=compute_file_hash(self.temp_csv_content.encode()),
            raw_data_hash=raw_data_hash,
            config_signature=config_sig_v2,
            row_count=len(df),
            skipped_rows=len(skipped_logs),
            status="成功",
            is_reanalysis=True,
        )

        updated, new_count, unchanged = update_events_for_reanalysis(
            new_events, new_temp_evidence, batch_v2, []
        )
        self.assertGreater(updated, 0)

        ev_after = get_event_by_id(ev.event_id)
        self.assertEqual(ev_after.assignee, "中班B")
        self.assertEqual(ev_after.deadline, "2025-06-12 12:00:00")
        self.assertEqual(ev_after.priority, Priority.HIGH.value)
        self.assertEqual(ev_after.version, 2)
        self.assertEqual(ev_after.last_updated_at, ev_before.last_updated_at)

        audits_after = get_audit_logs_for_event(ev.event_id)
        self.assertGreaterEqual(len(audits_after), 3)


class TestExportWithNewFields(TestBase):
    """Test CSV and JSON exports include new fields."""

    def _create_test_events_with_assignments(self):
        """Helper to create events with various assignments."""
        events = []
        for i in range(3):
            event = AnomalyEvent(
                box_id=f"BX-EXPORT-{i:03d}",
                start_time=f"2025-06-10 0{i+8}:00:00",
                end_time=f"2025-06-10 0{i+9}:00:00",
                max_temperature=-10.0 - i,
                duration_minutes=60,
            )
            events.append(event)
        save_events(events)

        update_event_assignment(
            events[0].event_id, "早班A", "2025-06-11 18:00:00", Priority.HIGH.value, "主管", "高优先级"
        )
        update_event_assignment(
            events[1].event_id, "中班B", "2025-06-12 12:00:00", Priority.MEDIUM.value, "主管", "中优先级"
        )

        return events

    def test_csv_export_includes_new_fields(self):
        """CSV export should include all new fields."""
        events = self._create_test_events_with_assignments()

        df = pd.DataFrame([e.to_dict() for e in load_events()])

        required_columns = [
            "assignee", "deadline", "priority", "last_updated_at", "version"
        ]
        for col in required_columns:
            self.assertIn(col, df.columns, f"CSV export missing column: {col}")

        ev0_row = df[df["event_id"] == events[0].event_id].iloc[0]
        self.assertEqual(ev0_row["assignee"], "早班A")
        self.assertEqual(ev0_row["priority"], Priority.HIGH.value)
        self.assertEqual(ev0_row["version"], 2)

    def test_json_export_includes_new_fields(self):
        """JSON export should include all new fields and audit logs."""
        events = self._create_test_events_with_assignments()

        all_events = load_events()
        export_data = [e.to_dict() for e in all_events]
        audit_data = []
        for e in all_events:
            audit_data.extend([l.to_dict() for l in get_audit_logs_for_event(e.event_id)])

        payload = {
            "events": export_data,
            "audit_logs": audit_data,
        }

        for ev in payload["events"]:
            self.assertIn("assignee", ev)
            self.assertIn("deadline", ev)
            self.assertIn("priority", ev)
            self.assertIn("last_updated_at", ev)
            self.assertIn("version", ev)

        for log in payload["audit_logs"]:
            self.assertIn("field_changed", log)
            self.assertIn("old_value", log)
            self.assertIn("new_value", log)

        assignee_logs = [l for l in payload["audit_logs"] if l["field_changed"] == "assignee"]
        self.assertEqual(len(assignee_logs), 2)

    def test_exported_audit_logs_have_detailed_info(self):
        """Exported audit logs should have detailed field change information."""
        events = self._create_test_events_with_assignments()

        logs = get_audit_logs_for_event(events[0].event_id)
        self.assertGreaterEqual(len(logs), 3)

        for log in logs:
            self.assertIsNotNone(log.field_changed)
            self.assertIsNotNone(log.old_value)
            self.assertIsNotNone(log.new_value)

        priority_log = next(l for l in logs if l.field_changed == "priority")
        self.assertEqual(priority_log.old_value, Priority.MEDIUM.value)
        self.assertEqual(priority_log.new_value, Priority.HIGH.value)


class TestOverdueCalculation(TestBase):
    """Test overdue status calculation."""

    def test_is_overdue_with_past_deadline(self):
        """Events with past deadlines should be overdue."""
        from app import _is_overdue
        self.assertTrue(_is_overdue("2020-01-01 12:00:00"))

    def test_is_overdue_with_future_deadline(self):
        """Events with future deadlines should not be overdue."""
        from app import _is_overdue
        self.assertFalse(_is_overdue("2099-12-31 23:59:59"))

    def test_is_overdue_with_empty_deadline(self):
        """Events without deadlines should not be overdue."""
        from app import _is_overdue
        self.assertFalse(_is_overdue(""))
        self.assertFalse(_is_overdue(None))

    def test_is_overdue_with_invalid_date(self):
        """Events with invalid deadlines should not be overdue."""
        from app import _is_overdue
        self.assertFalse(_is_overdue("invalid-date"))


class TestCsvExportAuditLog(TestBase):
    """Test that CSV export includes audit log rows with full traceability."""

    def _create_events_with_audit_trail(self):
        events = []
        for i in range(2):
            event = AnomalyEvent(
                box_id=f"BX-AUDIT-{i:03d}",
                start_time=f"2025-06-10 0{i+8}:00:00",
                end_time=f"2025-06-10 0{i+9}:00:00",
                max_temperature=-10.0 - i,
                duration_minutes=60,
            )
            events.append(event)
        save_events(events)

        update_event_assignment(
            events[0].event_id, "早班A", "2025-06-11 18:00:00",
            Priority.HIGH.value, "主管", "紧急分派",
        )
        update_event(
            events[1].event_id, EventStatus.CONFIRMED.value, "测试员", "确认超温",
        )

        return events

    def test_csv_contains_audit_log_rows(self):
        """CSV export must contain audit log rows, not just event rows."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        audit_rows = df[df["row_type"] == "审计日志"]
        self.assertGreater(len(audit_rows), 0, "CSV export has zero audit log rows")

    def test_csv_audit_log_has_required_columns(self):
        """CSV audit log rows must include action, field_changed, old_value, new_value, operator."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        required_cols = ["action", "field_changed", "old_value", "new_value", "operator"]
        for col in required_cols:
            self.assertIn(col, df.columns, f"CSV export missing column: {col}")

        audit_rows = df[df["row_type"] == "审计日志"]
        for col in required_cols:
            non_empty = audit_rows[col].dropna().astype(str).ne("").sum()
            self.assertGreater(non_empty, 0, f"CSV audit log column '{col}' is all empty")

    def test_csv_audit_log_traces_assignee_change(self):
        """CSV audit log must contain the assignee change from the assignment update."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        audit_rows = df[df["row_type"] == "审计日志"]
        assignee_logs = audit_rows[audit_rows["field_changed"] == "assignee"]
        self.assertGreater(len(assignee_logs), 0, "No assignee change found in CSV audit logs")

        row = assignee_logs.iloc[0]
        self.assertEqual(str(row["new_value"]).strip(), "早班A")

    def test_csv_audit_log_traces_priority_change(self):
        """CSV audit log must contain the priority change from the assignment update."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        audit_rows = df[df["row_type"] == "审计日志"]
        priority_logs = audit_rows[audit_rows["field_changed"] == "priority"]
        self.assertGreater(len(priority_logs), 0, "No priority change found in CSV audit logs")

        row = priority_logs.iloc[0]
        self.assertEqual(str(row["old_value"]).strip(), Priority.MEDIUM.value)
        self.assertEqual(str(row["new_value"]).strip(), Priority.HIGH.value)

    def test_csv_audit_log_traces_deadline_change(self):
        """CSV audit log must contain the deadline change from the assignment update."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        audit_rows = df[df["row_type"] == "审计日志"]
        deadline_logs = audit_rows[audit_rows["field_changed"] == "deadline"]
        self.assertGreater(len(deadline_logs), 0, "No deadline change found in CSV audit logs")

    def test_csv_audit_log_traces_status_change(self):
        """CSV audit log must contain the status change from the review update."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        audit_rows = df[df["row_type"] == "审计日志"]
        status_logs = audit_rows[audit_rows["field_changed"] == "status"]
        self.assertGreater(len(status_logs), 0, "No status change found in CSV audit logs")

    def test_csv_event_rows_still_intact(self):
        """CSV event rows must still contain all event fields after adding audit log support."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        event_rows = df[df["row_type"] == "事件"]
        self.assertEqual(len(event_rows), 2)

        required_event_cols = [
            "event_id", "box_id", "status", "priority", "assignee",
            "deadline", "start_time", "end_time", "max_temperature",
            "duration_minutes", "handler", "version", "created_at",
        ]
        for col in required_event_cols:
            self.assertIn(col, df.columns, f"CSV event rows missing column: {col}")

    def test_csv_audit_rows_carry_event_keys(self):
        """CSV audit log rows must carry event_id and box_id for traceability."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        audit_rows = df[df["row_type"] == "审计日志"]
        self.assertTrue(
            (audit_rows["event_id"].notna() & audit_rows["event_id"].astype(str).ne("")).all(),
            "Some audit log rows are missing event_id",
        )
        self.assertTrue(
            (audit_rows["box_id"].notna() & audit_rows["box_id"].astype(str).ne("")).all(),
            "Some audit log rows are missing box_id",
        )

    def test_csv_export_survives_restart(self):
        """CSV export content must be identical after a simulated restart (reload)."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export

        csv_before = build_csv_export(load_events())

        csv_after = build_csv_export(load_events())

        self.assertEqual(csv_before, csv_after)

    def test_json_export_survives_restart(self):
        """JSON export content must be identical after a simulated restart (reload)."""
        events = self._create_events_with_audit_trail()
        from app import build_json_export

        json_before = json.dumps(build_json_export(load_events()), ensure_ascii=False, sort_keys=True)

        json_after = json.dumps(build_json_export(load_events()), ensure_ascii=False, sort_keys=True)

        self.assertEqual(json_before, json_after)

    def test_csv_and_json_audit_logs_consistent(self):
        """Audit log entries in CSV and JSON exports must be consistent in count and content."""
        events = self._create_events_with_audit_trail()
        from app import build_csv_export, build_json_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))
        csv_audit_count = len(df[df["row_type"] == "审计日志"])

        json_payload = build_json_export(load_events())
        json_audit_count = len(json_payload["audit_logs"])

        self.assertEqual(csv_audit_count, json_audit_count)

        csv_audit_events = set(
            df[df["row_type"] == "审计日志"]["event_id"].dropna().astype(str)
        )
        json_audit_events = set(l["event_id"] for l in json_payload["audit_logs"])
        self.assertEqual(csv_audit_events, json_audit_events)

    def test_csv_no_audit_log_for_events_without_changes(self):
        """Events with no audit logs should only produce a single event row in CSV."""
        events = []
        for i in range(2):
            event = AnomalyEvent(
                box_id=f"BX-NOLOG-{i:03d}",
                start_time=f"2025-06-10 0{i+8}:00:00",
                end_time=f"2025-06-10 0{i+9}:00:00",
                max_temperature=-10.0 - i,
                duration_minutes=60,
            )
            events.append(event)
        save_events(events)

        from app import build_csv_export
        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        event_rows = df[df["row_type"] == "事件"]
        audit_rows = df[df["row_type"] == "审计日志"]
        self.assertEqual(len(event_rows), 2)
        self.assertEqual(len(audit_rows), 0)


class TestCsvExportMisreadRisk(TestBase):
    """Regression tests to prevent misreading: downstream using old assumption
    that CSV is a pure event list will miss audit log rows.
    """

    def _create_events_with_full_audit_trail(self):
        """Create 2 events: ev0 has assignment+review, ev1 only review."""
        events = []
        for i in range(2):
            event = AnomalyEvent(
                box_id=f"BX-MISREAD-{i:03d}",
                start_time=f"2025-06-10 0{i+8}:00:00",
                end_time=f"2025-06-10 0{i+9}:00:00",
                max_temperature=-10.0 - i,
                duration_minutes=60,
                status=EventStatus.PENDING.value,
            )
            events.append(event)
        save_events(events)

        update_event_assignment(
            events[0].event_id, "早班A", "2025-06-11 18:00:00",
            Priority.HIGH.value, "主管甲", "紧急分派",
        )
        update_event_assignment(
            events[0].event_id, "早班B", "2025-06-12 12:00:00",
            Priority.URGENT.value, "主管乙", "改期+升级",
        )
        update_event(
            events[0].event_id, EventStatus.CONFIRMED.value, "处理员A", "确认超温",
        )
        update_event(
            events[1].event_id, EventStatus.FALSE_ALARM.value, "处理员B", "误报排除",
        )
        return events

    def test_cannot_find_assignee_change_without_row_type_filter(self):
        """Regression: old parser that only reads '事件' rows WILL miss
        the operator and assignee change info — prove it so the test breaks
        if we accidentally revert CSV schema.
        """
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content))

        only_events = df[df["row_type"] == "事件"]
        operators_in_events = only_events["operator"].dropna().astype(str).ne("").sum()
        assignee_in_events = only_events["assignee"].dropna().astype(str).ne("").sum()

        self.assertEqual(
            operators_in_events, 0,
            "Regression: operator column must be EMPTY in event rows "
            "(this would otherwise hide the misread risk of skipping audit rows)",
        )
        self.assertGreaterEqual(
            assignee_in_events, 1,
            "Current assignee should still appear on the event row as snapshot",
        )

        only_audits = df[df["row_type"] == "审计日志"]
        operators_in_audits = only_audits["operator"].dropna().astype(str).ne("").sum()
        self.assertGreater(operators_in_audits, 0, "Operators only live in audit rows")

    def test_full_traceability_assignee_deadline_priority(self):
        """Directly check CSV contains assignee/deadline/priority change records
        with correct old/new values, and each change has a distinct operator + timestamp.
        """
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content), keep_default_na=False)
        audits = df[df["row_type"] == "审计日志"]

        assignee_changes = audits[audits["field_changed"] == "assignee"]
        self.assertEqual(len(assignee_changes), 2)  # ""->早班A, 早班A->早班B

        first_assign = assignee_changes[assignee_changes["new_value"] == "早班A"].iloc[0]
        self.assertEqual(first_assign["old_value"], "")
        self.assertEqual(first_assign["operator"], "主管甲")
        self.assertEqual(first_assign["remark"], "紧急分派")

        reassign = assignee_changes[assignee_changes["new_value"] == "早班B"].iloc[0]
        self.assertEqual(reassign["old_value"], "早班A")
        self.assertEqual(reassign["operator"], "主管乙")
        self.assertEqual(reassign["remark"], "改期+升级")

        deadline_changes = audits[audits["field_changed"] == "deadline"]
        self.assertEqual(len(deadline_changes), 2)

        priority_changes = audits[audits["field_changed"] == "priority"]
        self.assertGreaterEqual(len(priority_changes), 2)
        first_priority = priority_changes[priority_changes["new_value"] == Priority.HIGH.value].iloc[0]
        self.assertEqual(first_priority["old_value"], Priority.MEDIUM.value)

    def test_csv_null_values_are_empty_strings_not_nan(self):
        """CSV empty cells must be '' (empty string), never NaN / 'nan' literal."""
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content), keep_default_na=False, dtype=str)

        event_rows = df[df["row_type"] == "事件"]
        for col in ["action", "field_changed", "old_value", "new_value", "operator", "log_id", "log_timestamp"]:
            bad = event_rows[event_rows[col].str.lower().isin(["nan", "none", "null"])]
            self.assertEqual(
                len(bad), 0,
                f"Event rows' '{col}' contains NaN/NONE/NULL literal instead of empty string",
            )

        audit_rows = df[df["row_type"] == "审计日志"]
        for col in ["status", "priority", "assignee", "deadline", "start_time", "handler"]:
            bad = audit_rows[audit_rows[col].str.lower().isin(["nan", "none", "null"])]
            self.assertEqual(
                len(bad), 0,
                f"Audit rows' '{col}' contains NaN/NONE/NULL literal instead of empty string",
            )

    def test_csv_row_type_is_first_column(self):
        """row_type must be the first column so parsers can branch early."""
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        header_line = csv_content.splitlines()[0]
        first_column = header_line.split(",")[0]
        self.assertEqual(first_column, "row_type", "row_type must be column #0")

    def test_filtered_export_carries_all_history_logs_of_filtered_events(self):
        """Filtering by status must include ALL audit logs of the matched events,
        not only logs produced while in that status. This preserves traceability.
        """
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export

        all_csv = build_csv_export(load_events())
        df_all = pd.read_csv(io.StringIO(all_csv))
        ev0_logs_all = set(df_all[df_all["event_id"] == events[0].event_id]["log_id"].dropna())

        confirmed_events = [e for e in load_events() if e.status == EventStatus.CONFIRMED.value]
        confirmed_csv = build_csv_export(confirmed_events)
        df_filtered = pd.read_csv(io.StringIO(confirmed_csv))
        ev0_logs_filtered = set(df_filtered[df_filtered["event_id"] == events[0].event_id]["log_id"].dropna())

        self.assertEqual(
            ev0_logs_all, ev0_logs_filtered,
            "Filtering by status dropped audit logs for matched events; "
            "traceability broken (history for reassignment lost)",
        )

    def test_csv_encoding_matches_readme_utf8_bom(self):
        """Simulate the exact bytes a user downloads: UTF-8-SIG BOM must be present."""
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export

        csv_str = build_csv_export(load_events())
        csv_bytes = csv_str.encode("utf-8-sig")

        self.assertEqual(csv_bytes[:3], b"\xef\xbb\xbf", "CSV file must start with UTF-8 BOM")

        decoded_back = csv_bytes.decode("utf-8-sig")
        self.assertEqual(decoded_back, csv_str, "BOM round-trip decode must yield original str")

    def test_csv_json_field_values_consistent(self):
        """Values for identical event/log fields must match between CSV and JSON
        when using the same filter input (same filtered list)."""
        events = self._create_events_with_full_audit_trail()
        from app import build_csv_export, build_json_export

        filtered = load_events()
        csv_df = pd.read_csv(io.StringIO(build_csv_export(filtered)), keep_default_na=False)
        json_payload = build_json_export(filtered)

        csv_event = csv_df[csv_df["row_type"] == "事件"].iloc[0]
        json_event = json_payload["events"][0]
        for key in ["event_id", "box_id", "assignee", "priority", "deadline"]:
            self.assertEqual(str(csv_event[key]), str(json_event.get(key, "")))

        csv_audit = csv_df[csv_df["row_type"] == "审计日志"]
        for log in json_payload["audit_logs"]:
            rows = csv_audit[csv_audit["log_id"] == log["log_id"]]
            self.assertEqual(len(rows), 1, f"JSON log {log['log_id']} missing from CSV")
            row = rows.iloc[0]
            self.assertEqual(row["action"], log["action"])
            self.assertEqual(row["operator"], log["operator"])
            self.assertEqual(row["field_changed"], log["field_changed"])
            self.assertEqual(row["old_value"], log["old_value"])
            self.assertEqual(row["new_value"], log["new_value"])

    def test_readme_documents_field_changed_values(self):
        """README must list the common field_changed values we actually emit,
        so downstream can rely on them. If we add a new field_changed, update README.
        """
        import re
        readme_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "README.md"
        )
        with open(readme_path, "r", encoding="utf-8") as f:
            readme = f.read()

        events = self._create_events_with_full_audit_trail()
        logs = load_audit_logs()
        actually_emitted = sorted(set(l.field_changed for l in logs if l.field_changed))

        for fc in actually_emitted:
            self.assertIn(
                f"`{fc}`", readme,
                f"README missing field_changed value '{fc}' that the app actually emits; "
                "add to the CSV field_changed table in README",
            )

        documented = re.findall(r"`(assignee|deadline|priority|status|handler|handler_remark)`", readme)
        self.assertGreaterEqual(len(documented), 4, "README table seems truncated")

    def test_restart_roundtrip_persistence_export_readme_consistent(self):
        """End-to-end: save → simulate restart (reload all) → export CSV / JSON →
        assert values, row counts, and README-documented fields still hold.
        """
        from app import build_csv_export, build_json_export
        events = self._create_events_with_full_audit_trail()
        target_id = events[0].event_id
        before_logs = len(load_audit_logs())

        csv_before = build_csv_export(load_events())
        json_before = json.dumps(build_json_export(load_events()), ensure_ascii=False, sort_keys=True)

        reloaded_events = load_events()
        reloaded_logs = load_audit_logs()
        self.assertEqual(len(reloaded_logs), before_logs)

        csv_after = build_csv_export(reloaded_events)
        json_after = json.dumps(build_json_export(reloaded_events), ensure_ascii=False, sort_keys=True)

        self.assertEqual(csv_before, csv_after, "CSV export differs after simulated restart")
        self.assertEqual(json_before, json_after, "JSON export differs after simulated restart")

        reloaded_df = pd.read_csv(io.StringIO(csv_after), keep_default_na=False)
        target_rows = reloaded_df[reloaded_df["event_id"] == target_id]
        event_row = target_rows[target_rows["row_type"] == "事件"].iloc[0]
        audit_rows = target_rows[target_rows["row_type"] == "审计日志"]

        self.assertEqual(event_row["assignee"], "早班B")
        self.assertEqual(event_row["priority"], Priority.URGENT.value)
        self.assertEqual(event_row["status"], EventStatus.CONFIRMED.value)
        self.assertGreaterEqual(len(audit_rows), 6)  # 2*(a/d/p) + status + handler...


class TestAuditLogTimestampConsistency(TestBase):
    """Regression test for the audit log timestamp field name mismatch between CSV and JSON.

    Bug: README said JSON audit_logs fields match CSV completely, and downstream reads
    'log_timestamp' for operation time. But JSON was outputting 'timestamp' (the internal
    model field name), while CSV correctly used 'log_timestamp'.

    Fix: build_json_export now renames 'timestamp' to 'log_timestamp' for audit logs,
    matching CSV and README.
    """

    def _create_events_with_operations(self):
        """Create events with assignment, deadline, and priority changes."""
        events = []
        for i in range(2):
            event = AnomalyEvent(
                box_id=f"BX-TSTAMP-{i:03d}",
                start_time=f"2025-06-10 0{i+8}:00:00",
                end_time=f"2025-06-10 0{i+9}:00:00",
                max_temperature=-10.0 - i,
                duration_minutes=60,
                status=EventStatus.PENDING.value,
            )
            events.append(event)
        save_events(events)

        update_event_assignment(
            events[0].event_id, "早班A", "2025-06-11 18:00:00",
            Priority.HIGH.value, "主管甲", "第一次分派",
        )
        update_event_assignment(
            events[0].event_id, "早班B", "2025-06-12 12:00:00",
            Priority.URGENT.value, "主管乙", "改期并升级优先级",
        )
        update_event(
            events[0].event_id, EventStatus.CONFIRMED.value,
            "处理员A", "确认超温，已通知仓库",
        )
        return events

    def test_csv_audit_log_has_log_timestamp_column(self):
        """CSV audit log rows must have 'log_timestamp' column with valid timestamps."""
        events = self._create_events_with_operations()
        from app import build_csv_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content), keep_default_na=False)

        self.assertIn("log_timestamp", df.columns, "CSV missing 'log_timestamp' column")

        audit_rows = df[df["row_type"] == "审计日志"]
        self.assertGreater(len(audit_rows), 0)

        timestamps = audit_rows["log_timestamp"].dropna().astype(str)
        non_empty = timestamps[timestamps.ne("")]
        self.assertEqual(
            len(non_empty), len(audit_rows),
            "Some CSV audit log rows have empty log_timestamp",
        )

        for ts in non_empty:
            self.assertRegex(
                ts, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
                f"CSV log_timestamp '{ts}' has wrong format",
            )

    def test_json_audit_log_has_log_timestamp_field(self):
        """JSON audit_logs must have 'log_timestamp' field (not 'timestamp')."""
        events = self._create_events_with_operations()
        from app import build_json_export

        json_payload = build_json_export(load_events())
        audit_logs = json_payload["audit_logs"]

        self.assertGreater(len(audit_logs), 0)

        for log in audit_logs:
            self.assertIn(
                "log_timestamp", log,
                "JSON audit_logs missing 'log_timestamp' field; "
                "downstream reads this to get operation time",
            )
            self.assertNotIn(
                "timestamp", log,
                "JSON audit_logs still has old 'timestamp' field; "
                "should be renamed to 'log_timestamp'",
            )
            self.assertRegex(
                log["log_timestamp"],
                r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
                f"JSON log_timestamp '{log['log_timestamp']}' has wrong format",
            )

    def test_csv_and_json_audit_timestamps_match(self):
        """Audit log timestamps must be identical between CSV and JSON for same log_id."""
        events = self._create_events_with_operations()
        from app import build_csv_export, build_json_export

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content), keep_default_na=False)
        csv_audits = df[df["row_type"] == "审计日志"]

        json_payload = build_json_export(load_events())
        json_audits = {log["log_id"]: log for log in json_payload["audit_logs"]}

        self.assertEqual(len(csv_audits), len(json_audits))

        for _, csv_row in csv_audits.iterrows():
            log_id = csv_row["log_id"]
            self.assertIn(log_id, json_audits, f"Log {log_id} missing from JSON")
            json_log = json_audits[log_id]
            self.assertEqual(
                csv_row["log_timestamp"], json_log["log_timestamp"],
                f"Timestamp mismatch for log {log_id}: "
                f"CSV='{csv_row['log_timestamp']}', JSON='{json_log['log_timestamp']}'",
            )

    def test_downstream_can_read_operation_time_by_log_timestamp(self):
        """Simulate downstream reading operation time by 'log_timestamp' —
        this must work for both CSV and JSON, for all operation types:
        assignment, deadline change, priority adjustment, status change.
        """
        events = self._create_events_with_operations()
        from app import build_csv_export, build_json_export

        target_event_id = events[0].event_id

        csv_content = build_csv_export(load_events())
        df = pd.read_csv(io.StringIO(csv_content), keep_default_na=False)
        csv_audits = df[
            (df["row_type"] == "审计日志") &
            (df["event_id"] == target_event_id)
        ]

        json_payload = build_json_export(load_events())
        json_audits = [
            log for log in json_payload["audit_logs"]
            if log["event_id"] == target_event_id
        ]

        field_types = ["assignee", "deadline", "priority", "status", "handler"]
        for field in field_types:
            csv_logs = csv_audits[csv_audits["field_changed"] == field]
            json_logs = [log for log in json_audits if log["field_changed"] == field]

            self.assertGreater(
                len(csv_logs), 0,
                f"No CSV audit log for field_changed='{field}'",
            )
            self.assertGreater(
                len(json_logs), 0,
                f"No JSON audit log for field_changed='{field}'",
            )

            for _, row in csv_logs.iterrows():
                ts = row["log_timestamp"]
                self.assertIsNotNone(ts)
                self.assertNotEqual(ts, "")
                self.assertRegex(
                    ts, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
                    f"Cannot read CSV {field} operation time from log_timestamp",
                )

            for log in json_logs:
                ts = log["log_timestamp"]
                self.assertIsNotNone(ts)
                self.assertNotEqual(ts, "")
                self.assertRegex(
                    ts, r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
                    f"Cannot read JSON {field} operation time from log_timestamp",
                )

    def test_readme_documents_log_timestamp_consistency(self):
        """README must state that JSON audit_logs fields match CSV,
        and 'log_timestamp' is the field for operation time.
        """
        readme_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "README.md"
        )
        with open(readme_path, "r", encoding="utf-8") as f:
            readme = f.read()

        self.assertIn(
            "log_timestamp", readme,
            "README missing 'log_timestamp' — downstream uses this field name",
        )

        self.assertIn(
            "audit_logs", readme,
            "README missing JSON 'audit_logs' array description",
        )

        self.assertIn(
            "字段名 / 取值完全一致", readme,
            "README should state JSON and CSV fields are consistent",
        )

        csv_audit_section = readme[
            readme.find("操作记录行字段说明"):readme.find("定位一次变更")
        ]
        self.assertIn(
            "log_timestamp", csv_audit_section,
            "README CSV audit section must document log_timestamp",
        )

    def test_timestamp_values_are_correct_for_each_operation(self):
        """Each audit log entry must have a timestamp reflecting when the
        operation happened, not the event creation time. Timestamps should
        be monotonically increasing across sequential operations.
        """
        import time

        event = AnomalyEvent(
            box_id="BX-TSTAMP-SEQ",
            start_time="2025-06-10 08:00:00",
            end_time="2025-06-10 09:00:00",
            max_temperature=-10.0,
            duration_minutes=60,
        )
        save_events([event])

        event_created_at = event.created_at

        time.sleep(1)

        success, _ = update_event_assignment(
            event.event_id, "早班A", "2025-06-11 18:00:00",
            Priority.HIGH.value, "主管甲", "分派",
        )
        self.assertTrue(success)
        time.sleep(2)

        success, _ = update_event_assignment(
            event.event_id, "早班B", "2025-06-12 12:00:00",
            Priority.URGENT.value, "主管乙", "改期",
        )
        self.assertTrue(success)
        time.sleep(2)

        success, _ = update_event(
            event.event_id, EventStatus.CONFIRMED.value,
            "处理员A", "确认",
        )
        self.assertTrue(success)

        from app import build_json_export
        json_payload = build_json_export(load_events())
        audit_logs = sorted(
            json_payload["audit_logs"],
            key=lambda l: l["log_timestamp"],
        )

        self.assertGreaterEqual(len(audit_logs), 3)

        for log in audit_logs:
            self.assertGreater(
                log["log_timestamp"], event_created_at,
                f"Audit log timestamp {log['log_timestamp']} should be "
                f"after event creation time {event_created_at}",
            )

        for i in range(1, len(audit_logs)):
            self.assertGreaterEqual(
                audit_logs[i]["log_timestamp"],
                audit_logs[i - 1]["log_timestamp"],
                f"Audit log timestamps should be monotonically increasing: "
                f"log[{i}]={audit_logs[i]['log_timestamp']} < "
                f"log[{i-1}]={audit_logs[i-1]['log_timestamp']}",
            )

        field_order = ["assignee", "deadline", "priority", "assignee", "deadline", "priority", "status", "handler", "handler_remark"]
        for i, log in enumerate(audit_logs[:len(field_order)]):
            self.assertEqual(
                log["field_changed"], field_order[i],
                f"Log {i} field_changed mismatch: expected {field_order[i]}, got {log['field_changed']}",
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
