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
from core.models import AnomalyEvent, EventStatus, ImportBatch, SkippedRowLog
from core.persistence import (
    add_events,
    clear_all_for_test,
    get_audit_logs_for_event,
    get_evidence_for_event,
    get_events_by_raw_data_hash,
    get_skipped_logs_for_batch,
    is_exact_duplicate_batch,
    load_audit_logs,
    load_batches,
    load_events,
    load_evidence,
    load_skipped_logs,
    save_batches,
    save_events,
    update_event,
    update_events_for_reanalysis,
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
        self.assertEqual(len(old_audit), 1)
        self.assertEqual(old_audit[0].operator, "测试员")

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
        self.assertEqual(audit_after[0].operator, "测试员")
        self.assertEqual(audit_after[0].remark, "确认超温")

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

        self.assertEqual(len(load_audit_logs()), len(old_audit) + (1 if event_2.event_id != event_to_review.event_id else 0))

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
