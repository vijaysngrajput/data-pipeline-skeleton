import json
import pytest
import sys
from pathlib import Path

# Add flink-jobs to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "flink-jobs"))

from process_transform import (
    process_raw_event,
    is_valid_envelope,
    is_invalid_envelope,
    to_processed_json,
    to_dlq_json,
)


class TestProcessRawEventSuccess:
    """Test SUCCESS scenarios for event processing"""
    
    def test_valid_event_produces_valid_envelope(self, temp_registry, bootstrap_file):
        """SUCCESS: Valid event creates valid envelope with schema_id"""
        raw = json.dumps({
            "event_id": 0,
            "event_type": "test",
            "value": 100
        })
        
        result = process_raw_event(
            raw=raw,
            registry_dir=temp_registry,
            dataset="default_event",
            stream_name="test-topic",
            bootstrap_file=bootstrap_file,
            bootstrap_name="default_event_v1"
        )
        
        envelope = json.loads(result)
        assert envelope["valid"] is True
        assert "processed" in envelope
        
        processed = envelope["processed"]
        assert "timestamp_utc" in processed
        assert "schema_id" in processed
        assert processed["schema_id"] == "default_event_v1"
        assert processed["content"]["event_id"] == 0
        assert processed["content"]["event_type"] == "test"
        assert "source_stream" in processed
        assert "schema_resolved_from" in processed

    def test_valid_envelope_passes_filter(self, temp_registry, bootstrap_file):
        """SUCCESS: Valid envelope passes is_valid_envelope filter"""
        raw = json.dumps({"event_id": 1, "event_type": "test", "value": 200})
        envelope_json = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        assert is_valid_envelope(envelope_json) is True
        assert is_invalid_envelope(envelope_json) is False

    def test_processed_json_extraction(self, temp_registry, bootstrap_file):
        """SUCCESS: Processed JSON can be extracted from envelope"""
        raw = json.dumps({"event_id": 2, "event_type": "test", "value": 300})
        envelope_json = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        
        processed_json = to_processed_json(envelope_json)
        processed = json.loads(processed_json)
        assert "schema_id" in processed
        assert processed["content"]["event_id"] == 2


class TestProcessRawEventFailures:
    """Test FAILURE scenarios for event processing"""
    
    def test_json_parse_error_creates_dlq_record(self, temp_registry, bootstrap_file):
        """FAILURE: Malformed JSON goes to DLQ"""
        raw = "{invalid json"
        
        result = process_raw_event(
            raw=raw,
            registry_dir=temp_registry,
            dataset="default_event",
            stream_name="test-topic",
            bootstrap_file=bootstrap_file,
            bootstrap_name="default_event_v1"
        )
        
        envelope = json.loads(result)
        assert envelope["valid"] is False
        assert "dlq" in envelope
        assert envelope["dlq"]["schema_id"] is None
        assert "json_parse_error" in envelope["dlq"]["reason"]
        assert envelope["dlq"]["parsed_event"] is None

    def test_missing_required_field_creates_dlq(self, temp_registry, bootstrap_file):
        """FAILURE: Missing required field goes to DLQ"""
        raw = json.dumps({"event_type": "test", "value": 100})  # missing event_id
        
        result = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        
        envelope = json.loads(result)
        assert envelope["valid"] is False
        dlq = envelope["dlq"]
        assert dlq["schema_id"] is not None  # Schema ID should be set for validation failures
        assert "missing_required_field:event_id" in dlq["reason"]
        assert dlq["raw_event"] == raw

    def test_invalid_type_creates_dlq(self, temp_registry, bootstrap_file):
        """FAILURE: Invalid field type goes to DLQ"""
        raw = json.dumps({
            "event_id": "not_an_int",  # Should be int
            "event_type": "test",
            "value": 100
        })
        
        result = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        
        envelope = json.loads(result)
        assert envelope["valid"] is False
        dlq = envelope["dlq"]
        assert "invalid_type:event_id" in dlq["reason"]

    def test_constraint_violation_creates_dlq(self, temp_registry, bootstrap_file):
        """FAILURE: Constraint violation goes to DLQ"""
        raw = json.dumps({
            "event_id": 1,
            "event_type": "invalid_type",  # Must be "test"
            "value": 100
        })
        
        result = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        
        envelope = json.loads(result)
        assert envelope["valid"] is False
        dlq = envelope["dlq"]
        assert "constraint_violation:event_type" in dlq["reason"]

    def test_invalid_envelope_fails_filter(self, temp_registry, bootstrap_file):
        """FAILURE: Invalid envelope fails is_valid_envelope filter"""
        raw = json.dumps({"event_type": "test"})  # Missing required fields
        envelope_json = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        assert is_valid_envelope(envelope_json) is False
        assert is_invalid_envelope(envelope_json) is True

    def test_dlq_json_extraction(self, temp_registry, bootstrap_file):
        """FAILURE: DLQ JSON can be extracted from envelope"""
        raw = json.dumps({"event_id": 1})  # Missing other fields
        envelope_json = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        
        dlq_json = to_dlq_json(envelope_json)
        dlq = json.loads(dlq_json)
        assert "reason" in dlq
        assert "schema_id" in dlq
        assert "timestamp_utc" in dlq


class TestEnvelopeStructure:
    """Test envelope structure compliance"""
    
    def test_success_envelope_has_required_fields(self, temp_registry, bootstrap_file):
        """Processed envelope has all required fields"""
        raw = json.dumps({"event_id": 1, "event_type": "test", "value": 100})
        envelope_json = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        envelope = json.loads(envelope_json)
        processed = envelope["processed"]
        
        required_fields = ["timestamp_utc", "source_stream", "schema_id", "schema_resolved_from", "schema_evolved", "content"]
        for field in required_fields:
            assert field in processed, f"Missing required field: {field}"

    def test_dlq_envelope_has_required_fields(self, temp_registry, bootstrap_file):
        """DLQ envelope has all required fields"""
        raw = json.dumps({"event_id": 1})
        envelope_json = process_raw_event(
            raw, temp_registry, "default_event", "test-topic", bootstrap_file, "default_event_v1"
        )
        envelope = json.loads(envelope_json)
        dlq = envelope["dlq"]
        
        required_fields = ["timestamp_utc", "source_stream", "schema_id", "reason", "raw_event", "parsed_event"]
        for field in required_fields:
            assert field in dlq, f"Missing required DLQ field: {field}"
