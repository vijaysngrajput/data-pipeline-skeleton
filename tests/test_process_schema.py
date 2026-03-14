import json
import pytest
import sys
from pathlib import Path

# Add flink-jobs to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "flink-jobs"))

from process_schema import (
    build_schema_id,
    is_type_match,
    infer_type_name,
    validate_event,
    build_effective_schema,
)


class TestBuildSchemaId:
    """Test schema_id builder utility"""
    
    def test_build_schema_id_basic(self):
        assert build_schema_id("default_event", 1) == "default_event_v1"

    def test_build_schema_id_higher_version(self):
        assert build_schema_id("default_event", 42) == "default_event_v42"

    def test_build_schema_id_different_dataset(self):
        assert build_schema_id("user_event", 3) == "user_event_v3"


class TestIsTypeMatch:
    """Test type matching logic"""
    
    def test_int_match(self):
        assert is_type_match(42, "int") is True
        assert is_type_match(0, "int") is True
        assert is_type_match("42", "int") is False

    def test_str_match(self):
        assert is_type_match("hello", "str") is True
        assert is_type_match(42, "str") is False

    def test_float_match(self):
        assert is_type_match(3.14, "float") is True
        assert is_type_match(42, "float") is False

    def test_bool_match(self):
        assert is_type_match(True, "bool") is True
        assert is_type_match(1, "bool") is False

    def test_dict_match(self):
        assert is_type_match({}, "dict") is True
        assert is_type_match([], "dict") is False

    def test_list_match(self):
        assert is_type_match([1, 2], "list") is True
        assert is_type_match({}, "list") is False

    def test_unknown_type(self):
        assert is_type_match(42, "unknown") is False


class TestInferTypeName:
    """Test type inference"""
    
    def test_infer_bool(self):
        assert infer_type_name(True) == "bool"

    def test_infer_int(self):
        assert infer_type_name(42) == "int"

    def test_infer_float(self):
        assert infer_type_name(3.14) == "float"

    def test_infer_str(self):
        assert infer_type_name("hello") == "str"

    def test_infer_dict(self):
        assert infer_type_name({"a": 1}) == "dict"

    def test_infer_list(self):
        assert infer_type_name([1, 2]) == "list"


class TestValidateEvent:
    """Test event validation with schemas"""
    
    def test_valid_event_passes(self, valid_event_payload, base_schema):
        """SUCCESS: Valid event passes schema validation"""
        valid, reason = validate_event(valid_event_payload, base_schema)
        assert valid is True
        assert reason == "ok"

    def test_missing_required_field(self, base_schema):
        """FAILURE: Missing required field"""
        payload = {"event_type": "test", "value": 456}  # missing event_id
        valid, reason = validate_event(payload, base_schema)
        assert valid is False
        assert "missing_required_field:event_id" in reason

    def test_invalid_field_type(self, base_schema):
        """FAILURE: Invalid field type"""
        payload = {"event_id": "not_an_int", "event_type": "test", "value": 456}
        valid, reason = validate_event(payload, base_schema)
        assert valid is False
        assert "invalid_type:event_id" in reason

    def test_constraint_violation(self, base_schema):
        """FAILURE: Constraint violation"""
        payload = {"event_id": 123, "event_type": "invalid_type", "value": 456}
        valid, reason = validate_event(payload, base_schema)
        assert valid is False
        assert "constraint_violation:event_type" in reason

    def test_extra_fields_allowed(self, valid_event_payload, base_schema):
        """SUCCESS: Extra fields allowed by schema"""
        payload = {**valid_event_payload, "extra": "allowed"}
        valid, reason = validate_event(payload, base_schema)
        assert valid is True

    def test_extra_fields_rejected(self, valid_event_payload, strict_schema):
        """FAILURE: Extra fields not allowed by schema"""
        payload = {**valid_event_payload, "extra": "not_allowed"}
        valid, reason = validate_event(payload, strict_schema)
        assert valid is False
        assert "unknown_field:extra" in reason


class TestBuildEffectiveSchema:
    """Test schema evolution"""
    
    def test_schema_evolution_with_new_field(self, event_with_new_field, base_schema):
        """SUCCESS: Schema evolves with new field"""
        effective, evolved_fields = build_effective_schema(event_with_new_field, base_schema)
        assert "new_field" in effective["optional"]
        assert effective["optional"]["new_field"] == "int"
        assert evolved_fields == {"new_field": "int"}

    def test_no_evolution_no_new_fields(self, valid_event_payload, base_schema):
        """No evolution when no new fields"""
        effective, evolved_fields = build_effective_schema(valid_event_payload, base_schema)
        assert evolved_fields == {}
