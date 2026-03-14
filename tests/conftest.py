import json
import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def temp_registry():
    """Create a temporary schema registry directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir


@pytest.fixture
def bootstrap_file(tmp_path):
    """Create a bootstrap schema file."""
    bootstrap_data = {
        "default_event_v1": {
            "required": {"event_id": "int", "event_type": "str", "value": "int"},
            "optional": {},
            "allow_additional_fields": True,
            "constraints": {"event_type": ["test"]},
        }
    }
    bootstrap_path = tmp_path / "bootstrap.json"
    bootstrap_path.write_text(json.dumps(bootstrap_data), encoding="utf-8")
    return str(bootstrap_path)


@pytest.fixture
def base_schema():
    """Base schema for testing."""
    return {
        "required": {"event_id": "int", "event_type": "str", "value": "int"},
        "optional": {},
        "allow_additional_fields": True,
        "constraints": {"event_type": ["test"]},
    }


@pytest.fixture
def valid_event_payload():
    """Valid event payload that passes all validation."""
    return {"event_id": 123, "event_type": "test", "value": 456}


@pytest.fixture
def event_with_new_field():
    """Event with new field (triggers schema evolution)."""
    return {"event_id": 123, "event_type": "test", "value": 456, "new_field": 999}


@pytest.fixture
def strict_schema():
    """Schema that doesn't allow additional fields."""
    return {
        "required": {"event_id": "int", "event_type": "str", "value": "int"},
        "optional": {},
        "allow_additional_fields": False,
        "constraints": {},
    }
