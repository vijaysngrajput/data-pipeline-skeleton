import json
from datetime import datetime, timezone
from typing import Any, Dict

from process_schema import (
    build_effective_schema,
    build_schema_id,
    ensure_seed_schema,
    persist_evolved_schema_if_needed,
    resolve_schema_version,
    validate_event,
)


def process_raw_event(
    raw: str,
    registry_dir: str,
    dataset: str,
    stream_name: str,
    bootstrap_file: str,
    bootstrap_name: str,
) -> str:
    now_utc = datetime.now(timezone.utc).isoformat()

    try:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("payload_not_json_object")
    except Exception as err:
        envelope = {
            "valid": False,
            "dlq": {
                "timestamp_utc": now_utc,
                "source_stream": stream_name,
                "schema_id": None,
                "reason": f"json_parse_error:{err}",
                "raw_event": raw,
                "parsed_event": None,
            },
        }
        return json.dumps(envelope, separators=(",", ":"))

    ensure_seed_schema(
        registry_dir=registry_dir,
        dataset=dataset,
        bootstrap_file=bootstrap_file,
        bootstrap_name=bootstrap_name,
    )

    resolved_version, resolved_schema, resolution_mode = resolve_schema_version(
        payload=payload,
        registry_dir=registry_dir,
        dataset=dataset,
    )
    if resolved_schema is None or resolved_version is None:
        envelope = {
            "valid": False,
            "dlq": {
                "timestamp_utc": now_utc,
                "source_stream": stream_name,
                "schema_id": None,
                "reason": resolution_mode,
                "raw_event": raw,
                "parsed_event": payload,
            },
        }
        return json.dumps(envelope, separators=(",", ":"))

    effective_schema, evolved_fields = build_effective_schema(payload, resolved_schema)

    persisted_version, schema_evolved = persist_evolved_schema_if_needed(
        registry_dir=registry_dir,
        dataset=dataset,
        effective_schema=effective_schema,
        evolved_fields=evolved_fields,
        current_version=resolved_version,
    )

    active_schema = effective_schema
    valid, reason = validate_event(payload, active_schema)
    if not valid:
        schema_id = build_schema_id(dataset, persisted_version)
        envelope = {
            "valid": False,
            "dlq": {
                "timestamp_utc": now_utc,
                "source_stream": stream_name,
                "schema_id": schema_id,
                "reason": reason,
                "raw_event": raw,
                "parsed_event": payload,
            },
        }
        return json.dumps(envelope, separators=(",", ":"))

    processed_event = {
        "timestamp_utc": now_utc,
        "source_stream": stream_name,
        "schema_id": build_schema_id(dataset, persisted_version),
        "schema_resolved_from": resolution_mode,
        "schema_evolved": schema_evolved,
        "content": payload,
    }
    envelope = {"valid": True, "processed": processed_event}
    return json.dumps(envelope, separators=(",", ":"))


def is_valid_envelope(envelope_json: str) -> bool:
    return bool(json.loads(envelope_json)["valid"])


def is_invalid_envelope(envelope_json: str) -> bool:
    return not is_valid_envelope(envelope_json)


def to_processed_json(envelope_json: str) -> str:
    return json.dumps(json.loads(envelope_json)["processed"], separators=(",", ":"))


def to_dlq_json(envelope_json: str) -> str:
    return json.dumps(json.loads(envelope_json)["dlq"], separators=(",", ":"))
