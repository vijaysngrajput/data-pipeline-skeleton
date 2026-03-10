import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


VERSION_RE = re.compile(r"^v(\d+)\.json$")


def is_type_match(value: Any, expected: str) -> bool:
    mapping = {
        "int": int,
        "float": float,
        "str": str,
        "bool": bool,
        "dict": dict,
        "list": list,
    }
    expected_type = mapping.get(expected)
    if expected_type is None:
        return False
    return isinstance(value, expected_type)


def infer_type_name(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "str"
    if isinstance(value, dict):
        return "dict"
    if isinstance(value, list):
        return "list"
    return "str"


def validate_event(payload: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, str]:
    required = schema.get("required", {})
    optional = schema.get("optional", {})
    constraints = schema.get("constraints", {})
    allow_additional_fields = schema.get("allow_additional_fields", True)

    for field, expected_type in required.items():
        if field not in payload:
            return False, f"missing_required_field:{field}"
        if not is_type_match(payload[field], expected_type):
            return False, f"invalid_type:{field}:expected_{expected_type}"

    for field, expected_type in optional.items():
        if field in payload and not is_type_match(payload[field], expected_type):
            return False, f"invalid_type:{field}:expected_{expected_type}"

    if not allow_additional_fields:
        allowed_fields = set(required.keys()) | set(optional.keys())
        for field in payload.keys():
            if field not in allowed_fields:
                return False, f"unknown_field:{field}"

    for field, allowed_values in constraints.items():
        if field in payload and payload[field] not in allowed_values:
            return False, f"constraint_violation:{field}"

    return True, "ok"


def build_effective_schema(
    payload: Dict[str, Any],
    base_schema: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    effective = deepcopy(base_schema)
    required = effective.setdefault("required", {})
    optional = effective.setdefault("optional", {})
    effective.setdefault("constraints", {})
    allow_additional_fields = effective.get("allow_additional_fields", True)

    evolved_fields: Dict[str, str] = {}
    if not allow_additional_fields:
        return effective, evolved_fields

    known_fields = set(required.keys()) | set(optional.keys())
    for field, value in payload.items():
        if field in known_fields:
            continue
        inferred = infer_type_name(value)
        optional[field] = inferred
        evolved_fields[field] = inferred

    return effective, evolved_fields


def _dataset_dir(registry_dir: str, dataset: str) -> Path:
    path = Path(registry_dir) / dataset
    path.mkdir(parents=True, exist_ok=True)
    return path


def _version_file(dataset_dir: Path, version: int) -> Path:
    return dataset_dir / f"v{version}.json"


def _load_bootstrap_schema(bootstrap_file: str, bootstrap_name: str) -> Dict[str, Any]:
    with open(bootstrap_file, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, dict) and bootstrap_name in payload:
        return payload[bootstrap_name]
    if isinstance(payload, dict) and "required" in payload:
        return payload
    raise ValueError(f"Bootstrap schema '{bootstrap_name}' not found in {bootstrap_file}")


def ensure_seed_schema(
    registry_dir: str,
    dataset: str,
    bootstrap_file: str,
    bootstrap_name: str,
) -> None:
    ddir = _dataset_dir(registry_dir, dataset)
    if list(ddir.glob("v*.json")):
        return
    schema = _load_bootstrap_schema(bootstrap_file, bootstrap_name)
    _version_file(ddir, 1).write_text(
        json.dumps(schema, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def load_versions(registry_dir: str, dataset: str) -> List[int]:
    versions: List[int] = []
    for f in _dataset_dir(registry_dir, dataset).glob("v*.json"):
        m = VERSION_RE.match(f.name)
        if m:
            versions.append(int(m.group(1)))
    versions.sort()
    return versions


def load_schema_version(registry_dir: str, dataset: str, version: int) -> Dict[str, Any]:
    path = _version_file(_dataset_dir(registry_dir, dataset), version)
    if not path.exists():
        raise ValueError(f"schema version not found: {dataset}/v{version}.json")
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_signature(schema: Dict[str, Any]) -> str:
    return json.dumps(schema, sort_keys=True, separators=(",", ":"))


def find_matching_version(
    registry_dir: str,
    dataset: str,
    schema: Dict[str, Any],
) -> Optional[int]:
    target = _schema_signature(schema)
    for version in load_versions(registry_dir, dataset):
        existing = load_schema_version(registry_dir, dataset, version)
        if _schema_signature(existing) == target:
            return version
    return None


def resolve_schema_version(
    payload: Dict[str, Any],
    registry_dir: str,
    dataset: str,
) -> Tuple[Optional[int], Optional[Dict[str, Any]], str]:
    versions = load_versions(registry_dir, dataset)
    if not versions:
        return None, None, "schema_registry_empty"

    declared_version = payload.get("schema_version")
    if declared_version is not None:
        try:
            version = int(declared_version)
        except Exception:
            return None, None, "invalid_declared_schema_version"
        if version in versions:
            return version, load_schema_version(registry_dir, dataset, version), "declared"
        return None, None, "declared_schema_version_not_found"

    latest = versions[-1]
    return latest, load_schema_version(registry_dir, dataset, latest), "latest"


def persist_evolved_schema_if_needed(
    registry_dir: str,
    dataset: str,
    effective_schema: Dict[str, Any],
    evolved_fields: Dict[str, str],
    current_version: int,
) -> Tuple[int, bool]:
    if not evolved_fields:
        return current_version, False

    matched = find_matching_version(registry_dir, dataset, effective_schema)
    if matched is not None:
        return matched, matched != current_version

    versions = load_versions(registry_dir, dataset)
    new_version = (versions[-1] if versions else current_version) + 1
    path = _version_file(_dataset_dir(registry_dir, dataset), new_version)
    path.write_text(
        json.dumps(effective_schema, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return new_version, True
