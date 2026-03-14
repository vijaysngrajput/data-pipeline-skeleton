import json
from typing import Dict, Tuple, Optional
from datetime import datetime, timezone

from process_transform import process_raw_event
from process_schema import build_schema_id


class RecoveryHandler:
    """Base handler for DLQ recovery operations."""
    
    @staticmethod
    def add_backfill_metadata(
        processed_event: Dict,
        original_failure_reason: str,
        recovery_action: str
    ) -> Dict:
        """Add backfill metadata to successfully recovered event."""
        processed_event["backfill"] = {
            "source": "dlq_recovery",
            "original_failure_reason": original_failure_reason,
            "recovery_action": recovery_action,
            "recovery_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return processed_event


class TransientRecoveryHandler(RecoveryHandler):
    """Handle transient failures (typically Kafka timeouts)."""
    
    @staticmethod
    def recover(
        dlq_record: Dict,
        registry_dir: str,
        dataset: str,
        bootstrap_file: str,
        bootstrap_name: str
    ) -> Tuple[bool, Optional[Dict], str]:
        """
        Recover transient failure by replaying event through process transform.
        
        Returns: (success: bool, processed_event or None, error_message)
        """
        try:
            # Extract original event from producer DLQ
            original_event = dlq_record.get("event")
            if not original_event:
                return False, None, "missing_event_in_dlq"
            
            # Convert to JSON string for process_raw_event
            raw = json.dumps(original_event, separators=(",", ":"))
            stream_name = dlq_record.get("topic", "unknown")
            
            # Process through transform
            envelope_json = process_raw_event(
                raw=raw,
                registry_dir=registry_dir,
                dataset=dataset,
                stream_name=stream_name,
                bootstrap_file=bootstrap_file,
                bootstrap_name=bootstrap_name
            )
            
            envelope = json.loads(envelope_json)
            
            if envelope["valid"]:
                processed = envelope["processed"]
                processed = RecoveryHandler.add_backfill_metadata(
                    processed,
                    dlq_record.get("reason", "transient_failure"),
                    "kafka_retry"
                )
                return True, processed, ""
            else:
                # Still failed after recovery attempt
                reason = envelope["dlq"].get("reason", "unknown")
                return False, None, f"recovery_failed:{reason}"
        
        except Exception as err:
            return False, None, f"recovery_exception:{str(err)}"


class SchemaFixableRecoveryHandler(RecoveryHandler):
    """Handle schema-related failures."""
    
    @staticmethod
    def recover(
        dlq_record: Dict,
        registry_dir: str,
        dataset: str,
        bootstrap_file: str,
        bootstrap_name: str
    ) -> Tuple[bool, Optional[Dict], str]:
        """
        Recover schema fixable failure by:
        1. Ensuring seed schema exists
        2. Replaying event through process transform
        
        Returns: (success: bool, processed_event or None, error_message)
        """
        try:
            # For process DLQ records
            if "raw_event" in dlq_record:
                raw = dlq_record.get("raw_event")
                stream_name = dlq_record.get("source_stream", "unknown")
            # For producer DLQ records
            elif "event" in dlq_record:
                original_event = dlq_record.get("event")
                raw = json.dumps(original_event, separators=(",", ":"))
                stream_name = dlq_record.get("topic", "unknown")
            else:
                return False, None, "missing_event_data"
            
            # Replay through transform
            envelope_json = process_raw_event(
                raw=raw,
                registry_dir=registry_dir,
                dataset=dataset,
                stream_name=stream_name,
                bootstrap_file=bootstrap_file,
                bootstrap_name=bootstrap_name
            )
            
            envelope = json.loads(envelope_json)
            
            if envelope["valid"]:
                processed = envelope["processed"]
                processed = RecoveryHandler.add_backfill_metadata(
                    processed,
                    dlq_record.get("reason", "schema_fixable"),
                    "schema_heal"
                )
                return True, processed, ""
            else:
                reason = envelope["dlq"].get("reason", "unknown")
                return False, None, f"schema_recovery_failed:{reason}"
        
        except Exception as err:
            return False, None, f"schema_recovery_exception:{str(err)}"


class DataFixableRecoveryHandler(RecoveryHandler):
    """Handle data validation failures."""
    
    @staticmethod
    def recover(
        dlq_record: Dict,
        registry_dir: str,
        dataset: str,
        bootstrap_file: str,
        bootstrap_name: str
    ) -> Tuple[bool, Optional[Dict], str]:
        """
        Recover data fixable failure by:
        1. Attempting type coercion for invalid_type errors
        2. Filling defaults for missing_required_field
        3. Removing unknown fields
        4. Replaying through process transform
        
        Returns: (success: bool, processed_event or None, error_message)
        """
        try:
            # Only works with process DLQ records (has parsed_event)
            if "parsed_event" not in dlq_record:
                return False, None, "no_parsed_event_available"
            
            parsed = dlq_record.get("parsed_event")
            if not parsed:
                return False, None, "parsed_event_is_null"
            
            reason = dlq_record.get("reason", "")
            
            # Attempt fixes based on reason
            if "missing_required_field" in reason:
                # Try to infer missing field from context
                # This is a best-effort strategy
                field_name = reason.split(":")[-1] if ":" in reason else ""
                if field_name:
                    parsed[field_name] = None  # Add default None
            
            elif "invalid_type" in reason:
                # Attempt safe type coercion
                field_name = reason.split(":")[-1] if ":" in reason else ""
                if field_name and field_name in parsed:
                    value = parsed[field_name]
                    # Only attempt safe conversions
                    try:
                        if isinstance(value, str):
                            # Try int conversion
                            parsed[field_name] = int(value)
                    except (ValueError, TypeError):
                        pass
            
            elif "unknown_field" in reason:
                # Remove unknown fields
                field_name = reason.split(":")[-1] if ":" in reason else ""
                if field_name and field_name in parsed:
                    del parsed[field_name]
            
            # Replay through transform
            raw = json.dumps(parsed, separators=(",", ":"))
            stream_name = dlq_record.get("source_stream", "unknown")
            
            envelope_json = process_raw_event(
                raw=raw,
                registry_dir=registry_dir,
                dataset=dataset,
                stream_name=stream_name,
                bootstrap_file=bootstrap_file,
                bootstrap_name=bootstrap_name
            )
            
            envelope = json.loads(envelope_json)
            
            if envelope["valid"]:
                processed = envelope["processed"]
                processed = RecoveryHandler.add_backfill_metadata(
                    processed,
                    dlq_record.get("reason", "data_fixable"),
                    "data_coerce"
                )
                return True, processed, ""
            else:
                reason = envelope["dlq"].get("reason", "unknown")
                return False, None, f"data_recovery_failed:{reason}"
        
        except Exception as err:
            return False, None, f"data_recovery_exception:{str(err)}"


def get_recovery_handler(recovery_type: str):
    """Get the appropriate recovery handler by type."""
    handlers = {
        "transient": TransientRecoveryHandler,
        "schema_fixable": SchemaFixableRecoveryHandler,
        "data_fixable": DataFixableRecoveryHandler,
    }
    return handlers.get(recovery_type)
