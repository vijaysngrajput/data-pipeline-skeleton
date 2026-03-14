import json
from typing import Dict, Tuple
from enum import Enum


class RecoverabilityType(str, Enum):
    """Failure recoverability classification."""
    
    TRANSIENT = "transient"  # Infrastructure issue, retry Kafka publish
    SCHEMA_FIXABLE = "schema_fixable"  # Schema registry issue, auto-heal possible
    DATA_FIXABLE = "data_fixable"  # Data validation issue, can coerce/transform
    UNFIXABLE = "unfixable"  # Permanent error, requires manual intervention


class FailureReason(str, Enum):
    """Standardized failure reason categories."""
    
    # Producer DLQ failures
    KAFKA_TIMEOUT = "kafka_timeout"
    KAFKA_ERROR = "kafka_error"
    
    # Process DLQ failures - Parsing
    JSON_PARSE_ERROR = "json_parse_error"
    
    # Process DLQ failures - Schema Resolution
    SCHEMA_REGISTRY_EMPTY = "schema_registry_empty"
    INVALID_DECLARED_VERSION = "invalid_declared_schema_version"
    DECLARED_VERSION_NOT_FOUND = "declared_schema_version_not_found"
    
    # Process DLQ failures - Validation
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_TYPE = "invalid_type"
    CONSTRAINT_VIOLATION = "constraint_violation"
    UNKNOWN_FIELD = "unknown_field"


class FailureClassifier:
    """Classify DLQ failures by recoverability."""
    
    TRANSIENT_REASONS = {
        "KafkaTimeoutError",
        "KafkaConnectionError",
        "BrokerNotAvailable",
    }
    
    SCHEMA_FIXABLE_REASONS = {
        FailureReason.SCHEMA_REGISTRY_EMPTY,
        FailureReason.DECLARED_VERSION_NOT_FOUND,
        FailureReason.INVALID_DECLARED_VERSION,
    }
    
    DATA_FIXABLE_REASONS = {
        FailureReason.INVALID_TYPE,
        FailureReason.MISSING_REQUIRED_FIELD,
        FailureReason.UNKNOWN_FIELD,
    }
    
    UNFIXABLE_REASONS = {
        FailureReason.CONSTRAINT_VIOLATION,
        FailureReason.JSON_PARSE_ERROR,
    }
    
    @classmethod
    def classify_producer_failure(cls, dlq_record: Dict) -> RecoverabilityType:
        """
        Classify producer DLQ failure.
        
        Producer DLQ format:
        {
            "timestamp_utc": "...",
            "topic": "...",
            "reason": "KafkaTimeoutError: ...",
            "event": {...}
        }
        """
        reason = dlq_record.get("reason", "")
        
        # Check for transient errors
        for transient_reason in cls.TRANSIENT_REASONS:
            if transient_reason in reason:
                return RecoverabilityType.TRANSIENT
        
        # If Kafka error, assume transient
        if "Kafka" in reason or "kafka" in reason:
            return RecoverabilityType.TRANSIENT
        
        # Default to unfixable
        return RecoverabilityType.UNFIXABLE
    
    @classmethod
    def classify_process_failure(cls, dlq_record: Dict) -> RecoverabilityType:
        """
        Classify process service DLQ failure.
        
        Process DLQ format (new):
        {
            "timestamp_utc": "...",
            "source_stream": "...",
            "schema_id": "..." or null,
            "reason": "...",
            "raw_event": "...",
            "parsed_event": {...} or null
        }
        """
        reason = dlq_record.get("reason", "")
        
        # Check schema fixable
        for schema_reason in cls.SCHEMA_FIXABLE_REASONS:
            if schema_reason.value in reason:
                return RecoverabilityType.SCHEMA_FIXABLE
        
        # Check data fixable
        for data_reason in cls.DATA_FIXABLE_REASONS:
            if data_reason.value in reason:
                return RecoverabilityType.DATA_FIXABLE
        
        # Check unfixable (these require manual intervention)
        for unfixable_reason in cls.UNFIXABLE_REASONS:
            if unfixable_reason.value in reason:
                return RecoverabilityType.UNFIXABLE
        
        # Default to unfixable for unknown reasons
        return RecoverabilityType.UNFIXABLE
    
    @classmethod
    def should_retry(cls, failure_type: RecoverabilityType, strategy: str) -> bool:
        """
        Determine if failure should be retried based on strategy.
        
        Strategies:
        - "all": Retry all recoverable types
        - "smart_filtered": Skip data_fixable (requires high confidence validation)
        """
        if strategy == "all":
            return failure_type in [
                RecoverabilityType.TRANSIENT,
                RecoverabilityType.SCHEMA_FIXABLE,
                RecoverabilityType.DATA_FIXABLE,
            ]
        elif strategy == "smart_filtered":
            # Only retry transient and schema fixable, skip data fixable
            return failure_type in [
                RecoverabilityType.TRANSIENT,
                RecoverabilityType.SCHEMA_FIXABLE,
            ]
        
        return False
    
    @classmethod
    def get_recovery_action(cls, failure_type: RecoverabilityType) -> str:
        """Get the recovery action for a given failure type."""
        actions = {
            RecoverabilityType.TRANSIENT: "kafka_retry",
            RecoverabilityType.SCHEMA_FIXABLE: "schema_heal",
            RecoverabilityType.DATA_FIXABLE: "data_coerce",
            RecoverabilityType.UNFIXABLE: "manual_review",
        }
        return actions.get(failure_type, "unknown")
