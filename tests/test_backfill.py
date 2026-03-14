import pytest
import json
import tempfile
from pathlib import Path
import sys
from datetime import datetime

# Add flink-jobs to path
sys.path.insert(0, str(Path(__file__).parent.parent / "flink-jobs"))

from backfill_config import BackfillConfig, load_backfill_config
from backfill_classifier import FailureClassifier, RecoverabilityType, FailureReason
from backfill_recovery import (
    RecoveryHandler,
    TransientRecoveryHandler,
    SchemaFixableRecoveryHandler,
    DataFixableRecoveryHandler,
    get_recovery_handler,
)
from backfill_job import BackfillJobOrchestrator, BackfillMetrics


class TestFailureClassifier:
    """Test failure classification logic."""
    
    def test_classify_transient_kafka_timeout(self):
        """Classify Kafka timeout as transient."""
        dlq_record = {
            "reason": "KafkaTimeoutError: Connection timeout",
            "event": {"key": "value"},
            "timestamp": "2026-03-11T10:00:00Z"
        }
        classifier = FailureClassifier()
        result = classifier.classify_producer_failure(dlq_record)
        assert result == RecoverabilityType.TRANSIENT
    
    def test_classify_json_parse_error(self):
        """Classify JSON parse error as unfixable."""
        dlq_record = {
            "reason": "JSON_PARSE_ERROR",
            "raw_event": "not valid json",
            "timestamp": "2026-03-11T10:00:00Z"
        }
        classifier = FailureClassifier()
        result = classifier.classify_process_failure(dlq_record)
        assert result == RecoverabilityType.UNFIXABLE
    
    def test_classify_schema_registry_empty(self):
        """Classify empty schema registry as schema_fixable."""
        dlq_record = {
            "reason": "schema_registry_empty: No schemas found",
            "dataset": "default_event",
            "timestamp": "2026-03-11T10:00:00Z"
        }
        classifier = FailureClassifier()
        result = classifier.classify_process_failure(dlq_record)
        assert result == RecoverabilityType.SCHEMA_FIXABLE
    
    def test_classify_invalid_declared_version(self):
        """Classify invalid declared version as unfixable."""
        dlq_record = {
            "reason": "INVALID_DECLARED_VERSION",
            "dataset": "default_event",
            "declared_schema_version": "v999",
            "timestamp": "2026-03-11T10:00:00Z"
        }
        classifier = FailureClassifier()
        result = classifier.classify_process_failure(dlq_record)
        assert result == RecoverabilityType.UNFIXABLE
    
    def test_classify_missing_required_field(self):
        """Classify missing required field as data_fixable."""
        dlq_record = {
            "reason": "missing_required_field: Missing field 'timestamp'",
            "dataset": "default_event",
            "field": "timestamp",
            "timestamp": "2026-03-11T10:00:00Z"
        }
        classifier = FailureClassifier()
        result = classifier.classify_process_failure(dlq_record)
        assert result == RecoverabilityType.DATA_FIXABLE
    
    def test_should_retry_all_strategy(self):
        """Should retry all failures with 'all' strategy."""
        classifier = FailureClassifier()
        
        assert classifier.should_retry(RecoverabilityType.TRANSIENT, "all") is True
        assert classifier.should_retry(RecoverabilityType.SCHEMA_FIXABLE, "all") is True
        assert classifier.should_retry(RecoverabilityType.DATA_FIXABLE, "all") is True
        assert classifier.should_retry(RecoverabilityType.UNFIXABLE, "all") is False
    
    def test_should_retry_smart_filtered_strategy(self):
        """Should retry only certain failures with 'smart_filtered' strategy."""
        classifier = FailureClassifier()
        
        assert classifier.should_retry(RecoverabilityType.TRANSIENT, "smart_filtered") is True
        assert classifier.should_retry(RecoverabilityType.SCHEMA_FIXABLE, "smart_filtered") is True
        # smart_filtered skips data_fixable (risky)
        assert classifier.should_retry(RecoverabilityType.DATA_FIXABLE, "smart_filtered") is False
        assert classifier.should_retry(RecoverabilityType.UNFIXABLE, "smart_filtered") is False
    
    def test_get_recovery_action_transient(self):
        """Get correct recovery action for transient failures."""
        classifier = FailureClassifier()
        action = classifier.get_recovery_action(RecoverabilityType.TRANSIENT)
        assert action == "kafka_retry"
    
    def test_get_recovery_action_schema_fixable(self):
        """Get correct recovery action for schema fixable failures."""
        classifier = FailureClassifier()
        action = classifier.get_recovery_action(RecoverabilityType.SCHEMA_FIXABLE)
        assert action == "schema_heal"
    
    def test_get_recovery_action_data_fixable(self):
        """Get correct recovery action for data fixable failures."""
        classifier = FailureClassifier()
        action = classifier.get_recovery_action(RecoverabilityType.DATA_FIXABLE)
        assert action == "data_coerce"


class TestRecoveryHandlers:
    """Test recovery handler implementations."""
    
    def test_get_recovery_handler_transient(self):
        """Get correct handler for transient recovery."""
        handler = get_recovery_handler("transient")
        assert handler == TransientRecoveryHandler
    
    def test_get_recovery_handler_schema_fixable(self):
        """Get correct handler for schema fixable recovery."""
        handler = get_recovery_handler("schema_fixable")
        assert handler == SchemaFixableRecoveryHandler
    
    def test_get_recovery_handler_data_fixable(self):
        """Get correct handler for data fixable recovery."""
        handler = get_recovery_handler("data_fixable")
        assert handler == DataFixableRecoveryHandler
    
    def test_get_recovery_handler_unknown(self):
        """None returned for unknown recovery type."""
        handler = get_recovery_handler("unknown_type")
        assert handler is None
    
    def test_add_backfill_metadata(self):
        """Verify backfill metadata is correctly added to events."""
        event = {
            "timestamp": "2026-03-11T10:00:00Z",
            "content": {"data": "test"}
        }
        
        result = RecoveryHandler.add_backfill_metadata(
            event,
            "SCHEMA_REGISTRY_EMPTY",
            "schema_heal"
        )
        
        assert "backfill" in result
        assert result["backfill"]["source"] == "dlq_recovery"
        assert result["backfill"]["original_failure_reason"] == "SCHEMA_REGISTRY_EMPTY"
        assert result["backfill"]["recovery_action"] == "schema_heal"
        assert "recovery_timestamp" in result["backfill"]
        
        # Verify timestamp is ISO format
        ts = datetime.fromisoformat(result["backfill"]["recovery_timestamp"])
        assert ts is not None
    
    def test_add_backfill_metadata_preserves_event_data(self):
        """Verify backfill metadata addition preserves original event data."""
        event = {
            "timestamp": "2026-03-11T10:00:00Z",
            "content": {"data": "test"},
            "schema_id": "default_event_v3",
            "source_stream": "kafka_topic_1"
        }
        
        result = RecoveryHandler.add_backfill_metadata(
            event,
            "MISSING_REQUIRED_FIELD",
            "data_coerce"
        )
        
        # Original fields preserved
        assert result["timestamp"] == "2026-03-11T10:00:00Z"
        assert result["content"] == {"data": "test"}
        assert result["schema_id"] == "default_event_v3"
        assert result["source_stream"] == "kafka_topic_1"
        
        # New backfill metadata added
        assert "backfill" in result
        assert result["backfill"]["recovery_action"] == "data_coerce"


class TestBackfillMetrics:
    """Test metrics collection."""
    
    def test_metrics_initialization(self):
        """Verify metrics initialize with correct defaults."""
        metrics = BackfillMetrics()
        
        assert metrics.total_dlq_records == 0
        assert metrics.recovered_records == 0
        assert metrics.unrecoverable_records == 0
        assert metrics.records_by_reason == {}
        assert metrics.recovery_by_type == {}
        assert metrics.errors == []
    
    def test_metrics_to_dict(self):
        """Verify metrics conversion to dictionary."""
        metrics = BackfillMetrics()
        metrics.total_dlq_records = 100
        metrics.recovered_records = 85
        metrics.unrecoverable_records = 15
        metrics.records_by_reason = {"KAFKA_TIMEOUT": 50, "SCHEMA_REGISTRY_EMPTY": 35, "JSON_PARSE_ERROR": 15}
        metrics.recovery_by_type = {"transient": 50, "schema_fixable": 35}
        
        result = metrics.to_dict()
        
        assert result["total_dlq_records"] == 100
        assert result["recovered_records"] == 85
        assert result["unrecoverable_records"] == 15
        assert result["recovery_rate"] == 0.85
        assert "timestamp" in result
        assert result["records_by_reason"]["KAFKA_TIMEOUT"] == 50
        assert result["recovery_by_type"]["transient"] == 50
    
    def test_metrics_recovery_rate_calculation(self):
        """Verify recovery rate is correctly calculated."""
        metrics = BackfillMetrics()
        
        # Test with 0 records
        dict_result = metrics.to_dict()
        assert dict_result["recovery_rate"] == 0.0  # 0 / max(0, 1) = 0
        
        # Test with successful recovery
        metrics.total_dlq_records = 10
        metrics.recovered_records = 7
        dict_result = metrics.to_dict()
        assert dict_result["recovery_rate"] == 0.7
        
        # Test with all recovered
        metrics.total_dlq_records = 10
        metrics.recovered_records = 10
        dict_result = metrics.to_dict()
        assert dict_result["recovery_rate"] == 1.0
    
    def test_metrics_error_capping(self):
        """Verify errors list is capped at 10 items in to_dict()."""
        metrics = BackfillMetrics()
        metrics.errors = [f"error_{i}" for i in range(20)]
        
        result = metrics.to_dict()
        
        assert len(result["sample_errors"]) == 10
        assert result["sample_errors"][0] == "error_0"
        assert result["sample_errors"][9] == "error_9"


class TestBackfillJobOrchestrator:
    """Test backfill job orchestration."""
    
    @pytest.fixture
    def temp_backfill_env(self):
        """Setup temporary backfill environment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            
            # Create separate directories for producer and process DLQ
            process_dlq_dir = tmppath / "process_dlq"
            process_dlq_dir.mkdir(parents=True, exist_ok=True)
            
            # Create config for temp environment
            config = BackfillConfig(
                producer_dlq_path=str(tmppath / "producer_dlq.jsonl"),
                process_dlq_path=str(process_dlq_dir / "process_dlq.jsonl"),
                backfill_from="2026-03-09",
                backfill_to="2026-03-11",
                strategy="all",
                schema_registry_dir="/workspace/flink-jobs/schema_registry",
                schema_dataset="default_event",
                schema_bootstrap_file="/workspace/flink-jobs/schema_registry/default_event/v1.json",
                schema_bootstrap_name="bootstrap",
                processed_output_path=str(tmppath / "processed"),
                backfill_sink_path=str(tmppath / "backfilled.jsonl"),
                dead_sink_path=str(tmppath / "dead.jsonl"),
                metrics_output_path=str(tmppath / "metrics.json"),
                kafka_bootstrap_servers="localhost:9092",
                kafka_topic="events",
                max_retries=3,
                enable_schema_healing=True,
                enable_data_coercion=True
            )
            
            yield config, tmppath
    
    def test_orchestrator_initialization(self, temp_backfill_env):
        """Verify orchestrator initializes correctly."""
        config, tmppath = temp_backfill_env
        
        orchestrator = BackfillJobOrchestrator(config)
        
        assert orchestrator.config == config
        assert orchestrator.metrics is not None
        assert isinstance(orchestrator.metrics, BackfillMetrics)
        assert orchestrator.classifier is not None
    
    def test_orchestrator_classify_record_producer(self, temp_backfill_env):
        """Verify orchestrator can classify producer DLQ records."""
        config, tmppath = temp_backfill_env
        orchestrator = BackfillJobOrchestrator(config)
        
        record = {
            "reason": "KafkaTimeoutError: Connection timeout",
            "event": {"key": "value"},
            "timestamp": "2026-03-11T10:00:00Z"
        }
        
        result = orchestrator.classify_record("producer", record)
        assert result == RecoverabilityType.TRANSIENT
    
    def test_orchestrator_classify_record_process(self, temp_backfill_env):
        """Verify orchestrator can classify process DLQ records."""
        config, tmppath = temp_backfill_env
        orchestrator = BackfillJobOrchestrator(config)
        
        record = {
            "reason": "schema_registry_empty: No schemas found",
            "dataset": "default_event",
            "timestamp": "2026-03-11T10:00:00Z"
        }
        
        result = orchestrator.classify_record("process", record)
        assert result == RecoverabilityType.SCHEMA_FIXABLE
    
    def test_orchestrator_read_empty_dlq(self, temp_backfill_env):
        """Verify orchestrator handles empty DLQ gracefully."""
        config, tmppath = temp_backfill_env
        orchestrator = BackfillJobOrchestrator(config)
        
        # Producer DLQ doesn't exist yet
        records = list(orchestrator.read_dlq_records())
        assert len(records) == 0
    
    def test_orchestrator_read_producer_dlq(self, temp_backfill_env):
        """Verify orchestrator reads producer DLQ records."""
        config, tmppath = temp_backfill_env
        
        # Write producer DLQ records only (not process DLQ)
        producer_dlq = Path(config.producer_dlq_path)
        producer_dlq.parent.mkdir(parents=True, exist_ok=True)
        with open(producer_dlq, "w") as f:
            f.write(json.dumps({"reason": "KafkaTimeoutError", "event": {"key": "value"}}) + "\n")
            f.write(json.dumps({"reason": "KafkaTimeoutError", "event": {"key": "value2"}}) + "\n")
        
        # Don't create process DLQ so we only read producer
        orchestrator = BackfillJobOrchestrator(config)
        records = list(orchestrator.read_dlq_records())
        
        assert len(records) == 2
        assert records[0][0] == "producer"
        assert records[0][1]["reason"] == "KafkaTimeoutError"
    
    def test_orchestrator_metrics_tracking(self, temp_backfill_env):
        """Verify orchestrator tracks metrics correctly."""
        config, tmppath = temp_backfill_env
        orchestrator = BackfillJobOrchestrator(config)
        
        # Simulate metric updates
        orchestrator.metrics.total_dlq_records = 5
        orchestrator.metrics.recovered_records = 3
        orchestrator.metrics.unrecoverable_records = 2
        orchestrator.metrics.records_by_reason["KAFKA_TIMEOUT"] = 3
        orchestrator.metrics.records_by_reason["SCHEMA_REGISTRY_EMPTY"] = 2
        orchestrator.metrics.recovery_by_type["transient"] = 3
        orchestrator.metrics.recovery_by_type["schema_fixable"] = 0
        
        assert orchestrator.metrics.total_dlq_records == 5
        assert orchestrator.metrics.recovered_records == 3
        assert orchestrator.metrics.unrecoverable_records == 2
    
    def test_orchestrator_write_metrics(self, temp_backfill_env):
        """Verify orchestrator correctly writes metrics to file."""
        config, tmppath = temp_backfill_env
        orchestrator = BackfillJobOrchestrator(config)
        
        # Set up metrics
        orchestrator.metrics.total_dlq_records = 10
        orchestrator.metrics.recovered_records = 8
        orchestrator.metrics.records_by_reason = {"KAFKA_TIMEOUT": 10}
        
        orchestrator.write_metrics()
        
        metrics_file = Path(config.metrics_output_path)
        assert metrics_file.exists()
        
        with open(metrics_file, "r") as f:
            metrics_data = json.load(f)
        
        assert metrics_data["total_dlq_records"] == 10
        assert metrics_data["recovered_records"] == 8
        assert metrics_data["recovery_rate"] == 0.8
