import json
import sys
from pathlib import Path
from typing import Iterator, Dict, Tuple
from datetime import datetime

from backfill_config import load_backfill_config, BackfillConfig
from backfill_classifier import FailureClassifier, RecoverabilityType
from backfill_recovery import get_recovery_handler


class BackfillMetrics:
    """Track backfill job metrics."""
    
    def __init__(self):
        self.total_dlq_records = 0
        self.recovered_records = 0
        self.unrecoverable_records = 0
        self.records_by_reason = {}
        self.recovery_by_type = {}
        self.errors = []
    
    def to_dict(self):
        """Convert metrics to dictionary."""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "total_dlq_records": self.total_dlq_records,
            "recovered_records": self.recovered_records,
            "recovery_rate": self.recovered_records / max(self.total_dlq_records, 1),
            "unrecoverable_records": self.unrecoverable_records,
            "records_by_reason": self.records_by_reason,
            "recovery_by_type": self.recovery_by_type,
            "sample_errors": self.errors[:10],  # Keep first 10 errors
        }


class BackfillJobOrchestrator:
    """Orchestrate the backfill batch processing job."""
    
    def __init__(self, config: BackfillConfig):
        self.config = config
        self.metrics = BackfillMetrics()
        self.classifier = FailureClassifier()
    
    def read_dlq_records(self) -> Iterator[Tuple[str, Dict, str]]:
        """
        Read DLQ records from both producer and process DLQ.
        
        Yields: (source: "producer" | "process", record: Dict, line: str)
        """
        # Read producer DLQ
        producer_dlq_path = Path(self.config.producer_dlq_path)
        if producer_dlq_path.exists():
            with open(producer_dlq_path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        yield "producer", record, line.strip()
                    except json.JSONDecodeError as err:
                        self.metrics.errors.append(f"Producer DLQ parse error: {err}")
        
        # Read process DLQ (datetime-bucketed)
        process_dlq_dir = Path(self.config.process_dlq_path).parent
        if process_dlq_dir.exists():
            for dlq_file in process_dlq_dir.glob("**/*.jsonl"):
                with open(dlq_file, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            yield "process", record, line.strip()
                        except json.JSONDecodeError as err:
                            self.metrics.errors.append(f"Process DLQ parse error: {err}")
    
    def classify_record(self, source: str, record: Dict) -> RecoverabilityType:
        """Classify a DLQ record by recoverability."""
        if source == "producer":
            return self.classifier.classify_producer_failure(record)
        else:
            return self.classifier.classify_process_failure(record)
    
    def recover_record(
        self,
        source: str,
        record: Dict,
        failure_type: RecoverabilityType
    ) -> Tuple[bool, Dict, str]:
        """
        Attempt to recover a DLQ record.
        
        Returns: (success: bool, processed_event or error_record: Dict, error_message: str)
        """
        handler_class = get_recovery_handler(failure_type.value)
        
        if not handler_class:
            return False, record, f"no_handler_for_{failure_type}"
        
        try:
            success, processed, error_msg = handler_class.recover(
                record,
                registry_dir=self.config.schema_registry_dir,
                dataset=self.config.schema_dataset,
                bootstrap_file=self.config.schema_bootstrap_file,
                bootstrap_name=self.config.schema_bootstrap_name
            )
            
            return success, processed or record, error_msg
        
        except Exception as err:
            return False, record, f"recovery_handler_exception: {str(err)}"
    
    def process_dlq_batch(self):
        """
        Process the entire DLQ in batch mode.
        
        Writes to:
        - backfill_sink_path: successfully recovered events
        - dead_sink_path: unrecoverable events
        - metrics_output_path: recovery metrics
        """
        recovered_file = Path(self.config.backfill_sink_path)
        recovered_file.parent.mkdir(parents=True, exist_ok=True)
        
        dead_file = Path(self.config.dead_sink_path)
        dead_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(recovered_file, "a", encoding="utf-8") as recovered_f, \
             open(dead_file, "a", encoding="utf-8") as dead_f:
            
            for source, record, line in self.read_dlq_records():
                self.metrics.total_dlq_records += 1
                
                # Classify failure
                failure_type = self.classify_record(source, record)
                
                # Track by reason
                reason = record.get("reason", "unknown")
                self.metrics.records_by_reason[reason] = \
                    self.metrics.records_by_reason.get(reason, 0) + 1
                
                # Check if should retry
                should_retry = self.classifier.should_retry(failure_type, self.config.strategy)
                
                if not should_retry:
                    # Send directly to dead sink
                    dead_f.write(json.dumps(record, separators=(",", ":")) + "\n")
                    self.metrics.unrecoverable_records += 1
                    self.metrics.recovery_by_type[failure_type.value] = \
                        self.metrics.recovery_by_type.get(failure_type.value, 0) + 1
                    continue
                
                # Attempt recovery
                success, processed, error_msg = self.recover_record(source, record, failure_type)
                
                if success:
                    # Write to recovered sink
                    recovered_f.write(
                        json.dumps(processed, separators=(",", ":")) + "\n"
                    )
                    self.metrics.recovered_records += 1
                    self.metrics.recovery_by_type[failure_type.value] = \
                        self.metrics.recovery_by_type.get(failure_type.value, 0) + 1
                    print(f"✓ Recovered: {failure_type.value} - {reason[:50]}")
                else:
                    # Still unrecoverable, send to dead sink
                    dead_f.write(json.dumps(record, separators=(",", ":")) + "\n")
                    self.metrics.unrecoverable_records += 1
                    if error_msg:
                        self.metrics.errors.append(f"{reason}: {error_msg}")
                    print(f"✗ Failed: {failure_type.value} - {reason[:50]} - {error_msg[:30]}")
    
    def write_metrics(self):
        """Write recovery metrics to output file."""
        metrics_file = Path(self.config.metrics_output_path)
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(self.metrics.to_dict(), f, indent=2)
    
    def run(self):
        """Execute the backfill job."""
        print(f"Starting backfill job with strategy: {self.config.strategy}")
        print(f"Date range: {self.config.backfill_from} to {self.config.backfill_to}")
        
        self.process_dlq_batch()
        self.write_metrics()
        
        print("\n" + "="*60)
        print("Backfill Job Summary")
        print("="*60)
        print(f"Total DLQ records: {self.metrics.total_dlq_records}")
        print(f"Successfully recovered: {self.metrics.recovered_records}")
        print(f"Unrecoverable: {self.metrics.unrecoverable_records}")
        print(f"Recovery rate: {self.metrics.recovered_records / max(self.metrics.total_dlq_records, 1):.1%}")
        print(f"\nMetrics saved to: {self.config.metrics_output_path}")
        print(f"Recovered events: {self.config.backfill_sink_path}")
        print(f"Dead events: {self.config.dead_sink_path}")
        print("="*60)


def main():
    """Entry point for backfill batch job."""
    config = load_backfill_config()
    orchestrator = BackfillJobOrchestrator(config)
    orchestrator.run()


if __name__ == "__main__":
    main()
