import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


class LocalJsonlSink:
    def __init__(self, directory: str, filename: str) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / filename

    def write(self, record: Dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, separators=(",", ":")) + os.linesep)


def build_dlq_record(event: Dict[str, Any], reason: str, topic: str) -> Dict[str, Any]:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "topic": topic,
        "reason": reason,
        "event": event,
    }
