from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class ToolCallLogEvent:
    """Single JSONL event for OCR correction tool-call tracing."""

    phase: str
    turn_index: int
    tool_name: str
    args: dict[str, Any]
    status: str
    details: dict[str, Any] | None = None

    def to_json(self) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": self.phase,
            "turn_index": self.turn_index,
            "tool_name": self.tool_name,
            "args": self.args,
            "status": self.status,
        }
        if self.details:
            payload["details"] = self.details
        return json.dumps(payload, ensure_ascii=False)


class ToolCallLogger:
    """Append-only UTF-8 JSONL logger for OCR tool-call events."""

    def __init__(self, output_path: Path):
        self.output_path = output_path.expanduser().resolve()
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def write_event(self, event: ToolCallLogEvent) -> None:
        with self.output_path.open("a", encoding="utf-8", newline="\n") as output_file:
            # Single write reduces (but does not eliminate) interleaving risk under
            # multi-process append; full atomicity would require OS-level locking.
            output_file.write(event.to_json() + "\n")
