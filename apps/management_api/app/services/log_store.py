from __future__ import annotations

import threading
from collections import defaultdict, deque


class RunLogStore:
    def __init__(self, max_lines: int = 2000) -> None:
        self._lock = threading.Lock()
        self._max_lines = max_lines
        self._logs: dict[str, deque[str]] = defaultdict(lambda: deque(maxlen=self._max_lines))

    def append(self, run_id: str, line: str) -> None:
        with self._lock:
            self._logs[run_id].append(line)

    def get_since(self, run_id: str, cursor: int) -> tuple[list[str], int]:
        with self._lock:
            lines = list(self._logs.get(run_id, deque()))
        if cursor >= len(lines):
            return [], len(lines)
        return lines[cursor:], len(lines)


run_log_store = RunLogStore()
