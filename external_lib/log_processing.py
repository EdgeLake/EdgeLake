# log_processing.py
import logging
from threading import Lock
from typing import Dict, List, Optional


class ListHandler(logging.Handler):
    """
    Logging handler that stores formatted log lines grouped by level name.
    Thread-safe. Exposes get_logs(level) and clear(level).
    """

    def __init__(self, max_per_level: Optional[int] = None):
        super().__init__()
        self._lock = Lock()
        self._records: Dict[str, List[str]] = {}
        self.max_per_level = max_per_level

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            msg = record.getMessage()
        level = record.levelname.lower()
        with self._lock:
            if level not in self._records:
                self._records[level] = []
            self._records[level].append(msg)
            if self.max_per_level is not None and len(self._records[level]) > self.max_per_level:
                # drop oldest to bound memory
                del self._records[level][0]

    def get_logs(self, level: Optional[str] = None):
        """
        If level is None, return a shallow copy of the whole dict {level: [lines]}.
        If level is provided (e.g. "info" or "error") return a list copy.
        """
        with self._lock:
            if level is None:
                return {k: v.copy() for k, v in self._records.items()}
            return self._records.get(level.lower(), []).copy()

    def clear(self, level: Optional[str] = None) -> None:
        """Clear logs for a specific level or all if level is None."""
        with self._lock:
            if level is None:
                self._records.clear()
            else:
                self._records.pop(level.lower(), None)