"""
Instructor sign-in by PIN.

The PIN lives only in the environment variable INSTRUCTOR_PIN (set it in the
Render dashboard). When it is unset, instructor mode is disabled and every
visitor is a student. Failed attempts are throttled per client address.
"""
import hmac
import os
import threading
import time
from collections import deque

PIN_ENV_VAR = "INSTRUCTOR_PIN"
MAX_PIN_LENGTH = 64
MAX_FAILURES = 5
FAILURE_WINDOW_SECONDS = 60


def pin_configured() -> bool:
    return bool(os.environ.get(PIN_ENV_VAR, "").strip())


def check_pin(candidate) -> bool:
    """Constant-time comparison against the configured PIN."""
    expected = os.environ.get(PIN_ENV_VAR, "").strip()
    if not expected or not isinstance(candidate, str):
        return False
    if not candidate or len(candidate) > MAX_PIN_LENGTH:
        return False
    return hmac.compare_digest(candidate.strip().encode(), expected.encode())


class LoginThrottle:
    """Allows at most MAX_FAILURES failed attempts per key per window."""

    def __init__(self, max_failures=MAX_FAILURES, window=FAILURE_WINDOW_SECONDS, clock=time.monotonic):
        self._max = max_failures
        self._window = window
        self._clock = clock
        self._failures = {}
        self._lock = threading.Lock()

    def _recent(self, key):
        cutoff = self._clock() - self._window
        stamps = self._failures.get(key, deque())
        while stamps and stamps[0] < cutoff:
            stamps.popleft()
        return stamps

    def is_blocked(self, key) -> bool:
        with self._lock:
            return len(self._recent(key)) >= self._max

    def record_failure(self, key) -> None:
        with self._lock:
            stamps = self._recent(key)
            stamps.append(self._clock())
            self._failures[key] = stamps

    def reset(self, key) -> None:
        with self._lock:
            self._failures.pop(key, None)
