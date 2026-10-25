from __future__ import annotations

import time
from collections.abc import Callable

import pytest

from dtsched import Store

class FakeClock:

    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start
    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def db_path(tmp_path) -> str:
    return str(tmp_path / "tasks.db")

@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()

@pytest.fixture
def store(db_path, clock) -> Store:
    # rand=1.0 makes jittered backoff deterministic: the full delay.
    s = Store(db_path, clock=clock, rand=lambda: 1.0)
    yield s
    s.close()

def wait_for(pred: Callable[[], object], timeout: float = 15.0, interval: float = 0.05):
    """Poll "pred" until it returns something truthy, or fail the test."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = pred()
        if value:
            return value
        time.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")
