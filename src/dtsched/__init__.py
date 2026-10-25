"""dtsched: a distributed task scheduler backed by SQLite.

Producers submit tasks to a shared store; worker processes claim them under a
lease, run them with a timeout and report back; a coordinator detects dead
workers, recovers their tasks and fires recurring schedules.
"""

from .backoff import backoff_delay
from .coordinator import Coordinator
from .cron import CronError, CronSchedule
from .store import ReapResult, Store, SubmitResult, Task
from .worker import TaskContext, Worker

__all__ = [
    "Coordinator",
    "CronError",
    "CronSchedule",
    "ReapResult",
    "Store",
    "SubmitResult",
    "Task",
    "TaskContext",
    "Worker",
    "backoff_delay",
]
__version__ = "0.1.0"
