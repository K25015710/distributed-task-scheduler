"""A small five-field cron expression parser.

Supports the usual syntax for "minute hour day-of-month month day-of-week":
"*", single values, ranges "a-b", steps "*/n" and "a-b/n", and comma
lists. Day-of-week runs 0-6 with Sunday as 0 (7 is also accepted as Sunday).
As in Vixie cron, when both day-of-month and day-of-week are restricted a day
matches if *either* field matches.

All times are UTC, so schedules don't shift when daylight saving changes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

_FIELDS = (
    ("minute", 0, 59),
    ("hour", 0, 23),
    ("day of month", 1, 31),
    ("month", 1, 12),
    ("day of week", 0, 7),
)

# Searching further than this without a match means the expression can never
# fire (e.g. "0 0 31 2 *", February 31st).
_MAX_SEARCH = timedelta(days=366 * 5)


class CronError(ValueError):
    """Raised for a malformed cron expression."""


def _parse_field(text: str, name: str, lo: int, hi: int) -> frozenset[int]:
    values: set[int] = set()
    for part in text.split(","):
        if not part:
            raise CronError(f"empty item in {name} field {text!r}")
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            if not step_text.isdigit() or int(step_text) == 0:
                raise CronError(f"bad step in {name} field: {step_text!r}")
            step = int(step_text)
        if part == "*":
            start, end = lo, hi
        elif "-" in part:
            a, b = part.split("-", 1)
            if not (a.isdigit() and b.isdigit()):
                raise CronError(f"bad range in {name} field: {part!r}")
            start, end = int(a), int(b)
        elif part.isdigit():
            start = end = int(part)
            if step != 1:  # "5/10" means "from 5 to the max, every 10"
                end = hi
        else:
            raise CronError(f"bad value in {name} field: {part!r}")
        if not (lo <= start <= hi and lo <= end <= hi) or start > end:
            raise CronError(f"{name} value out of range {lo}-{hi}: {part!r}")
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True)
class CronSchedule:
    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]  # 0 = Sunday
    day_restricted: bool
    weekday_restricted: bool

    @classmethod
    def parse(cls, expression: str) -> CronSchedule:
        parts = expression.split()
        if len(parts) != 5:
            raise CronError(f"expected 5 fields, got {len(parts)}: {expression!r}")
        sets = [_parse_field(p, *spec) for p, spec in zip(parts, _FIELDS, strict=True)]
        weekdays = frozenset(d % 7 for d in sets[4])
        return cls(
            expression=expression,
            minutes=sets[0],
            hours=sets[1],
            days=sets[2],
            months=sets[3],
            weekdays=weekdays,
            day_restricted=parts[2] != "*",
            weekday_restricted=parts[4] != "*",
        )

    def _day_matches(self, dt: datetime) -> bool:
        dom = dt.day in self.days
        dow = (dt.isoweekday() % 7) in self.weekdays  # isoweekday: Mon=1..Sun=7
        if self.day_restricted and self.weekday_restricted:
            return dom or dow
        return dom and dow

    def next_after(self, ts: float) -> float:
        """Return the first matching time strictly after "ts" (Unix seconds)."""
        start = datetime.fromtimestamp(ts, tz=timezone.utc)
        dt = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
        limit = start + _MAX_SEARCH
        while dt <= limit:
            if dt.month not in self.months:
                # Jump to the first minute of next month.
                year, month = (dt.year + 1, 1) if dt.month == 12 else (dt.year, dt.month + 1)
                dt = dt.replace(year=year, month=month, day=1, hour=0, minute=0)
                continue
            if not self._day_matches(dt):
                dt = (dt + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if dt.hour not in self.hours:
                dt = (dt + timedelta(hours=1)).replace(minute=0)
                continue
            if dt.minute not in self.minutes:
                dt += timedelta(minutes=1)
                continue
            return dt.timestamp()
        raise CronError(f"cron expression never fires: {self.expression!r}")
