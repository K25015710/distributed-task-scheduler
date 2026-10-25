from datetime import datetime, timezone

import pytest

from dtsched import CronError, CronSchedule, backoff_delay


def ts(*args: int) -> float:
    return datetime(*args, tzinfo=timezone.utc).timestamp()


# backoff

def test_backoff_doubles_without_jitter():
    delays = [backoff_delay(n, base=1, cap=1000, jitter=False) for n in range(1, 6)]
    assert delays == [1, 2, 4, 8, 16]

def test_backoff_is_capped():
    assert backoff_delay(20, base=1, cap=30, jitter=False) == 30

def test_backoff_huge_attempt_does_not_overflow():
    assert backoff_delay(10_000, base=1, cap=60, jitter=False) == 60

@pytest.mark.parametrize("r, expected", [(0.0, 4.0), (0.5, 6.0), (1.0, 8.0)])
def test_backoff_equal_jitter_bounds(r, expected):
    # attempt 4 -> 8s; equal jitter draws from [4, 8]
    assert backoff_delay(4, base=1, cap=100, rand=lambda: r) == expected

def test_backoff_jitter_spreads_values():
    values = {round(backoff_delay(3, base=1, cap=100), 6) for _ in range(50)}
    assert len(values) > 10
    assert all(2 <= v <= 4 for v in values)

@pytest.mark.parametrize("attempt, base", [(0, 1), (1, -1)])
def test_backoff_rejects_bad_input(attempt, base):
    with pytest.raises(ValueError):
        backoff_delay(attempt, base=base)

# cron
def test_cron_every_minute():
    assert CronSchedule.parse("* * * * *").next_after(ts(2026, 1, 1, 12, 0, 30)) == ts(
        2026, 1, 1, 12, 1
    )

def test_cron_next_is_strictly_after():
    # Exactly on a match -> the *next* one.
    s = CronSchedule.parse("*/15 * * * *")
    assert s.next_after(ts(2026, 1, 1, 12, 15)) == ts(2026, 1, 1, 12, 30)

def test_cron_step_and_range():
    s = CronSchedule.parse("0 9-17/4 * * *")
    assert s.hours == {9, 13, 17}
    assert s.next_after(ts(2026, 1, 1, 13, 0)) == ts(2026, 1, 1, 17, 0)
    assert s.next_after(ts(2026, 1, 1, 17, 0)) == ts(2026, 1, 2, 9, 0)

def test_cron_lists():
    s = CronSchedule.parse("5,35 0 * * *")
    assert s.next_after(ts(2026, 1, 1, 0, 6)) == ts(2026, 1, 1, 0, 35)

def test_cron_month_rollover_and_leap_day():
    s = CronSchedule.parse("0 0 29 2 *")
    assert s.next_after(ts(2026, 3, 1)) == ts(2028, 2, 29)

def test_cron_weekday_sunday_is_0_and_7():
    # 2026-01-04 is a Sunday.
    for expr in ("0 12 * * 0", "0 12 * * 7"):
        assert CronSchedule.parse(expr).next_after(ts(2026, 1, 1)) == ts(2026, 1, 4, 12)

def test_cron_dom_or_dow_when_both_restricted():
    # "the 15th, or any Monday": from Thu 2026-01-01 the first hit is Mon 01-05.
    s = CronSchedule.parse("0 0 15 * 1")
    assert s.next_after(ts(2026, 1, 1)) == ts(2026, 1, 5)
    assert s.next_after(ts(2026, 1, 12, 1)) == ts(2026, 1, 15)

def test_cron_year_rollover():
    s = CronSchedule.parse("30 23 31 12 *")
    assert s.next_after(ts(2026, 12, 31, 23, 30)) == ts(2027, 12, 31, 23, 30)

@pytest.mark.parametrize(
    "expr",
    [
        "* * * *",
        "60 * * * *",
        "* 24 * * *",
        "*/0 * * * *",
        "a * * * *",
        "5-1 * * * *",
        "1,,2 * * * *",
    ],
)
def test_cron_rejects_malformed(expr):
    with pytest.raises(CronError):
        CronSchedule.parse(expr)

def test_cron_impossible_date_raises():
    with pytest.raises(CronError, match="never fires"):
        CronSchedule.parse("0 0 31 2 *").next_after(ts(2026, 1, 1))
