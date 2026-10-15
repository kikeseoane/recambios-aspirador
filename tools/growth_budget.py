"""Persistent daily allowance measured in percentage points of weekly quota."""
from datetime import datetime, timedelta, timezone
import json
import math


def update_budget(previous, decision, now=None):
    now = now or datetime.now(timezone.utc)
    # Before the 09:00 Madrid trigger in both summer and winter.
    # A fixed boundary avoids replenishing the budget during a normal run.
    day = (now.astimezone(timezone.utc) - timedelta(hours=7)).date().isoformat()
    used = decision.get("weekly_used_percent")
    reset = decision.get("weekly_resets_at")
    if (isinstance(used, bool) or not isinstance(used, (int, float))
            or not math.isfinite(used) or not 0 <= used <= 100
            or not isinstance(reset, (int, float)) or not math.isfinite(reset) or reset <= 0):
        raise RuntimeError("Weekly quota unknown; cannot enforce daily 10-point budget")
    # `consumed` is the daily observed spend while Grow is running.  It is
    # deliberately reset at the daily boundary; usage between runs belongs to
    # another session and must not consume today's daily allowance.
    if not previous:
        previous = dict(day=day, consumed=0, last_used=used, reset=reset,
                        weekly_consumed=0)
    elif reset != previous["reset"]:
        # A subscription-window reset does not create a second 10-point daily
        # allowance. It does begin a fresh, explicitly tracked Grow week.
        previous = dict(previous, reset=reset, last_used=used, weekly_consumed=0)
        if previous["day"] != day:
            previous["day"] = day
            previous["consumed"] = 0
        else:
            previous["consumed"] += used
    elif previous["day"] != day:
        previous = dict(previous, day=day, consumed=0, last_used=used)
    else:
        delta = max(0, used - previous["last_used"])
        previous = dict(previous, consumed=previous["consumed"] + delta,
                        weekly_consumed=previous.get("weekly_consumed", 0) + delta,
                        last_used=max(used, previous["last_used"]))
    # Check both at each quota reading. At the threshold the next model action
    # is never started; an in-flight action can only be observed afterwards.
    return previous, bool(decision["allowed"] and previous["consumed"] < 10
                          and previous.get("weekly_consumed", 0) < 70)


def check_budget(path, decision):
    return check_budget_status(path, decision)[0]


def check_budget_status(path, decision):
    previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    state, allowed = update_budget(previous, decision)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state), encoding="utf-8")
    temporary.replace(path)
    if state["consumed"] >= 10:
        return allowed, "daily_budget"
    if state.get("weekly_consumed", 0) >= 70:
        return allowed, "weekly_budget"
    if not decision["allowed"]:
        return allowed, decision.get("reason", "quota_unavailable")
    return allowed, "ready"
