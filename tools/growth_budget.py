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
    if not previous or previous["day"] != day:
        previous = dict(day=day, consumed=0, last_used=used, reset=reset)
    elif reset != previous["reset"]:
        # Reset does not grant another allowance in the same day.
        previous = dict(previous, consumed=previous["consumed"] + used, reset=reset, last_used=used)
    else:
        previous = dict(previous, consumed=previous["consumed"] + max(0, used - previous["last_used"]),
                        last_used=max(used, previous["last_used"]))
    return previous, bool(decision["allowed"] and previous["consumed"] < 10)


def check_budget(path, decision):
    previous = json.loads(path.read_text(encoding="utf-8")) if path.exists() else None
    state, allowed = update_budget(previous, decision)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state), encoding="utf-8")
    temporary.replace(path)
    return allowed
