"""Keep a known offer while an attempted replacement remains unverified."""
from datetime import date, timedelta


USABLE_STATUSES = {"compatible_alto", "compatible_probable", "dudoso"}
UNRESOLVED_STATUSES = {"pending_ai_validation", "fallback_buy_new", "sin_cobertura"}


def refresh_due(offer, cutoff):
    return bool(cutoff and offer.get("url") and not offer.get("needs_url")
                and str(offer.get("updated_at") or "") < cutoff)


def stale_cutoff(today, days):
    return (date.fromisoformat(today) - timedelta(days=days)).isoformat() if days > 0 else None


def preserve_verified_offer(previous, refreshed, today, default_url):
    old_url = str(previous.get("url") or "").strip()
    if (not old_url or old_url == default_url or previous.get("needs_url")
            or previous.get("compatibility_status") not in USABLE_STATUSES
            or refreshed.get("compatibility_status") not in UNRESOLVED_STATUSES):
        return refreshed
    # Failure to find or validate a replacement is not evidence that the old
    # product disappeared. The independent liveness check handles dead links.
    kept = dict(previous)
    kept["updated_at"] = today
    kept["last_attempted_at"] = today
    kept["debug_refresh_failure_stage"] = str(
        refreshed.get("debug_failure_stage") or refreshed.get("ai_validation_reason")
        or refreshed.get("compatibility_status") or "unknown")[:160]
    return kept
