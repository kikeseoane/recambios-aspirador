"""One-shot local worker for the daily Codex growth cycle.

Windows starts this program once. It prepares the isolated change locally and
then asks GitHub Actions to validate and publish the result. GitHub never waits
for this PC and this PC never polls GitHub for pending work.
"""
import argparse
import json
import re
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import growth_worker as legacy


REPO = legacy.REPO
ROOT = legacy.ROOT
STATE = legacy.STATE
BRANCH_RE = re.compile(r"automation/codex-growth-[a-z0-9-]+$")


def output(result):
    value = result.stdout
    return value if isinstance(value, str) else value.decode()


def release_hold(checkout):
    legacy.run(sys.executable, str(ROOT / "tools" / "growth_hold.py"), "release", cwd=checkout)


def hold_exists():
    return bool(output(legacy.run("git", "ls-remote", "--heads", "origin", "automation/growth-hold", check=False)).strip())


def acquire_remote_hold():
    """Reserve the shared GitHub concurrency group before reading main."""
    request_id = uuid.uuid4().hex
    title = f"Reserva Grow {request_id}"
    legacy.run("gh", "workflow", "run", "growth-acquire.yml", "--repo", REPO,
               "--ref", "main", "-f", f"request_id={request_id}")
    deadline = time.monotonic() + 70 * 60
    run_id = None
    while time.monotonic() < deadline:
        runs = json.loads(output(legacy.run(
            "gh", "run", "list", "--repo", REPO, "--workflow", "growth-acquire.yml",
            "--limit", "20", "--json", "databaseId,status,conclusion,displayTitle")))
        match = next((item for item in runs if item.get("displayTitle") == title), None)
        if match:
            run_id = str(match["databaseId"])
            if match["status"] == "completed":
                if match["conclusion"] == "success":
                    (STATE / "growth-lock.txt").write_text(request_id, encoding="utf-8")
                    return
                raise RuntimeError(f"Could not acquire growth lock (run {run_id}: {match['conclusion']})")
        time.sleep(5)
    raise RuntimeError(f"Timed out waiting for the GitHub growth lock ({run_id or request_id})")


def discard_confirmed_checkpoint():
    """The publisher deletes its temporary branch only after a successful push."""
    pending_branch = STATE / "pending-branch.txt"
    if not pending_branch.exists():
        return
    branch = pending_branch.read_text(encoding="utf-8").strip()
    if not BRANCH_RE.fullmatch(branch):
        return
    remote = output(legacy.run("git", "ls-remote", "--heads", "origin", branch, check=False)).strip()
    if remote:
        return
    for name in ("pending.patch", "pending-base.txt", "pending-branch.txt", "growth-lock.txt"):
        (STATE / name).unlink(missing_ok=True)


def new_checkout():
    branch = f"automation/codex-growth-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
    checkout = STATE / f"run-{branch.rsplit('-', 1)[-1]}"
    legacy.run("git", "clone", "--depth", "1", "--no-checkout", f"https://github.com/{REPO}.git", str(checkout))
    legacy.run("git", "fetch", "--depth", "1", "origin", "main", cwd=checkout)
    base = output(legacy.run("git", "rev-parse", "FETCH_HEAD", cwd=checkout)).strip()
    legacy.run("git", "checkout", "-b", branch, base, cwd=checkout)
    legacy.run("git", "config", "user.name", "Codex growth", cwd=checkout)
    legacy.run("git", "config", "user.email", "github-actions[bot]@users.noreply.github.com", cwd=checkout)
    return checkout, branch, base


def restore_checkpoint(checkout):
    pending = STATE / "pending.patch"
    if not pending.exists():
        return
    old_base = (STATE / "pending-base.txt").read_text(encoding="utf-8").strip()
    legacy.run("git", "fetch", "origin", old_base, cwd=checkout)
    legacy.run("git", "apply", "--3way", str(pending), cwd=checkout)


def dispatch_publisher(branch, base, dry_run):
    if not BRANCH_RE.fullmatch(branch) or not re.fullmatch(r"[0-9a-f]{40}", base):
        raise RuntimeError("Invalid publication identity")
    legacy.run("gh", "workflow", "run", "codex-growth.yml", "--repo", REPO, "--ref", "main",
               "-f", f"result_branch={branch}", "-f", f"base_sha={base}",
               "-f", f"dry_run={'true' if dry_run else 'false'}")


def request_five_hour_retry(decision):
    reset = decision.get("five_hour_resets_at")
    if not isinstance(reset, (int, float)) or reset <= time.time():
        raise RuntimeError("Five-hour reset unavailable for retry")
    retry_at = datetime.fromtimestamp(reset + 60, timezone.utc)
    (STATE / "retry-request.json").write_text(json.dumps({
        "reason": "five_hour_reserve", "at_utc": retry_at.isoformat().replace("+00:00", "Z")
    }), encoding="utf-8")


def run_once(dry_run=False, schedule_five_hour_retry=False):
    with legacy.single_worker() as acquired:
        if not acquired:
            raise RuntimeError("Another growth run is already active")
        discard_confirmed_checkpoint()
        try:
            decision = legacy.preflight()
            (STATE / "last-quota.json").write_text(json.dumps(decision), encoding="utf-8")
            if not dry_run:
                budget_ok, budget_reason = legacy.check_budget_status(STATE / "daily-budget.json", decision, track_usage=False)
                if not budget_ok:
                    if schedule_five_hour_retry and budget_reason == "five_hour_reserve":
                        request_five_hour_retry(decision)
                    return 0
            pending_patch = (STATE / "pending.patch").exists()
            pending_publication = (STATE / "pending-branch.txt").exists()
            if pending_publication and not dry_run:
                raise RuntimeError("A prior growth result is still awaiting GitHub publication")
            if not dry_run and not ((STATE / "growth-lock.txt").exists() and hold_exists()):
                acquire_remote_hold()
            # The lock is now held (or this is an isolated dry run), so this is
            # the newest stable main available to the local worker.
            checkout, branch, base = new_checkout()
            # The queue may have delayed the lock; verify the short window again.
            decision = legacy.preflight()
            (STATE / "last-quota.json").write_text(json.dumps(decision), encoding="utf-8")
            if dry_run:
                status = "ready"
            elif not (budget_result := legacy.check_budget_status(STATE / "daily-budget.json", decision, track_usage=False))[0]:
                if schedule_five_hour_retry and budget_result[1] == "five_hour_reserve":
                    request_five_hour_retry(decision)
                status = "deferred"
            else:
                restore_checkpoint(checkout)
                try:
                    status = legacy.invoke_codex(checkout, decision["model"])
                finally:
                    legacy.save_checkpoint(checkout, base)
            if status == "deferred-five-hour" and schedule_five_hour_retry:
                decision = legacy.preflight()
                if legacy.check_budget_status(STATE / "daily-budget.json", decision, track_usage=False)[1] == "five_hour_reserve":
                    request_five_hour_retry(decision)
            if status in ("deferred", "deferred-five-hour"):
                if not pending_patch and not (STATE / "pending.patch").exists() and not dry_run:
                    release_hold(checkout)
                    (STATE / "growth-lock.txt").unlink(missing_ok=True)
                return 0
            if status != "ready":
                raise RuntimeError(f"Local Codex did not produce a publishable result: {status}")
            legacy.stage_patch(checkout)
            legacy.run("git", "commit", "--allow-empty", "-m", "growth-result:ready", cwd=checkout)
            legacy.run("git", "push", "origin", f"HEAD:refs/heads/{branch}", cwd=checkout)
            if not dry_run:
                (STATE / "pending-branch.txt").write_text(branch, encoding="utf-8")
            dispatch_publisher(branch, base, dry_run)
            return 0
        except Exception as error:
            (STATE / "last-error.txt").write_text(str(error), encoding="utf-8")
            # Do not leave offers paused for an infrastructure failure before
            # Codex changed anything. A partial patch or submitted result keeps
            # the hold so a later run can continue it safely.
            if (not dry_run and (STATE / "growth-lock.txt").exists()
                    and not (STATE / "pending.patch").exists()
                    and not (STATE / "pending-branch.txt").exists()):
                try:
                    release_hold(locals().get("checkout", ROOT))
                    (STATE / "growth-lock.txt").unlink(missing_ok=True)
                except Exception as release_error:
                    (STATE / "last-error.txt").write_text(
                        f"{error}\nCould not release empty growth hold: {release_error}", encoding="utf-8")
            raise


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run one local Codex growth cycle")
    parser.add_argument("--dry-run", action="store_true", help="Validate GitHub publishing without calling Codex")
    parser.add_argument("--schedule-five-hour-retry", action="store_true")
    args = parser.parse_args(argv)
    try:
        return run_once(args.dry_run, args.schedule_five_hour_retry)
    except Exception as error:
        print(f"growth worker failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
