"""Local scheduled worker. GitHub holds the lock and validates/publishes remotely.

Only polls our main-branch workflow; it is NOT a GitHub self-hosted runner.
Runtime checkpoints remain local and never contain login credentials.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from contextlib import contextmanager

from codex_quota import AppServer, codex_binary, preflight, quota_decision
from publish_growth import allowed
from growth_budget import check_budget, check_budget_status
import growth_analytics

REPO = "kikeseoane/recambios-aspirador"
ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".growth-runtime"


def run(*args, cwd=ROOT, check=True, **kwargs):
    return subprocess.run(list(args), cwd=cwd, check=check, capture_output=True, **kwargs)


def gh_json(*args):
    return json.loads(run("gh", *args).stdout)


@contextmanager
def single_worker():
    import msvcrt
    STATE.mkdir(exist_ok=True)
    with (STATE / "worker.lock").open("a+b") as lock:
        lock.seek(0)
        if lock.read(1) == b"":
            lock.write(b"0")
            lock.flush()
        lock.seek(0)
        try:
            msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            yield False
            return
        try:
            yield True
        finally:
            lock.seek(0)
            msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)


def stage_patch(checkout):
    paths = set(filter(None, run("git", "diff", "--name-only", "-z", "HEAD", cwd=checkout).stdout.decode().split("\0")))
    paths.update(filter(None, run("git", "ls-files", "--others", "--exclude-standard", "-z", cwd=checkout).stdout.decode().split("\0")))
    if any(not allowed(p) for p in paths) or len(paths) > 60:
        raise RuntimeError("Out-of-scope or oversized patch; not publishing")
    if paths:
        run("git", "add", "--", *sorted(paths), cwd=checkout)
    return run("git", "diff", "--cached", "--binary", cwd=checkout).stdout


def save_checkpoint(checkout, base):
    patch = stage_patch(checkout)
    if patch:
        (STATE / "pending.patch").write_bytes(patch)
        (STATE / "pending-base.txt").write_text(base, encoding="utf-8")


def stop_process(process):
    # Only the exact child process tree created by this worker.
    subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, check=False)
    process.wait(timeout=15)


def invoke_codex(checkout, model):
    env = {k: v for k, v in os.environ.items() if k not in
           ("OPENAI_API_KEY", "CODEX_API_KEY", "AZURE_OPENAI_API_KEY", "GH_TOKEN", "GITHUB_TOKEN",
            "GROWTH_GOOGLE_ACCESS_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS")}
    # Keep the read-only API snapshot inside Git metadata: Codex can audit it,
    # while git status/stage_patch can never publish it with a growth change.
    analytics_path = checkout / ".git" / "growth-analytics.json"
    try:
        analytics = growth_analytics.collect()
    except Exception:
        analytics = {"period": growth_analytics.completed_period(),
                     "search_console": {"status": "error", "reason": "No se pudo consultar Google."},
                     "ga4": {"status": "error", "reason": "No se pudo consultar Google."}}
    analytics_path.write_text(json.dumps(analytics, ensure_ascii=False), encoding="utf-8")
    prompt = (checkout / "docs/growth-prompt.md").read_text(encoding="utf-8")
    prompt += "\nEl trabajador local delega validacion Hugo, commit final y push a main en GitHub. No ejecutes Hugo localmente.\n"
    if (STATE / "pending.patch").exists():
        prompt += "\nHay trabajo pendiente restaurado en el diff: continua y termina ESA mejora, no elijas otra. Revisa el diff y docs/growth-log.md antes de actuar.\n"
    with (STATE / "last-codex.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen([
            codex_binary(), "--search", "--ask-for-approval", "never", "exec",
            "--ignore-user-config", "--ignore-rules", "--sandbox", "workspace-write",
            "-c", 'windows.sandbox="unelevated"',
            "--model", model, "-c", 'model_reasoning_effort="medium"',
            "--color", "never", "--cd", str(checkout), "-",
        ], stdin=subprocess.PIPE, stdout=log, stderr=log, env=env, text=True, encoding="utf-8")
        process.stdin.write(prompt)
        process.stdin.close()
        deadline = time.monotonic() + 18 * 60
        try:
            while process.poll() is None:
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    decision = preflight()
                    within_budget, reason = check_budget_status(STATE / "daily-budget.json", decision)
                    if not within_budget or time.monotonic() >= deadline:
                        stop_process(process)
                        return "deferred-five-hour" if reason == "five_hour_reserve" else "deferred"
            # Also account for short runs and the final in-flight request.
            within_budget, reason = check_budget_status(STATE / "daily-budget.json", preflight())
            if not within_budget:
                return "deferred-five-hour" if reason == "five_hour_reserve" else "deferred"
            if process.returncode == 0:
                # A conversational refusal can exit zero. Require the promised
                # run record so blocked tool access is not reported as success.
                changed = run("git", "status", "--porcelain", "--", "docs/growth-log.md", cwd=checkout).stdout
                if not changed.strip():
                    return "failed"
                return "ready"
            # Exhaustion is a normal deferral; other failures need an alert.
            decision = preflight()
            return "deferred-five-hour" if decision.get("reason") == "five_hour_reserve" else ("deferred" if not decision["allowed"] else "failed")
        finally:
            if process.poll() is None:
                stop_process(process)


def handle(request, run_id):
    if str(request["run_id"]) != str(run_id) or request["repository"] != REPO:
        raise RuntimeError("Invalid request identity")
    base = request["base"]
    if len(base) != 40 or any(c not in "0123456789abcdef" for c in base):
        raise RuntimeError("Invalid base commit")
    branch = f"automation/codex-growth-{run_id}"
    checkout = STATE / f"run-{run_id}"
    # A separate disposable checkout leaves the user's dirty working tree alone.
    if not checkout.exists():
        run("git", "clone", "--depth", "1", "--no-checkout", f"https://github.com/{REPO}.git", str(checkout))
    # The exact base comes from our trusted main-branch workflow artifact.
    run("git", "fetch", "--depth", "1", "origin", base, cwd=checkout)
    run("git", "checkout", "-b", branch, base, cwd=checkout)
    run("git", "config", "user.name", "Codex growth", cwd=checkout)
    run("git", "config", "user.email", "github-actions[bot]@users.noreply.github.com", cwd=checkout)
    status = "failed"
    try:
        decision = preflight()
        (STATE / "last-quota.json").write_text(json.dumps(decision), encoding="utf-8")
        if request.get("dry_run"):
            # No inference: still test remote Hugo validation below the reserve.
            status = "ready"
        elif not check_budget(STATE / "daily-budget.json", decision):
            status = "deferred"
        else:
            pending = STATE / "pending.patch"
            if pending.exists():
                old_base = (STATE / "pending-base.txt").read_text(encoding="utf-8").strip()
                run("git", "fetch", "origin", old_base, cwd=checkout)
                run("git", "apply", "--3way", str(pending), cwd=checkout)
            try:
                status = invoke_codex(checkout, decision["model"])
            finally:
                # Preserve edits even if quota lookup or process control fails.
                save_checkpoint(checkout, base)
    except Exception as error:
        (STATE / "last-error.txt").write_text(str(error), encoding="utf-8")
        status = "failed"
    if status == "ready":
        stage_patch(checkout)
        run("git", "commit", "--allow-empty", "-m", "growth-result:ready", cwd=checkout)
    else:
        # Never publish unfinished edits. The signal is a clean, empty commit
        # created in a second worktree, retaining the original files locally.
        signal = STATE / f"signal-{run_id}"
        run("git", "worktree", "add", "--detach", str(signal), base, cwd=checkout)
        # Flag whether an improvement is genuinely in progress: pending.patch
        # only exists once Codex started modifying things. GitHub keeps the
        # offers hold only for "deferred-pending"; a plain "deferred" (e.g. no
        # quota, nothing started) must release the hold so the sync is not
        # paused forever.
        result = status
        if status == "deferred" and (STATE / "pending.patch").exists():
            result = "deferred-pending"
        run("git", "commit", "--allow-empty", "-m", f"growth-result:{result}", cwd=signal)
        checkout = signal
    run("git", "push", "origin", f"HEAD:refs/heads/{branch}", cwd=checkout)
    (STATE / f"handled-{run_id}").write_text(status, encoding="utf-8")
    # Keep checkpoint until cloud validation and publication really succeed.
    if status == "ready" and not request.get("dry_run"):
        (STATE / "pending-run.txt").write_text(str(run_id), encoding="utf-8")
    return status


def main():
    with single_worker() as acquired:
        if not acquired:
            return
        prior = STATE / "pending-run.txt"
        if prior.exists():
            result = gh_json("api", f"repos/{REPO}/actions/runs/{prior.read_text().strip()}")
            if result.get("conclusion") == "success":
                for name in ("pending.patch", "pending-base.txt", "pending-run.txt"):
                    (STATE / name).unlink(missing_ok=True)
        runs = gh_json("api", f"repos/{REPO}/actions/workflows/codex-growth.yml/runs?status=in_progress&per_page=10")
        for item in runs.get("workflow_runs", []):
            if item.get("head_branch") != "main" or item.get("event") not in ("schedule", "workflow_dispatch"):
                continue
            run_id = item["id"]
            if (STATE / f"handled-{run_id}").exists():
                continue
            artifacts = gh_json("api", f"repos/{REPO}/actions/runs/{run_id}/artifacts")
            if not any(a["name"] == "growth-request" and not a["expired"] for a in artifacts["artifacts"]):
                continue
            request_dir = STATE / f"request-{run_id}"
            request_dir.mkdir(exist_ok=True)
            run("gh", "run", "download", str(run_id), "--repo", REPO, "--name", "growth-request", "--dir", str(request_dir))
            print(handle(json.loads((request_dir / "request.json").read_text()), run_id))
            return


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        STATE.mkdir(exist_ok=True)
        (STATE / "worker-error.txt").write_text(str(error), encoding="utf-8")
        raise
