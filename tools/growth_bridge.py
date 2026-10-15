"""GitHub side: validate and publish one result sent by the local worker."""
import os
from pathlib import Path
import re
import subprocess


BRANCH_RE = re.compile(r"automation/codex-growth-[a-z0-9-]+$")


def run(*args, check=True, **kwargs):
    result = subprocess.run(list(args), check=False, capture_output=True, **kwargs)
    if check and result.returncode:
        print(result.stdout.decode(errors="replace"))
        print(result.stderr.decode(errors="replace"))
        result.check_returncode()
    return result


def main():
    branch = os.environ["GROWTH_RESULT_BRANCH"]
    requested_base = os.environ["GROWTH_BASE_SHA"]
    if not BRANCH_RE.fullmatch(branch):
        raise RuntimeError("Invalid local result branch")
    if not re.fullmatch(r"[0-9a-f]{40}", requested_base):
        raise RuntimeError("Invalid expected base commit")
    base = run("git", "rev-parse", "HEAD").stdout.decode().strip()
    if base != requested_base:
        raise RuntimeError("main changed before publication; local patch was not applied")
    run("git", "fetch", "origin", branch)
    head = run("git", "rev-parse", "FETCH_HEAD").stdout.decode().strip()
    parent = run("git", "rev-parse", f"{head}^").stdout.decode().strip()
    if parent != base:
        raise RuntimeError("Result must be a single commit based on the exact request")
    subject = run("git", "show", "-s", "--format=%s", head).stdout.decode().strip()
    patch = run("git", "diff", "--binary", base, head).stdout
    if subject == "growth-result:deferred-pending" and not patch:
        # An improvement is genuinely half-done: keep the offers hold so the
        # sync does not modify catalogs/offers under the pending patch.
        print("Unfinished improvement in progress; keeping offers hold for next run")
        return
    if subject == "growth-result:deferred" and not patch:
        # Nothing was started (e.g. no quota). Releasing the hold here prevents
        # the sync from staying paused indefinitely when the grow never ran.
        print("Quota reserve or time limit reached and nothing started; releasing offers hold")
        if os.environ.get("GROWTH_DRY_RUN") != "true":
            run("python", str(Path(__file__).with_name("growth_hold.py")), "release")
        return
    if subject != "growth-result:ready":
        raise RuntimeError("Local Codex failed; see local worker logs")
    if patch:
        run("git", "apply", "--index", "--binary", "-", input=patch)
    # Uses original controls, not any validator supplied by the agent.
    run("python", os.environ["GROWTH_PUBLISHER"])
    # Release only after successful validation/publication, never on deferral
    # or failures. Dry runs must not release an earlier unfinished task's hold.
    if os.environ.get("GROWTH_DRY_RUN") != "true":
        run("python", str(Path(__file__).with_name("growth_hold.py")), "release")
    print("Daily cycle complete")


if __name__ == "__main__":
    main()
