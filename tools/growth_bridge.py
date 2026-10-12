"""GitHub side: wait for a bounded result from the local worker."""
import json
import os
from pathlib import Path
import subprocess
import time


def run(*args, check=True, **kwargs):
    result = subprocess.run(list(args), check=False, capture_output=True, **kwargs)
    if check and result.returncode:
        print(result.stdout.decode(errors="replace"))
        print(result.stderr.decode(errors="replace"))
        result.check_returncode()
    return result


def main():
    run_id = os.environ["GITHUB_RUN_ID"]
    branch = f"automation/codex-growth-{run_id}"
    base = run("git", "rev-parse", "HEAD").stdout.decode().strip()
    deadline = time.monotonic() + 27 * 60
    while time.monotonic() < deadline:
        found = run("git", "ls-remote", "--heads", "origin", branch)
        if found.stdout.strip():
            break
        time.sleep(20)
    else:
        raise RuntimeError("Local PC did not return a result within 27 minutes")
    run("git", "fetch", "origin", branch)
    head = run("git", "rev-parse", "FETCH_HEAD").stdout.decode().strip()
    parent = run("git", "rev-parse", f"{head}^").stdout.decode().strip()
    if parent != base:
        raise RuntimeError("Result must be a single commit based on the exact request")
    subject = run("git", "show", "-s", "--format=%s", head).stdout.decode().strip()
    patch = run("git", "diff", "--binary", base, head).stdout
    if subject == "growth-result:deferred" and not patch:
        print("Quota reserve or time limit reached; wait until next daily run")
        return
    if subject != "growth-result:ready":
        raise RuntimeError("Local Codex failed; see local worker logs")
    if patch:
        run("git", "apply", "--index", "--binary", "-", input=patch)
    # Uses original controls, not any validator supplied by the agent.
    run("python", os.environ["GROWTH_PUBLISHER"])
    print("Daily cycle complete")


if __name__ == "__main__":
    main()
