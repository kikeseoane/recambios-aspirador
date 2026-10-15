"""Durable offers hold. Called only while holding the shared workflow lock."""
import subprocess
import sys

REF = "refs/heads/automation/growth-hold"


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout.strip()


def held():
    return bool(git("ls-remote", "--heads", "origin", REF))


def main(action, require_free=False):
    if action == "acquire":
        if held():
            if require_free:
                raise RuntimeError("Growth lock is already held")
            print("already-held")
            return False
        git("push", "origin", f"HEAD:{REF}")
        print("acquired")
        return True
    elif action == "release":
        if held():
            git("push", "origin", f":{REF}")
    elif action == "status":
        print("true" if held() else "false")
    else:
        raise ValueError("Unknown hold operation")


if __name__ == "__main__":
    action = sys.argv[1]
    main(action, "--require-free" in sys.argv[2:])
