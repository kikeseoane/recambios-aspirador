"""Read subscription quota via Codex app-server; never fall back to paid API."""
import argparse
import json
import math
import os
from pathlib import Path
import queue
import shutil
import subprocess
import threading
import time


def codex_binary():
    if os.name == "nt":
        native = Path(os.environ["APPDATA"]) / "npm/node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe"
        if native.is_file():
            return str(native)
    binary = shutil.which("codex")
    if not binary or binary.lower().endswith((".cmd", ".bat")):
        raise RuntimeError("Native Codex executable unavailable")
    return binary


class AppServer:
    def __init__(self):
        env = {k: v for k, v in os.environ.items() if k not in
               ("OPENAI_API_KEY", "CODEX_API_KEY", "AZURE_OPENAI_API_KEY")}
        self.process = subprocess.Popen(
            [codex_binary(), "app-server", "--stdio"], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            encoding="utf-8", env=env,
        )
        self.messages = queue.Queue()
        self.sequence = 0
        threading.Thread(target=self._read, daemon=True).start()
        self.call("initialize", {"clientInfo": {"name": "affiliate_growth", "version": "1.0"}})
        self.send({"method": "initialized"})

    def _read(self):
        for line in self.process.stdout:
            try:
                self.messages.put(json.loads(line))
            except ValueError:
                pass

    def send(self, value):
        self.process.stdin.write(json.dumps(value) + "\n")
        self.process.stdin.flush()

    def call(self, method, params=None):
        self.sequence += 1
        request = {"id": self.sequence, "method": method}
        if params is not None:
            request["params"] = params
        self.send(request)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                message = self.messages.get(timeout=max(.1, deadline - time.monotonic()))
            except queue.Empty:
                break
            if message.get("id") == self.sequence:
                if "error" in message:
                    raise RuntimeError(f"Codex could not answer {method}")
                return message["result"]
        raise RuntimeError(f"Timeout querying {method}")

    def close(self):
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()


def quota_decision(result, reserve=20):
    buckets = result.get("rateLimitsByLimitId") or {}
    bucket = buckets.get("codex") or result.get("rateLimits") or {}
    windows = [bucket[k] for k in ("primary", "secondary") if bucket.get(k)]
    if not windows:
        raise RuntimeError("Quota unknown: do not start")
    remaining = []
    for window in windows:
        used = window.get("usedPercent")
        if isinstance(used, bool) or not isinstance(used, (int, float)) or not math.isfinite(used) or not 0 <= used <= 100:
            raise RuntimeError("Invalid quota: do not start")
        remaining.append(100 - used)
    minimum = min(remaining)
    return {"allowed": minimum > reserve and not bucket.get("rateLimitReachedType"),
            "remaining_percent": minimum, "reserve_percent": reserve,
            "reset_times": [w.get("resetsAt") for w in windows],
            "reason": "ready" if minimum > reserve and not bucket.get("rateLimitReachedType") else "defer_until_next_day"}


def select_model(models):
    available = [m for m in models if not m.get("hidden")]
    # Small daily changes: prefer the account's available Luna model, not a
    # guessed API model. Do not silently upgrade to a more expensive family.
    luna = [m for m in available if "luna" in m.get("model", "").lower()]
    if not luna:
        raise RuntimeError("No available Luna model; explicit configuration required")
    preferred = next((m for m in luna if m.get("isDefault")), luna[0])
    return preferred["model"]


def preflight():
    server = AppServer()
    try:
        account = server.call("account/read", {"refreshToken": False}).get("account") or {}
        if account.get("type") != "chatgpt":
            raise RuntimeError("ChatGPT subscription login required; API fallback forbidden")
        decision = quota_decision(server.call("account/rateLimits/read"))
        if decision["allowed"]:
            models = server.call("model/list", {"limit": 100})
            decision["model"] = select_model(models.get("data", []))
        return decision
    finally:
        server.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.parse_args()
    try:
        print(json.dumps(preflight()))
    except Exception as error:
        print(json.dumps({"allowed": False, "reason": str(error)}))
        raise SystemExit(1)
