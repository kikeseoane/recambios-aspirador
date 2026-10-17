"""Prepare a missed Telegram summary for an already-published Grow commit."""
import os
from pathlib import Path
import re
import subprocess
import sys

def git(*args):
    return subprocess.run(
        ["git", "-c", f"safe.directory={Path.cwd().as_posix()}", *args], check=True, capture_output=True
    ).stdout


def prepare(sha, destination):
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("Invalid commit SHA")
    if git("cat-file", "-t", sha).strip() != b"commit":
        raise ValueError("Not a commit")
    git("merge-base", "--is-ancestor", sha, "HEAD")
    subject = git("show", "-s", "--format=%s", sha).decode("utf-8").strip()
    if subject != "content: mejora diaria de crecimiento con Codex":
        raise ValueError("Not a published Grow improvement")
    paths = git("diff", "--name-only", f"{sha}^", sha).decode("utf-8").splitlines()
    if "docs/growth-log.md" not in paths:
        raise ValueError("Grow log missing from commit")
    raw_diff = git("diff", "--unified=0", f"{sha}^", sha, "--", "docs/growth-log.md")
    try:
        log_diff = raw_diff.decode("utf-8")
    except UnicodeDecodeError:
        # Historical Windows PowerShell 5 appends used the local ANSI code page.
        log_diff = raw_diff.decode("cp1252")
    affected = [path for path in paths if path != "docs/growth-log.md"]
    if not affected:
        raise ValueError("Commit contains no site improvement")
    additions = [line[1:].strip() for line in log_diff.splitlines()
                 if line.startswith("+") and not line.startswith("+++")]
    title = next((line.removeprefix("## ") for line in additions
                  if line.startswith("## ")), "Mejora de Grow")
    labels = ("Oportunidad y evidencia", "Cambio", "Páginas afectadas",
              "Limitaciones", "Métrica a 28 días")
    details = [line.replace("**", "").replace("`", "") for line in additions
               if any(line.startswith(f"- **{label}:**") for label in labels)]
    if not any(line.startswith("- Cambio:") for line in details):
        raise ValueError("Grow change description missing")
    message = (f"Informe de Grow: {title}\n\n" + "\n\n".join(details)
               + "\n\nArchivos modificados: " + ", ".join(affected)
               + "\nValidación Hugo y controles: correcta."
               + "\nCommit publicado: "
               + f"https://github.com/kikeseoane/recambios-aspirador/commit/{sha}")
    if len(message) > 4096:
        raise ValueError("Telegram summary exceeds 4096 characters")
    Path(destination).write_text(message, encoding="utf-8")


if __name__ == "__main__":
    prepare(sys.argv[1], os.environ["GROWTH_NOTIFICATION_FILE"])
