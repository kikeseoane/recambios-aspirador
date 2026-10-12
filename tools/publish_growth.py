"""Validate and publish a bounded Codex patch from an isolated GitHub runner."""
import os
from pathlib import Path
import subprocess
import sys


ALLOWED_PREFIXES = ("content/", "layouts/", "static/", "assets/", "data/brands/")
ALLOWED_FILES = {
    "data/catalog_parts.yaml", "data/catalog_brands.yaml", "data/catalog_skus.yaml",
    "data/aspiradores.yaml", "docs/growth-log.md",
}


def allowed(path):
    return (path in ALLOWED_FILES or path.startswith(ALLOWED_PREFIXES)) and not any(
        part.startswith(".") or part == ".." for part in Path(path).parts
    )


def git(*args, check=True):
    return subprocess.run(["git", *args], check=check, text=True, capture_output=True)


def changed_paths():
    tracked = git("diff", "--name-only", "-z", "HEAD").stdout.split("\0")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z").stdout.split("\0")
    return sorted(set(filter(None, tracked + untracked)))


def validate():
    git("diff", "--check", "HEAD")
    subprocess.run(["hugo", "--minify", "--cleanDestinationDir"], check=True)
    subprocess.run([sys.executable, str(Path(__file__).with_name("check_growth_build.py"))], check=True)


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise RuntimeError("Publish only from the isolated GitHub Actions runner")
    paths = changed_paths()
    forbidden = [p for p in paths if not allowed(p)]
    if forbidden:
        raise RuntimeError(f"Out-of-scope changes: {forbidden}")
    if len(paths) > 60:
        raise RuntimeError("Daily patch exceeds 60 files; refusing broad publication")
    if not paths:
        print("No changes to publish")
        return
    if "docs/growth-log.md" not in paths:
        raise RuntimeError("Missing evidence and outcome in docs/growth-log.md")
    for path in paths:
        if Path(path).is_symlink():
            raise RuntimeError(f"Symlinks are not allowed: {path}")
    validate()
    git("add", "--", *paths)
    git("commit", "-m", "content: mejora diaria de crecimiento con Codex")
    for attempt in range(3):
        git("fetch", "origin", "main")
        rebased = git("rebase", "origin/main", check=False)
        if rebased.returncode:
            git("rebase", "--abort", check=False)
            raise RuntimeError("Concurrent conflicting changes; patch not published")
        # Revalidate against the latest offers/templates, not the original snapshot.
        validate()
        pushed = git("push", "origin", "HEAD:main", check=False)
        if pushed.returncode == 0:
            print("Published", git("rev-parse", "HEAD").stdout.strip())
            return
        print(f"Push rejected ({attempt + 1}/3); fetching and validating again")
    raise RuntimeError("Push failed after three attempts; never force-push")


if __name__ == "__main__":
    main()
