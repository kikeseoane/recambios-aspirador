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
    return subprocess.run(["git", *args], check=check, text=True,
                          encoding="utf-8", errors="replace", capture_output=True)


def changed_paths():
    tracked = git("diff", "--name-only", "-z", "HEAD").stdout.split("\0")
    untracked = git("ls-files", "--others", "--exclude-standard", "-z").stdout.split("\0")
    return sorted(set(filter(None, tracked + untracked)))


def validate():
    # GitHub's locale may differ from the Windows worker's. Reject malformed
    # source text before pushing, while git() stays safe for notifications.
    Path("docs/growth-log.md").read_text(encoding="utf-8")
    git("diff", "--check", "HEAD")
    subprocess.run(["hugo", "--minify", "--cleanDestinationDir"], check=True)
    # Controls can be copied to RUNNER_TEMP. Data still belongs to the checkout.
    import check_growth_build
    check_growth_build.ROOT = Path.cwd()
    check_growth_build.main()


def improvement_message(paths, log_diff, commit_url):
    affected = [p for p in paths if p != "docs/growth-log.md"]
    if not affected:
        return None
    additions = "\n".join(line[1:] for line in log_diff.splitlines()
                          if line.startswith("+") and not line.startswith("+++"))
    details = additions.strip()[:1800] or "Consulta el commit para ver el detalle de la mejora."
    files = "\n".join(f"- {p}" for p in affected[:8])[:900]
    if len(affected) > 8:
        files += f"\nY {len(affected) - 8} archivos mas."
    return ("Mejora diaria publicada en GitHub\n\n"
            f"Cambio y motivo:\n{details}\n\nPaginas/archivos afectados:\n{files}"
            "\n\nValidacion Hugo y controles: correctos."
            "\nDespliegue Cloudflare: pendiente de confirmacion.\n"
            f"{commit_url}")


def record_publication(sha):
    output = os.environ.get("GITHUB_OUTPUT")
    report = os.environ.get("GROWTH_NOTIFICATION_FILE")
    if not output or not report:
        return
    paths = git("diff", "--name-only", f"{sha}^", sha).stdout.splitlines()
    log_diff = git("diff", "--unified=0", f"{sha}^", sha, "--", "docs/growth-log.md").stdout
    message = improvement_message(paths, log_diff,
        f"{os.environ['GITHUB_SERVER_URL']}/{os.environ['GITHUB_REPOSITORY']}/commit/{sha}")
    if message:
        Path(report).write_text(message, encoding="utf-8")
        with open(output, "a", encoding="utf-8") as handle:
            handle.write("published=true\n")


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
        validate()
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
            sha = git("rev-parse", "HEAD").stdout.strip()
            print("Published", sha)
            record_publication(sha)
            return
        print(f"Push rejected ({attempt + 1}/3); fetching and validating again")
    raise RuntimeError("Push failed after three attempts; never force-push")


if __name__ == "__main__":
    main()
