from pathlib import Path

ROOT = Path(".")
EXTS = {".html", ".md", ".yaml", ".yml", ".toml", ".json", ".xml", ".css", ".js"}

def fix_text(s: str) -> str:
    try:
        b = s.encode("latin1")
    except UnicodeEncodeError:
        return s
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return s

def should_process(p: Path) -> bool:
    if p.suffix.lower() not in EXTS:
        return False
    parts = {x.lower() for x in p.parts}
    if "public" in parts or "resources" in parts or "node_modules" in parts:
        return False
    return True

changed = []
for p in ROOT.rglob("*"):
    if not p.is_file() or not should_process(p):
        continue
    try:
        txt = p.read_text(encoding="utf-8", errors="strict")
    except Exception:
        continue

    if not any(x in txt for x in ("Ã", "â€", "â†", "Â")):
        continue

    fixed = fix_text(txt)
    if fixed != txt:
        p.write_text(fixed, encoding="utf-8", newline="\n")
        changed.append(str(p))

print("Fixed files:", len(changed))
for f in changed:
    print(" -", f)
