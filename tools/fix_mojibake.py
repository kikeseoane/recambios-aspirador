Vfrom __future__ import annotations
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXTS = {".md", ".html", ".toml", ".yaml", ".yml", ".json", ".txt"}

def looks_mojibake(s: str) -> bool:
    # patrones típicos en ES cuando UTF-8 se ve como Latin-1
    return ("Ã" in s) or ("Â" in s)

def fix_text(s: str) -> str:
    # Si el fichero contiene "BaterÃ­a", normalmente es:
    # bytes(UTF-8) -> interpretados como Latin-1 => string mojibake
    # Para arreglarlo: string.encode('latin-1') -> bytes originales -> decode('utf-8')
    return s.encode("latin-1", errors="strict").decode("utf-8", errors="strict")

def main() -> int:
    changed = 0
    scanned = 0

    for p in ROOT.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() not in EXTS:
            continue

        scanned += 1

        # Primero intenta leer como UTF-8 (lo normal en Hugo/repos)
        try:
            txt = p.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            # Si algún fichero está en cp1252, léelo así para inspección
            try:
                txt = p.read_text(encoding="cp1252")
            except Exception:
                continue

        if not looks_mojibake(txt):
            continue

        try:
            fixed = fix_text(txt)
        except Exception:
            # Si no encaja el patrón latin1->utf8, no tocamos el fichero
            continue

        if fixed != txt:
            # Backup al lado (por seguridad)
            bak = p.with_suffix(p.suffix + ".bak")
            if not bak.exists():
                bak.write_text(txt, encoding="utf-8")

            p.write_text(fixed, encoding="utf-8", newline="\n")
            changed += 1
            print(f"[OK] {p.relative_to(ROOT)}")

    print(f"\nScanned: {scanned} | Fixed: {changed}")
    print("Backups: *.bak (puedes borrarlos cuando verifiques)")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
