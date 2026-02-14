from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET_DIRS = [ROOT / "content", ROOT / "layouts", ROOT / "data"]
EXTS = {".md", ".html", ".toml", ".yaml", ".yml"}

def looks_mojibake(s: str) -> bool:
    # señales típicas
    return any(x in s for x in ("Â", "Ã", "â€", "â€™", "â€œ", "â€�", "â€“", "â€”"))

def fix_mojibake(s: str) -> str:
    """
    Arreglo típico: bytes UTF-8 leídos como Latin-1 -> aparecen Ã¡, Â·, etc.
    Si el texto contiene mojibake, intentamos el "roundtrip" latin1->utf8.
    """
    if not looks_mojibake(s):
        return s
    try:
        return s.encode("latin1", errors="strict").decode("utf-8", errors="strict")
    except Exception:
        return s

def read_text_best_effort(p: Path) -> tuple[str, str]:
    """
    Lee como UTF-8; si falla, prueba cp1252/latin-1.
    Devuelve (texto, encoding_usado)
    """
    raw = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            return raw.decode(enc), enc
        except Exception:
            pass
    # último recurso
    return raw.decode("latin1", errors="replace"), "latin1/replace"

def process_file(p: Path) -> bool:
    text, enc = read_text_best_effort(p)
    fixed = fix_mojibake(text)

    # además, corrige algunos tokens frecuentes aunque no pase el detector
    fixed = fixed.replace("Â·", "·")

    if fixed != text or enc not in ("utf-8", "utf-8-sig"):
        # escribe SIEMPRE en UTF-8 (sin BOM)
        p.write_text(fixed, encoding="utf-8", newline="\n")
        return True
    return False

def iter_files():
    for base in TARGET_DIRS:
        if not base.exists():
            continue
        for p in base.rglob("*"):
            if p.is_file() and p.suffix.lower() in EXTS:
                yield p

def main():
    changed = 0
    scanned = 0
    for p in iter_files():
        scanned += 1
        if process_file(p):
            changed += 1
            print(f"FIXED: {p.relative_to(ROOT)}")
    print(f"\nDone. Scanned={scanned}, changed={changed}")

if __name__ == "__main__":
    main()
