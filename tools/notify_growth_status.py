"""Send a daily Grow status through the configured Telegram notifier."""
from datetime import date
import os
from pathlib import Path
import sys

import notify_growth


def message(report_date, reason, gsc_status, ga4_status, result="no-change"):
    date.fromisoformat(report_date)
    if not reason or len(reason) > 1600 or any(c in reason for c in "\r\n\x00"):
        raise ValueError("Invalid Grow status reason")
    labels = {"ok": "disponible", "error": "error de consulta", "unavailable": "no disponible"}
    if gsc_status not in labels or ga4_status not in labels:
        raise ValueError("Invalid analytics status")
    if result not in ("no-change", "published"):
        raise ValueError("Invalid Grow result")
    outcome = ("Resultado: mejora publicada en GitHub.\n"
               if result == "published" else "Resultado: sin nueva mejora publicada.\n")
    closing = ("El despliegue público puede tardar unos minutos."
               if result == "published" else "El sitio no se modificó en esta revisión.")
    return (f"Grow: revisión diaria del {report_date}\n\n"
            f"{outcome}"
            f"Motivo: {reason}\n\n"
            f"Search Console: {labels[gsc_status]}. GA4: {labels[ga4_status]}.\n"
            f"{closing}")


def main():
    report = message(os.environ["GROWTH_REPORT_DATE"], os.environ["GROWTH_REASON"],
                     os.environ["GROWTH_GSC_STATUS"], os.environ["GROWTH_GA4_STATUS"],
                     os.environ.get("GROWTH_RESULT", "no-change"))
    Path(os.environ["GROWTH_NOTIFICATION_FILE"]).write_text(report, encoding="utf-8")
    notify_growth.main()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("No se pudo enviar el estado diario de Grow.", file=sys.stderr)
        raise SystemExit(1)
