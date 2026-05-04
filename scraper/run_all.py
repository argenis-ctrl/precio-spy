"""
Ejecuta todos los scrapers en secuencia.
Usar: python -m scraper.run_all
"""

import logging
import sys
from pathlib import Path

# Asegurar que el directorio raíz esté en el path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from db.models import init_db
from scraper import belenus, bellmeclinic, cela, lasertam
from scraper import promo_scanner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_all")


def main():
    log.info("=== Iniciando scraping semanal ===")
    init_db()

    scrapers = [
        ("Belenus",      belenus.scrape),
        ("Lasertam",     lasertam.scrape),
        ("Cela",         cela.scrape),
        ("Bellmeclinic", bellmeclinic.scrape),
    ]

    results = {}
    for name, fn in scrapers:
        try:
            log.info(f"--- Scrapeando {name} ---")
            n = fn()
            results[name] = f"✓ {n} registros"
        except Exception as e:
            log.error(f"Error en {name}: {e}", exc_info=True)
            results[name] = f"✗ Error: {e}"

    log.info("=== Resumen ===")
    for name, result in results.items():
        log.info(f"  {name}: {result}")

    log.info("--- Escaneando promociones ---")
    try:
        new_promos = promo_scanner.scan_all()
        log.info(f"  Promos: {new_promos} nuevas detectadas")
    except Exception as e:
        log.error(f"Error en promo_scanner: {e}", exc_info=True)

    log.info("=== Scraping finalizado ===")


if __name__ == "__main__":
    main()
