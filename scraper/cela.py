"""
Scraper para Cela (cela.cl).
Extrae precios desde schema.org JSON-LD embebido en cada página de zona.
No necesita Playwright: el JSON con precio está en el HTML estático.
"""

import json
import logging
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from db.models import delete_latest_run, get_competitor_id, insert_price_records
from scraper.zones import clean_price, normalize_zone

log = logging.getLogger(__name__)
COMPETITOR = "Cela"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# URLs completas extraídas del sitemap de cela.cl
ZONE_URLS_F = [
    ("F", "https://www.cela.cl/depilacion-laser-femenina-axilas"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-piernas-completas"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-media-pierna"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-muslos-completos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-muslo-posterior"),
    ("F", "https://www.cela.cl/depilacion-femenina-muslo-anterior"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-rebaje-completo"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-gluteos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-gluteos-medios"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-linea-interglutea"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-abdomen"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-linea-infraumbilical"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-linea-ombligo"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-brazos-completos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-antebrazos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-hombros"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-espalda-completa"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-espalda-superior"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-espalda-inferior"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-senos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-areolas"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-rostro-completo"),
    ("F", "https://www.cela.cl/depilacion-laser-rostro-inferior"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-bozo"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-menton"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-entrecejo"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-patillas"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-cuello-completo"),
    ("F", "https://www.cela.cl/depilacion-femenina-cuello-anterior"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-cuello-posterior"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-manos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-pies"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-rodillas"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-orejas"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-frente"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-muslos-internos"),
    ("F", "https://www.cela.cl/depilacion-laser-femenina-cabeza"),
]

ZONE_URLS_M = [
    ("M", "https://www.cela.cl/depilacion-laser-masculina-axilas"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-piernas-completas"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-barba-completa"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-rostro-completo"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-rostro-inferior"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-torax"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-abdomen"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-espalda-completa"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-espalda-superior"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-espalda-inferior"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-brazos-completos"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-gluteos"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-gluteos-medios"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-linea-interglutea"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-rebaje-completo"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-hombros"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-entrecejo"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-sobrecejo"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-patillas"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-cuello-completo"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-cuello-anterior"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-cuello-posterior"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-manos"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-pies"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-orejas"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-nariz"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-frente"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-cabeza"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-base-barba"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-barba-candado"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-delineado-barba"),
    ("M", "https://www.cela.cl/depilacion-lsaer-femenina-nariz"),  # typo en sitemap
    ("M", "https://www.cela.cl/depilacion-laser-masculina-pomulos"),
    ("M", "https://www.cela.cl/depilacion-laser-masculina-pies"),
]


def _fetch(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
        log.debug(f"Cela {url} → HTTP {r.status_code}")
    except Exception as e:
        log.debug(f"Cela fetch error {url}: {e}")
    return None


def _extract_schema_price(soup: BeautifulSoup) -> tuple[str | None, int | None]:
    """
    Extrae nombre y precio desde el JSON-LD schema.org/Product embebido.
    Retorna (product_name, price_clp).
    """
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "Product"), None)
            if not data or data.get("@type") != "Product":
                continue

            name = data.get("name", "")
            offers = data.get("offers", {})

            # Puede ser Offer o AggregateOffer
            if isinstance(offers, list):
                offers = offers[0] if offers else {}

            price = clean_price(offers.get("price"))
            return name, price

        except (json.JSONDecodeError, AttributeError):
            continue
    return None, None


def _sessions_from_name(name: str) -> int | None:
    """
    Intenta extraer número de sesiones del nombre del producto.
    Ej: 'Axilas 6 sesiones' → 6
    Ignora 'X sesiones gratis' (es una promo, no el conteo del paquete).
    """
    # Ignorar "X sesiones gratis"
    cleaned = re.sub(r"\+?\s*\d+\s*sesiones?\s*gratis", "", name, flags=re.IGNORECASE)
    m = re.search(r"(\d+)\s*sesiones?", cleaned, re.IGNORECASE)
    return int(m.group(1)) if m else None


def scrape() -> int:
    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    run_id = now
    delete_latest_run(comp_id)
    records = []
    seen_urls = set()

    all_zones = ZONE_URLS_F + ZONE_URLS_M

    for gender, url in all_zones:
        if url in seen_urls:
            continue
        seen_urls.add(url)

        soup = _fetch(url)
        if soup is None:
            log.debug(f"Cela: sin respuesta para {url}")
            continue

        raw_name, price = _extract_schema_price(soup)
        if not raw_name or not price:
            log.debug(f"Cela: sin precio en schema.org para {url}")
            continue

        zone_name = normalize_zone(raw_name)
        sessions = _sessions_from_name(raw_name)

        records.append({
            "competitor_id": comp_id,
            "zone_name": zone_name,
            "zone_raw": raw_name,
            "gender": gender,
            "sessions": sessions,
            "price": price,
            "original_price": None,
            "discount_pct": None,
            "scraped_at": now,
            "run_id": run_id,
        })
        log.debug(f"Cela [{gender}] {zone_name}: ${price:,} (ses={sessions})")

    insert_price_records(records)
    log.info(f"Cela: {len(records)} registros insertados")
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scrape()
    print(f"Cela: {n} registros guardados")
