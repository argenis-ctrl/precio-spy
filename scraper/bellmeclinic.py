"""
Scraper para Bellmeclinic (bellmeclinic.cl).
Usa la API WooCommerce Store v1 con User-Agent de iPhone (503/403 con UA de escritorio).
Zonas vendidas por tamaño: XS, S, M, L, XL con 1, 3 o 6 sesiones.
"""

import html
import logging
import re
from datetime import datetime, timezone

import requests

from db.models import get_competitor_id, insert_price_records_if_changed
from scraper.zones import clean_price

log = logging.getLogger(__name__)
COMPETITOR = "Bellmeclinic"

BASE_URL = "https://www.bellmeclinic.cl"
API_URL  = f"{BASE_URL}/wp-json/wc/store/v1/products"

# iPhone UA es necesario — UA de escritorio retorna 403
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Mobile/15E148 Safari/604.1"
    ),
    "Accept": "application/json, */*",
    "Accept-Language": "es-CL,es;q=0.9",
    "Referer": f"{BASE_URL}/",
}

# Mapa de tamaño de zona → nombre canónico
_ZONE_SIZE_MAP = {
    "xs": "Zona XS",
    "s":  "Zona S",
    "m":  "Zona M",
    "l":  "Zona L",
    "xl": "Zona XL",
}

# Sesiones según sufijo en slug/nombre
_SESSION_RE = re.compile(
    r"r(\d+)s"            # r1s, r3s, r6s en el slug
    r"|(\d+)\s*sesion",   # "6 sesiones" en el nombre
    re.IGNORECASE,
)


def _parse_product(name: str, slug: str) -> tuple[str | None, int | None]:
    """
    Extrae (zone_name, sessions) del nombre o slug del producto.
    Ejemplos:
      'Zona XS – 6 Sesiones'  → ('Zona XS', 6)
      'zona-xs-r1s'           → ('Zona XS', 1)
      'Zona M'                → ('Zona M', 6)   # default 6
    Retorna (None, None) si no es un producto de zona laser.
    """
    text = name.lower()
    slug_l = slug.lower()

    # Debe contener "zona" para ser un producto de zona
    if "zona" not in text and "zona" not in slug_l:
        return None, None

    # Extraer tamaño (XS/S/M/L/XL)
    size_match = re.search(r"\bzona[- ]+(xs|xl|[sml])\b", text + " " + slug_l)
    if not size_match:
        return None, None
    size = size_match.group(1)
    zone_name = _ZONE_SIZE_MAP.get(size)
    if not zone_name:
        return None, None

    # Extraer sesiones del nombre o slug
    m = _SESSION_RE.search(text) or _SESSION_RE.search(slug_l)
    if m:
        sessions = int(m.group(1) or m.group(2))
    else:
        sessions = 6  # Bellmeclinic vende en paquetes de 6 por defecto

    return zone_name, sessions


def _fetch_all_products() -> list[dict]:
    products = []
    page = 1
    while True:
        try:
            r = requests.get(
                API_URL,
                params={"per_page": 100, "page": page},
                headers=HEADERS,
                timeout=20,
            )
            if r.status_code == 400:
                break  # no más páginas
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            products.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        except Exception as e:
            log.warning(f"Bellmeclinic API error (página {page}): {e}")
            break
    return products


def scrape() -> int:
    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    run_id = now

    products = _fetch_all_products()
    if not products:
        log.warning("Bellmeclinic: sin productos obtenidos de la API")
        return 0

    log.info(f"Bellmeclinic: {len(products)} productos obtenidos")

    records = []
    for p in products:
        name = html.unescape(p.get("name", "")).strip()
        slug = p.get("slug", "").strip()

        zone_name, sessions = _parse_product(name, slug)
        if not zone_name:
            continue

        prices = p.get("prices", {})
        price = clean_price(prices.get("price"))
        reg   = clean_price(prices.get("regular_price"))

        if not price:
            continue

        original = reg if reg and reg > price else None
        disc = round((1 - price / original) * 100, 1) if original else None

        records.append({
            "competitor_id":  comp_id,
            "zone_name":      zone_name,
            "zone_raw":       name,
            "gender":         "F",   # Bellmeclinic es clínica femenina
            "sessions":       sessions,
            "price":          price,
            "original_price": original,
            "discount_pct":   disc,
            "scraped_at":     now,
            "run_id":         run_id,
        })

    if records:
        insert_price_records_if_changed(records)
    log.info(f"Bellmeclinic: {len(records)} registros insertados")
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scrape()
    print(f"Bellmeclinic: {n} registros guardados")
