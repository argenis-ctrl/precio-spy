"""
Scraper para Belenus (belenus.cl) - Shopify store.
Usa el endpoint /products.json que no requiere autenticación.
"""

import logging
from datetime import datetime, timezone

import requests

from db.models import get_competitor_id, insert_price_records_if_changed
from scraper.zones import clean_price, detect_gender, normalize_zone

log = logging.getLogger(__name__)

BASE_URL = "https://belenus.cl/products.json"
COMPETITOR = "Belenus"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def _fetch_all_products() -> list[dict]:
    products = []
    page = 1
    while True:
        resp = requests.get(
            BASE_URL,
            params={"limit": 250, "page": page},
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json().get("products", [])
        if not data:
            break
        products.extend(data)
        page += 1
    log.info(f"Belenus: {len(products)} productos obtenidos")
    return products


def _is_laser_product(product: dict) -> bool:
    title = product.get("product_type", "") + " " + product.get("title", "")
    keywords = ["depilac", "laser", "láser", "sesion", "sesión", "zona", "pierna",
                "axilas", "rostro", "brasil", "bikini", "espalda", "brazo"]
    return any(k in title.lower() for k in keywords)


def scrape() -> int:
    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    run_id = now
    products = _fetch_all_products()
    records = []

    for prod in products:
        if not _is_laser_product(prod):
            continue

        raw_name = prod.get("title", "").strip()
        zone_name = normalize_zone(raw_name)
        gender = detect_gender(raw_name)

        for variant in prod.get("variants", []):
            title_var = variant.get("title", "")
            sessions = _extract_sessions(title_var)

            price = clean_price(variant.get("price"))
            cmp = clean_price(variant.get("compare_at_price"))
            if price is None:
                continue

            orig = cmp if cmp and cmp > price else None
            disc = round((1 - price / orig) * 100, 1) if orig else None

            records.append({
                "competitor_id": comp_id,
                "zone_name": zone_name,
                "zone_raw": raw_name,
                "gender": gender,
                "sessions": sessions,
                "price": price,
                "original_price": orig,
                "discount_pct": disc,
                "scraped_at": now,
                "run_id": run_id,
            })

    changed = insert_price_records_if_changed(records)
    log.info(f"Belenus: {len(records)} zonas scrapeadas, {changed} con precio nuevo/cambiado")
    return changed


def _extract_sessions(variant_title: str) -> int | None:
    import re
    t = variant_title.lower()
    m = re.search(r"(\d+)\s*sesi[oó]n", t)
    if m:
        return int(m.group(1))
    if "1ª sesión" in t or "primera" in t or "1 ses" in t:
        return 1
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scrape()
    print(f"Belenus: {n} registros guardados")
