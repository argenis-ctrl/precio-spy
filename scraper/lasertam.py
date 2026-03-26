"""
Scraper para Lasertam (lasertam.com) - WooCommerce store.
Usa la API pública wp-json/wc/store/v1 para obtener precios exactos
por variante de sesiones (1, 3, 6, 9 sesiones).
"""

import logging
import re
from datetime import datetime, timezone

import requests

from db.models import get_competitor_id, insert_price_records_if_changed
from scraper.zones import clean_price, detect_gender, normalize_zone

log = logging.getLogger(__name__)
COMPETITOR = "Lasertam"
BASE = "https://lasertam.com/wp-json/wc/store/v1"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# Mapeo de texto de variante → número de sesiones
SESSION_MAP = {
    "1 sesión": 1, "1 sesi": 1, "1 ses": 1,
    "3 sesiones": 3, "3 ses": 3,
    "6 sesiones": 6, "6 ses": 6,
    "9 sesiones": 9, "9 ses": 9,
    "2 sesión": 2, "2 ses": 2,
}

# Categorías a excluir (no son zonas de depilación)
SKIP_KEYWORDS = ["giftcard", "tarjeta regalo", "vip", "gift"]


def _get(url: str, params: dict = None) -> dict | list | None:
    try:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning(f"Error fetching {url}: {e}")
        return None


def _parse_sessions(variant_label: str) -> int | None:
    text = variant_label.lower().strip()
    for key, val in SESSION_MAP.items():
        if key.lower() in text:
            return val
    m = re.search(r"(\d+)\s*sesi", text)
    return int(m.group(1)) if m else None


def _is_laser_product(product: dict) -> bool:
    name = product.get("name", "").lower()
    if any(k in name for k in SKIP_KEYWORDS):
        return False
    attrs = product.get("attributes", [])
    return any("sesi" in a.get("name", "").lower() for a in attrs)


def _detect_gender_from_name(name: str) -> str:
    lower = name.lower()
    if any(w in lower for w in ["masculin", "hombre", " masc"]):
        return "M"
    return "F"  # Lasertam es mayoritariamente femenino por default


def scrape() -> int:
    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    run_id = now
    records = []

    # Obtener todos los productos
    all_products = []
    page = 1
    while True:
        data = _get(f"{BASE}/products", params={"per_page": 100, "page": page})
        if not data:
            break
        all_products.extend(data)
        if len(data) < 100:
            break
        page += 1

    log.info(f"Lasertam: {len(all_products)} productos encontrados en API")

    laser_products = [p for p in all_products if _is_laser_product(p)]
    log.info(f"Lasertam: {len(laser_products)} productos de depilación láser")

    for prod in laser_products:
        raw_name = prod.get("name", "").strip()
        zone_name = normalize_zone(raw_name)
        gender = _detect_gender_from_name(raw_name)

        variations = prod.get("variations", [])
        if not variations:
            # Producto simple sin variantes
            prices = prod.get("prices", {})
            price = clean_price(prices.get("price") or prices.get("sale_price"))
            orig = clean_price(prices.get("regular_price"))
            if price:
                records.append({
                    "competitor_id": comp_id,
                    "zone_name": zone_name,
                    "zone_raw": raw_name,
                    "gender": gender,
                    "sessions": None,
                    "price": price,
                    "original_price": orig if orig and orig > price else None,
                    "discount_pct": round((1 - price / orig) * 100, 1) if orig and orig > price else None,
                    "scraped_at": now,
                    "run_id": run_id,
                })
            continue

        # Obtener precio de cada variante
        for var in variations:
            var_id = var.get("id")
            var_attrs = var.get("attributes", [])

            # Detectar número de sesiones del atributo
            sessions = None
            for attr in var_attrs:
                if "sesi" in attr.get("name", "").lower():
                    sessions = _parse_sessions(attr.get("value", ""))
                    break

            # Obtener precio de la variante individualmente
            var_data = _get(f"{BASE}/products/{var_id}")
            if not var_data:
                continue

            var_prices = var_data.get("prices", {})
            price = clean_price(var_prices.get("price") or var_prices.get("sale_price"))
            orig = clean_price(var_prices.get("regular_price"))

            if not price:
                continue

            # Precio original solo si es mayor al de oferta
            real_orig = orig if orig and orig > price else None
            disc = round((1 - price / real_orig) * 100, 1) if real_orig else None

            records.append({
                "competitor_id": comp_id,
                "zone_name": zone_name,
                "zone_raw": raw_name,
                "gender": gender,
                "sessions": sessions,
                "price": price,
                "original_price": real_orig,
                "run_id": run_id,
                "discount_pct": disc,
                "scraped_at": now,
                "run_id": run_id,
            })

    changed = insert_price_records_if_changed(records)
    log.info(f"Lasertam: {len(records)} zonas scrapeadas, {changed} con precio nuevo/cambiado")
    return changed


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scrape()
    print(f"Lasertam: {n} registros guardados")
