"""
Scraper para Bellmeclinic (bellmeclinic.cl).
El sitio retorna 403 con requests simples; intenta con headers avanzados
y como fallback usa Playwright.
"""

import logging
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from db.models import delete_latest_run, get_competitor_id, insert_price_records
from scraper.zones import clean_price, detect_gender, normalize_zone

log = logging.getLogger(__name__)
COMPETITOR = "Bellmeclinic"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0",
}

URLS_TO_TRY = [
    "https://www.bellmeclinic.cl/servicios/depilacion-laser/",
    "https://www.bellmeclinic.cl/depilacion-laser/",
    "https://www.bellmeclinic.cl/tratamientos/depilacion-laser/",
    "https://www.bellmeclinic.cl/precios/",
    "https://www.bellmeclinic.cl/servicios/",
    "https://www.bellmeclinic.cl/",
]


def _fetch_with_session(url: str) -> BeautifulSoup | None:
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        # Primero hacer GET al home para obtener cookies
        session.get("https://www.bellmeclinic.cl/", timeout=15)
        resp = session.get(url, timeout=20)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, "html.parser")
        log.warning(f"Bellmeclinic: {url} -> HTTP {resp.status_code}")
    except Exception as e:
        log.warning(f"Bellmeclinic fetch error {url}: {e}")
    return None


def _extract_prices(soup: BeautifulSoup, comp_id: int, now: str, run_id: str = "") -> list[dict]:
    records = []
    text = soup.get_text(" ")

    # Buscar patrones zona + precio
    # Ej: "Axilas $15.000", "Piernas completas: $45.000"
    zone_price_pattern = re.findall(
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ\s]+)\s*:?\s*\$\s*([\d.,]+)",
        text,
    )
    for zone_raw, price_str in zone_price_pattern:
        zone_raw = zone_raw.strip()
        price = clean_price(price_str)
        if not price or price < 1000 or price > 2_000_000:
            continue
        if len(zone_raw) < 3 or len(zone_raw) > 60:
            continue

        zone_name = normalize_zone(zone_raw)
        gender = detect_gender(zone_raw)

        records.append({
            "competitor_id": comp_id,
            "zone_name": zone_name,
            "zone_raw": zone_raw,
            "gender": gender,
            "sessions": 1,
            "price": price,
            "original_price": None,
            "discount_pct": None,
            "scraped_at": now,
            "run_id": run_id,
        })

    return records


def _try_playwright() -> list[dict]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("Playwright no instalado.")
        return []

    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    records = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent=HEADERS["User-Agent"],
            locale="es-CL",
        )
        page_obj = ctx.new_page()

        for url in URLS_TO_TRY:
            try:
                resp = page_obj.goto(url, wait_until="networkidle", timeout=25000)
                if resp and resp.status == 200:
                    html = page_obj.content()
                    soup = BeautifulSoup(html, "html.parser")
                    recs = _extract_prices(soup, comp_id, now)
                    if recs:
                        records.extend(recs)
                        log.info(f"Bellmeclinic Playwright {url}: {len(recs)} registros")
                        break
            except Exception as e:
                log.warning(f"Bellmeclinic Playwright error {url}: {e}")

        browser.close()
    return records


def scrape() -> int:
    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    run_id = now
    delete_latest_run(comp_id)
    records = []

    # Intentar con requests primero
    for url in URLS_TO_TRY:
        soup = _fetch_with_session(url)
        if soup:
            recs = _extract_prices(soup, comp_id, now)
            if recs:
                records.extend(recs)
                log.info(f"Bellmeclinic requests {url}: {len(recs)} registros")
                break

    # Si requests falla, intentar Playwright
    if not records:
        log.info("Intentando Playwright para Bellmeclinic...")
        records = _try_playwright()

    if records:
        insert_price_records(records)
    log.info(f"Bellmeclinic: {len(records)} registros insertados")
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scrape()
    print(f"Bellmeclinic: {n} registros guardados")
