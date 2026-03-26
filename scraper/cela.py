"""
Scraper para Cela (cela.cl).
Usa la API GraphQL del CMS evup.com.br para obtener TODOS los paquetes
con precios exactos por sesiones (6 u 8 sesiones).
Credenciales embebidas en el JS público del sitio.
"""

import logging
from datetime import datetime, timezone

import requests

from db.models import delete_latest_run, get_competitor_id, insert_price_records
from scraper.zones import clean_price, normalize_zone

log = logging.getLogger(__name__)
COMPETITOR = "Cela"

CMS_BASE        = "https://cms.evup.com.br"
TOKEN_URL       = f"{CMS_BASE}/identity-server/connect/token"
GQL_URL         = f"{CMS_BASE}/api/content/ecommerce-chile/graphql"
CLIENT_ID       = "ecommerce-chile:default"
CLIENT_SECRET   = "zi8wmukrc93nfjab8ykxd2izbchaqwvkcg8ssq06jokx"

HEADERS_WEB = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Origin":   "https://www.cela.cl",
    "Referer":  "https://www.cela.cl/",
}

GQL_QUERY = """
{
  queryPacoteServicosContents(top: 500) {
    flatData {
      ecommerceDescription
      urlKey
      price
      mainPrice
      serviceItemQuantity
    }
  }
}
"""


def _get_token() -> str | None:
    try:
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "scope":         "squidex-api",
            },
            headers=HEADERS_WEB,
            timeout=15,
        )
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception as e:
        log.warning(f"Cela: no se pudo obtener token: {e}")
        return None


def _fetch_packages(token: str) -> list[dict]:
    try:
        r = requests.post(
            GQL_URL,
            json={"query": GQL_QUERY},
            headers={**HEADERS_WEB, "Authorization": f"Bearer {token}",
                     "Content-Type": "application/json"},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            log.warning(f"Cela GQL errors: {data['errors'][:2]}")
            return []
        return data.get("data", {}).get("queryPacoteServicosContents", [])
    except Exception as e:
        log.warning(f"Cela GQL fetch error: {e}")
        return []


def _detect_gender(desc: str) -> str:
    low = desc.lower()
    if "masculin" in low or "hombre" in low:
        return "M"
    return "F"


def _clean_desc(desc: str) -> str:
    """Elimina texto de promo del nombre para normalizar la zona."""
    import re
    # Quitar '+X sesiones gratis', 'Precio especial', etc.
    cleaned = re.sub(r"\+?\s*\d+\s*sesiones?\s*gratis", "", desc, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*-\s*$", "", cleaned.strip())
    return cleaned.strip()


def scrape() -> int:
    comp_id = get_competitor_id(COMPETITOR)
    now = datetime.now(timezone.utc).isoformat()
    run_id = now

    token = _get_token()
    if not token:
        log.error("Cela: no se pudo autenticar con CMS")
        return 0

    packages = _fetch_packages(token)
    if not packages:
        log.warning("Cela: sin paquetes obtenidos del CMS")
        return 0

    log.info(f"Cela: {len(packages)} paquetes obtenidos del CMS")
    delete_latest_run(comp_id)

    records = []
    for pkg in packages:
        fd = pkg.get("flatData", {})

        raw_desc  = fd.get("ecommerceDescription", "").strip()
        price     = clean_price(fd.get("price"))
        main_price = clean_price(fd.get("mainPrice"))
        sessions  = fd.get("serviceItemQuantity")

        if not raw_desc or not price:
            continue

        sessions = int(sessions) if sessions else None
        gender   = _detect_gender(raw_desc)
        zone_name = normalize_zone(_clean_desc(raw_desc))

        # Precio original solo si es mayor al de oferta
        real_orig = main_price if main_price and main_price > price else None
        disc = round((1 - price / real_orig) * 100, 1) if real_orig else None

        records.append({
            "competitor_id":  comp_id,
            "zone_name":      zone_name,
            "zone_raw":       raw_desc,
            "gender":         gender,
            "sessions":       sessions,
            "price":          price,
            "original_price": real_orig,
            "discount_pct":   disc,
            "scraped_at":     now,
            "run_id":         run_id,
        })

    insert_price_records(records)
    log.info(f"Cela: {len(records)} registros insertados")
    return len(records)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scrape()
    print(f"Cela: {n} registros guardados")
