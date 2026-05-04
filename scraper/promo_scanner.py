"""
Detector de promociones en sitios competidores.
Método principal: screenshot con Playwright + Claude Vision (ve lo que ve el usuario).
Fallback: scraping estático de texto HTML.
"""
import base64
import hashlib
import logging
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from db.models import get_competitor_id, get_connection

log = logging.getLogger(__name__)

_PROMO_RE = re.compile(
    r"(\d+\s*%)"
    r"|(\bcup[oó]n\b)"
    r"|(\bc[oó]digo\b)"
    r"|(\boferta\b)"
    r"|(\bdescuento\b|\bdcto\b)"
    r"|(\bgratis\b)"
    r"|(\bpromo\b)"
    r"|(\brebaje\b)"
    r"|(\b2x1\b|\b3x2\b)"
    r"|(\bahorra\b)"
    r"|(\bsale\b)",
    re.IGNORECASE,
)

_PRODUCT_CLASS_RE = re.compile(
    r"(product|item|card|grid|catalog|collection|listing|cart|checkout)",
    re.IGNORECASE,
)

SCAN_PAGES = {
    "Belenus":      ["https://belenus.cl"],
    "Cela":         ["https://www.cela.cl"],
    "Bellmeclinic": ["https://www.bellmeclinic.cl"],
    "Lasertam":     ["https://lasertam.com"],
}

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-CL,es;q=0.9",
}


def _text_hash(competitor_id: int, text: str) -> str:
    return hashlib.md5(f"{competitor_id}:{text.lower().strip()}".encode()).hexdigest()


# ── Método principal: Playwright screenshot ────────────────────────────────

def _screenshot_page(url: str) -> bytes | None:
    """Renderiza la página con Playwright y retorna screenshot PNG del viewport."""
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 800})
            page.goto(url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(2500)  # esperar sliders/animaciones
            screenshot = page.screenshot(
                clip={"x": 0, "y": 0, "width": 1280, "height": 750}
            )
            browser.close()
            return screenshot
    except Exception as e:
        log.warning(f"Playwright no disponible para {url}: {e}")
        return None


def _analyze_screenshot_with_claude(screenshot: bytes, page_url: str) -> list[dict]:
    """Manda screenshot a Claude Haiku Vision y extrae todas las promos visibles."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return []
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": base64.standard_b64encode(screenshot).decode(),
                        },
                    },
                    {
                        "type": "text",
                        "text": (
                            "Analiza este screenshot de una web de depilación láser. "
                            "Lista TODAS las promociones, descuentos, cupones y códigos visibles "
                            "(barras de anuncio, banners, pop-ups, sliders, texto destacado). "
                            "Por cada promo, escribe UNA línea: PROMO: [descripción breve con código si aplica]. "
                            "Si no hay ninguna promoción visible, responde solo: SIN_PROMO"
                        ),
                    },
                ],
            }],
        )
        promos = []
        for line in msg.content[0].text.strip().splitlines():
            line = line.strip()
            if line.upper().startswith("PROMO:"):
                text = line[6:].strip()
                if text:
                    promos.append({
                        "promo_text": text,
                        "page_url": page_url,
                        "source": "screenshot",
                    })
        return promos
    except Exception as e:
        log.warning(f"Error analizando screenshot de {page_url}: {e}")
        return []


# ── Fallback: scraping estático ────────────────────────────────────────────

def _fetch_page(url: str) -> BeautifulSoup | None:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        log.warning(f"No se pudo cargar {url}: {e}")
    return None


def _in_product_context(el) -> bool:
    try:
        for parent in el.parents:
            cls = " ".join(parent.get("class", []))
            if _PRODUCT_CLASS_RE.search(cls):
                return True
    except Exception:
        pass
    return False


def _extract_promo_texts(soup: BeautifulSoup, page_url: str) -> list[dict]:
    results = []
    seen: set[str] = set()
    for el in soup.find_all(["p", "span", "div", "h1", "h2", "h3", "a", "strong", "li"]):
        text = el.get_text(separator=" ", strip=True)
        if not (6 <= len(text) <= 200):
            continue
        if not _PROMO_RE.search(text):
            continue
        if _in_product_context(el):
            continue
        normalized = " ".join(text.split()).lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        results.append({"promo_text": text.strip(), "page_url": page_url, "source": "text"})
        if len(results) >= 8:
            break
    return results


def _find_banner_images(soup: BeautifulSoup, base_url: str) -> list[str]:
    banner_re = re.compile(
        r"(banner|hero|slider|carousel|promo|offer|feature|announcement|campaign)",
        re.IGNORECASE,
    )
    logo_re = re.compile(r"(logo|icon|avatar|star|arrow|check|close|menu|sprite)", re.IGNORECASE)
    candidates: list[str] = []
    seen: set[str] = set()

    for container in soup.find_all(["div", "section", "header"], class_=banner_re):
        for img in container.find_all("img", src=True):
            src = urljoin(base_url, img["src"])
            if src not in seen and src.startswith("http"):
                seen.add(src)
                candidates.append(src)

    if not candidates:
        for img in soup.find_all("img", src=True):
            src = urljoin(base_url, img["src"])
            if not src.startswith("http") or logo_re.search(src):
                continue
            w = img.get("width", "")
            if str(w).isdigit() and int(w) < 200:
                continue
            if src not in seen:
                seen.add(src)
                candidates.append(src)
            if len(candidates) >= 5:
                break

    return candidates[:3]


def _analyze_image_url_with_claude(image_url: str) -> str | None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        img_resp = requests.get(image_url, headers=_HEADERS, timeout=15)
        if img_resp.status_code != 200 or len(img_resp.content) < 1000:
            return None
        content_type = img_resp.headers.get("content-type", "image/jpeg").split(";")[0].strip()
        if "image" not in content_type or len(img_resp.content) > 1_500_000:
            return None
        if content_type not in ("image/jpeg", "image/png", "image/gif", "image/webp"):
            content_type = "image/jpeg"

        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=150,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": content_type,
                            "data": base64.standard_b64encode(img_resp.content).decode(),
                        },
                    },
                    {
                        "type": "text",
                        "text": (
                            "¿Hay alguna promoción, descuento, cupón o código de descuento visible? "
                            "Si hay, responde SOLO: PROMO: [texto exacto]. "
                            "Si no hay, responde solo: SIN_PROMO"
                        ),
                    },
                ],
            }],
        )
        resp = msg.content[0].text.strip()
        if resp.upper().startswith("PROMO:"):
            return resp[6:].strip()
        return None
    except Exception as e:
        log.warning(f"Error analizando imagen {image_url}: {e}")
        return None


# ── Persistencia ───────────────────────────────────────────────────────────

def _upsert_promotions(competitor_id: int, promos: list[dict], now: str) -> int:
    conn = get_connection()
    inserted = 0
    seen_hashes: list[str] = []

    for p in promos:
        h = _text_hash(competitor_id, p["promo_text"])
        seen_hashes.append(h)
        existing = conn.execute(
            "SELECT id FROM promotions WHERE competitor_id=? AND text_hash=?",
            (competitor_id, h),
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE promotions SET last_seen_at=?, is_active=1 WHERE id=?",
                (now, existing["id"]),
            )
        else:
            conn.execute(
                """
                INSERT INTO promotions
                    (competitor_id, source, promo_text, image_url, page_url,
                     detected_at, last_seen_at, is_active, text_hash)
                VALUES (?,?,?,?,?,?,?,1,?)
                """,
                (
                    competitor_id,
                    p.get("source", "text"),
                    p["promo_text"],
                    p.get("image_url"),
                    p.get("page_url", ""),
                    now,
                    now,
                    h,
                ),
            )
            inserted += 1

    # Marcar inactivas las que ya no se ven
    if seen_hashes:
        placeholders = ",".join("?" * len(seen_hashes))
        conn.execute(
            f"UPDATE promotions SET is_active=0 "
            f"WHERE competitor_id=? AND text_hash NOT IN ({placeholders})",
            [competitor_id] + seen_hashes,
        )
    else:
        conn.execute(
            "UPDATE promotions SET is_active=0 WHERE competitor_id=?",
            (competitor_id,),
        )

    conn.commit()
    conn.close()
    return inserted


# ── Entrada principal ──────────────────────────────────────────────────────

def scan_competitor(competitor_name: str) -> int:
    comp_id = get_competitor_id(competitor_name)
    pages = SCAN_PAGES.get(competitor_name, [])
    if not pages:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    all_promos: list[dict] = []

    for page_url in pages:
        screenshot = _screenshot_page(page_url)

        if screenshot and os.environ.get("ANTHROPIC_API_KEY"):
            # Método principal: screenshot renderizado → Claude Vision
            found = _analyze_screenshot_with_claude(screenshot, page_url)
            all_promos.extend(found)
            log.info(f"{competitor_name}: {len(found)} promos vía screenshot en {page_url}")
        else:
            # Fallback: scraping estático
            soup = _fetch_page(page_url)
            if soup:
                all_promos.extend(_extract_promo_texts(soup, page_url))
                if os.environ.get("ANTHROPIC_API_KEY"):
                    for img_url in _find_banner_images(soup, page_url):
                        promo_text = _analyze_image_url_with_claude(img_url)
                        if promo_text:
                            all_promos.append({
                                "promo_text": promo_text,
                                "image_url": img_url,
                                "page_url": page_url,
                                "source": "image",
                            })

    new_count = _upsert_promotions(comp_id, all_promos, now)
    log.info(f"{competitor_name}: {len(all_promos)} promos total, {new_count} nuevas")
    return new_count


def scan_all() -> int:
    total = 0
    for name in SCAN_PAGES:
        try:
            total += scan_competitor(name)
        except Exception as e:
            log.error(f"Error escaneando promos de {name}: {e}", exc_info=True)
    return total


if __name__ == "__main__":
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    logging.basicConfig(level=logging.INFO)
    from db.models import init_db
    init_db()
    n = scan_all()
    print(f"Nuevas promos detectadas: {n}")
