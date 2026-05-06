"""
Informe de Ventas Lasertam — WooCommerce API
Página Streamlit con selector de rango de fechas, históricos y exportación PDF.
"""

import io
import json
import re
import tempfile
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from fpdf import FPDF
from requests.auth import HTTPBasicAuth

# ── Config ────────────────────────────────────────────────────────────────────
WC_URL   = "https://lasertam.com/wp-json/wc/v3"
AUTH     = HTTPBasicAuth("ck_d54578316281a23fbd19223eba88970dd29e5459",
                         "cs_4adb80948e5e0513d7fb0e1f05206e301e419430")
TZ_CL    = ZoneInfo("America/Santiago")

SESSION_RE = re.compile(r'(\d+)\s*sesi', re.I)

CHILE_REGIONS = {
    "RM":"Región Metropolitana","VS":"Valparaíso","BI":"Biobío",
    "AR":"La Araucanía","MA":"Maule","LI":"O'Higgins","AN":"Antofagasta",
    "CO":"Coquimbo","LL":"Los Lagos","LR":"Los Ríos","TA":"Tarapacá",
    "AT":"Atacama","AI":"Aysén","ML":"Magallanes","AP":"Arica y Parinacota",
    "NB":"Ñuble",
}
MONTH_NAMES = {
    1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",
    5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",
    9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre",
}
SOURCE_LABELS = {
    "google":      "Google",
    "adwords":     "Google Ads",
    "ig":          "Instagram",
    "instagram":   "Instagram",
    "facebook":    "Facebook",
    "fb":          "Facebook",
    "newsletter":  "Email / Newsletter",
    "email":       "Email / Newsletter",
    "direct":      "Directo",
    "(direct)":    "Directo",
    "organic":     "Orgánico",
    "l.wl.co":     "WhatsApp Link",
    "whatsapp":    "WhatsApp",
    "tiktok":      "TikTok",
    "bing":        "Bing",
    "yahoo":       "Yahoo",
    "cl.search.yahoo.com": "Yahoo Search",
}
COLORS = ["#8b5cf6","#06b6d4","#10b981","#f59e0b","#ef4444",
          "#ec4899","#6366f1","#84cc16","#f97316","#14b8a6"]

# ── Helpers ───────────────────────────────────────────────────────────────────
def normalize_region(raw: str) -> str:
    key = raw.upper().replace("CL-", "")
    return CHILE_REGIONS.get(key, raw) if raw else "Sin datos"

def fmt_clp(n: float) -> str:
    return "$" + f"{int(n):,}".replace(",", ".")

def _detect_channel(meta: dict) -> str:
    """Construye el nombre de canal legible desde los meta de WooCommerce Order Attribution."""
    utm_source  = meta.get("_wc_order_attribution_utm_source", "").lower().strip()
    utm_medium  = meta.get("_wc_order_attribution_utm_medium", "").lower().strip()
    source_type = meta.get("_wc_order_attribution_source_type", "").lower().strip()
    referrer    = meta.get("_wc_order_attribution_referrer", "").lower().strip()

    # Fuente con label legible
    raw = utm_source or source_type or referrer

    # Ignorar "utm" como valor de fuente (es solo el tipo, no la fuente real)
    if raw in ("utm", ""):
        # Intentar construir desde medium
        if utm_medium:
            return SOURCE_LABELS.get(utm_medium, utm_medium.title())
        return "Directo / Orgánico"

    label = SOURCE_LABELS.get(raw, None)
    if label:
        # Enriquecer: Google + cpc → Google Ads
        if label == "Google" and utm_medium in ("cpc", "paidsearch", "ppc", "adwords"):
            return "Google Ads"
        if label == "Google" and utm_medium in ("organic", ""):
            return "Google Orgánico"
        return label

    # Dominio referrer → acortar a dominio raíz
    if referrer and raw == referrer:
        parts = raw.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:]).title()

    return raw.title() if raw else "Directo / Orgánico"


def detect_sessions(item: dict) -> int | None:
    for meta in item.get("meta_data", []):
        if "sesi" in str(meta.get("key", "")).lower():
            m = SESSION_RE.search(str(meta.get("value", "")))
            if m:
                return int(m.group(1))
    m = SESSION_RE.search(item.get("name", ""))
    return int(m.group(1)) if m else None

def dates_to_iso(d_from, d_to):
    """Convierte date objects a ISO con zona Chile (inicio y fin de día)."""
    after  = datetime(d_from.year, d_from.month, d_from.day, 0, 0, 0, tzinfo=TZ_CL).isoformat()
    before = datetime(d_to.year,   d_to.month,   d_to.day,  23, 59, 59, tzinfo=TZ_CL).isoformat()
    return after, before

# ── API ───────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_orders(after: str, before: str) -> list:
    items, page = [], 1
    while True:
        r = requests.get(f"{WC_URL}/orders", auth=AUTH, timeout=30, params={
            "per_page": 100, "page": page,
            "after": after, "before": before,
            "status": "completed,processing",
        })
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        items.extend(data)
        if len(data) < 100:
            break
        page += 1
    return items

@st.cache_data(ttl=3600, show_spinner=False)
def _had_prior_orders(cid: int, email: str, before_iso: str) -> bool:
    """True si el cliente (por ID o email) NO tiene órdenes antes del período = es New."""
    try:
        params = {"before": before_iso, "per_page": 1,
                  **( {"customer": cid} if cid > 0 else {"billing_email": email} )}
        r = requests.get(f"{WC_URL}/orders", auth=AUTH, params=params, timeout=5)
        return int(r.headers.get("X-WP-Total", "0")) == 0
    except Exception:
        return False

# ── Métricas ──────────────────────────────────────────────────────────────────
def compute_metrics(orders: list, after: str) -> dict:

    totals       = [float(o.get("total", 0)) for o in orders]
    total_ventas = sum(totals)
    n_ordenes    = len(orders)
    ticket_avg   = total_ventas / n_ordenes if n_ordenes else 0

    prod_units   = Counter()
    prod_revenue = defaultdict(float)
    pack_units   = Counter({1: 0, 3: 0, 6: 0, 9: 0})
    pack_revenue = defaultdict(float)
    reg_orders   = Counter()
    reg_revenue  = defaultdict(float)
    chan_orders  = Counter()
    chan_revenue = defaultdict(float)

    for o in orders:
        total_o = float(o.get("total", 0))

        # Productos y packs
        for item in o.get("line_items", []):
            name = item.get("name", "Desconocido")
            qty  = item.get("quantity", 1)
            rev  = float(item.get("subtotal", 0))
            prod_units[name]   += qty
            prod_revenue[name] += rev
            s = detect_sessions(item)
            if s in (1, 3, 6, 9):
                pack_units[s]   += qty
                pack_revenue[s] += rev

        # Región
        state = normalize_region((o.get("billing") or {}).get("state") or "")
        reg_orders[state]  += 1
        reg_revenue[state] += total_o

        # Canal — lógica mejorada para nombres legibles
        meta_map = {m["key"]: str(m.get("value") or "").strip() for m in o.get("meta_data", [])}
        source = _detect_channel(meta_map)
        chan_orders[source]  += 1
        chan_revenue[source] += total_o

    # Nuevos vs Returning — llamadas paralelas para no bloquear cargando orden por orden
    # Recopilar clientes únicos primero
    unique: dict[tuple, bool] = {}
    for o in orders:
        cid   = o.get("customer_id", 0)
        email = ((o.get("billing") or {}).get("email") or "").lower().strip()
        key   = (cid, email)
        if key not in unique:
            unique[key] = False

    def _check(key):
        cid, email = key
        return key, _had_prior_orders(cid, email, after)

    with ThreadPoolExecutor(max_workers=12) as pool:
        for key, result in pool.map(_check, unique.keys()):
            unique[key] = result

    new_c = ret_c = 0
    order_rows = []

    for o in orders:
        cid      = o.get("customer_id", 0)
        bill     = o.get("billing", {}) or {}
        name     = f"{bill.get('first_name','')} {bill.get('last_name','')}".strip() or "—"
        email    = (bill.get("email") or "").lower().strip()
        date_str = (o.get("date_created") or "")[:10]

        is_new = unique.get((cid, email), False)
        tipo   = "New" if is_new else "Returning"
        if is_new: new_c += 1
        else:       ret_c += 1

        order_rows.append({
            "Fecha":            date_str,
            "Nombre":           name,
            "Email":            email,
            "Total":            float(o.get("total", 0)),
            "Tipo de cliente":  tipo,
        })

    return {
        "total_ventas": total_ventas, "n_ordenes": n_ordenes, "ticket_avg": ticket_avg,
        "prod_units": prod_units, "prod_revenue": prod_revenue,
        "pack_units": pack_units, "pack_revenue": pack_revenue,
        "reg_orders": reg_orders, "reg_revenue": reg_revenue,
        "chan_orders": chan_orders, "chan_revenue": chan_revenue,
        "new_c": new_c, "ret_c": ret_c,
        "order_rows": order_rows,
    }

# ── PDF Temas ─────────────────────────────────────────────────────────────────
PDF_THEMES = {
    "Oscuro": {
        "page_bg":    (15,  15,  26),
        "header_bg":  (10,  10,  20),
        "card_bg":    (26,  26,  46),
        "text":       (226, 232, 240),
        "text_sub":   (148, 163, 184),
        "accent":     (139, 92,  246),
        "accent2":    (6,   182, 212),
        "row_even":   (26,  26,  50),
        "row_odd":    (20,  20,  40),
        "th_bg":      (40,  40,  80),
        "bar_colors": ["#8b5cf6","#06b6d4","#10b981","#f59e0b","#ef4444","#ec4899"],
        "mpl_style":  "dark_background",
    },
    "Claro": {
        "page_bg":    (255, 255, 255),
        "header_bg":  (139, 92,  246),
        "card_bg":    (245, 245, 255),
        "text":       (30,  30,  50),
        "text_sub":   (100, 100, 130),
        "accent":     (139, 92,  246),
        "accent2":    (6,   182, 212),
        "row_even":   (248, 248, 255),
        "row_odd":    (255, 255, 255),
        "th_bg":      (220, 210, 255),
        "bar_colors": ["#8b5cf6","#06b6d4","#10b981","#f59e0b","#ef4444","#ec4899"],
        "mpl_style":  "seaborn-v0_8-whitegrid",
    },
    "Blanco y Negro": {
        "page_bg":    (255, 255, 255),
        "header_bg":  (30,  30,  30),
        "card_bg":    (240, 240, 240),
        "text":       (0,   0,   0),
        "text_sub":   (80,  80,  80),
        "accent":     (50,  50,  50),
        "accent2":    (100, 100, 100),
        "row_even":   (240, 240, 240),
        "row_odd":    (255, 255, 255),
        "th_bg":      (180, 180, 180),
        "bar_colors": ["#333","#555","#777","#999","#bbb","#ddd"],
        "mpl_style":  "grayscale",
    },
}

def _chart_bar(labels, values, title, t: dict, color_idx=0, horizontal=False) -> bytes:
    try:
        plt.style.use(t["mpl_style"])
    except Exception:
        plt.style.use("default")
    bar_color = t["bar_colors"][color_idx % len(t["bar_colors"])]
    fig, ax = plt.subplots(figsize=(7, 3.5) if not horizontal else (7, max(3, len(labels)*0.45)))
    if horizontal:
        ax.barh(labels, values, color=bar_color)
        ax.invert_yaxis()
    else:
        ax.bar(labels, values, color=bar_color)
        plt.xticks(rotation=15, ha="right", fontsize=8)
    ax.set_title(title, fontsize=10, fontweight="bold", pad=8)
    ax.tick_params(labelsize=8)
    ax.spines[["top","right"]].set_visible(False)
    plt.tight_layout()
    buf = io.BytesIO()
    bg = tuple(c/255 for c in t["page_bg"])
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor=bg)
    plt.close(fig)
    plt.style.use("default")
    buf.seek(0)
    return buf.read()

def _chart_pie(labels, values, title, t: dict) -> bytes:
    try:
        plt.style.use(t["mpl_style"])
    except Exception:
        plt.style.use("default")
    fig, ax = plt.subplots(figsize=(5, 4))
    ax.pie(values, labels=labels, autopct="%1.1f%%",
           colors=t["bar_colors"], startangle=90,
           wedgeprops=dict(width=0.6))
    ax.set_title(title, fontsize=10, fontweight="bold")
    plt.tight_layout()
    buf = io.BytesIO()
    bg = tuple(c/255 for c in t["page_bg"])
    fig.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor=bg)
    plt.close(fig)
    plt.style.use("default")
    buf.seek(0)
    return buf.read()

def _img_cell(pdf: FPDF, img_bytes: bytes, w: float, h: float):
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(img_bytes)
        f.flush()
        pdf.image(f.name, x=pdf.get_x(), y=pdf.get_y(), w=w, h=h)

def build_pdf(m: dict, label_rango: str, theme_name: str = "Claro") -> bytes:
    t      = PDF_THEMES[theme_name]
    top10  = m["prod_units"].most_common(10)
    tchans = m["chan_orders"].most_common(8)
    tregs  = m["reg_orders"].most_common(8)

    pack_labels = [f"{n} Ses{'.' if n==1 else 'iones'}" for n in [1,3,6,9]]

    # ── Generar gráficos con el tema ─────────────────────────────────────────
    img_packs    = _chart_bar(pack_labels, [m["pack_units"][n] for n in [1,3,6,9]],
                              "Packs — Unidades", t)
    img_tipo     = _chart_pie(["New","Returning"], [m["new_c"], m["ret_c"]],
                              "Tipo de cliente", t)
    img_chan     = _chart_pie([c for c,_ in tchans], [n for _,n in tchans],
                              "Canales de venta", t)
    img_reg      = _chart_bar([r for r,_ in tregs], [n for _,n in tregs],
                              "Ventas por Region", t, color_idx=1, horizontal=True)
    img_prods    = _chart_bar([n[:40] for n,_ in top10], [u for _,u in top10],
                              "Top Productos — Unidades", t, horizontal=True)
    img_pack_rev = _chart_bar(pack_labels, [int(m["pack_revenue"][n]) for n in [1,3,6,9]],
                              "Ingresos por Pack (CLP)", t, color_idx=3)

    # ── Helpers con tema ─────────────────────────────────────────────────────
    def pg_bg():
        """Rellena el fondo de la página con el color del tema."""
        pdf.set_fill_color(*t["page_bg"])
        pdf.rect(0, 0, 210, 297, "F")

    def section_title(txt):
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*t["accent"])
        pdf.cell(0, 7, txt, ln=True)
        pdf.set_text_color(*t["text"])

    def table_header(headers, widths):
        pdf.set_fill_color(*t["th_bg"])
        pdf.set_font("Helvetica", "B", 8)
        pdf.set_text_color(*t["text"])
        for h, w in zip(headers, widths):
            pdf.cell(w, 7, h, border=1, fill=True)
        pdf.ln()

    def table_row(cells, widths, idx, aligns=None, tipo=None):
        fill_color = t["row_even"] if idx % 2 == 0 else t["row_odd"]
        pdf.set_fill_color(*fill_color)
        pdf.set_font("Helvetica", "", 7.5)
        for j, (cell, w) in enumerate(zip(cells, widths)):
            align = (aligns[j] if aligns else "L")
            if tipo and j == len(cells) - 1:
                pdf.set_text_color(*(16,185,129) if tipo == "New" else t["accent"])
            else:
                pdf.set_text_color(*t["text"])
            pdf.cell(w, 6.5, str(cell), border=1, fill=True, align=align)
        pdf.set_text_color(*t["text"])
        pdf.ln()

    # ── PDF ──────────────────────────────────────────────────────────────────
    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.set_margins(12, 12, 12)

    now_str = datetime.now(TZ_CL).strftime("%d/%m/%Y %H:%M")

    # ── Página 1 ─────────────────────────────────────────────────────────────
    pdf.add_page()
    pg_bg()

    # Header banner
    pdf.set_fill_color(*t["header_bg"])
    pdf.rect(0, 0, 210, 32, "F")
    pdf.set_xy(12, 7)
    pdf.set_text_color(*t["accent"])
    pdf.set_font("Helvetica", "B", 17)
    pdf.cell(55, 9, "LASERTAM", ln=False)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 9, f"Informe de Ventas  |  {label_rango}", ln=False)
    pdf.set_xy(12, 22)
    pdf.set_text_color(200, 200, 220)
    pdf.set_font("Helvetica", "", 7.5)
    pdf.cell(0, 6, f"Generado el {now_str}  ·  {m['n_ordenes']} ordenes  ·  America/Santiago")
    pdf.set_y(38)

    # KPI tarjetas
    kpi_colors = [t["accent"], t["accent2"], (80,80,80), (16,185,129), (245,158,11)]
    kpi_data   = [
        ("Ventas Totales",  fmt_clp(m["total_ventas"])),
        ("Ticket Promedio", fmt_clp(m["ticket_avg"])),
        ("Total Ordenes",   str(m["n_ordenes"])),
        ("Clientes New",    str(m["new_c"])),
        ("Returning",       str(m["ret_c"])),
    ]
    cw = 37
    for (lbl, val), col in zip(kpi_data, kpi_colors):
        x, y = pdf.get_x(), pdf.get_y()
        pdf.set_fill_color(*t["card_bg"])
        pdf.rect(x, y, cw - 2, 19, "F")
        pdf.set_draw_color(*col)
        pdf.set_line_width(0.9)
        pdf.line(x, y, x, y + 19)
        pdf.set_line_width(0.2)
        pdf.set_text_color(*t["text_sub"])
        pdf.set_font("Helvetica", "", 6)
        pdf.set_xy(x + 2.5, y + 2)
        pdf.cell(cw - 5, 4, lbl.upper())
        pdf.set_text_color(*t["text"])
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_xy(x + 2.5, y + 7.5)
        pdf.cell(cw - 5, 7, val)
        pdf.set_xy(x + cw, y)

    pdf.ln(26)
    section_title("Packs por N de Sesiones  &  Tipo de cliente")
    row_y = pdf.get_y()
    _img_cell(pdf, img_packs, w=92, h=58); pdf.set_xy(106, row_y)
    _img_cell(pdf, img_tipo,  w=92, h=58); pdf.ln(62)

    section_title("Canales de venta  &  Ventas por Region")
    row_y = pdf.get_y()
    _img_cell(pdf, img_chan, w=92, h=62); pdf.set_xy(106, row_y)
    _img_cell(pdf, img_reg,  w=92, h=62); pdf.ln(66)

    # ── Página 2 ─────────────────────────────────────────────────────────────
    pdf.add_page()
    pg_bg()

    section_title("Top 10 Productos mas vendidos")
    _img_cell(pdf, img_prods, w=186, h=75); pdf.ln(78)

    section_title("Ingresos por Pack (CLP)")
    _img_cell(pdf, img_pack_rev, w=186, h=55); pdf.ln(58)

    section_title("Detalle top productos")
    table_header(["#","Producto","Unidades","Ingresos CLP"], [10,110,28,38])
    for i, (name, units) in enumerate(top10):
        table_row(
            [f"#{i+1}", name[:55], str(units), fmt_clp(m["prod_revenue"][name])],
            [10, 110, 28, 38], i, aligns=["L","L","C","R"]
        )

    # ── Página 3 ─────────────────────────────────────────────────────────────
    pdf.add_page()
    pg_bg()

    section_title(f"Detalle de ordenes por tipo de cliente  ({m['n_ordenes']} ordenes)")
    table_header(["Fecha","Nombre","Email","Total CLP","Tipo"], [22,44,62,30,28])
    pdf.set_font("Helvetica", "", 7)
    for i, row in enumerate(m["order_rows"]):
        table_row(
            [row["Fecha"], row["Nombre"][:28], row["Email"][:36],
             fmt_clp(row["Total"]), row["Tipo de cliente"]],
            [22, 44, 62, 30, 28], i,
            aligns=["L","L","L","R","C"],
            tipo=row["Tipo de cliente"]
        )

    # Footer
    pdf.set_y(-12)
    pdf.set_font("Helvetica", "", 7)
    pdf.set_text_color(*t["text_sub"])
    pdf.cell(0, 5, f"Lasertam Analytics  ·  {label_rango}  ·  WooCommerce REST API", align="C")

    return bytes(pdf.output())


def build_html(m: dict, label_rango: str) -> str:
    mn    = label_rango
    top10 = m["prod_units"].most_common(10)
    tchans = m["chan_orders"].most_common()
    tregs  = m["reg_orders"].most_common(10)

    jpl = json.dumps([f"{n} Sesión{'es' if n>1 else ''}" for n in [1,3,6,9]], ensure_ascii=False)
    jpd = json.dumps([m["pack_units"][n]    for n in [1,3,6,9]])
    jpr = json.dumps([int(m["pack_revenue"][n]) for n in [1,3,6,9]])
    jcl = json.dumps([c for c,_ in tchans], ensure_ascii=False)
    jcd = json.dumps([n for _,n in tchans])
    jol = json.dumps([n for n,_ in top10],  ensure_ascii=False)
    jod = json.dumps([u for _,u in top10])
    jrl = json.dumps([r for r,_ in tregs],  ensure_ascii=False)
    jrd = json.dumps([n for _,n in tregs])
    # New vs Returning para dona
    jnl = json.dumps(["New","Returning"], ensure_ascii=False)
    jnd = json.dumps([m["new_c"], m["ret_c"]])

    top_chan_name  = tchans[0][0] if tchans else "—"
    top_chan_count = tchans[0][1] if tchans else 0
    now_str = datetime.now(TZ_CL).strftime('%d/%m/%Y %H:%M')

    prod_table = "".join(
        f'<tr><td class="rank">#{i+1}</td><td>{name}</td>'
        f'<td class="num">{u:,}</td><td class="num">{fmt_clp(m["prod_revenue"][name])}</td></tr>'
        for i,(name,u) in enumerate(top10)
    )
    cli_rows = "".join(
        f'<tr><td>{r["Fecha"]}</td><td>{r["Nombre"]}</td><td>{r["Email"]}</td>'
        f'<td class="num">{fmt_clp(r["Total"])}</td>'
        f'<td class="tipo-{"new" if r["Tipo de cliente"]=="New" else "ret"}">'
        f'{r["Tipo de cliente"]}</td></tr>'
        for r in m["order_rows"]
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ventas {mn} {year} — Lasertam</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{{--p:#8b5cf6;--pl:#a78bfa;--cy:#06b6d4;--gr:#10b981;--am:#f59e0b;
  --bg:#0f0f1a;--card:#1a1a2e;--text:#e2e8f0;--mu:#94a3b8;--bo:#2d3748}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text)}}
.hdr{{background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);padding:2rem 3rem;
  border-bottom:2px solid var(--p);display:flex;align-items:center;
  justify-content:space-between;flex-wrap:wrap;gap:1rem}}
.hdr h1{{font-size:1.7rem;font-weight:700}}.hdr h1 span{{color:var(--pl)}}
.hdr p{{color:var(--mu);font-size:.85rem;margin-top:.2rem}}
.badge{{background:var(--p);color:#fff;padding:.3rem .9rem;border-radius:999px;font-size:.75rem;font-weight:600}}
.wrap{{max-width:1400px;margin:0 auto;padding:2rem 1.5rem}}
.kpis{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:1rem;margin-bottom:2rem}}
.kpi{{background:var(--card);border:1px solid var(--bo);border-radius:12px;padding:1.4rem;border-top:3px solid var(--p)}}
.kpi.c{{border-top-color:var(--cy)}}.kpi.g{{border-top-color:var(--gr)}}.kpi.a{{border-top-color:var(--am)}}
.kpi label{{font-size:.68rem;text-transform:uppercase;letter-spacing:.06em;color:var(--mu)}}
.kpi .val{{font-size:1.8rem;font-weight:700;margin:.35rem 0 .2rem;line-height:1.1}}
.kpi .sub{{font-size:.76rem;color:var(--mu)}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-bottom:1.5rem}}
@media(max-width:800px){{.g2{{grid-template-columns:1fr}}}}
.card{{background:var(--card);border:1px solid var(--bo);border-radius:12px;padding:1.5rem;margin-bottom:1.5rem}}
.card h2{{font-size:.95rem;font-weight:600;margin-bottom:1.1rem;color:var(--pl)}}
.card canvas{{max-height:280px}}
table{{width:100%;border-collapse:collapse;font-size:.82rem}}
th{{text-align:left;padding:.6rem 1rem;background:#1e2a3a;color:var(--mu);
  text-transform:uppercase;font-size:.67rem;letter-spacing:.05em}}
td{{padding:.55rem 1rem;border-bottom:1px solid var(--bo)}}
tr:hover td{{background:#1e293b}}
.rank{{color:var(--pl);font-weight:700}}.num{{font-weight:600}}
.tipo-new{{color:#10b981;font-weight:600}}.tipo-ret{{color:#8b5cf6;font-weight:600}}
.foot{{text-align:center;color:var(--mu);font-size:.72rem;padding:1.5rem;
  border-top:1px solid var(--bo);margin-top:1rem}}
</style>
</head>
<body>
<div class="hdr">
  <div>
    <p>LASERTAM · DEPILACIÓN LÁSER CHILE</p>
    <h1>Informe de Ventas — <span>{mn} {year}</span></h1>
    <p>Generado el {now_str} (Chile) · {m["n_ordenes"]} órdenes</p>
  </div>
  <span class="badge">WooCommerce API</span>
</div>

<div class="wrap">
  <div class="kpis">
    <div class="kpi">
      <label>Ventas totales</label>
      <div class="val">{fmt_clp(m["total_ventas"])}</div>
      <div class="sub">CLP · {m["n_ordenes"]} órdenes</div>
    </div>
    <div class="kpi c">
      <label>Ticket promedio</label>
      <div class="val">{fmt_clp(m["ticket_avg"])}</div>
      <div class="sub">por orden</div>
    </div>
    <div class="kpi g">
      <label>Clientes New</label>
      <div class="val">{m["new_c"]}</div>
      <div class="sub">primera compra</div>
    </div>
    <div class="kpi a">
      <label>Clientes Returning</label>
      <div class="val">{m["ret_c"]}</div>
      <div class="sub">compraron antes</div>
    </div>
    <div class="kpi" style="border-top-color:#ec4899">
      <label>Canal top</label>
      <div class="val" style="font-size:.95rem;word-break:break-word">{top_chan_name}</div>
      <div class="sub">{top_chan_count} órdenes</div>
    </div>
  </div>

  <div class="g2">
    <div class="card"><h2>Packs por N° Sesiones — Unidades</h2><canvas id="p1"></canvas></div>
    <div class="card"><h2>Tipo de cliente (New vs Returning)</h2><canvas id="p6"></canvas></div>
  </div>

  <div class="g2">
    <div class="card"><h2>Canales de venta</h2><canvas id="p2"></canvas></div>
    <div class="card"><h2>Ventas por Región</h2><canvas id="p4"></canvas></div>
  </div>

  <div class="card"><h2>Top 10 Productos más vendidos</h2><canvas id="p3" style="max-height:340px"></canvas></div>

  <div class="g2">
    <div class="card"><h2>Ingresos por Pack (CLP)</h2><canvas id="p5"></canvas></div>
    <div class="card"><h2>Detalle top productos</h2>
      <table>
        <thead><tr><th>#</th><th>Producto</th><th>Unidades</th><th>Ingresos</th></tr></thead>
        <tbody>{prod_table}</tbody>
      </table>
    </div>
  </div>

  <div class="card">
    <h2>Detalle de órdenes — Tipo de cliente</h2>
    <table>
      <thead><tr><th>Fecha</th><th>Nombre</th><th>Email</th><th>Total</th><th>Tipo</th></tr></thead>
      <tbody>{cli_rows}</tbody>
    </table>
  </div>
</div>

<div class="foot">Lasertam Analytics · {mn} {year} · WooCommerce REST API · America/Santiago</div>

<script>
const C=['#8b5cf6','#06b6d4','#10b981','#f59e0b','#ef4444','#ec4899','#6366f1','#84cc16','#f97316','#14b8a6'];
const ax={{ticks:{{color:'#94a3b8'}},grid:{{color:'#1e293b'}}}};
const base={{responsive:true,plugins:{{legend:{{labels:{{color:'#94a3b8'}}}}}},scales:{{x:ax,y:ax}}}};
const donut={{responsive:true,plugins:{{legend:{{position:'right',labels:{{color:'#94a3b8',boxWidth:12}}}}}}}};
const hbar={{responsive:true,indexAxis:'y',plugins:{{legend:{{labels:{{color:'#94a3b8'}}}}}},scales:{{x:ax,y:ax}}}};
new Chart(document.getElementById('p1'),{{type:'bar',data:{{labels:{jpl},datasets:[{{label:'Unidades',data:{jpd},backgroundColor:C.slice(0,4),borderRadius:6}}]}},options:base}});
new Chart(document.getElementById('p6'),{{type:'doughnut',data:{{labels:{jnl},datasets:[{{data:{jnd},backgroundColor:['#10b981','#8b5cf6'],borderWidth:2}}]}},options:donut}});
new Chart(document.getElementById('p2'),{{type:'doughnut',data:{{labels:{jcl},datasets:[{{data:{jcd},backgroundColor:C,borderWidth:2}}]}},options:donut}});
new Chart(document.getElementById('p4'),{{type:'bar',data:{{labels:{jrl},datasets:[{{label:'Órdenes',data:{jrd},backgroundColor:'#06b6d4',borderRadius:4}}]}},options:base}});
new Chart(document.getElementById('p3'),{{type:'bar',data:{{labels:{jol},datasets:[{{label:'Unidades',data:{jod},backgroundColor:'#8b5cf6',borderRadius:4}}]}},options:hbar}});
new Chart(document.getElementById('p5'),{{type:'bar',data:{{labels:{jpl},datasets:[{{label:'CLP',data:{jpr},backgroundColor:C.slice(0,4),borderRadius:6}}]}},options:base}});
</script>
</body></html>"""


# ── Streamlit UI ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="Ventas · Lasertam", page_icon="💜", layout="wide")

# ── Session state para fechas ─────────────────────────────────────────────────
from datetime import date, timedelta

now_cl = datetime.now(TZ_CL).date()
_first_of_month = now_cl.replace(day=1)

if "sel_tema" not in st.session_state:
    st.session_state.sel_tema = "Oscuro"
if "d_from" not in st.session_state:
    st.session_state.d_from = _first_of_month
if "d_to" not in st.session_state:
    st.session_state.d_to = now_cl

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 16px 0;border-bottom:1px solid #1e293b;margin-bottom:8px;">
        <div style="font-size:18px;font-weight:800;color:#f1f5f9;letter-spacing:-0.5px;">PrecioSpy</div>
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#0ea5e9;font-weight:600;">Analytics &amp; Competencia</div>
    </div>
    """, unsafe_allow_html=True)
    st.subheader("Rango de fechas")

    # Presets
    st.caption("Preajustes")
    p1, p2 = st.columns(2)
    with p1:
        if st.button("Este mes",      use_container_width=True):
            st.session_state.d_from = now_cl.replace(day=1)
            st.session_state.d_to   = now_cl
            st.rerun()
        if st.button("Este año",      use_container_width=True):
            st.session_state.d_from = now_cl.replace(month=1, day=1)
            st.session_state.d_to   = now_cl
            st.rerun()
        if st.button("Últimos 7 días", use_container_width=True):
            st.session_state.d_from = now_cl - timedelta(days=6)
            st.session_state.d_to   = now_cl
            st.rerun()
    with p2:
        if st.button("Mes anterior",  use_container_width=True):
            first = now_cl.replace(day=1)
            last_month_end   = first - timedelta(days=1)
            last_month_start = last_month_end.replace(day=1)
            st.session_state.d_from = last_month_start
            st.session_state.d_to   = last_month_end
            st.rerun()
        if st.button("Últimos 30 días", use_container_width=True):
            st.session_state.d_from = now_cl - timedelta(days=29)
            st.session_state.d_to   = now_cl
            st.rerun()
        if st.button("Últimos 90 días", use_container_width=True):
            st.session_state.d_from = now_cl - timedelta(days=89)
            st.session_state.d_to   = now_cl
            st.rerun()

    # Selector personalizado
    st.caption("Personalizado")
    sel_range = st.date_input(
        "Desde / Hasta",
        value=(st.session_state.d_from, st.session_state.d_to),
        min_value=date(2022, 1, 1),
        max_value=now_cl,
        format="DD/MM/YYYY",
        label_visibility="collapsed",
    )
    if isinstance(sel_range, (list, tuple)) and len(sel_range) == 2:
        st.session_state.d_from, st.session_state.d_to = sel_range[0], sel_range[1]

    d_from = st.session_state.d_from
    d_to   = st.session_state.d_to

    st.markdown("---")
    run = st.button("Cargar informe", type="primary", use_container_width=True)
    st.caption("Zona horaria: America/Santiago · Cache 30 min")

    st.markdown("---")
    st.subheader("Tema de colores")
    st.radio(
        "Tema",
        options=["Oscuro", "Claro", "Blanco y Negro"],
        key="sel_tema",
        label_visibility="collapsed",
    )

# ── Tema del dashboard ────────────────────────────────────────────────────────
sel_tema = st.session_state.sel_tema

_DASH_CSS = {
    "Oscuro": """
<style>
/* ── Fondo ── */
.stApp,[data-testid="stAppViewContainer"],[data-testid="stHeader"]
  {background-color:#0f0f1a !important;}
section[data-testid="stSidebar"]{background-color:#0a0a14 !important;}

/* ── Navegación app / ventas ── */
[data-testid="stSidebarNav"]{background:transparent !important;padding:.25rem 0 .5rem 0 !important;}
[data-testid="stSidebarNav"] a{color:#475569 !important;border-radius:8px !important;
  padding:6px 12px !important;font-size:13px !important;font-weight:500 !important;}
[data-testid="stSidebarNav"] a:hover{background:#1e293b !important;color:#94a3b8 !important;}
[data-testid="stSidebarNav"] a[aria-current="page"]{
  background:rgba(139,92,246,.18) !important;color:#a78bfa !important;font-weight:600 !important;}
[data-testid="stSidebarNav"] a span{color:inherit !important;}

/* ── Botones sidebar (presets de fecha) ── */
section[data-testid="stSidebar"] .stButton>button{
  background:#131c2e !important;color:#94a3b8 !important;
  border:1px solid #1e293b !important;border-radius:8px !important;
  font-size:12px !important;font-weight:500 !important;}
section[data-testid="stSidebar"] .stButton>button:hover{
  background:#1e293b !important;color:#cbd5e1 !important;border-color:#334155 !important;}
section[data-testid="stSidebar"] button[kind="primary"]{
  background:#7c3aed !important;color:#fff !important;border:none !important;
  font-size:14px !important;font-weight:600 !important;}
section[data-testid="stSidebar"] button[kind="primary"]:hover{background:#6d28d9 !important;}

/* ── Date input ── */
section[data-testid="stSidebar"] .stDateInput input{
  background:#131c2e !important;color:#e2e8f0 !important;border-color:#1e293b !important;}

/* ── Texto sidebar ── */
section[data-testid="stSidebar"] .stMarkdown p,
section[data-testid="stSidebar"] .stCaption,
section[data-testid="stSidebar"] small{color:#475569 !important;}
section[data-testid="stSidebar"] h2,section[data-testid="stSidebar"] h3{color:#e2e8f0 !important;}
section[data-testid="stSidebar"] hr{border-color:#1e293b !important;}
section[data-testid="stSidebar"] .stRadio label{color:#94a3b8 !important;}

/* ── Métricas ── */
div[data-testid="metric-container"]{
  background:#1a1a2e !important;border:1px solid #1e293b !important;
  border-radius:12px;padding:1rem 1.2rem;border-top:3px solid #8b5cf6 !important;}
[data-testid="stMetricValue"]{color:#e2e8f0 !important;}
[data-testid="stMetricLabel"]{color:#64748b !important;}

/* ── Texto principal ── */
h1,h2,h3,.stMarkdown h1,.stMarkdown h2,.stMarkdown h3{color:#e2e8f0 !important;}
p,.stMarkdown p,.stText{color:#94a3b8 !important;}
label,.stSelectbox label,.stMultiSelect label{color:#94a3b8 !important;}
hr{border-color:#1e293b !important;}

/* ── Tabs del informe ── */
.stTabs [data-baseweb="tab-list"]{background:transparent !important;border-bottom:2px solid #1e293b !important;}
.stTabs [data-baseweb="tab"]{background:transparent !important;color:#475569 !important;}
.stTabs [data-baseweb="tab"]:hover{color:#94a3b8 !important;background:transparent !important;}
.stTabs [aria-selected="true"]{color:#a78bfa !important;border-bottom:2px solid #8b5cf6 !important;background:transparent !important;}
.stTabs [data-baseweb="tab-panel"]{background:transparent !important;}

/* ── Dataframes ── */
[data-testid="stDataFrame"],[data-testid="stDataFrame"] *{background:#111827 !important;}
[data-testid="stDataFrame"] th{background:#0d1117 !important;color:#64748b !important;}
[data-testid="stDataFrame"] td{color:#94a3b8 !important;}
.dvn-scroller{background:#111827 !important;}

/* ── Selectbox / Multiselect ── */
[data-baseweb="select"] [data-baseweb="popover"]{background:#1e293b !important;}
[data-testid="stMultiSelect"] [data-baseweb="tag"]{background:#312e81 !important;color:#c4b5fd !important;}
[data-baseweb="select"] input{color:#e2e8f0 !important;}

/* ── Info / Warning boxes ── */
[data-testid="stAlert"]{background:#1e293b !important;border-color:#334155 !important;}
[data-testid="stAlert"] p{color:#94a3b8 !important;}

/* ── Spinner ── */
[data-testid="stSpinner"] p{color:#94a3b8 !important;}

/* ── Divider ── */
[data-testid="stHorizontalBlock"] hr{border-color:#1e293b !important;}

/* ── Gráficos Plotly: fondo transparente ── */
[data-testid="stPlotlyChart"]{background:transparent !important;}
[data-testid="stPlotlyChart"]>div{background:transparent !important;}
[data-testid="stPlotlyChart"] .svg-container{background:transparent !important;}

/* ── Ocultar menú Streamlit ── */
#MainMenu,footer,[data-testid="stDecoration"]{visibility:hidden;}
</style>""",
    "Claro": """
<style>
.stApp,[data-testid="stAppViewContainer"],[data-testid="stHeader"]
  {background-color:#f8f8ff !important;}
section[data-testid="stSidebar"]{background-color:#ede9fe !important;}
div[data-testid="metric-container"]{
  background:#fff !important;border:1px solid #c4b5fd !important;
  border-radius:12px;padding:1rem 1.2rem;border-top:3px solid #8b5cf6 !important;}
#MainMenu,footer,[data-testid="stDecoration"]{visibility:hidden;}
</style>""",
    "Blanco y Negro": """
<style>
.stApp,[data-testid="stAppViewContainer"],[data-testid="stHeader"]
  {background-color:#fff !important;}
section[data-testid="stSidebar"]{background-color:#f0f0f0 !important;}
div[data-testid="metric-container"]{
  background:#f0f0f0 !important;border:1px solid #999 !important;
  border-radius:12px;padding:1rem 1.2rem;border-top:3px solid #333 !important;}
#MainMenu,footer,[data-testid="stDecoration"]{visibility:hidden;}
</style>""",
}
st.markdown(_DASH_CSS[sel_tema], unsafe_allow_html=True)

plotly_tpl = "plotly_dark" if sel_tema == "Oscuro" else "plotly_white"
_pbg = {"paper_bgcolor": "rgba(0,0,0,0)", "plot_bgcolor": "rgba(0,0,0,0)"} if sel_tema == "Oscuro" else {}

# ── Título ────────────────────────────────────────────────────────────────────
label_rango = f"{d_from.strftime('%d %b %Y')} → {d_to.strftime('%d %b %Y')}"
st.title(f"Informe de Ventas — {label_rango}")

if not run:
    st.info("Selecciona el rango de fechas y haz clic en **Cargar informe**.")
    st.stop()

# ── Carga de datos ────────────────────────────────────────────────────────────
after_iso, before_iso = dates_to_iso(d_from, d_to)

with st.spinner(f"Descargando órdenes {label_rango}..."):
    try:
        orders = fetch_orders(after_iso, before_iso)
    except Exception as e:
        st.error(f"Error al conectar con WooCommerce: {e}")
        st.stop()

if not orders:
    st.warning("No hay órdenes completadas o en proceso para este período.")
    st.stop()

with st.spinner("Analizando clientes (New vs Returning)..."):
    m = compute_metrics(orders, after_iso)

# ── KPIs ──────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Ventas totales",  fmt_clp(m["total_ventas"]))
k2.metric("Ticket promedio", fmt_clp(m["ticket_avg"]))
k3.metric("Total órdenes",   m["n_ordenes"])
k4.metric("New",             m["new_c"],  help="Primera compra (por email)")
k5.metric("Returning",       m["ret_c"],  help="Ya compraron antes (por email)")

st.markdown("---")

# ── Packs + Tipo de cliente ───────────────────────────────────────────────────
col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Packs por N° Sesiones")
    pack_df = pd.DataFrame({
        "Pack":     [f"{n} Sesión{'es' if n>1 else ''}" for n in [1,3,6,9]],
        "Unidades": [m["pack_units"][n]        for n in [1,3,6,9]],
        "Ingresos": [int(m["pack_revenue"][n]) for n in [1,3,6,9]],
    })
    t1, t2 = st.tabs(["Unidades", "Ingresos CLP"])
    with t1:
        fig = px.bar(pack_df, x="Pack", y="Unidades", color="Pack",
                     color_discrete_sequence=COLORS, template=plotly_tpl)
        fig.update_layout(showlegend=False, margin=dict(t=10), **_pbg)
        st.plotly_chart(fig, use_container_width=True)
    with t2:
        fig = px.bar(pack_df, x="Pack", y="Ingresos", color="Pack",
                     color_discrete_sequence=COLORS, template=plotly_tpl)
        fig.update_layout(showlegend=False, margin=dict(t=10), **_pbg)
        st.plotly_chart(fig, use_container_width=True)

with col_b:
    st.subheader("Tipo de cliente")
    cli_df = pd.DataFrame({
        "Tipo":    ["New", "Returning"],
        "Órdenes": [m["new_c"], m["ret_c"]],
    })
    fig = px.pie(cli_df, names="Tipo", values="Órdenes", hole=0.5,
                 color_discrete_sequence=["#10b981","#8b5cf6"],
                 template=plotly_tpl)
    fig.update_layout(margin=dict(t=10), **_pbg)
    st.plotly_chart(fig, use_container_width=True)

# ── Canales + Regiones ────────────────────────────────────────────────────────
col_c, col_d = st.columns(2)

with col_c:
    st.subheader("Canales de venta")
    chans = m["chan_orders"].most_common()
    if chans:
        fig = px.pie(pd.DataFrame(chans, columns=["Canal","Órdenes"]),
                     names="Canal", values="Órdenes", hole=0.4,
                     color_discrete_sequence=COLORS, template=plotly_tpl)
        fig.update_layout(margin=dict(t=10))
        st.plotly_chart(fig, use_container_width=True)

with col_d:
    st.subheader("Ventas por Región")
    regs = m["reg_orders"].most_common(10)
    if regs:
        reg_df = pd.DataFrame(regs, columns=["Región","Órdenes"])
        fig = px.bar(reg_df, x="Órdenes", y="Región", orientation="h",
                     color_discrete_sequence=["#06b6d4"], template=plotly_tpl)
        fig.update_layout(margin=dict(t=10), yaxis={"categoryorder":"total ascending"}, **_pbg)
        st.plotly_chart(fig, use_container_width=True)

# ── Top productos ─────────────────────────────────────────────────────────────
st.subheader("Top 10 Productos más vendidos")
top10 = m["prod_units"].most_common(10)
if top10:
    prod_df = pd.DataFrame([
        {"Producto": n, "Unidades": u, "Ingresos CLP": int(m["prod_revenue"][n])}
        for n,u in top10
    ])
    tu, tr, tt = st.tabs(["Unidades", "Ingresos CLP", "Tabla"])
    with tu:
        fig = px.bar(prod_df, x="Unidades", y="Producto", orientation="h",
                     color_discrete_sequence=["#8b5cf6"], template=plotly_tpl)
        fig.update_layout(margin=dict(t=10), yaxis={"categoryorder":"total ascending"}, **_pbg)
        st.plotly_chart(fig, use_container_width=True)
    with tr:
        fig = px.bar(prod_df, x="Ingresos CLP", y="Producto", orientation="h",
                     color_discrete_sequence=["#06b6d4"], template=plotly_tpl)
        fig.update_layout(margin=dict(t=10), yaxis={"categoryorder":"total ascending"}, **_pbg)
        st.plotly_chart(fig, use_container_width=True)
    with tt:
        show = prod_df.copy()
        show["Ingresos CLP"] = show["Ingresos CLP"].apply(fmt_clp)
        st.dataframe(show, use_container_width=True, hide_index=True)

st.markdown("---")

# ── Tabla New vs Returning ────────────────────────────────────────────────────
st.subheader("Órdenes por tipo de cliente")

orders_df = pd.DataFrame(m["order_rows"])

col_f1, col_f2 = st.columns([1, 3])
with col_f1:
    tipo_filter = st.multiselect(
        "Filtrar",
        options=["New", "Returning"],
        default=["New", "Returning"],
    )

filtered = orders_df[orders_df["Tipo de cliente"].isin(tipo_filter)].copy()
filtered["Total"] = filtered["Total"].apply(fmt_clp)

def _color_tipo(val):
    return "color: #10b981; font-weight:600" if val == "New" else "color: #8b5cf6; font-weight:600"

st.dataframe(
    filtered.style.map(_color_tipo, subset=["Tipo de cliente"]),
    use_container_width=True,
    hide_index=True,
)

# ── Exportar ──────────────────────────────────────────────────────────────────
st.markdown("---")
with st.spinner("Generando PDF..."):
    pdf_bytes = build_pdf(m, label_rango, theme_name=sel_tema)

fname = f"informe_ventas_lasertam_{d_from}_{d_to}.pdf"
st.download_button(
    label="⬇ Descargar informe PDF",
    data=pdf_bytes,
    file_name=fname,
    mime="application/pdf",
    type="primary",
    use_container_width=True,
)
