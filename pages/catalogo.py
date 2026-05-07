"""
Catálogo de Servicios Lasertam
Todos los productos activos publicados en lasertam.com (WooCommerce API)
"""

import re
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import streamlit as st
from requests.auth import HTTPBasicAuth

# ── Config ─────────────────────────────────────────────────────────────────────
WC_URL = "https://lasertam.com/wp-json/wc/v3"
AUTH   = HTTPBasicAuth(
    "ck_d54578316281a23fbd19223eba88970dd29e5459",
    "cs_4adb80948e5e0513d7fb0e1f05206e301e419430",
)
TZ_CL  = ZoneInfo("America/Santiago")

SESSION_RE = re.compile(r'(\d+)\s*sesi', re.I)

DARK_CSS = """
<style>
.stApp,[data-testid="stAppViewContainer"],[data-testid="stHeader"]
  {background-color:#0f0f1a !important;}
section[data-testid="stSidebar"]{background-color:#0a0a14 !important;}
[data-testid="stSidebarNav"]{background:transparent !important;padding:.25rem 0 .5rem 0 !important;}
[data-testid="stSidebarNav"] a{color:#475569 !important;border-radius:8px !important;
  padding:6px 12px !important;font-size:13px !important;font-weight:500 !important;}
[data-testid="stSidebarNav"] a:hover{background:#1e293b !important;color:#94a3b8 !important;}
[data-testid="stSidebarNav"] a[aria-current="page"]{
  background:rgba(139,92,246,.18) !important;color:#a78bfa !important;font-weight:600 !important;}
section[data-testid="stSidebar"] hr{border-color:#1e293b !important;}
section[data-testid="stSidebar"] .stMarkdown p,
section[data-testid="stSidebar"] .stCaption{color:#475569 !important;}
section[data-testid="stSidebar"] .stRadio label{color:#94a3b8 !important;}
section[data-testid="stSidebar"] h2,section[data-testid="stSidebar"] h3{color:#e2e8f0 !important;}
div[data-testid="metric-container"]{background:#1a1a2e !important;border:1px solid #1e293b !important;
  border-radius:12px;padding:1rem 1.2rem;border-top:3px solid #8b5cf6 !important;}
[data-testid="stMetricValue"]{color:#e2e8f0 !important;}
[data-testid="stMetricLabel"]{color:#64748b !important;}
h1,h2,h3,.stMarkdown h1,.stMarkdown h2{color:#e2e8f0 !important;}
p,.stMarkdown p{color:#94a3b8 !important;}
label{color:#94a3b8 !important;}
hr{border-color:#1e293b !important;}
[data-testid="stDataFrame"],[data-testid="stDataFrame"] *{background:#111827 !important;}
[data-testid="stDataFrame"] th{background:#0d1117 !important;color:#64748b !important;}
[data-testid="stDataFrame"] td{color:#94a3b8 !important;}
[data-testid="stAlert"]{background:#1e293b !important;border-color:#334155 !important;}
[data-testid="stAlert"] p{color:#94a3b8 !important;}
[data-testid="stPlotlyChart"]{background:transparent !important;}
.stTabs [data-baseweb="tab-list"]{background:transparent !important;border-bottom:2px solid #1e293b !important;}
.stTabs [data-baseweb="tab"]{background:transparent !important;color:#475569 !important;}
.stTabs [aria-selected="true"]{color:#a78bfa !important;border-bottom:2px solid #8b5cf6 !important;background:transparent !important;}
#MainMenu,footer,[data-testid="stDecoration"]{visibility:hidden;}
</style>
"""

# ── Helpers ─────────────────────────────────────────────────────────────────────
def fmt_clp(n) -> str:
    try:
        v = int(float(n))
        return "$" + f"{v:,}".replace(",", ".") if v > 0 else "—"
    except Exception:
        return "—"

def detect_sessions(name: str) -> int | None:
    m = SESSION_RE.search(name)
    return int(m.group(1)) if m else None

def pct_desc(price, regular) -> str:
    try:
        p, r = float(price), float(regular)
        if r > p > 0:
            return f"-{int((1 - p/r)*100)}%"
    except Exception:
        pass
    return ""

# ── API ─────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_products() -> list:
    items, page = [], 1
    while True:
        r = requests.get(f"{WC_URL}/products", auth=AUTH, timeout=30, params={
            "per_page": 100, "page": page, "status": "publish",
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

# ── Page config ─────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Catálogo · Lasertam", page_icon="💜", layout="wide")

# ── Session state ───────────────────────────────────────────────────────────────
if "sel_tema" not in st.session_state:
    st.session_state.sel_tema = "Oscuro"

# ── Sidebar ─────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 16px 0;border-bottom:1px solid #1e293b;margin-bottom:8px;">
        <div style="font-size:18px;font-weight:800;color:#f1f5f9;letter-spacing:-0.5px;">PrecioSpy</div>
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#0ea5e9;font-weight:600;">Analytics &amp; Competencia</div>
    </div>
    """, unsafe_allow_html=True)

    st.divider()
    st.caption("**Tema de colores**")
    st.radio("Tema", ["Oscuro", "Claro", "Blanco y Negro"],
             key="sel_tema", label_visibility="collapsed")

# ── CSS ─────────────────────────────────────────────────────────────────────────
if st.session_state.sel_tema == "Oscuro":
    st.markdown(DARK_CSS, unsafe_allow_html=True)

# ── Carga ───────────────────────────────────────────────────────────────────────
st.title("Catálogo de Servicios — Lasertam")
st.caption("Productos activos publicados en lasertam.com · Caché 30 min")

with st.spinner("Cargando productos desde lasertam.com..."):
    try:
        products = fetch_products()
    except Exception as e:
        st.error(f"Error al conectar con WooCommerce: {e}")
        st.stop()

if not products:
    st.warning("No se encontraron productos publicados.")
    st.stop()

# ── Construir DataFrame ──────────────────────────────────────────────────────────
rows = []
for p in products:
    name       = p.get("name", "")
    price      = p.get("price", "0") or "0"
    reg_price  = p.get("regular_price", "") or ""
    on_sale    = bool(p.get("on_sale", False))
    ptype      = p.get("type", "simple")
    cats       = [c["name"] for c in p.get("categories", [])]
    permalink  = p.get("permalink", "")
    sessions   = detect_sessions(name)
    desc_pct   = pct_desc(price, reg_price) if on_sale else ""

    rows.append({
        "Nombre":         name,
        "Sesiones":       sessions,
        "Precio":         fmt_clp(price),
        "Normal":         fmt_clp(reg_price) if on_sale else "—",
        "Dcto.":          desc_pct,
        "En oferta":      "Sí ✦" if on_sale else "No",
        "Tipo":           "Variable" if ptype == "variable" else "Simple",
        "Categorías":     ", ".join(cats) if cats else "—",
        "Ver online":     permalink,
        # para filtros internos:
        "_sessions":      sessions,
        "_on_sale":       on_sale,
        "_price":         float(price) if price else 0,
        "_cats":          cats,
    })

df = pd.DataFrame(rows)

# ── KPIs ─────────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Productos activos",  len(df))
k2.metric("En oferta",          int(df["_on_sale"].sum()))
k3.metric("Precio promedio",    fmt_clp(df["_price"].replace(0, pd.NA).mean()))
k4.metric("Packs 6 sesiones",   int((df["_sessions"] == 6).sum()))
k5.metric("Packs 9 sesiones",   int((df["_sessions"] == 9).sum()))

st.divider()

# ── Filtros ──────────────────────────────────────────────────────────────────────
fc1, fc2, fc3, fc4 = st.columns([3, 1, 1, 1])

with fc1:
    search = st.text_input("Buscar", placeholder="ej: axilas, piernas, rostro, pack…",
                           label_visibility="collapsed")
with fc2:
    sess_vals = sorted({int(s) for s in df["_sessions"].dropna()})
    sess_sel  = st.selectbox("Sesiones", ["Todas"] + sess_vals,
        format_func=lambda x: "Todas las sesiones" if x == "Todas"
                              else f"{x} sesión" if x == 1 else f"{x} sesiones",
        label_visibility="collapsed")
with fc3:
    oferta_sel = st.selectbox("Estado", ["Todos", "En oferta", "Precio normal"],
                              label_visibility="collapsed")
with fc4:
    if st.button("Limpiar filtros", use_container_width=True):
        st.rerun()

# ── Tabs de vista ────────────────────────────────────────────────────────────────
tab_todos, tab_packs, tab_zonas = st.tabs(["Todos", "Por N° de sesiones", "Por categoría"])

def apply_filters(data: pd.DataFrame) -> pd.DataFrame:
    out = data.copy()
    if search:
        out = out[out["Nombre"].str.contains(search, case=False, na=False)]
    if sess_sel != "Todas":
        out = out[out["_sessions"] == int(sess_sel)]
    if oferta_sel == "En oferta":
        out = out[out["_on_sale"]]
    elif oferta_sel == "Precio normal":
        out = out[~out["_on_sale"]]
    return out

DISPLAY_COLS = ["Nombre", "Sesiones", "Precio", "Normal", "Dcto.", "En oferta", "Categorías"]

def show_table(data: pd.DataFrame):
    if data.empty:
        st.info("No hay productos con estos filtros.")
        return
    show = data[DISPLAY_COLS].copy()
    show["Sesiones"] = show["Sesiones"].apply(
        lambda x: f"{int(x)} ses." if pd.notna(x) and x else "—")
    st.caption(f"{len(show)} productos")
    st.dataframe(show, use_container_width=True, hide_index=True)

# ── Tab 1: Todos ─────────────────────────────────────────────────────────────────
with tab_todos:
    show_table(apply_filters(df))

# ── Tab 2: Por N° de sesiones ────────────────────────────────────────────────────
with tab_packs:
    filtered_packs = apply_filters(df)
    sin_sesiones   = filtered_packs[filtered_packs["_sessions"].isna()]
    con_sesiones   = filtered_packs[filtered_packs["_sessions"].notna()]

    for n_ses in sorted(con_sesiones["_sessions"].dropna().unique()):
        grupo = con_sesiones[con_sesiones["_sessions"] == n_ses]
        lbl   = "1 Sesión" if n_ses == 1 else f"{int(n_ses)} Sesiones"
        with st.expander(f"**Pack {lbl}** — {len(grupo)} productos", expanded=(n_ses in (6, 9))):
            show_table(grupo)

    if not sin_sesiones.empty:
        with st.expander(f"**Sin sesión definida** — {len(sin_sesiones)} productos"):
            show_table(sin_sesiones)

# ── Tab 3: Por categoría ─────────────────────────────────────────────────────────
with tab_zonas:
    filtered_cats = apply_filters(df)
    all_cats = sorted({c for cats in filtered_cats["_cats"] for c in cats})

    if not all_cats:
        st.info("Sin categorías disponibles con estos filtros.")
    else:
        for cat in all_cats:
            grupo = filtered_cats[filtered_cats["_cats"].apply(lambda x: cat in x)]
            if grupo.empty:
                continue
            with st.expander(f"**{cat}** — {len(grupo)} productos", expanded=False):
                show_table(grupo)
