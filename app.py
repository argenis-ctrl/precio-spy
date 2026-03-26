"""
Dashboard de Monitoreo de Precios - Depilación Láser
Lasertam vs. Competencia | Chile
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from db.models import DB_PATH, init_db

# ── Cupones conocidos ──────────────────────────────────────────────────────
_COUPONS_PATH = Path(__file__).parent / "coupons.json"

def load_coupons() -> dict:
    """Retorna {competitor: best_discount_pct} para aplicar al precio."""
    if not _COUPONS_PATH.exists():
        return {}
    try:
        data = json.loads(_COUPONS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    result = {}
    for competitor, coupons in data.items():
        best = 0
        for c in coupons:
            if c.get("type") == "pct":
                best = max(best, c["value"])
        if best > 0:
            result[competitor] = best
    return result

COUPONS = load_coupons()  # {"Cela": 20, ...}

# ── Configuración de página ────────────────────────────────────────────────
st.set_page_config(
    page_title="PrecioSpy · Depilación Láser",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded",
)

COMPETITOR_COLORS = {
    "Belenus":      "#ff7f0e",
    "Cela":         "#2ca02c",
    "Bellmeclinic": "#d62728",
    "Lasertam":     "#1f77b4",
}

COMPANIES_ORDER = ["Lasertam", "Belenus", "Cela", "Bellmeclinic"]
CO_SHORT  = {"Lasertam": "Lasertam", "Belenus": "Belenus", "Cela": "Cela", "Bellmeclinic": "Bellme."}
CO_HEADER = {"Lasertam": "#dbeafe", "Belenus": "#fff3e0", "Cela": "#e8f5e9", "Bellmeclinic": "#fce4ec"}
CO_TEXT   = {"Lasertam": "#1e40af", "Belenus": "#e65100", "Cela": "#1b5e20", "Bellmeclinic": "#880e4f"}

# ── Helpers de DB ──────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    init_db()
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def run_query(sql: str, params=()) -> pd.DataFrame:
    conn = get_conn()
    return pd.read_sql_query(sql, conn, params=params)


# ── Carga de datos ─────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_latest_prices(gender_filter: str, active_coupons: tuple = ()) -> pd.DataFrame:
    """active_coupons: tuple de (competitor, pct) para los cupones activados."""
    gender_clause = "AND pr.gender = ?" if gender_filter != "Todos" else ""
    q = f"""
    SELECT
        c.name                  AS competitor,
        c.is_self,
        pr.zone_name,
        pr.gender,
        pr.sessions,
        MIN(pr.price)           AS price,
        MIN(pr.original_price)  AS original_price,
        MAX(pr.discount_pct)    AS discount_pct,
        MAX(pr.scraped_at)      AS scraped_at
    FROM price_records pr
    JOIN competitors c ON c.id = pr.competitor_id
    WHERE pr.run_id = (
        SELECT run_id FROM price_records pr2
        WHERE pr2.competitor_id = pr.competitor_id
        ORDER BY scraped_at DESC LIMIT 1
    )
    {gender_clause}
    GROUP BY c.name, pr.zone_name, pr.gender, pr.sessions
    """
    params = []
    if gender_filter != "Todos":
        params.append(gender_filter[0])
    df = run_query(q, params)

    # Aplicar solo los cupones que el usuario tiene activados
    active_map = dict(active_coupons)
    if not df.empty and active_map:
        for competitor, pct in active_map.items():
            mask = df["competitor"] == competitor
            if not mask.any():
                continue
            factor = 1 - pct / 100
            # Si no había precio original, el precio listado pasa a ser el "normal"
            no_orig = mask & df["original_price"].isna()
            df.loc[no_orig, "original_price"] = df.loc[no_orig, "price"]
            # Precio con cupón
            df.loc[mask, "price"] = (df.loc[mask, "price"] * factor).round(0).astype(int)
            # Recalcular descuento total (precio final vs precio normal)
            df.loc[mask, "discount_pct"] = (
                (1 - df.loc[mask, "price"] / df.loc[mask, "original_price"]) * 100
            ).round(1)
            df.loc[mask, "has_coupon"] = True

    if "has_coupon" not in df.columns:
        df["has_coupon"] = False

    return df


@st.cache_data(ttl=300)
def load_price_history(zone: str, gender: str, sessions) -> pd.DataFrame:
    q = """
    SELECT c.name AS competitor, pr.price, pr.sessions, DATE(pr.scraped_at) AS fecha
    FROM price_records pr
    JOIN competitors c ON c.id = pr.competitor_id
    WHERE pr.zone_name = ? AND pr.gender = ?
    """
    params = [zone, gender]
    if sessions:
        q += " AND pr.sessions = ?"
        params.append(sessions)
    q += " ORDER BY pr.scraped_at"
    return run_query(q, params)


@st.cache_data(ttl=300)
def load_scrape_dates() -> pd.DataFrame:
    return run_query("""
        SELECT c.name, MAX(pr.scraped_at) AS ultimo_scrape
        FROM price_records pr
        JOIN competitors c ON c.id = pr.competitor_id
        GROUP BY c.name
    """)


# ── Utilidades de formato ──────────────────────────────────────────────────

def fmt_clp(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"${int(val):,}".replace(",", ".")


# ── Tabla HTML comparativa ─────────────────────────────────────────────────

TABLE_CSS = """
<style>
.cmp-wrap { overflow-x: auto; margin-top: 8px; }
.cmp-table {
    border-collapse: collapse;
    width: 100%;
    font-size: 12.5px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
.cmp-table th, .cmp-table td {
    border: 1px solid #e5e7eb;
    padding: 6px 8px;
    white-space: nowrap;
    text-align: center;
    vertical-align: middle;
}
.cmp-table .zone-col {
    text-align: left;
    font-weight: 600;
    min-width: 150px;
    background: #f9fafb;
    position: sticky;
    left: 0;
    z-index: 1;
    border-right: 2px solid #d1d5db;
}
.cmp-table .ses-hdr {
    background: #f3f4f6;
    font-weight: 700;
    font-size: 13px;
    border-bottom: 2px solid #d1d5db;
    padding: 8px 4px;
}
.co-hdr {
    font-weight: 700;
    font-size: 11px;
    padding: 4px 6px !important;
    border-bottom: 2px solid;
}
.cmp-table .price-cell { min-width: 95px; padding: 5px 8px; }
.cmp-table .no-data { color: #d1d5db; font-size: 11px; }
.orig { text-decoration: line-through; color: #9ca3af; font-size: 10.5px; display: block; line-height: 1.2; }
.offer { font-weight: 700; font-size: 13.5px; color: #111827; display: block; line-height: 1.4; }
.disc-badge {
    display: inline-block;
    background: #dcfce7;
    color: #15803d;
    border-radius: 4px;
    padding: 0px 5px;
    font-size: 10px;
    font-weight: 700;
    margin-top: 1px;
}
.coupon-badge {
    display: inline-block;
    background: #fef9c3;
    color: #854d0e;
    border-radius: 4px;
    padding: 0px 5px;
    font-size: 9px;
    font-weight: 700;
    margin-left: 2px;
    border: 1px solid #fde68a;
}
.cheapest { background: #f0fdf4 !important; }
.most-exp { background: #fff1f2 !important; }
.zebra { background: #fafafa; }
.zone-head-row th { border-top: 3px solid #e5e7eb; }
</style>
"""


def build_comparison_table(df: pd.DataFrame, sessions_list: list, search: str = "") -> str:
    """Genera tabla HTML comparativa zona × empresa × sesiones."""

    companies = [c for c in COMPANIES_ORDER if c in df["competitor"].values]
    if not companies:
        return "<p>Sin datos.</p>"

    # Lookup rápido: (zone, company, sessions) → dict
    lookup: dict = {}
    for _, r in df.iterrows():
        ses = int(r["sessions"]) if pd.notna(r["sessions"]) else None
        key = (r["zone_name"], r["competitor"], ses)
        cur = lookup.get(key)
        if cur is None or r["price"] < cur["price"]:
            lookup[key] = {
                "price":      r["price"],
                "original":   r["original_price"] if pd.notna(r.get("original_price")) else None,
                "discount":   r["discount_pct"]   if pd.notna(r.get("discount_pct"))   else None,
                "has_coupon": bool(r.get("has_coupon", False)),
            }

    # Filtrar y ordenar zonas
    zones = sorted(df["zone_name"].unique())
    if search:
        zones = [z for z in zones if search.lower() in z.lower()]

    def make_cell(zone, company, ses):
        d = lookup.get((zone, company, ses))
        if not d:
            return '<td class="no-data price-cell">—</td>'

        price      = d["price"]
        orig       = d["original"]
        disc       = d["discount"]
        has_coupon = d.get("has_coupon", False)

        # Hallar si es el más barato o el más caro en esta zona×sesión
        prices_here = [lookup[(zone, co, ses)]["price"]
                       for co in companies if (zone, co, ses) in lookup]
        is_cheapest  = prices_here and price == min(prices_here)
        is_most_exp  = prices_here and price == max(prices_here) and len(prices_here) > 1

        cell_class = "price-cell cheapest" if is_cheapest else (
                     "price-cell most-exp" if is_most_exp else "price-cell")

        inner = ""
        if orig and orig > price:
            inner += f'<span class="orig">{fmt_clp(orig)}</span>'
        inner += f'<span class="offer">{fmt_clp(price)}</span>'
        if disc and disc > 0:
            inner += f'<span class="disc-badge">-{int(disc)}%</span>'
        if has_coupon:
            inner += f'<span class="coupon-badge">🏷 cupón</span>'

        return f'<td class="{cell_class}">{inner}</td>'

    # ── Construir HTML ──────────────────────────────────────────────────────
    html = TABLE_CSS + '<div class="cmp-wrap"><table class="cmp-table">'

    # Fila 1: sesiones (spanning)
    html += '<thead><tr class="zone-head-row">'
    html += '<th class="zone-col" rowspan="2">Zona</th>'
    for ses in sessions_list:
        lbl = f"{ses} ses." if ses else "Paquete"
        html += f'<th class="ses-hdr" colspan="{len(companies)}">{lbl}</th>'
    html += '</tr>'

    # Fila 2: empresas
    html += '<tr>'
    for _ in sessions_list:
        for co in companies:
            bg   = CO_HEADER.get(co, "#f3f4f6")
            clr  = CO_TEXT.get(co,   "#111827")
            brd  = CO_TEXT.get(co,   "#9ca3af")
            name = CO_SHORT.get(co, co)
            html += (f'<th class="co-hdr" '
                     f'style="background:{bg};color:{clr};border-bottom-color:{brd}">'
                     f'{name}</th>')
    html += '</tr></thead><tbody>'

    # Filas de datos
    for i, zone in enumerate(zones):
        row_class = "zebra" if i % 2 == 0 else ""
        html += f'<tr class="{row_class}">'
        html += f'<td class="zone-col">{zone}</td>'
        for ses in sessions_list:
            for co in companies:
                html += make_cell(zone, co, ses)
        html += '</tr>'

    html += '</tbody></table></div>'
    return html


# ── Sidebar ────────────────────────────────────────────────────────────────

with st.sidebar:
    try:
        st.image("https://lasertam.com/wp-content/uploads/2021/09/logo-lasertam.png",
                 use_container_width=True)
    except Exception:
        pass
    st.title("PrecioSpy")
    st.caption("Monitoreo de precios · Depilación Láser · Chile")
    st.divider()

    gender_filter = st.radio("Género", ["Femenino", "Masculino", "Todos"], index=0)
    st.divider()

    # Cupones activos
    if COUPONS:
        st.caption("**Cupones de competencia:**")
        apply_coupons = {}
        for competitor, pct in COUPONS.items():
            apply_coupons[competitor] = st.toggle(
                f"{competitor} -{pct}%",
                value=True,
                help=f"Aplicar cupón conocido de {competitor} ({pct}% de descuento adicional)",
            )
        st.divider()
    else:
        apply_coupons = {}

    if st.button("🔄 Actualizar datos ahora", type="primary", use_container_width=True):
        with st.spinner("Scrapeando sitios... (puede tardar ~10 min)"):
            try:
                import subprocess
                result = subprocess.run(
                    [sys.executable, "-m", "scraper.run_all"],
                    capture_output=True, text=True, timeout=700,
                )
                st.cache_data.clear()
                if result.returncode == 0:
                    st.success("Datos actualizados ✓")
                else:
                    st.warning(f"Completado con advertencias:\n{result.stderr[-500:]}")
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    dates_df = load_scrape_dates()
    if not dates_df.empty:
        st.caption("**Último scrape:**")
        for _, row in dates_df.iterrows():
            dt = row["ultimo_scrape"]
            if dt:
                try:
                    dt_parsed = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
                    dt_str = dt_parsed.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    dt_str = str(dt)[:16]
                st.caption(f"• {row['name']}: {dt_str}")


# ── Main ───────────────────────────────────────────────────────────────────

st.title("💡 Monitor de Precios · Depilación Láser")
st.caption("Lasertam vs. Belenus · Cela · Bellmeclinic")

# Construir tuple de cupones activos (hasheable para el cache)
active_coupons_tuple = tuple(
    (c, pct) for c, pct in COUPONS.items()
    if apply_coupons.get(c, False)
)

df_all = load_latest_prices(gender_filter, active_coupons_tuple)

if df_all.empty:
    st.info("No hay datos aún. Haz clic en **Actualizar datos ahora** para iniciar el primer scraping.")
    st.stop()

df_lasertam = df_all[df_all["competitor"] == "Lasertam"]
df_comp     = df_all[df_all["competitor"] != "Lasertam"]

# ── Tabs ───────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Comparación por zona",
    "🏆 Ranking",
    "📈 Historial",
    "💰 Descuentos activos",
])

# ── TAB 1: Tabla comparativa ───────────────────────────────────────────────
with tab1:

    # Controles
    col_s, col_ses, col_ord = st.columns([3, 2, 2])
    search_q = col_s.text_input("Buscar zona…", placeholder="Ej: Axilas, Piernas…", label_visibility="collapsed")
    ses_options = sorted([s for s in df_all["sessions"].dropna().unique() if s], key=int)
    ses_sel = col_ses.selectbox(
        "Sesiones",
        options=[None] + [int(s) for s in ses_options],
        format_func=lambda x: "Todas las sesiones" if x is None else f"{x} sesión{'es' if x > 1 else ''}",
        label_visibility="collapsed",
    )
    sort_by = col_ord.selectbox(
        "Ordenar",
        ["Nombre A→Z", "Precio Lasertam ↑", "Precio Lasertam ↓"],
        label_visibility="collapsed",
    )

    # KPIs (datos Lasertam filtrados)
    df_lt_tab = df_lasertam.copy()
    if ses_sel:
        df_lt_tab = df_lt_tab[df_lt_tab["sessions"] == ses_sel]

    k1, k2, k3, k4 = st.columns(4)
    n_zones = df_all["zone_name"].nunique()
    k1.metric("Zonas mostradas", n_zones)
    if not df_lt_tab.empty:
        k2.metric("Precio mín. Lasertam", fmt_clp(df_lt_tab["price"].min()))
        k3.metric("Precio máx. Lasertam", fmt_clp(df_lt_tab["price"].max()))
        avg_disc = df_lt_tab["discount_pct"].mean()
        k4.metric("Descuento promedio",
                  f"{avg_disc:.0f}%" if pd.notna(avg_disc) else "—")
    st.divider()

    # Ordenar df
    df_tab = df_all.copy()
    if ses_sel:
        df_tab = df_tab[df_tab["sessions"] == ses_sel]

    if sort_by == "Precio Lasertam ↑":
        lt_order = df_lt_tab.groupby("zone_name")["price"].min().reset_index()
        lt_order.columns = ["zone_name", "_sort"]
        df_tab = df_tab.merge(lt_order, on="zone_name", how="left").sort_values("_sort").drop(columns="_sort")
    elif sort_by == "Precio Lasertam ↓":
        lt_order = df_lt_tab.groupby("zone_name")["price"].min().reset_index()
        lt_order.columns = ["zone_name", "_sort"]
        df_tab = df_tab.merge(lt_order, on="zone_name", how="left").sort_values("_sort", ascending=False).drop(columns="_sort")

    # Determinar sesiones a mostrar en la tabla
    if ses_sel:
        sessions_to_show = [ses_sel]
    else:
        sessions_to_show = sorted(
            [int(s) for s in df_tab["sessions"].dropna().unique()],
            key=int
        )

    coupon_info = "  |  Cupones aplicados: " + ", ".join(
        f"{c} -{pct}%" for c, pct in COUPONS.items()
    ) if COUPONS else ""
    st.caption(
        "🟢 Verde = precio más barato · 🔴 Rojo claro = precio más caro · "
        "tachado = precio sin descuento · 🏷 = precio con cupón aplicado" + coupon_info
    )

    html_table = build_comparison_table(df_tab, sessions_to_show, search=search_q)
    st.markdown(html_table, unsafe_allow_html=True)


# ── TAB 2: Ranking ─────────────────────────────────────────────────────────
with tab2:
    st.subheader("¿Dónde está Lasertam más barato o más caro?")

    if df_lasertam.empty:
        st.info("No hay datos de Lasertam.")
    else:
        for comp_name in sorted(df_comp["competitor"].unique()):
            df_c = df_comp[df_comp["competitor"] == comp_name].copy()
            merged_c = df_c.merge(
                df_lasertam[["zone_name", "gender", "sessions", "price"]].rename(
                    columns={"price": "lt_price"}),
                on=["zone_name", "gender", "sessions"],
                how="inner",
            )
            if merged_c.empty:
                continue

            merged_c["diferencia"]     = merged_c["price"] - merged_c["lt_price"]
            merged_c["diferencia_pct"] = (merged_c["diferencia"] / merged_c["lt_price"] * 100).round(1)
            merged_c["status"]         = merged_c["diferencia"].apply(
                lambda d: "🟢 Lasertam más barato" if d > 0 else (
                          "🔴 Lasertam más caro"   if d < 0 else "➖ Igual"))

            n_baratas = (merged_c["diferencia"] > 0).sum()
            n_caras   = (merged_c["diferencia"] < 0).sum()

            with st.expander(
                f"Lasertam vs. **{comp_name}**  —  "
                f"🟢 {n_baratas} zonas más barato · 🔴 {n_caras} zonas más caro",
                expanded=True,
            ):
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    fig = px.bar(
                        merged_c.sort_values("diferencia", ascending=False),
                        x="zone_name", y="diferencia", color="diferencia",
                        color_continuous_scale=["#dc2626", "#e5e7eb", "#16a34a"],
                        title=f"Diferencia: {comp_name} − Lasertam (CLP)",
                        labels={"diferencia": "Diferencia ($)", "zone_name": "Zona"},
                    )
                    fig.update_layout(
                        coloraxis_showscale=False,
                        xaxis_tickangle=-35,
                        height=380,
                    )
                    fig.add_hline(y=0, line_dash="dot", line_color="gray")
                    st.plotly_chart(fig, use_container_width=True)

                with col_b:
                    show = merged_c[["zone_name", "sessions", "lt_price", "price",
                                     "diferencia_pct", "status"]].copy()
                    show["lt_price"]       = show["lt_price"].apply(fmt_clp)
                    show["price"]          = show["price"].apply(fmt_clp)
                    show["diferencia_pct"] = show["diferencia_pct"].apply(lambda x: f"{x:+.1f}%")
                    show["sessions"]       = show["sessions"].apply(
                        lambda s: f"{int(s)} ses." if pd.notna(s) else "Paq.")
                    show.columns = ["Zona", "Ses.", "Lasertam", comp_name, "Dif.%", ""]
                    st.dataframe(show, use_container_width=True, hide_index=True)


# ── TAB 3: Historial ───────────────────────────────────────────────────────
with tab3:
    st.subheader("Evolución histórica de precios")

    all_zones = run_query(
        "SELECT DISTINCT zone_name FROM price_records ORDER BY zone_name"
    )["zone_name"].tolist()

    col_h1, col_h2, col_h3 = st.columns(3)
    zone_h    = col_h1.selectbox("Zona", all_zones, key="hist_zone")
    gender_h  = col_h2.radio("Género", ["F", "M"], key="hist_gender", horizontal=True)
    sessions_h = col_h3.selectbox(
        "Sesiones", [None, 1, 3, 6, 9], key="hist_ses",
        format_func=lambda x: "Todas" if x is None else str(x),
    )

    df_hist = load_price_history(zone_h, gender_h, sessions_h)

    if df_hist.empty:
        st.info("No hay historial aún. Los datos se acumulan con cada scraping semanal.")
    else:
        fig3 = px.line(
            df_hist, x="fecha", y="price", color="competitor",
            color_discrete_map=COMPETITOR_COLORS, markers=True,
            title=f"Historial · {zone_h}",
            labels={"price": "Precio CLP", "fecha": "Fecha", "competitor": "Empresa"},
        )
        fig3.update_layout(yaxis_tickformat="$,.0f")
        st.plotly_chart(fig3, use_container_width=True)

        pivot_h = df_hist.pivot_table(
            index="fecha", columns="competitor", values="price", aggfunc="min"
        ).reset_index()
        for col in pivot_h.columns[1:]:
            pivot_h[col] = pivot_h[col].apply(fmt_clp)
        st.dataframe(pivot_h, use_container_width=True, hide_index=True)


# ── TAB 4: Descuentos activos ──────────────────────────────────────────────
with tab4:
    st.subheader("Descuentos y promociones activas")

    df_disc = df_all[df_all["discount_pct"].notna() & (df_all["discount_pct"] > 0)].copy()

    if df_disc.empty:
        st.info("No se detectaron descuentos en el último scraping.")
    else:
        df_disc_sorted = df_disc.sort_values("discount_pct", ascending=False)

        fig4 = px.bar(
            df_disc_sorted.head(30),
            x="zone_name", y="discount_pct", color="competitor",
            color_discrete_map=COMPETITOR_COLORS, barmode="group",
            title="Top descuentos activos (%)",
            labels={"discount_pct": "Descuento %", "zone_name": "Zona", "competitor": "Empresa"},
        )
        fig4.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig4, use_container_width=True)

        disp = df_disc_sorted[["competitor", "zone_name", "gender", "sessions",
                               "price", "original_price", "discount_pct"]].copy()
        disp["price"]          = disp["price"].apply(fmt_clp)
        disp["original_price"] = disp["original_price"].apply(fmt_clp)
        disp["discount_pct"]   = disp["discount_pct"].apply(lambda x: f"{x:.0f}%")
        disp["sessions"]       = disp["sessions"].apply(
            lambda s: f"{int(s)} ses." if pd.notna(s) else "Paquete")
        disp.columns = ["Empresa", "Zona", "Gén.", "Ses.", "Precio Oferta", "Precio Normal", "Dcto."]
        st.dataframe(disp, use_container_width=True, hide_index=True)


# ── Footer ─────────────────────────────────────────────────────────────────
st.divider()
st.caption("PrecioSpy · Lasertam · Datos actualizados semanalmente · Chile 🇨🇱")
