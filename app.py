"""
Dashboard de Monitoreo de Precios - Depilación Láser
Lasertam vs. Competencia | Chile
"""

import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent))
from db.models import DB_PATH, init_db

# ── Configuración de página ────────────────────────────────────────────────
st.set_page_config(
    page_title="PrecioSpy · Depilación Láser",
    page_icon="💡",
    layout="wide",
    initial_sidebar_state="expanded",
)

LASERTAM_COLOR = "#1f77b4"
COMPETITOR_COLORS = {
    "Belenus":      "#ff7f0e",
    "Cela":         "#2ca02c",
    "Bellmeclinic": "#d62728",
    "Lasertam":     LASERTAM_COLOR,
}

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
def load_latest_prices(gender_filter: str, sessions_filter: int | None) -> pd.DataFrame:
    """
    Último precio por competidor × zona × sesiones.
    Usa el run_id más reciente por competidor para evitar duplicados entre corridas.
    MIN(price) por zona/sesion para quedarse con el mejor precio disponible
    (excluye packs que solo incluyen la zona como parte de un combo más caro).
    """
    gender_clause = "AND pr.gender = ?" if gender_filter != "Todos" else ""
    sessions_clause = "AND pr.sessions = ?" if sessions_filter else ""

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
        -- Solo el run más reciente por competidor
        SELECT run_id FROM price_records pr2
        WHERE pr2.competitor_id = pr.competitor_id
        ORDER BY scraped_at DESC LIMIT 1
    )
    {gender_clause}
    {sessions_clause}
    GROUP BY c.name, pr.zone_name, pr.gender, pr.sessions
    """
    params = []
    if gender_filter != "Todos":
        params.append(gender_filter[0])
    if sessions_filter:
        params.append(sessions_filter)

    df = run_query(q, params)
    return df


@st.cache_data(ttl=300)
def load_price_history(zone: str, gender: str, sessions: int | None) -> pd.DataFrame:
    q = """
    SELECT
        c.name      AS competitor,
        pr.price,
        pr.sessions,
        DATE(pr.scraped_at) AS fecha
    FROM price_records pr
    JOIN competitors c ON c.id = pr.competitor_id
    WHERE pr.zone_name = ?
      AND pr.gender    = ?
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


@st.cache_data(ttl=300)
def load_all_zones() -> list[str]:
    df = run_query("SELECT DISTINCT zone_name FROM price_records ORDER BY zone_name")
    return df["zone_name"].tolist() if not df.empty else []


# ── Utilidades de formato ──────────────────────────────────────────────────

def fmt_clp(val) -> str:
    if pd.isna(val) or val is None:
        return "—"
    return f"${int(val):,}".replace(",", ".")


def color_row(row, lasertam_price: float | None):
    """Verde si Lasertam es más barato, rojo si es más caro."""
    if lasertam_price is None or pd.isna(row["price"]):
        return [""] * len(row)
    diff = row["price"] - lasertam_price
    if diff > 0:
        color = "background-color: #d4edda"  # verde (Lasertam más barato)
    elif diff < 0:
        color = "background-color: #f8d7da"  # rojo (Lasertam más caro)
    else:
        color = ""
    return [color] * len(row)


# ── Sidebar ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://lasertam.com/wp-content/uploads/2021/09/logo-lasertam.png",
             use_container_width=True)
    st.title("PrecioSpy")
    st.caption("Monitoreo de precios · Depilación Láser · Chile")
    st.divider()

    gender_filter = st.radio("Género", ["Femenino", "Masculino", "Todos"], index=0)
    sessions_opt = st.selectbox(
        "Sesiones",
        [None, 1, 3, 6, 9],
        format_func=lambda x: "Todas" if x is None else f"{x} sesión{'es' if x > 1 else ''}",
        help="Lasertam vende 1/3/6/9 ses. · Belenus 1/3/6 · Cela precio de paquete"
    )
    st.divider()

    # Botón de scraping manual
    if st.button("🔄 Actualizar datos ahora", type="primary", use_container_width=True):
        with st.spinner("Scrapeando sitios..."):
            try:
                import subprocess
                result = subprocess.run(
                    [sys.executable, "-m", "scraper.run_all"],
                    capture_output=True, text=True, timeout=300
                )
                st.cache_data.clear()
                if result.returncode == 0:
                    st.success("Datos actualizados ✓")
                else:
                    st.warning(f"Completado con advertencias:\n{result.stderr[-500:]}")
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    # Fechas del último scrape
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

gender_code = {"Femenino": "F", "Masculino": "M", "Todos": None}[gender_filter]

df_all = load_latest_prices(gender_filter, sessions_opt)

if df_all.empty:
    st.info("No hay datos aún. Haz clic en **Actualizar datos ahora** en el panel izquierdo para iniciar el primer scraping.")
    st.stop()

# ── KPI Cards ──────────────────────────────────────────────────────────────

df_lasertam = df_all[df_all["competitor"] == "Lasertam"]
df_comp = df_all[df_all["competitor"] != "Lasertam"]

# Zonas donde Lasertam es más barato / más caro
if not df_lasertam.empty and not df_comp.empty:
    merged = df_comp.merge(
        df_lasertam[["zone_name", "gender", "sessions", "price"]],
        on=["zone_name", "gender", "sessions"],
        suffixes=("_comp", "_lt"),
    )
    if not merged.empty:
        mas_barato = (merged["price_lt"] < merged["price_comp"]).sum()
        mas_caro = (merged["price_lt"] > merged["price_comp"]).sum()
        iguales = (merged["price_lt"] == merged["price_comp"]).sum()
        total_comp = len(merged)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Zonas comparadas", total_comp)
        col2.metric("✅ Más barato que competencia", mas_barato,
                    help="Zonas donde Lasertam tiene precio menor")
        col3.metric("❌ Más caro que competencia", mas_caro,
                    help="Zonas donde competidores son más baratos")
        col4.metric("➖ Precio igual", iguales)
        st.divider()

# ── Tabs ───────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Comparación por zona",
    "🏆 Ranking de precios",
    "📈 Historial de precios",
    "💰 Descuentos activos",
])

# ── TAB 1: Comparación por zona ────────────────────────────────────────────
with tab1:
    st.subheader("Comparación de precios por zona")

    zones = load_all_zones()
    if not zones:
        st.info("Sin datos de zonas.")
    else:
        selected_zone = st.selectbox("Selecciona una zona", zones)
        df_zone = df_all[df_all["zone_name"] == selected_zone].copy()

        if df_zone.empty:
            st.warning("Sin datos para esta zona.")
        else:
            # Etiqueta sesiones: None → "Paquete"
            df_zone["ses_label"] = df_zone["sessions"].apply(
                lambda s: "Paquete" if pd.isna(s) or s is None else f"{int(s)} ses."
            )

            # Gráfico de barras agrupado por sesiones
            fig = px.bar(
                df_zone.sort_values(["sessions", "price"]),
                x="ses_label",
                y="price",
                color="competitor",
                color_discrete_map=COMPETITOR_COLORS,
                barmode="group",
                text="price",
                title=f"Precios por sesiones · {selected_zone}",
                labels={"price": "Precio CLP", "ses_label": "Sesiones", "competitor": "Empresa"},
                category_orders={"ses_label": ["1 ses.", "3 ses.", "6 ses.", "9 ses.", "Paquete"]},
            )
            fig.update_traces(
                texttemplate="%{text:,.0f}",
                textposition="outside",
            )
            fig.update_layout(yaxis_tickformat=",.0f", height=420)
            st.plotly_chart(fig, use_container_width=True)

            # Tabla pivoteada: filas=empresa, columnas=sesiones
            st.caption("💡 Verde = Lasertam más barato · Rojo = Lasertam más caro")

            pivot = df_zone.pivot_table(
                index="competitor",
                columns="ses_label",
                values="price",
                aggfunc="min",
            ).reset_index()
            pivot = pivot.rename(columns={"competitor": "Empresa"})

            # Ordenar columnas de sesiones
            ses_cols = [c for c in ["1 ses.", "3 ses.", "6 ses.", "9 ses.", "Paquete"] if c in pivot.columns]
            pivot = pivot[["Empresa"] + ses_cols]

            # Guardar valores numéricos para colorear ANTES de formatear
            pivot_num = pivot.copy()
            lt_row = pivot_num[pivot_num["Empresa"] == "Lasertam"]

            def highlight_vs_lasertam(row):
                styles = [""] * len(row)
                if row["Empresa"] == "Lasertam":
                    return styles
                for i, col in enumerate(row.index[1:], 1):
                    if col not in lt_row.columns or lt_row.empty:
                        continue
                    lt_vals = lt_row[col].values
                    if len(lt_vals) == 0 or pd.isna(lt_vals[0]):
                        continue
                    cell = row[col]
                    if pd.isna(cell):
                        continue
                    try:
                        if float(cell) > float(lt_vals[0]):
                            styles[i] = "background-color: #d4edda; color: #155724"
                        elif float(cell) < float(lt_vals[0]):
                            styles[i] = "background-color: #f8d7da; color: #721c24"
                    except (ValueError, TypeError):
                        pass
                return styles

            # Aplicar estilo sobre valores numéricos, luego formatear para mostrar
            pivot_display = pivot.copy()
            for col in ses_cols:
                pivot_display[col] = pivot_display[col].apply(
                    lambda x: fmt_clp(x) if pd.notna(x) else "—"
                )

            # Construir tabla con colores usando pivot numérico para la lógica
            styled = pivot_num.style.apply(highlight_vs_lasertam, axis=1).format(
                {col: lambda x: fmt_clp(x) if pd.notna(x) else "—" for col in ses_cols}
            )
            st.dataframe(styled, use_container_width=True, hide_index=True)

            # Detalle de descuentos para la zona
            df_disc_zone = df_zone[df_zone["discount_pct"].notna() & (df_zone["discount_pct"] > 0)]
            if not df_disc_zone.empty:
                st.caption("**Descuentos activos en esta zona:**")
                for _, r in df_disc_zone.iterrows():
                    ses_txt = f"{int(r['sessions'])} ses." if pd.notna(r['sessions']) else "Paquete"
                    st.caption(
                        f"• **{r['competitor']}** ({ses_txt}): "
                        f"{fmt_clp(r['price'])} → normal {fmt_clp(r['original_price'])} "
                        f"(-{r['discount_pct']:.0f}%)"
                    )

# ── TAB 2: Ranking de precios ──────────────────────────────────────────────
with tab2:
    st.subheader("Resumen: ¿Dónde está Lasertam más barato o más caro?")

    if df_lasertam.empty:
        st.info("No hay datos de Lasertam.")
    else:
        # Merge: precio de Lasertam vs. cada competidor por zona
        for comp_name in df_comp["competitor"].unique():
            df_c = df_comp[df_comp["competitor"] == comp_name].copy()
            merged_c = df_c.merge(
                df_lasertam[["zone_name", "gender", "sessions", "price"]].rename(columns={"price": "lt_price"}),
                on=["zone_name", "gender", "sessions"],
                how="inner",
            )
            if merged_c.empty:
                continue

            merged_c["diferencia"] = merged_c["price"] - merged_c["lt_price"]
            merged_c["diferencia_pct"] = (merged_c["diferencia"] / merged_c["lt_price"] * 100).round(1)
            merged_c["status"] = merged_c["diferencia"].apply(
                lambda d: "🟢 Lasertam más barato" if d > 0 else ("🔴 Lasertam más caro" if d < 0 else "➖ Igual")
            )

            with st.expander(f"Lasertam vs. {comp_name}", expanded=True):
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    fig2 = px.bar(
                        merged_c.sort_values("diferencia", ascending=False),
                        x="zone_name",
                        y="diferencia",
                        color="diferencia",
                        color_continuous_scale=["#d62728", "#aaaaaa", "#2ca02c"],
                        title=f"Diferencia de precio: {comp_name} − Lasertam (CLP)",
                        labels={"diferencia": "Diferencia ($)", "zone_name": "Zona"},
                    )
                    fig2.update_layout(coloraxis_showscale=False, xaxis_tickangle=-35)
                    st.plotly_chart(fig2, use_container_width=True)

                with col_b:
                    show2 = merged_c[["zone_name", "lt_price", "price", "diferencia_pct", "status"]].copy()
                    show2["lt_price"] = show2["lt_price"].apply(fmt_clp)
                    show2["price"] = show2["price"].apply(fmt_clp)
                    show2["diferencia_pct"] = show2["diferencia_pct"].apply(lambda x: f"{x:+.1f}%")
                    show2.columns = ["Zona", "Lasertam", comp_name, "Dif. %", "Estado"]
                    st.dataframe(show2, use_container_width=True, hide_index=True)

# ── TAB 3: Historial de precios ────────────────────────────────────────────
with tab3:
    st.subheader("Evolución histórica de precios")

    zones_h = load_all_zones()
    col_h1, col_h2, col_h3 = st.columns(3)
    zone_h = col_h1.selectbox("Zona", zones_h, key="hist_zone")
    gender_h = col_h2.radio("Género", ["F", "M"], key="hist_gender", horizontal=True)
    sessions_h = col_h3.selectbox("Sesiones", [None, 1, 3, 6], key="hist_ses",
                                   format_func=lambda x: "Todas" if x is None else str(x))

    df_hist = load_price_history(zone_h, gender_h, sessions_h)

    if df_hist.empty:
        st.info("No hay historial aún. Los datos se acumulan con cada scraping semanal.")
    else:
        fig3 = px.line(
            df_hist,
            x="fecha",
            y="price",
            color="competitor",
            color_discrete_map=COMPETITOR_COLORS,
            markers=True,
            title=f"Historial de precios · {zone_h}",
            labels={"price": "Precio CLP", "fecha": "Fecha", "competitor": "Empresa"},
        )
        fig3.update_layout(yaxis_tickformat="$,.0f")
        st.plotly_chart(fig3, use_container_width=True)

        # Tabla de historia
        pivot = df_hist.pivot_table(
            index="fecha", columns="competitor", values="price", aggfunc="min"
        ).reset_index()
        for col in pivot.columns[1:]:
            pivot[col] = pivot[col].apply(fmt_clp)
        st.dataframe(pivot, use_container_width=True, hide_index=True)

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
            x="zone_name",
            y="discount_pct",
            color="competitor",
            color_discrete_map=COMPETITOR_COLORS,
            barmode="group",
            title="Top descuentos activos (%)",
            labels={"discount_pct": "Descuento %", "zone_name": "Zona", "competitor": "Empresa"},
        )
        fig4.update_layout(xaxis_tickangle=-35)
        st.plotly_chart(fig4, use_container_width=True)

        # Tabla de descuentos
        disp_disc = df_disc_sorted[["competitor", "zone_name", "gender", "sessions",
                                     "price", "original_price", "discount_pct"]].copy()
        disp_disc["price"] = disp_disc["price"].apply(fmt_clp)
        disp_disc["original_price"] = disp_disc["original_price"].apply(fmt_clp)
        disp_disc["discount_pct"] = disp_disc["discount_pct"].apply(lambda x: f"{x:.0f}%")
        disp_disc.columns = ["Empresa", "Zona", "Género", "Sesiones",
                              "Precio Oferta", "Precio Normal", "Descuento"]
        st.dataframe(disp_disc, use_container_width=True, hide_index=True)

# ── Footer ─────────────────────────────────────────────────────────────────
st.divider()
st.caption("PrecioSpy · Lasertam · Datos actualizados semanalmente automáticamente · Chile 🇨🇱")
