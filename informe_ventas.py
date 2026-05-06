#!/usr/bin/env python3
"""
informe_ventas.py — Informe mensual de ventas Lasertam via WooCommerce API
Genera un reporte HTML en ~/Downloads/informe_ventas_lasertam_YYYY_MM.html
"""

import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

TZ_CHILE = ZoneInfo("America/Santiago")

import requests
from requests.auth import HTTPBasicAuth

# ── Config ────────────────────────────────────────────────────────────────────
WC_URL = "https://lasertam.com/wp-json/wc/v3"
CK     = "ck_d54578316281a23fbd19223eba88970dd29e5459"
CS     = "cs_4adb80948e5e0513d7fb0e1f05206e301e419430"
AUTH   = HTTPBasicAuth(CK, CS)

NOW    = datetime.now(TZ_CHILE)
YEAR   = NOW.year
MONTH  = NOW.month
AFTER  = datetime(YEAR, MONTH, 1, 0, 0, 0, tzinfo=TZ_CHILE).isoformat()
if MONTH == 12:
    BEFORE = datetime(YEAR + 1, 1, 1, 0, 0, 0, tzinfo=TZ_CHILE).isoformat()
else:
    BEFORE = datetime(YEAR, MONTH + 1, 1, 0, 0, 0, tzinfo=TZ_CHILE).isoformat()

MONTH_NAME = {
    1:"Enero",2:"Febrero",3:"Marzo",4:"Abril",
    5:"Mayo",6:"Junio",7:"Julio",8:"Agosto",
    9:"Septiembre",10:"Octubre",11:"Noviembre",12:"Diciembre"
}[MONTH]

CHILE_REGIONS = {
    "RM":"Región Metropolitana","VS":"Valparaíso","BI":"Biobío",
    "AR":"La Araucanía","MA":"Maule","LI":"O'Higgins","AN":"Antofagasta",
    "CO":"Coquimbo","LL":"Los Lagos","LR":"Los Ríos","TA":"Tarapacá",
    "AT":"Atacama","AI":"Aysén","ML":"Magallanes","AP":"Arica y Parinacota",
    "NB":"Ñuble",
}

SESSION_RE = re.compile(r'(\d+)\s*sesi', re.I)


# ── Helpers ───────────────────────────────────────────────────────────────────
def wc_get(endpoint, params=None):
    """Descarga todas las páginas de un endpoint WooCommerce."""
    items, page = [], 1
    while True:
        p = {"per_page": 100, "page": page, **(params or {})}
        r = requests.get(f"{WC_URL}/{endpoint}", auth=AUTH, params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        items.extend(data)
        if len(data) < 100:
            break
        page += 1
    return items


def normalize_region(raw: str) -> str:
    key = raw.upper().replace("CL-", "")
    return CHILE_REGIONS.get(key, raw) if raw else "Sin datos"


def fmt_clp(n: float) -> str:
    return "$" + f"{int(n):,}".replace(",", ".")


def detect_sessions(item: dict) -> int | None:
    """Extrae número de sesiones de los meta_data o del nombre del ítem."""
    for meta in item.get("meta_data", []):
        if "sesi" in str(meta.get("key", "")).lower():
            m = SESSION_RE.search(str(meta.get("value", "")))
            if m:
                return int(m.group(1))
    m = SESSION_RE.search(item.get("name", ""))
    return int(m.group(1)) if m else None


# ── Fetch órdenes ─────────────────────────────────────────────────────────────
print(f"Cargando órdenes de {MONTH_NAME} {YEAR}...")
orders = wc_get("orders", {
    "after": AFTER,
    "before": BEFORE,
    "status": "completed,processing",
})
print(f"  {len(orders)} órdenes encontradas")

if not orders:
    print("Sin órdenes para el período. Saliendo.")
    sys.exit(0)

# ── Métricas base ─────────────────────────────────────────────────────────────
totals          = [float(o.get("total", 0)) for o in orders]
total_ventas    = sum(totals)
total_ordenes   = len(orders)
ticket_promedio = total_ventas / total_ordenes

# Productos más vendidos
prod_units   = Counter()
prod_revenue = defaultdict(float)
for o in orders:
    for item in o.get("line_items", []):
        name = item.get("name", "Desconocido")
        prod_units[name]   += item.get("quantity", 1)
        prod_revenue[name] += float(item.get("subtotal", 0))
top10 = prod_units.most_common(10)

# Packs por sesiones (1, 3, 6, 9)
pack_units   = Counter({1: 0, 3: 0, 6: 0, 9: 0})
pack_revenue = defaultdict(float)
for o in orders:
    for item in o.get("line_items", []):
        sessions = detect_sessions(item)
        if sessions in (1, 3, 6, 9):
            pack_units[sessions]   += item.get("quantity", 1)
            pack_revenue[sessions] += float(item.get("subtotal", 0))

# Regiones
reg_orders  = Counter()
reg_revenue = defaultdict(float)
for o in orders:
    state = normalize_region((o.get("billing") or {}).get("state") or "")
    reg_orders[state]  += 1
    reg_revenue[state] += float(o.get("total", 0))

# Canales (WooCommerce Order Attribution)
ATTR_KEYS = [
    "_wc_order_attribution_utm_source",
    "_wc_order_attribution_source_type",
    "_wc_order_attribution_utm_medium",
    "_wc_order_attribution_referrer",
]
chan_orders  = Counter()
chan_revenue = defaultdict(float)
for o in orders:
    meta_map = {m["key"]: str(m.get("value") or "").strip() for m in o.get("meta_data", [])}
    source = next((meta_map[k] for k in ATTR_KEYS if meta_map.get(k)), "Directo / Orgánico")
    chan_orders[source]  += 1
    chan_revenue[source] += float(o.get("total", 0))

# Nuevos vs Recurrentes
print("Analizando clientes nuevos vs recurrentes (esto puede tomar un momento)...")
new_c = ret_c = guest = 0
seen_cids: dict[int, str] = {}
for o in orders:
    cid = o.get("customer_id", 0)
    if cid == 0:
        guest += 1
        continue
    if cid not in seen_cids:
        try:
            r = requests.get(
                f"{WC_URL}/orders", auth=AUTH,
                params={"customer": cid, "before": AFTER, "per_page": 1},
                timeout=10,
            )
            prev = int(r.headers.get("X-WP-Total", "0"))
        except Exception:
            prev = 0
        seen_cids[cid] = "new" if prev == 0 else "ret"
    if seen_cids[cid] == "new":
        new_c += 1
    else:
        ret_c += 1

print(f"  Nuevos: {new_c} | Recurrentes: {ret_c} | Invitados: {guest}")

# ── Preparar datos JS ─────────────────────────────────────────────────────────
top_reg  = reg_orders.most_common(10)
top_chan  = chan_orders.most_common()

pack_labels = [f"{n} Sesión{'es' if n > 1 else ''}" for n in [1, 3, 6, 9]]

j_pack_labels = json.dumps(pack_labels, ensure_ascii=False)
j_pack_data   = json.dumps([pack_units[n] for n in [1, 3, 6, 9]])
j_pack_rev    = json.dumps([int(pack_revenue[n]) for n in [1, 3, 6, 9]])
j_chan_labels = json.dumps([c for c, _ in top_chan], ensure_ascii=False)
j_chan_data   = json.dumps([n for _, n in top_chan])
j_prod_labels = json.dumps([n for n, _ in top10], ensure_ascii=False)
j_prod_data   = json.dumps([u for _, u in top10])
j_reg_labels  = json.dumps([r for r, _ in top_reg], ensure_ascii=False)
j_reg_data    = json.dumps([n for _, n in top_reg])

top_chan_name  = top_chan[0][0] if top_chan else "—"
top_chan_count = top_chan[0][1] if top_chan else 0

# ── Tabla top productos ───────────────────────────────────────────────────────
table_rows = "".join(
    f'<tr>'
    f'<td class="rank">#{i+1}</td>'
    f'<td>{name}</td>'
    f'<td class="num">{units:,}</td>'
    f'<td class="num">{fmt_clp(prod_revenue[name])}</td>'
    f'</tr>'
    for i, (name, units) in enumerate(top10)
)

# ── HTML ──────────────────────────────────────────────────────────────────────
html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Informe Ventas {MONTH_NAME} {YEAR} — Lasertam</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
  --primary:#8b5cf6;--pl:#a78bfa;--cyan:#06b6d4;--green:#10b981;
  --amber:#f59e0b;--bg:#0f0f1a;--card:#1a1a2e;--text:#e2e8f0;
  --muted:#94a3b8;--border:#2d3748;
}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text)}}
.header{{
  background:linear-gradient(135deg,#1a1a2e,#16213e,#0f3460);
  padding:2rem 3rem;border-bottom:2px solid var(--primary);
  display:flex;align-items:center;justify-content:space-between;gap:1rem;
  flex-wrap:wrap;
}}
.header h1{{font-size:1.7rem;font-weight:700}}
.header h1 span{{color:var(--pl)}}
.header p{{color:var(--muted);font-size:.85rem;margin-top:.25rem}}
.badge{{
  background:var(--primary);color:#fff;padding:.3rem .9rem;
  border-radius:999px;font-size:.75rem;font-weight:600;white-space:nowrap;
  align-self:flex-start;
}}
.wrap{{max-width:1400px;margin:0 auto;padding:2rem 1.5rem}}
.kpis{{
  display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));
  gap:1rem;margin-bottom:2rem;
}}
.kpi{{
  background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:1.5rem;border-top:3px solid var(--primary);
}}
.kpi.c{{border-top-color:var(--cyan)}}
.kpi.g{{border-top-color:var(--green)}}
.kpi.a{{border-top-color:var(--amber)}}
.kpi label{{font-size:.7rem;text-transform:uppercase;letter-spacing:.05em;color:var(--muted)}}
.kpi .val{{font-size:1.9rem;font-weight:700;margin:.4rem 0 .2rem;line-height:1.1}}
.kpi .sub{{font-size:.78rem;color:var(--muted)}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;margin-bottom:1.5rem}}
@media(max-width:800px){{.g2{{grid-template-columns:1fr}}}}
.card{{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:1.5rem;margin-bottom:0}}
.mb{{margin-bottom:1.5rem}}
.card h2{{font-size:.95rem;font-weight:600;margin-bottom:1.2rem;color:var(--pl)}}
.card canvas{{max-height:280px}}
table{{width:100%;border-collapse:collapse;font-size:.83rem}}
th{{
  text-align:left;padding:.65rem 1rem;background:#1e2a3a;color:var(--muted);
  text-transform:uppercase;font-size:.68rem;letter-spacing:.05em;
}}
td{{padding:.6rem 1rem;border-bottom:1px solid var(--border)}}
tr:hover td{{background:#1e293b}}
.rank{{color:var(--pl);font-weight:700}}
.num{{font-weight:600}}
.foot{{
  text-align:center;color:var(--muted);font-size:.73rem;
  padding:1.5rem;border-top:1px solid var(--border);margin-top:2rem;
}}
</style>
</head>
<body>

<div class="header">
  <div>
    <p>LASERTAM · DEPILACIÓN LÁSER CHILE</p>
    <h1>Informe de Ventas — <span>{MONTH_NAME} {YEAR}</span></h1>
    <p>Generado el {NOW.strftime('%d/%m/%Y %H:%M')} UTC · {total_ordenes} órdenes procesadas</p>
  </div>
  <span class="badge">WooCommerce API</span>
</div>

<div class="wrap">

  <!-- KPIs -->
  <div class="kpis">
    <div class="kpi">
      <label>Ventas totales</label>
      <div class="val">{fmt_clp(total_ventas)}</div>
      <div class="sub">CLP · {total_ordenes} órdenes</div>
    </div>
    <div class="kpi c">
      <label>Ticket promedio</label>
      <div class="val">{fmt_clp(ticket_promedio)}</div>
      <div class="sub">por orden</div>
    </div>
    <div class="kpi g">
      <label>Clientes nuevos</label>
      <div class="val">{new_c}</div>
      <div class="sub">{ret_c} recurrentes · {guest} invitados</div>
    </div>
    <div class="kpi a">
      <label>Canal top</label>
      <div class="val" style="font-size:1rem;word-break:break-word">{top_chan_name}</div>
      <div class="sub">{top_chan_count} órdenes</div>
    </div>
  </div>

  <!-- Packs + Canales -->
  <div class="g2">
    <div class="card">
      <h2>Packs por N° Sesiones — Unidades vendidas</h2>
      <canvas id="cPacks"></canvas>
    </div>
    <div class="card">
      <h2>Canales de venta (órdenes)</h2>
      <canvas id="cCanales"></canvas>
    </div>
  </div>

  <!-- Top productos -->
  <div class="card mb">
    <h2>Top 10 Productos más vendidos (unidades)</h2>
    <canvas id="cProds" style="max-height:340px"></canvas>
  </div>

  <!-- Regiones + tabla -->
  <div class="g2">
    <div class="card">
      <h2>Ventas por Región (N° órdenes)</h2>
      <canvas id="cRegs"></canvas>
    </div>
    <div class="card">
      <h2>Detalle top productos</h2>
      <table>
        <thead>
          <tr><th>#</th><th>Producto</th><th>Unidades</th><th>Ingresos</th></tr>
        </thead>
        <tbody>{table_rows}</tbody>
      </table>
    </div>
  </div>

  <!-- Ingresos por pack -->
  <div class="card mb" style="margin-top:1.5rem">
    <h2>Ingresos por tipo de Pack (CLP)</h2>
    <canvas id="cPackRev" style="max-height:210px"></canvas>
  </div>

</div>

<div class="foot">
  Lasertam Analytics · {MONTH_NAME} {YEAR} · Datos extraídos de WooCommerce REST API
</div>

<script>
const C = ['#8b5cf6','#06b6d4','#10b981','#f59e0b','#ef4444','#ec4899','#6366f1','#84cc16','#f97316','#14b8a6'];
const ax = {{ticks:{{color:'#94a3b8'}},grid:{{color:'#1e293b'}}}};
const base = {{responsive:true,plugins:{{legend:{{labels:{{color:'#94a3b8'}}}}}},scales:{{x:ax,y:ax}}}};
const donut = {{responsive:true,plugins:{{legend:{{position:'right',labels:{{color:'#94a3b8',boxWidth:12}}}}}}}};
const hbar = {{responsive:true,indexAxis:'y',plugins:{{legend:{{labels:{{color:'#94a3b8'}}}}}},scales:{{x:ax,y:ax}}}};

new Chart(document.getElementById('cPacks'), {{
  type: 'bar',
  data: {{
    labels: {j_pack_labels},
    datasets: [{{label:'Unidades',data:{j_pack_data},backgroundColor:C.slice(0,4),borderRadius:6}}]
  }},
  options: base
}});

new Chart(document.getElementById('cCanales'), {{
  type: 'doughnut',
  data: {{
    labels: {j_chan_labels},
    datasets: [{{data:{j_chan_data},backgroundColor:C,borderWidth:2}}]
  }},
  options: donut
}});

new Chart(document.getElementById('cProds'), {{
  type: 'bar',
  data: {{
    labels: {j_prod_labels},
    datasets: [{{label:'Unidades',data:{j_prod_data},backgroundColor:'#8b5cf6',borderRadius:4}}]
  }},
  options: hbar
}});

new Chart(document.getElementById('cRegs'), {{
  type: 'bar',
  data: {{
    labels: {j_reg_labels},
    datasets: [{{label:'Órdenes',data:{j_reg_data},backgroundColor:'#06b6d4',borderRadius:4}}]
  }},
  options: base
}});

new Chart(document.getElementById('cPackRev'), {{
  type: 'bar',
  data: {{
    labels: {j_pack_labels},
    datasets: [{{label:'Ingresos CLP',data:{j_pack_rev},backgroundColor:C.slice(0,4),borderRadius:6}}]
  }},
  options: base
}});
</script>
</body>
</html>"""

# ── Guardar ───────────────────────────────────────────────────────────────────
out = Path.home() / "Downloads" / f"informe_ventas_lasertam_{YEAR}_{MONTH:02d}.html"
out.write_text(html, encoding="utf-8")
print(f"\nInforme generado: {out}")
print("Abre el archivo en tu navegador para ver el reporte.")
