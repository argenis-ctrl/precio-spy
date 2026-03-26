"""
Database models and initialization for precio-spy.
Uses SQLite via sqlite3 directly (no ORM dependency).
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "precios.db"


def get_connection() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS competitors (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT    UNIQUE NOT NULL,
            url  TEXT    NOT NULL,
            is_self INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS price_records (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor_id  INTEGER NOT NULL,
            zone_name      TEXT    NOT NULL,   -- nombre normalizado
            zone_raw       TEXT,               -- nombre original del sitio
            gender         TEXT    DEFAULT 'F', -- F / M / U
            sessions       INTEGER,            -- 1, 3, 6, 9 sesiones
            price          INTEGER,            -- precio oferta CLP
            original_price INTEGER,            -- precio antes del descuento
            discount_pct   REAL,               -- % descuento calculado
            scraped_at     TEXT    NOT NULL,   -- ISO timestamp
            run_id         TEXT,               -- ID de batch de scraping (timestamp inicio)
            FOREIGN KEY (competitor_id) REFERENCES competitors(id)
        );

        CREATE INDEX IF NOT EXISTS idx_pr_comp_zone
            ON price_records(competitor_id, zone_name, gender, sessions);
        CREATE INDEX IF NOT EXISTS idx_pr_scraped
            ON price_records(scraped_at);
    """)

    # Insertar competidores base si no existen
    competitors = [
        ("Lasertam",     "https://lasertam.com",         1),
        ("Belenus",      "https://belenus.cl",            0),
        ("Cela",         "https://www.cela.cl",           0),
        ("Bellmeclinic", "https://www.bellmeclinic.cl",   0),
    ]
    cur.executemany(
        "INSERT OR IGNORE INTO competitors(name, url, is_self) VALUES (?,?,?)",
        competitors,
    )
    conn.commit()
    conn.close()


def get_competitor_id(name: str) -> int:
    conn = get_connection()
    row = conn.execute("SELECT id FROM competitors WHERE name=?", (name,)).fetchone()
    conn.close()
    if row is None:
        raise ValueError(f"Competitor '{name}' not found in DB")
    return row["id"]


def delete_latest_run(competitor_id: int):
    """
    Elimina los registros del último run de un competidor
    (el run más reciente, para poder reemplazarlo con datos frescos).
    """
    conn = get_connection()
    last_run = conn.execute(
        "SELECT run_id FROM price_records WHERE competitor_id=? ORDER BY scraped_at DESC LIMIT 1",
        (competitor_id,)
    ).fetchone()
    if last_run and last_run["run_id"]:
        conn.execute(
            "DELETE FROM price_records WHERE competitor_id=? AND run_id=?",
            (competitor_id, last_run["run_id"])
        )
        conn.commit()
    conn.close()


def insert_price_records(records: list[dict]):
    """
    Inserta registros de precios. Cada registro debe incluir run_id.
    records: list of dicts con keys:
        competitor_id, zone_name, zone_raw, gender, sessions,
        price, original_price, discount_pct, scraped_at, run_id
    """
    if not records:
        return
    conn = get_connection()
    conn.executemany(
        """
        INSERT INTO price_records
            (competitor_id, zone_name, zone_raw, gender, sessions,
             price, original_price, discount_pct, scraped_at, run_id)
        VALUES
            (:competitor_id, :zone_name, :zone_raw, :gender, :sessions,
             :price, :original_price, :discount_pct, :scraped_at, :run_id)
        """,
        records,
    )
    conn.commit()
    conn.close()
