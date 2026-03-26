"""
Normalización de nombres de zonas entre competidores.
Mapea variantes textuales a un nombre canónico.
"""

import re

# Mapa de normalización: substring (lowercase) -> nombre canónico
_ZONE_MAP = [
    # --- Cuerpo completo ---
    ("cuerpo completo", "Cuerpo Completo"),
    # --- Piernas ---
    ("piernas completas", "Piernas Completas"),
    ("pierna completa", "Piernas Completas"),
    ("piernas", "Piernas Completas"),
    ("muslos", "Muslos"),
    ("media pierna", "Media Pierna"),
    ("medias piernas", "Media Pierna"),
    # --- Bikini / Rebaje ---
    ("full brazilian", "Bikini Full Brazilian"),
    ("full brasil", "Bikini Full Brazilian"),
    ("rebaje total", "Bikini Total"),
    ("rebaje brasileño", "Bikini Brasileño"),
    ("rebaje brasil", "Bikini Brasileño"),
    ("rebaje parcial", "Bikini Parcial"),
    ("bikini", "Bikini Parcial"),
    ("glúteo", "Glúteos"),
    ("gluteo", "Glúteos"),
    # --- Axilas ---
    ("axilas", "Axilas"),
    ("axila", "Axilas"),
    # --- Brazos ---
    ("brazo completo", "Brazos Completos"),
    ("brazos completos", "Brazos Completos"),
    ("antebrazo", "Antebrazos"),
    # --- Espalda ---
    ("espalda completa", "Espalda Completa"),
    ("espalda", "Espalda Completa"),
    # --- Rostro ---
    ("rostro completo", "Rostro Completo"),
    ("rostro inferior", "Rostro Inferior"),
    ("rostro", "Rostro Completo"),
    ("mejillas", "Mejillas"),
    ("mentón", "Mentón"),
    ("menton", "Mentón"),
    ("patillas", "Patillas"),
    ("bozo", "Bozo"),
    ("entrecejo", "Entrecejo"),
    ("labio", "Labio"),
    # --- Torso ---
    ("torso anterior", "Torso Anterior"),
    ("torso", "Torso Anterior"),
    ("vientre", "Vientre"),
    ("abdomen", "Vientre"),
    # --- Otras ---
    ("areola", "Areolas"),
    ("areolas", "Areolas"),
    ("hombros", "Hombros"),
    ("cuello", "Cuello"),
    ("línea alba", "Línea Alba"),
    ("linea alba", "Línea Alba"),
    ("manos", "Manos"),
    ("pies", "Pies"),
    ("ingle", "Ingle"),
    ("interglúteo", "Interglúteo"),
    ("intergluteo", "Interglúteo"),
    ("nariz", "Nariz y Orejas"),
    ("orejas", "Nariz y Orejas"),
]


def normalize_zone(raw: str) -> str:
    """Devuelve el nombre canónico para una zona dada."""
    text = raw.lower().strip()
    # Quitar prefijos comunes
    text = re.sub(r"depilaci[oó]n\s+l[aá]ser\s*", "", text)
    text = re.sub(r"(femenin[ao]|masculin[ao])\s*", "", text)
    text = text.strip()

    for keyword, canonical in _ZONE_MAP:
        if keyword in text:
            return canonical
    # Si no matchea, devolver capitalizado
    return raw.strip().title()


def detect_gender(raw: str) -> str:
    """Detecta género (F/M/U) del nombre de la zona."""
    text = raw.lower()
    if any(w in text for w in ["masculin", " masc", "hombre", "masculino"]):
        return "M"
    if any(w in text for w in ["femeni", "mujer", "femenin"]):
        return "F"
    return "F"  # default femenino en este rubro


def detect_sessions(raw: str) -> int | None:
    """Extrae número de sesiones si aparece en el texto."""
    m = re.search(r"(\d+)\s*sesi[oó]n", raw.lower())
    if m:
        return int(m.group(1))
    return None


def clean_price(raw) -> int | None:
    """Convierte string de precio a entero CLP. Ej: '$9.990' -> 9990."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return int(raw)
    s = str(raw).replace("$", "").replace(".", "").replace(",", "").strip()
    s = re.sub(r"[^\d]", "", s)
    return int(s) if s else None
