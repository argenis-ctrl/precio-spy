"""
Normalización de nombres de zonas entre competidores.
Mapea variantes textuales a un nombre canónico.
"""

import re

# Mapa de normalización: substring (lowercase) -> nombre canónico
# IMPORTANTE: los combos deben ir ANTES que las zonas individuales
_ZONE_MAP = [

    # ── COMBOS 3+ ZONAS (más específicos primero) ─────────────────────────
    ("piernas completas, full brazilian y axilas",  "Piernas + Rebaje Brasileño + Axilas"),
    ("piernas completas, full brazilian",           "Piernas + Rebaje Brasileño + Axilas"),
    ("piernas completas + rebaje brasil",           "Piernas + Rebaje Brasileño + Axilas"),
    ("piernas completas + rebaje brasileño",        "Piernas + Rebaje Brasileño + Axilas"),
    ("media pierna, rebaje total y axilas",         "Media Pierna + Axilas + Rebaje Total"),
    ("media pierna + axilas + rebaje",              "Media Pierna + Axilas + Rebaje Total"),
    ("medias piernas + axilas + rebaje",            "Media Pierna + Axilas + Rebaje Total"),
    ("rebaje total, brazo completo y axilas",       "Brazos Completos + Axilas + Rebaje Total"),
    ("brazos completos + axilas + rebaje total",    "Brazos Completos + Axilas + Rebaje Total"),
    ("brazos completos + axilas + rebaje brasil",   "Brazos Completos + Axilas + Rebaje Brasileño"),
    ("rebaje total, bozo y axilas",                 "Rebaje Total + Bozo + Axilas"),
    ("brazos completos, manos y dedos",             "Brazos Completos + Manos"),
    ("brazos completos + manos",                    "Brazos Completos + Manos"),
    ("entrecejo, nariz y orejas",                   "Entrecejo + Nariz + Orejas"),
    ("entrecejo + nariz + orejas",                  "Entrecejo + Nariz + Orejas"),
    ("bozo + mentón + patillas",                    "Bozo + Mentón + Patillas"),
    ("espalda completa + abdomen",                  "Espalda + Vientre"),
    ("espalda y cuello posterior",                  "Espalda + Cuello Posterior"),
    ("espalda completa + hombros",                  "Espalda Completa + Hombros"),
    ("espalda completa y hombros",                  "Espalda Completa + Hombros"),

    # ── COMBOS 2 ZONAS ────────────────────────────────────────────────────
    ("piernas completas + axilas + rebaje total",   "Piernas Completas + Axilas + Rebaje Total"),
    ("piernas completas y axilas",                  "Piernas Completas + Axilas"),
    ("piernas completas + axilas",                  "Piernas Completas + Axilas"),
    ("piernas completas y full brazilian",          "Piernas Completas + Rebaje Brasileño"),
    ("piernas completas + rebaje",                  "Piernas Completas + Rebaje Brasileño"),
    ("media pierna y rebaje total",                 "Media Pierna + Rebaje Total"),
    ("media pierna y pies",                         "Media Pierna + Pies"),
    ("rebaje total y axilas",                       "Rebaje Total + Axilas"),
    ("rebaje total + axilas",                       "Rebaje Total + Axilas"),
    ("full brazilian y axilas",                     "Rebaje Brasileño + Axilas"),
    ("full brazilian + axilas",                     "Rebaje Brasileño + Axilas"),
    ("rebaje brasileño + axilas",                   "Rebaje Brasileño + Axilas"),
    ("rebaje brasil + axilas",                      "Rebaje Brasileño + Axilas"),
    ("full brazilian y glúteos",                    "Rebaje Brasileño + Glúteos"),
    ("rebaje brasileño + glúteos",                  "Rebaje Brasileño + Glúteos"),
    ("brazos completos y hombros",                  "Brazos Completos + Hombros"),
    ("brazos completos + hombros",                  "Brazos Completos + Hombros"),
    ("brazo superior y hombros",                    "Brazo Superior + Hombros"),
    ("barba y cuello",                              "Barba + Cuello"),
    ("barba + cuello",                              "Barba + Cuello"),
    ("delineado de barba",                          "Barba + Cuello Completo"),
    ("bigote y mentón",                             "Bozo + Mentón"),
    ("bigote + mentón",                             "Bozo + Mentón"),
    ("bozo y mentón",                               "Bozo + Mentón"),
    ("bozo + mentón",                               "Bozo + Mentón"),
    ("bozo y patillas",                             "Bozo + Patillas"),
    ("bozo + patillas",                             "Bozo + Patillas"),
    ("bigote + axilas",                             "Bozo + Axilas"),
    ("axilas + patillas",                           "Axilas + Patillas"),
    ("antebrazo + manos",                           "Antebrazos + Manos"),
    ("manos y dedos",                               "Manos + Dedos"),
    ("manos + dedos",                               "Manos + Dedos"),
    ("pies y dedos",                                "Pies + Dedos"),
    ("pies + dedos",                                "Pies + Dedos"),
    ("frente + patillas",                           "Frente + Patillas"),
    ("rostro inferior y cuello",                    "Rostro Inferior + Cuello"),
    ("rostro inferior + cuello",                    "Rostro Inferior + Cuello"),
    ("rostro completo y cuello",                    "Rostro Completo + Cuello"),
    ("rostro completo + cuello",                    "Rostro Completo + Cuello"),

    # ── ZONAS INDIVIDUALES ────────────────────────────────────────────────
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
    # "Full Brazilian" (Belenus) = "Rebaje Brasileño" (Lasertam) → mismo canónico
    ("full brazilian", "Bikini Brasileño"),
    ("full brasil",    "Bikini Brasileño"),
    ("rebaje completo", "Bikini Total"),   # término Cela = Rebaje Total
    ("rebaje total",   "Bikini Total"),
    ("rebaje brasileño", "Bikini Brasileño"),
    ("rebaje brasil",  "Bikini Brasileño"),
    ("rebaje parcial", "Bikini Parcial"),
    ("bikini",         "Bikini Parcial"),
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
    ("bozo",   "Bozo"),
    ("bigote", "Bozo"),   # "bigote" = labio superior = misma zona que "bozo"
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
