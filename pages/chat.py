"""
Chat IA — Asistente Lasertam powered by Google Gemini
"""

import streamlit as st
import google.generativeai as genai

# ── Config ─────────────────────────────────────────────────────────────────────
try:
    GEMINI_KEY = st.secrets["GEMINI_API_KEY"]
except Exception:
    import os
    from dotenv import load_dotenv
    load_dotenv()
    GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")

SYSTEM_PROMPT = """Eres un asistente de negocio especializado para Lasertam,
una clínica de depilación láser en Chile. Ayudas al equipo con:

- Análisis de ventas y métricas del negocio
- Estrategias de marketing y precios
- Comparación con competidores (Belenus, Cela, etc.)
- Consultas sobre servicios de depilación láser
- Redacción de copy publicitario
- Interpretación de datos del dashboard PrecioSpy

Responde siempre en español, de forma clara y directa.
Cuando hables de precios, usa el formato CLP chileno ($9.990).
Si no sabes algo con certeza, dilo claramente."""

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
section[data-testid="stSidebar"] .stButton>button{
  background:#131c2e !important;color:#94a3b8 !important;
  border:1px solid #1e293b !important;border-radius:8px !important;font-size:13px !important;}
section[data-testid="stSidebar"] .stButton>button:hover{
  background:#1e293b !important;color:#cbd5e1 !important;}
h1,h2,h3,.stMarkdown h1,.stMarkdown h2{color:#e2e8f0 !important;}
p,.stMarkdown p{color:#94a3b8 !important;}
hr{border-color:#1e293b !important;}
/* Chat messages */
[data-testid="stChatMessage"]{background:#1a1a2e !important;border:1px solid #1e293b !important;border-radius:12px !important;}
[data-testid="stChatMessage"] p{color:#e2e8f0 !important;}
[data-testid="stChatInput"]{background:#131c2e !important;border-color:#1e293b !important;}
[data-testid="stChatInput"] textarea{color:#e2e8f0 !important;background:#131c2e !important;}
[data-testid="stAlert"]{background:#1e293b !important;border-color:#334155 !important;}
[data-testid="stAlert"] p{color:#94a3b8 !important;}
#MainMenu,footer,[data-testid="stDecoration"]{visibility:hidden;}
</style>
"""

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Chat IA · Lasertam", page_icon="🤖", layout="wide")

if "sel_tema" not in st.session_state:
    st.session_state.sel_tema = "Oscuro"

# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:8px 0 16px 0;border-bottom:1px solid #1e293b;margin-bottom:8px;">
        <div style="font-size:18px;font-weight:800;color:#f1f5f9;letter-spacing:-0.5px;">PrecioSpy</div>
        <div style="font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:#0ea5e9;font-weight:600;">Analytics &amp; Competencia</div>
    </div>
    """, unsafe_allow_html=True)

    if st.button("Limpiar conversación", use_container_width=True):
        st.session_state.chat_history = []
        st.rerun()

    st.divider()
    st.caption("**Modelo**")
    model_choice = st.selectbox(
        "Modelo Gemini",
        ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"],
        label_visibility="collapsed",
    )

    st.divider()
    st.caption("**Tema de colores**")
    st.radio("Tema", ["Oscuro", "Claro", "Blanco y Negro"],
             key="sel_tema", label_visibility="collapsed")

# ── CSS ────────────────────────────────────────────────────────────────────────
if st.session_state.sel_tema == "Oscuro":
    st.markdown(DARK_CSS, unsafe_allow_html=True)

# ── Init Gemini ────────────────────────────────────────────────────────────────
if not GEMINI_KEY:
    st.error("No se encontró GEMINI_API_KEY. Agrega la key en Streamlit Secrets.")
    st.stop()

genai.configure(api_key=GEMINI_KEY)

# ── Historial ─────────────────────────────────────────────────────────────────
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

# ── UI ────────────────────────────────────────────────────────────────────────
st.title("Chat IA — Asistente Lasertam")
st.caption(f"Powered by Google Gemini · {model_choice} · Especializado en Lasertam")

# Mostrar historial
for msg in st.session_state.chat_history:
    with st.chat_message(msg["role"], avatar="🤖" if msg["role"] == "assistant" else "👤"):
        st.markdown(msg["content"])

# Input del usuario
if prompt := st.chat_input("Pregunta algo sobre ventas, precios, competencia..."):
    # Mostrar mensaje del usuario
    with st.chat_message("user", avatar="👤"):
        st.markdown(prompt)
    st.session_state.chat_history.append({"role": "user", "content": prompt})

    # Llamar a Gemini
    with st.chat_message("assistant", avatar="🤖"):
        with st.spinner("Pensando..."):
            try:
                model = genai.GenerativeModel(
                    model_name=model_choice,
                    system_instruction=SYSTEM_PROMPT,
                )
                # Construir historial en formato Gemini
                gemini_history = []
                for msg in st.session_state.chat_history[:-1]:
                    role = "user" if msg["role"] == "user" else "model"
                    gemini_history.append({"role": role, "parts": [msg["content"]]})

                chat = model.start_chat(history=gemini_history)
                response = chat.send_message(prompt)
                answer = response.text
            except Exception as e:
                answer = f"Error al conectar con Gemini: {e}"

        st.markdown(answer)
        st.session_state.chat_history.append({"role": "assistant", "content": answer})
