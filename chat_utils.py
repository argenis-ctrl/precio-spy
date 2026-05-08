"""
Widget de chat con Gemini — aparece en sidebar de todas las páginas.
"""

import time
import streamlit as st

SYSTEM_PROMPT = """Eres un asistente de negocio especializado para Lasertam,
una clínica de depilación láser en Chile. Ayudas al equipo con:
- Análisis de ventas y métricas del negocio
- Estrategias de marketing y precios
- Comparación con competidores (Belenus, Cela, etc.)
- Consultas sobre servicios de depilación láser
- Redacción de copy publicitario para redes sociales
- Interpretación de datos del dashboard PrecioSpy
Responde siempre en español, de forma clara, directa y concisa.
Cuando hables de precios usa formato CLP ($9.990)."""


def _get_gemini_key() -> str:
    try:
        return st.secrets["GEMINI_API_KEY"]
    except Exception:
        import os
        from dotenv import load_dotenv
        load_dotenv()
        return os.getenv("GEMINI_API_KEY", "")


def _call_gemini(prompt: str, history: list, model_name: str) -> str:
    try:
        import google.generativeai as genai
        key = _get_gemini_key()
        if not key:
            return "⚠️ API key de Gemini no configurada."
        genai.configure(api_key=key)
        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=SYSTEM_PROMPT,
        )
        gemini_history = []
        for msg in history[:-1]:
            role = "user" if msg["role"] == "user" else "model"
            gemini_history.append({"role": role, "parts": [msg["content"]]})
        chat = model.start_chat(history=gemini_history)
        resp = chat.send_message(prompt)
        return resp.text
    except Exception as e:
        err = str(e)
        if "429" in err or "quota" in err.lower():
            return (
                "⚠️ Cuota de Gemini agotada por ahora. Espera unos minutos e intenta de nuevo, "
                "o revisa tu plan en [Google AI Studio](https://aistudio.google.com)."
            )
        return f"⚠️ Error: {err[:200]}"


def render_sidebar_chat():
    """Renderiza el widget de chat en el sidebar. Llamar dentro de `with st.sidebar:`."""

    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []
    if "chat_model" not in st.session_state:
        st.session_state.chat_model = "gemini-2.0-flash"

    st.divider()

    with st.expander("💬 Chat IA", expanded=st.session_state.get("chat_open", False)):
        st.session_state.chat_open = True

        # Selector de modelo
        st.session_state.chat_model = st.selectbox(
            "Modelo",
            ["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"],
            index=["gemini-2.0-flash", "gemini-1.5-flash", "gemini-1.5-pro"].index(
                st.session_state.chat_model
            ),
            label_visibility="collapsed",
            key="chat_model_select",
        )

        # Historial (últimos 6 mensajes para no saturar la barra)
        history = st.session_state.chat_history
        if history:
            for msg in history[-6:]:
                icon = "🤖" if msg["role"] == "assistant" else "👤"
                role_label = "**Gemini**" if msg["role"] == "assistant" else "**Tú**"
                st.markdown(
                    f"""<div style="margin:4px 0;padding:8px 10px;border-radius:8px;
                    background:{'#1e293b' if msg['role']=='assistant' else '#0f172a'};
                    border-left:3px solid {'#8b5cf6' if msg['role']=='assistant' else '#06b6d4'};
                    font-size:12.5px;color:#e2e8f0;line-height:1.5;">
                    {icon} {role_label}<br>{msg['content']}</div>""",
                    unsafe_allow_html=True,
                )
            st.markdown("")

        # Input
        user_input = st.text_area(
            "Mensaje",
            placeholder="Pregunta sobre ventas, precios, competencia...",
            height=80,
            key="sidebar_chat_input",
            label_visibility="collapsed",
        )

        col1, col2 = st.columns([3, 1])
        send = col1.button("Enviar", type="primary", use_container_width=True, key="chat_send_btn")
        col2.button("🗑", use_container_width=True, key="chat_clear_btn",
                    on_click=lambda: st.session_state.update({"chat_history": []}))

        if send and user_input.strip():
            st.session_state.chat_history.append({"role": "user", "content": user_input.strip()})
            with st.spinner("Pensando..."):
                answer = _call_gemini(
                    user_input.strip(),
                    st.session_state.chat_history,
                    st.session_state.chat_model,
                )
            st.session_state.chat_history.append({"role": "assistant", "content": answer})
            st.rerun()
