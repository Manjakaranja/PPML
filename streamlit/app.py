import streamlit as st
from streamlit.components.v1 import html
import pandas as pd
import time
import re
import requests
from streamlit_lottie import st_lottie
import json

# ====================== CONFIG ======================
st.set_page_config(
    page_title="Fly On Time",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded",
)

API_BASE = "https://stoneray-ppml-fastapi.hf.space"
API_URL = f"{API_BASE}/predict"


# ====================== LOAD LOTTIE ======================
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)


lottie_plane = load_lottiefile("anims/plane.json")  # Animation dans la sidebar
lottie_loader = load_lottiefile("anims/loader_plane.json")  # Pour le chargement

# ====================== CUSTOM CSS ======================
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Orbitron:wght@500;600;700&family=Inter:wght@400;500&display=swap');

        .stApp {
            background-color: #010e27;
        }

        .main-title {
            font-family: 'Orbitron', sans-serif;
            font-size: 4.1rem;
            font-weight: 700;
            color: #ffffff;
            text-align: center;
            margin-bottom: 0.4rem;
            letter-spacing: 3px;
            text-shadow: 0 4px 20px rgba(75, 232, 224, 0.25);
        }

        .subtitle {
            font-family: 'Inter', sans-serif;
            color: #6b98b6;
            text-align: center;
            font-size: 1.45rem;
            margin-bottom: 2rem;
            letter-spacing: 1.5px;
        }

        .stMetric {
            background-color: #031538;
            padding: 1rem;
            border-radius: 12px;
            border: 1px solid #4d74c3;
        }
        .stMetric label {
            color: #d4d4d4;
        }
        hr {
            border-color: #4d74c3;
        }
    </style>
""",
    unsafe_allow_html=True,
)

# ====================== HEADER AVEC IMAGE DE FOND ======================
st.markdown(
    f"""
    <style>
        .header-bg {{
            position: relative;
            height: 420px;
            background-image: url('https://ppml2026.s3.eu-north-1.amazonaws.com/logo_flyOnTime.png');
            background-size: cover;
            background-position: center;
            border-radius: 12px;
            margin-bottom: 2rem;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.7);
        }}
        .header-overlay {{
            position: absolute;
            inset: 0;
            background: linear-gradient(rgba(1,14,39,0.75), rgba(1,14,39,0.90));
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            text-align: center;
        }}
    </style>

    <div class="header-bg">
        <div class="header-overlay">
            <h1 class="main-title">FlyOnTime</h1>
            <p class="subtitle">La data au cœur du tarmac.</p>
        </div>
    </div>
""",
    unsafe_allow_html=True,
)

# ====================== SIDEBAR AVEC ANIMATION LOTTIE ======================

# ====================== SIDEBAR ======================
with st.sidebar:
    st.header("🛫 Navigation")

    if lottie_plane:
        st_lottie(lottie_plane, height=160, speed=0.8, key="sidebar_plane")

    st.markdown("**Analyse de vol**")
    st.markdown("• Prédiction en temps réel")
    st.markdown("• Causes probables de retard")
    st.markdown("---")

    st.caption("Made with 💙 by PPML team")


# ====================== FORMULAIRE ======================
st.subheader("🔍 Analyse de votre vol")

with st.form("vol_form", clear_on_submit=True):
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        flight_number = st.text_input(
            "Numéro de vol",
            placeholder="AF1234 ou BA 456",
            max_chars=8,
            help="Ex: AF1234, LH12, FR4567",
        )

    with col2:
        col_date, col_time = st.columns(2)
        with col_date:
            selected_date = st.date_input(
                "Date du vol", value=pd.to_datetime("2026-04-22").date()
            )
        with col_time:
            selected_time = st.time_input(
                "Heure de départ",
                value=pd.to_datetime("14:30").time(),
                step=pd.Timedelta(minutes=5),
            )

    selected_datetime = pd.to_datetime(f"{selected_date} {selected_time}")

    with col3:
        departure_airport = st.selectbox(
            "Aéroport de départ",
            [
                "CDG - Paris Charles de Gaulle",
                "ORY - Paris Orly",
                "NCE - Nice Côte d'Azur",
                "LYS - Lyon Saint-Exupéry",
                "MRS - Marseille Provence",
            ],
        )

    with col4:
        arrival_airport = st.selectbox(
            "Aéroport d'arrivée",
            [
                "CDG - Paris Charles de Gaulle",
                "ORY - Paris Orly",
                "NCE - Nice Côte d'Azur",
                "LYS - Lyon Saint-Exupéry",
                "MRS - Marseille Provence",
            ],
        )

    submitted = st.form_submit_button("Analyser le vol", use_container_width=True)

    if submitted:
        flight = flight_number.strip().upper()
        pattern = r"^[A-Z0-9]{1,3}\s?\d{1,4}$"
        inputs_valides = True
        if not flight or not re.match(pattern, flight):
            st.error("Format de numéro de vol invalide. Exemples : AF1234, BA 456")
            inputs_valides = False
        if departure_airport == arrival_airport:
            st.error("Selectionnez un aéroport de départ et d'arrivée différent svp")
            inputs_valides = False
        if inputs_valides:
            payload = {
                "flight_number": flight,
                "date": selected_datetime.isoformat(),
                "departure_airport": departure_airport,
                "arrival_airport": arrival_airport,
            }
            # ====================== LOADING AVEC LOTTIE (CORRIGÉ) ======================
            loading_placeholder = st.empty()

            with loading_placeholder.container():
                col_c, col_d, col_e = st.columns([1, 3, 1])
                with col_d:
                    if lottie_loader:
                        st_lottie(lottie_loader, height=170, speed=0.95, key="analysis")
                    st.markdown(
                        """
                        <div style="text-align:center; margin-top:10px;">
                            <p style="color:#4be8e0; font-size:1.3rem;">Analyse en cours...</p>
                            <p style="color:#6b98b6;">Croisement météo + historique des vols</p>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

            # ====================== APPEL API ======================
            try:
                response = requests.post(API_URL, json=payload, timeout=15)
                response.raise_for_status()
                result = response.json()
            except Exception as e:
                loading_placeholder.empty()
                st.error(f"Erreur lors de l'appel API : {e}")
                st.stop()

            # On enlève l'animation de chargement une fois l'API terminée
            loading_placeholder.empty()

            # ====================== MOCK RESULTATS ======================
            #  varier ces valeurs pour tester les différents cas
            # delay_minutes = 42  # valeur pour tester (15, 35, 75...)
            # delay_prob = 0.76
            delay_minutes = result.get(
                "predicted_arrival_delay_minutes"
            )  # valeur par défaut pour test
            delay_prob = result.get("delay_probability")  # valeur par défaut pour test
            if delay_minutes >= 60:
                risk_color = "risk-high"
                risk_label = "🔴 Retard important"
                risk_text = " mieux vaut anticiper un possible décalage"
            elif delay_minutes >= 30:
                risk_color = "risk-medium"
                risk_label = "🟠 Retard modéré"
                risk_text = "une légère perturbation reste possible"
            else:
                risk_color = "risk-low"
                risk_label = "🟢 Retard faible ou à l'heure"
                risk_text = "retard peu probable sur ce vol"

            # ====================== RÉSULTATS (style cartes aviation) ======================
            st.markdown("---")
            st.subheader("📊 Résultat de l'analyse")

            col_res1, col_res2 = st.columns(2)

            with col_res1:
                st.metric(
                    label="Retard estimé à l'arrivée",
                    value=f"{round(delay_minutes)} min",
                    delta=risk_label,
                )

            with col_res2:
                st.metric(label="Niveau de risque", value=risk_label)

            # ====================== ZONE D'INFORMATIONS REPLIABLE ======================
            with st.expander("📋 Détails complets du vol analysé", expanded=True):
                st.markdown("**✈️ Vol analysé :** " + f"**{flight}**")

                col1, col2 = st.columns(2)

                with col1:
                    st.write(
                        "**Date et heure :**",
                        selected_date.strftime("%d/%m/%Y à %H:%M"),
                    )
                    st.write("**Aéroport de départ :**", departure_airport)
                    st.write("**Aéroport d'arrivée :**", arrival_airport)

                with col2:
                    st.write("**Probabilité de retard :**", f"**{delay_prob:.1%}**")
                    # on arrondit a la minute
                    st.write(
                        "**Retard estimé :**", f"**{round(delay_minutes)} minutes**"
                    )

                # Niveau de risque
                st.write("**Niveau de risque :**", f"**{risk_label}**")
            # Petite section "Causes probables" (mock – très utile visuellement)
            st.subheader("🔎 Causes probables identifiées")
            causes = [
                "Météo orageuse à destination",
                "Congestion au départ (CDG)",
                "Rotation équipage serrée",
            ]
            for cause in causes:
                st.markdown(f"• {cause}")

# ====================== FOOTER ======================
st.markdown("---")

st.markdown("📌 **FlyOnTime MVP** • Données live On Demand • ")
