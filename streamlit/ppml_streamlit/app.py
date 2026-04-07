# streamlit_app.py

import streamlit as st
import pandas as pd
import requests
import numpy as np
from datetime import datetime, date
from dotenv import load_dotenv
import os
import boto3

# Charger les variables d'environnement
load_dotenv()

st.set_page_config(page_title="PPML - Prédiction Retards", layout="wide")

st.title("✈️ Prédiction des Retards Aériens")
st.markdown("### API FastAPI + Pipeline complet")

# API_URL = "https://patjedhahf-ppml-api-hf.hf.space/predict"
API_URL = "https://patjedhahf-ppml-api-hf.hf.space/predire_et_sauvegarder"

# Mapping ICAO → Nom complet
airport_names = {
    "LFPG": "CDG - Paris Charles de Gaulle",
    "LFPO": "ORY - Paris Orly",
    "LFMN": "NCE - Nice Côte d'Azur",
    "LFLL": "LYS - Lyon Saint-Exupéry",
    "LFML": "MRS - Marseille Provence",
}


# ==================== CHARGEMENT DES DONNÉES ====================


@st.cache_data(show_spinner=True, ttl=600)
def load_future_data_from_s3():
    bucket_name = (os.getenv("BUCKET"),)
    try:
        session = boto3.Session(
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION", "eu-north-1"),
        )
        bucket = os.getenv("BUCKET")
        s3_key = "datasets/vue_future.parquet"
        df = pd.read_parquet(f"s3://{bucket}/{s3_key}")
        # Nettoyage léger
        if "delay_minutes" in df.columns:
            df["delay_minutes"] = df["delay_minutes"].where(
                df["delay_minutes"].isna(), None
            )

        df["airport_name"] = df["icao"].map(airport_names).fillna(df["icao"])

        st.success(f"✅ {len(df):,} vols chargés depuis S3")
        return df
    except Exception as e:
        st.error(f"❌ Erreur chargement depuis S3 : {e}")
        st.info(
            "Vérifiez que le bucket S3 est accessible et que les credentials sont corrects."
        )
        return pd.DataFrame()


df_future = load_future_data_from_s3()

if df_future.empty:
    st.stop()


def make_json_serializable(record: dict) -> dict:
    """Convertit tous les types non JSON serializable (Timestamp, date, NaN, numpy types, etc.)"""
    clean_record = {}
    for key, value in record.items():
        if pd.isna(value):
            clean_record[key] = None
        elif isinstance(value, (pd.Timestamp, datetime, date)):
            clean_record[key] = value.isoformat()
        elif isinstance(value, (np.integer, np.floating)):
            clean_record[key] = value.item()
        elif isinstance(value, np.ndarray):
            clean_record[key] = value.tolist()
        else:
            clean_record[key] = value
    return clean_record


# ==================== MODE : UN SEUL VOL ====================
st.sidebar.header("Mode de prédiction")

mode = st.sidebar.radio(
    "Que voulez-vous faire ?",
    ["Prédire un seul vol", "Prédire plusieurs vols"],
    key="predict_mode",
)

if mode == "Prédire un seul vol":
    st.sidebar.subheader("Sélectionnez un vol")

    df_future["display"] = df_future.apply(
        lambda row: (
            f"{row['airport_name']} | {row['type'].upper()} | {row['airline']} | Heure: {row.get('scheduled_hour')} | ID: {row['mouvement_id']}"
        ),
        axis=1,
    )

    selected_display = st.sidebar.selectbox(
        "Choisissez un vol", options=df_future["display"].tolist()
    )

    if st.sidebar.button("🔮 Prédire ce vol", type="primary"):
        selected_row = df_future[df_future["display"] == selected_display].iloc[0]

        record = make_json_serializable(selected_row.to_dict())
        payload = {"records": [record]}

        with st.spinner("Prédiction en cours..."):
            try:
                response = requests.post(API_URL, json=payload)

                if response.status_code == 200:
                    data = response.json()
                    pred = data["predictions"][0]

                    st.success(
                        f"**Retard prédit : {pred['predicted_delay_minutes']:.2f} minutes**"
                    )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric(
                            "Aéroport",
                            f"{pred['icao']} - {airport_names.get(pred['icao'], '')}",
                        )
                    with col2:
                        st.metric("Type", pred["type"])
                    with col3:
                        st.metric("Heure", f"{pred['scheduled_hour']}h")

                    st.info(f"**Compagnie** : {pred['airline']}")
                    st.info(
                        f"**Terminal départ** : {pred.get('terminal_dep', 'N/A')} | **Terminal arrivée** : {pred.get('terminal_arr', 'N/A')}"
                    )
                else:
                    st.error(f"Erreur API ({response.status_code}) : {response.text}")
            except Exception as e:
                st.error(f"Erreur de connexion : {e}")

# ==================== MODE : PLUSIEURS VOLS ====================
else:
    st.sidebar.subheader("Filtres pour sélection multiple")

    selected_airports = st.sidebar.multiselect(
        "Aéroports",
        options=sorted(df_future["airport_name"].unique()),
        default=sorted(df_future["airport_name"].unique()),
    )

    selected_airlines = st.sidebar.multiselect(
        "Compagnies", options=sorted(df_future["airline"].unique()), default=[]
    )

    selected_types = st.sidebar.multiselect(
        "Type de vol",
        options=["arrival", "departure"],
        default=["arrival", "departure"],
    )

    filtered_df = df_future.copy()
    if selected_airports:
        filtered_df = filtered_df[filtered_df["airport_name"].isin(selected_airports)]
    if selected_airlines:
        filtered_df = filtered_df[filtered_df["airline"].isin(selected_airlines)]
    if selected_types:
        filtered_df = filtered_df[filtered_df["type"].isin(selected_types)]

    st.sidebar.write(f"**{len(filtered_df)} vols** sélectionnés")

    if st.sidebar.button("🔮 Prédire les vols sélectionnés", type="primary"):
        if len(filtered_df) == 0:
            st.warning("Aucun vol ne correspond aux filtres.")
        else:
            records = [
                make_json_serializable(row.to_dict())
                for _, row in filtered_df.iterrows()
            ]
            payload = {"records": records}

            with st.spinner(f"Prédiction de {len(records)} vols en cours..."):
                try:
                    response = requests.post(API_URL, json=payload)

                    if response.status_code == 200:
                        data = response.json()
                        preds = data["predictions"]

                        st.success(
                            f"✅ {data['total_predictions']} prédictions générées avec succès !"
                        )
                        st.info(
                            f"📊 Envoyés : {len(records)} | Retournés : {data['total_predictions']}"
                        )

                        # Création du DataFrame avec plus de précision
                        display_data = []
                        for p in preds:
                            # Gestion de l'heure précise
                            heure_affichee = p.get("scheduled_hour", "—")
                            date_affichee = ""

                            # Si on a scheduled_utc (format ISO), on l'utilise pour plus de précision
                            if p.get("scheduled_utc"):
                                try:
                                    date_affichee = p["scheduled_utc"]
                                except:
                                    pass

                            delay = p["predicted_delay_minutes"]

                            display_data.append(
                                {
                                    "ID": p.get("mouvement_id", "—"),
                                    "Aéroport": p["icao"],
                                    "Nom Aéroport": airport_names.get(
                                        p["icao"], p["icao"]
                                    ),
                                    "Type": p["type"].capitalize(),
                                    "Compagnie": p["airline"],
                                    "Heure": heure_affichee,
                                    "Date": date_affichee,
                                    "Retard prédit (min)": round(delay, 1),
                                    "Gravité": "🔴 Important"
                                    if delay >= 60
                                    else "🟠 Modéré"
                                    if delay >= 30
                                    else "🟡 Faible"
                                    if delay >= 15
                                    else "🟢 Mineur",
                                }
                            )

                        df_display = pd.DataFrame(display_data)

                        # Tri par retard décroissant
                        df_display = df_display.sort_values(
                            by="Retard prédit (min)", ascending=False
                        )

                        # Affichage du tableau
                        st.dataframe(
                            df_display,
                            use_container_width=True,
                            height=650,
                            hide_index=True,
                        )

                        # Statistiques
                        avg = df_display["Retard prédit (min)"].mean()
                        med = df_display["Retard prédit (min)"].median()
                        high = (df_display["Retard prédit (min)"] > 30).sum()
                        very_high = (df_display["Retard prédit (min)"] > 60).sum()

                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Moyenne", f"{avg:.1f} min")
                        col2.metric("Médiane", f"{med:.1f} min")
                        col3.metric("Vols > 30 min", high)
                        col4.metric("Vols > 60 min", very_high)

                    else:
                        st.error(
                            f"Erreur API ({response.status_code}) : {response.text}"
                        )

                except Exception as e:
                    st.error(f"Erreur lors de la requête : {e}")

st.caption("API FastAPI • Pipeline complet chargé depuis MLflow")
