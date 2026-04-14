"""
db_vols_aeroports.py
Gestion de la table vols_aeroports : création + insertion sans doublons.
Corrections apportées :
  - URL.create() pour encoder les caractères spéciaux du mot de passe (ex: !)
  - Indentation du try/except corrigée
  - INSERT ON CONFLICT DO NOTHING pour vraie gestion des doublons
  - sslmode + pool_pre_ping dans create_engine
  - TABLE_NAME déclaré une seule fois
  - Travail sur une copie du DataFrame (pas de mutation)
  - Statements SQL séparés dans init
"""

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.engine import URL
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ─────────────────────────────────────────────
# 1. CONNEXION
# ─────────────────────────────────────────────

load_dotenv()

DB_USER     = os.getenv("PRJ_CDG_USER_ADMIN")
DB_PASSWORD = os.getenv("PRJ_CDG_PASS_ADMIN")
DB_HOST     = os.getenv("PRJ_CDG_HOST_ADMIN")
DB_PORT     = os.getenv("PORT", "5432")          # fallback sur le port standard
DB_NAME     = os.getenv("PRJ_CDG_DBNAME_ADMIN")

# URL.create() encode automatiquement les caractères spéciaux du mot de passe
# (ex: ! → %21), ce que le f-string classique ne fait pas
connection_url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
    query={"sslmode": "require"},
)

engine = create_engine(
    connection_url,
    pool_pre_ping=True,   # évite les connexions idle mortes (indispensable avec NeonDB)
)


# ─────────────────────────────────────────────
# 2. CRÉATION DE LA TABLE
# ─────────────────────────────────────────────

TABLE_NAME = "vols_aeroports"   # déclaré une seule fois, réutilisé partout

def init_table_vols_aeroports(engine):
    """Crée la table et ses index si inexistants."""

    req_create_table = text("""
        CREATE TABLE IF NOT EXISTS vols_aeroports (
            id                SERIAL PRIMARY KEY,
            icao              TEXT NOT NULL,
            type              TEXT NOT NULL,
            flight_number     TEXT,
            status            TEXT,
            airline           TEXT,
            scheduled_utc     TIMESTAMPTZ,
            revised_utc       TIMESTAMPTZ,
            runway_utc        TIMESTAMPTZ,
            delay_minutes     DOUBLE PRECISION,
            terminal_dep      TEXT,
            terminal_arr      TEXT,
            destination_icao  TEXT,

            CONSTRAINT unique_vol
                UNIQUE (icao, flight_number, scheduled_utc, type)
        );
    """)

    # Statements séparés → erreur fine si un index pose problème
    req_idx_icao = text("""
        CREATE INDEX IF NOT EXISTS idx_vols_icao_scheduled
            ON vols_aeroports (icao, scheduled_utc);
    """)

    req_idx_flight = text("""
        CREATE INDEX IF NOT EXISTS idx_vols_flight_number
            ON vols_aeroports (flight_number);
    """)

    with engine.connect() as conn:
        conn.execute(req_create_table)
        conn.execute(req_idx_icao)
        conn.execute(req_idx_flight)
        conn.commit()

    print(f"Table '{TABLE_NAME}' créée si inexistante, avec contrainte UNIQUE.")
    print(f"   Clé unique : (icao, flight_number, scheduled_utc, type)\n")


# ─────────────────────────────────────────────
# 3. INSERTION SANS DOUBLONS
# ─────────────────────────────────────────────

def inserer_vols_aeroports(engine, df_vols):
    """
    Insère les vols dans la table en ignorant les doublons.
    Utilise INSERT ... ON CONFLICT DO NOTHING pour une vraie gestion
    des doublons ligne par ligne (contrairement à to_sql qui annule
    tout le batch à la première violation de contrainte UNIQUE).
    """

    if df_vols.empty:
        print("Aucune donnée à insérer.")
        return

    # Copie défensive : on ne modifie pas le DataFrame original
    df = df_vols.copy()

    # Conversion des timestamps en UTC pour SQLAlchemy/PostgreSQL
    for col in ["scheduled_utc", "revised_utc", "runway_utc"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    print(f"\nTentative d'insertion de {len(df):,} lignes dans '{TABLE_NAME}'...")

    try:
        # Reflection de la table pour construire le statement typé
        metadata = MetaData()
        table = Table(TABLE_NAME, metadata, autoload_with=engine)

        records = df.to_dict(orient="records")

        with engine.connect() as conn:
            # INSERT ... ON CONFLICT DO NOTHING
            # → seules les lignes vraiment nouvelles sont insérées,
            #   les doublons sont ignorés silencieusement, sans rollback.
            stmt = (
                pg_insert(table)
                .values(records)
                .on_conflict_do_nothing(constraint="unique_vol")
            )
            result = conn.execute(stmt)
            conn.commit()

        nb_inseres = result.rowcount
        nb_ignores = len(df) - nb_inseres
        print(f"✅ Insertion terminée avec succès !")
        print(f"   {nb_inseres:,} ligne(s) insérée(s).")
        if nb_ignores:
            print(f"   {nb_ignores:,} doublon(s) ignoré(s) (contrainte UNIQUE).")

    except Exception as e:
        print(f"\n❌ Erreur inattendue lors de l'insertion : {e}")
        raise

    print("\n=== Fin SQL ===")


# ─────────────────────────────────────────────
# 4. POINT D'ENTRÉE
# ─────────────────────────────────────────────

if __name__ == "__main__":
    df_vols = pd.read_parquet("AeroDataBox_vols_60jours.parquet")
    init_table_vols_aeroports(engine)
    inserer_vols_aeroports(engine, df_vols)
