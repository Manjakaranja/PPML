import pandas as pd
import sys

def main():
    # Utilisation : python extract_row40_to_kv_csv.py <numero_ligne>
    if len(sys.argv) < 2:
        print("Usage: python extract_row40_to_kv_csv.py <numero_ligne>")
        sys.exit(1)
    try:
        line_num = int(sys.argv[1])
        if line_num < 1:
            raise ValueError
    except ValueError:
        print("Le numéro de ligne doit être un entier >= 1.")
        sys.exit(1)

    source_path = "FlightsAndMeteo.csv"
    output_path = f"FlightsAndMeteo_row{line_num}_kv.csv"

    # Lire le header
    with open(source_path, encoding="utf-8-sig") as f:
        header = f.readline().strip().split(",")

    # Charger la ligne demandée (index = line_num - 1)
    df = pd.read_csv(source_path, skiprows=line_num-1, nrows=1, header=None, encoding="utf-8-sig")
    if df.empty:
        print(f"La ligne {line_num} n'existe pas dans le fichier.")
        sys.exit(1)
    row = df.iloc[0].tolist()


    # Préparer extraction météo
    try:
        idx_actual_departure = header.index("actual_departure")
        idx_actual_arrival = header.index("actual_arrival")
        idx_origin = header.index("airport_origin")
        idx_dest = header.index("airport_destination")
    except ValueError as e:
        print("actual_departure, actual_arrival, airport_origin ou airport_destination introuvable dans le header.")
        sys.exit(1)

    actual_departure = str(row[idx_actual_departure])[:10]
    actual_arrival = str(row[idx_actual_arrival])[:10]
    airport_origin = str(row[idx_origin]).strip()
    airport_dest = str(row[idx_dest]).strip()

    # Charger le fichier météo
    meteo_path = "OutputDataMeteo/meteo_aeroports_210_derniers_jours.csv"
    try:
        meteo_df = pd.read_csv(meteo_path, sep=';', encoding="utf-8-sig")
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier météo: {e}")
        sys.exit(1)
    meteo_df.columns = [c.strip() for c in meteo_df.columns]
    meteo_df['icao'] = meteo_df['icao'].str.strip()
    meteo_df['time'] = meteo_df['time'].str[:10]

    # Pour chaque champ *_dep ou *_arr, extraire la valeur météo correspondante
    verif_col = []
    meteo_line_idx = []
    # Récupérer l'heure de X_time_dep pour le décalage
    try:
        idx_time_dep = header.index("time_dep")
        time_dep_val = row[idx_time_dep]
    except ValueError:
        time_dep_val = ""
    import re
    def extract_hour_int(val):
        m = re.search(r"(\d{2}):(\d{2})", str(val))
        return int(m.group(1)) if m else 0

    hour_offset_dep = extract_hour_int(time_dep_val)
    try:
        idx_time_arr = header.index("time_arr")
        time_arr_val = row[idx_time_arr]
    except ValueError:
        time_arr_val = ""
    hour_offset_arr = extract_hour_int(time_arr_val)

    for col, val in zip(header, row):
        if col.endswith('_dep'):
            meteo_col = col.replace('_dep', '')
            # Filtrer toutes les lignes météo du jour et aéroport
            filt = (meteo_df['time'] == actual_departure) & (meteo_df['icao'] == airport_origin)
            subset = meteo_df.loc[filt]
            if meteo_col in meteo_df.columns and not subset.empty:
                idx = hour_offset_dep if hour_offset_dep < len(subset) else 0
                val_meteo = subset.iloc[idx][meteo_col] if len(subset) > idx else ""
                verif_col.append(str(val_meteo))
                meteo_line_idx.append(str(subset.index[idx]+2) if len(subset) > idx else "")
            else:
                verif_col.append("")
                meteo_line_idx.append("")
        elif col.endswith('_arr'):
            meteo_col = col.replace('_arr', '')
            filt = (meteo_df['time'] == actual_arrival) & (meteo_df['icao'] == airport_dest)
            subset = meteo_df.loc[filt]
            if meteo_col in meteo_df.columns and not subset.empty:
                idx = hour_offset_arr if hour_offset_arr < len(subset) else 0
                val_meteo = subset.iloc[idx][meteo_col] if len(subset) > idx else ""
                verif_col.append(str(val_meteo))
                meteo_line_idx.append(str(subset.index[idx]+2) if len(subset) > idx else "")
            else:
                verif_col.append("")
                meteo_line_idx.append("")
        else:
            verif_col.append("")
            meteo_line_idx.append("")


    # Créer le DataFrame clé/valeur avec colonne verif
    kv_df = pd.DataFrame({
        "colonne": header,
        "valeur": row,
        "verif": verif_col
    })

    # Ajouter en bas la ligne spéciale pour les index météo (deux valeurs distinctes max)
    if any(meteo_line_idx):
        dep_idx = None
        arr_idx = None
        for col, idx in zip(header, meteo_line_idx):
            if col.endswith('_dep') and idx and dep_idx is None:
                dep_idx = idx
            if col.endswith('_arr') and idx and arr_idx is None:
                arr_idx = idx
            if dep_idx and arr_idx:
                break
        idxs = []
        if dep_idx:
            idxs.append(dep_idx)
        if arr_idx:
            idxs.append(arr_idx)
        kv_df = pd.concat([
            kv_df,
            pd.DataFrame({
                "colonne": ["meteo_row_index"],
                "valeur": [""],
                "verif": [";".join(idxs)]
            })
        ], ignore_index=True)

    # Ajouter les lignes time_dep et time_arr en fin de fichier

    try:
        idx_time_dep = header.index("time_dep")
        time_dep_val = row[idx_time_dep]
    except ValueError:
        time_dep_val = ""
    try:
        idx_time_arr = header.index("time_arr")
        time_arr_val = row[idx_time_arr]
    except ValueError:
        time_arr_val = ""

    # Extraire uniquement l'heure (si format YYYY-MM-DD HH:MM:SS+00:00 ou similaire)
    def extract_hour(val):
        import re
        if not val:
            return ""
        # Recherche HH:MM:SS ou HH:MM
        m = re.search(r"(\d{2}:\d{2}(?::\d{2})?)", str(val))
        return m.group(1) if m else ""

    time_dep_hour = extract_hour(time_dep_val)
    time_arr_hour = extract_hour(time_arr_val)

    # Ajout du préfixe X_ aux trois dernières lignes spéciales
    kv_df = pd.concat([
        kv_df,
        pd.DataFrame({
            "colonne": ["X_meteo_row_index"],
            "valeur": [""],
            "verif": [kv_df[kv_df['colonne'] == 'meteo_row_index']['verif'].iloc[0]] if 'meteo_row_index' in kv_df['colonne'].values else [""]
        }) if 'meteo_row_index' in kv_df['colonne'].values else pd.DataFrame(),
        pd.DataFrame({
            "colonne": ["X_time_dep"],
            "valeur": [""],
            "verif": [time_dep_hour]
        }),
        pd.DataFrame({
            "colonne": ["X_time_arr"],
            "valeur": [""],
            "verif": [time_arr_hour]
        })
    ], ignore_index=True)

    # Supprimer l'ancienne ligne meteo_row_index si présente
    kv_df = kv_df[kv_df['colonne'] != 'meteo_row_index']

    # Sauvegarder le résultat
    kv_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Fichier généré : {output_path}")

if __name__ == "__main__":
    main()
