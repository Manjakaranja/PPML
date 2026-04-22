import os
import sys
import pandas as pd


def get_request_dir() -> str:
    return os.getenv("REQUEST_DIR", ".")


def get_source_path() -> str:
    request_dir = get_request_dir()
    return os.path.join(request_dir, "FlightsAndMeteoAndJFVacancesAndGreves_Single.csv")


def build_output_path(line_num: int) -> str:
    request_dir = get_request_dir()
    return os.path.join(
        request_dir,
        f"FlightsAndMeteoAndJFVacancesAndGreves_Single_row{line_num}_kv.csv"
    )


def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_row_FULL_to_kv_csv.py <numero_ligne>")
        sys.exit(1)

    try:
        line_num = int(sys.argv[1])
        if line_num < 1:
            raise ValueError
    except ValueError:
        print("Le numéro de ligne doit être un entier >= 1.")
        sys.exit(1)

    source_path = get_source_path()
    output_path = build_output_path(line_num)

    if not os.path.exists(source_path):
        print(f"Fichier source introuvable : {source_path}")
        sys.exit(1)

    # Lire le fichier complet proprement
    df_source = pd.read_csv(source_path, encoding="utf-8-sig")

    if line_num > len(df_source):
        print(f"La ligne {line_num} n'existe pas dans le fichier.")
        sys.exit(1)

    row = df_source.iloc[line_num - 1]

    kv_df = pd.DataFrame({
        "Nom de colonne": df_source.columns.tolist(),
        "valeur": row.tolist()
    })

    kv_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"Fichier généré : {output_path}")


if __name__ == "__main__":
    main()