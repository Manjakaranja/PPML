import glob
import os
import shutil


def clean_files_in_dir(directory: str, patterns: list[str]):
    if not directory or not os.path.exists(directory):
        return

    for pattern in patterns:
        for f in glob.glob(os.path.join(directory, pattern)):
            try:
                if os.path.isfile(f):
                    os.remove(f)
                    print(f"Fichier supprimé : {f}")
            except Exception as e:
                print(f"Erreur lors de la suppression de {f} : {e}")


def clean_request_dir():
    request_dir = os.getenv("REQUEST_DIR")
    output_single = os.getenv("REQUEST_OUTPUT_SINGLE")
    output_meteo = os.getenv("REQUEST_OUTPUT_METEO")
    output_greves = os.getenv("REQUEST_OUTPUT_GREVES")
    output_jf = os.getenv("REQUEST_OUTPUT_JF")

    if not request_dir:
        print("REQUEST_DIR absent -> fallback ancien comportement désactivé par sécurité.")
        return

    os.makedirs(request_dir, exist_ok=True)

    for d in [output_single, output_meteo, output_greves, output_jf]:
        if d:
            os.makedirs(d, exist_ok=True)

    patterns = ["*.csv", "*.json", "*.parquet"]

    clean_files_in_dir(request_dir, patterns)
    clean_files_in_dir(output_single, patterns)
    clean_files_in_dir(output_meteo, patterns)
    clean_files_in_dir(output_greves, patterns)
    clean_files_in_dir(output_jf, patterns)

    # Supprime aussi d'éventuels sous-dossiers résiduels dans REQUEST_DIR
    for item in glob.glob(os.path.join(request_dir, "*")):
        try:
            if os.path.isdir(item) and item not in {
                output_single,
                output_meteo,
                output_greves,
                output_jf,
            }:
                shutil.rmtree(item)
                print(f"Dossier supprimé : {item}")
        except Exception as e:
            print(f"Erreur lors de la suppression de {item} : {e}")


if __name__ == "__main__":
    clean_request_dir()