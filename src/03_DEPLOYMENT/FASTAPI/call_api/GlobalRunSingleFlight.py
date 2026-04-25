import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
FLIGHT_SELECTION_PATH = BASE_DIR / "flight_selection.json"

OUTPUT_ROOT = BASE_DIR / "output"

LEGACY_OUTPUT_SINGLE = BASE_DIR / "OutputSingleFlight"
LEGACY_OUTPUT_GREVES = BASE_DIR / "OutputDataGreves"
LEGACY_OUTPUT_METEO = BASE_DIR / "OutputDataMeteo"
LEGACY_OUTPUT_JF = BASE_DIR / "OutputJFVacances"

ENABLE_S3_UPLOAD = os.getenv("ENABLE_S3_UPLOAD", "0") == "1"


def safe_load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def parse_input():
    args = sys.argv[1:]

    if len(args) >= 3:
        flight_number = args[0].strip()
        selected_date_raw = args[1].strip()
        departure_airport = args[2].strip()
        arrival_airport = args[3].strip() if len(args) >= 4 else ""
    else:
        flight_data = safe_load_json(FLIGHT_SELECTION_PATH)
        flight_number = str(flight_data.get("flight_number", "")).strip()
        selected_date_raw = str(flight_data.get("selected_date", flight_data.get("date", ""))).strip()
        departure_airport = str(flight_data.get("departure_airport", "")).strip()
        arrival_airport = str(flight_data.get("arrival_airport", "")).strip()

    return flight_number, selected_date_raw, departure_airport, arrival_airport


def normalize_date_and_hour(selected_date_raw: str):
    if not selected_date_raw:
        return "", ""

    selected_date_raw = selected_date_raw.replace("T", " ").strip()

    try:
        dt = datetime.fromisoformat(selected_date_raw)
        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")
    except ValueError:
        pass

    parts = selected_date_raw.split()
    flight_date = parts[0] if len(parts) >= 1 else ""
    hour = parts[1] if len(parts) >= 2 else ""

    return flight_date, hour


def normalize_flight_number(value: str) -> str:
    if not value:
        return ""
    return str(value).replace(" ", "").upper().strip()


def build_request_id(flight_number: str) -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"requete_{normalize_flight_number(flight_number)}_{ts}"


def ensure_clean_dir(path: Path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def prepare_request_dirs(request_id: str):
    request_dir = OUTPUT_ROOT / request_id
    ensure_clean_dir(request_dir)

    dirs = {
        "request_dir": request_dir,
        "single": request_dir / "OutputSingleFlight",
        "greves": request_dir / "OutputDataGreves",
        "meteo": request_dir / "OutputDataMeteo",
        "jf": request_dir / "OutputJFVacances",
    }

    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    return dirs


def resolve_script_path(script_name: str) -> Path:
    return BASE_DIR / script_name


def run_script(cmd_args: list[str], env: dict):
    script_name = cmd_args[0]
    script_path = resolve_script_path(script_name)

    if not script_path.exists():
        raise FileNotFoundError(f"Script introuvable : {script_path}")

    cmd = [sys.executable, str(script_path)] + cmd_args[1:]

    print(f"\n=== RUN === {' '.join(cmd)}")
    completed = subprocess.run(
        cmd,
        check=True,
        cwd=str(BASE_DIR),
        env=env,
    )
    print(f"=== OK === {script_name}")
    return completed


def get_latest_single_flight_csv(search_dir: Path) -> Path:
    candidates = sorted(
        [
            p for p in search_dir.glob("SingleFlightData_*.csv")
            if "RespAPI" not in p.name and "ColList" not in p.name
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not candidates:
        raise FileNotFoundError(f"Aucun fichier SingleFlightData_*.csv trouvé dans {search_dir}")

    return candidates[0]


def extract_destination_from_single_flight_csv(search_dir: Path) -> str:
    csv_path = get_latest_single_flight_csv(search_dir)
    print(f"[INFO] Lecture du fichier single flight : {csv_path}")

    try:
        df = pd.read_csv(csv_path)
    except Exception:
        df = pd.read_csv(csv_path, sep=";")

    if df.empty:
        raise ValueError(f"Le fichier {csv_path} est vide")

    possible_cols = [
        "airport_destination",
        "arrival_airport",
        "destination_airport",
        "airport_arrival",
    ]

    destination_col = None
    for col in possible_cols:
        if col in df.columns:
            destination_col = col
            break

    if destination_col is None:
        raise KeyError(f"Aucune colonne destination trouvée dans {csv_path}. Colonnes: {df.columns.tolist()}")

    destination = str(df.iloc[0][destination_col]).strip()

    if not destination:
        raise ValueError(f"Destination vide dans la colonne {destination_col} du fichier {csv_path}")

    print(f"[INFO] Destination trouvée automatiquement : {destination}")
    return destination


def move_file_if_exists(src: Path, dst: Path):
    if src.exists() and src.is_file():
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))


def move_dir_contents(src_dir: Path, dst_dir: Path):
    if not src_dir.exists():
        return

    dst_dir.mkdir(parents=True, exist_ok=True)

    for item in src_dir.iterdir():
        target = dst_dir / item.name
        if item.is_dir():
            if target.exists():
                shutil.rmtree(target)
            shutil.move(str(item), str(target))
        else:
            if target.exists():
                target.unlink()
            shutil.move(str(item), str(target))


def collect_legacy_outputs(request_dirs: dict):
    move_dir_contents(LEGACY_OUTPUT_SINGLE, request_dirs["single"])
    move_dir_contents(LEGACY_OUTPUT_GREVES, request_dirs["greves"])
    move_dir_contents(LEGACY_OUTPUT_METEO, request_dirs["meteo"])
    move_dir_contents(LEGACY_OUTPUT_JF, request_dirs["jf"])

    flat_files = [
        "FlightsAndMeteo_Single.csv",
        "FlightsAndMeteoAndJFVacances_Single.csv",
        "FlightsAndMeteoAndJFVacancesAndGreves_Single.csv",
    ]

    for filename in flat_files:
        src = BASE_DIR / filename
        dst = request_dirs["request_dir"] / filename
        move_file_if_exists(src, dst)


def convert_signoff_csv_to_parquet(request_dir: Path, request_id: str) -> Path:
    candidates = sorted(
        [
            p for p in request_dir.glob("SignoffFlightsDataset_Single*.csv")
            if "ColList" not in p.name
        ],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not candidates:
        single_candidates = sorted(
            [
                p for p in (request_dir / "OutputSingleFlight").glob("SignoffFlightsDataset_Single*.csv")
                if "ColList" not in p.name
            ],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        candidates = single_candidates

    if not candidates:
        raise FileNotFoundError(f"Aucun fichier SignoffFlightsDataset_Single*.csv exploitable trouvé dans {request_dir}")

    latest_csv = candidates[0]
    parquet_path = request_dir / f"SignoffFlightsDataset_Single_{request_id}.parquet"

    print(f"[INFO] Conversion CSV -> Parquet : {latest_csv} -> {parquet_path}")
    df = pd.read_csv(latest_csv)
    df.to_parquet(parquet_path, index=False)

    return parquet_path


def get_status_file_path(request_dirs: dict) -> Path:
    return request_dirs["request_dir"] / "flight_request_status.json"


def read_request_status(request_dirs: dict) -> dict:
    status_path = get_status_file_path(request_dirs)
    if not status_path.exists():
        return {}

    with open(status_path, "r", encoding="utf-8") as f:
        return json.load(f)


def is_blocking_status(status_payload: dict) -> bool:
    return status_payload.get("status") in {
        "error_flight_not_found",
        "error_time_mismatch",
        "error_api_unavailable",
    }


def upload_logs_if_needed(env: dict):
    if not ENABLE_S3_UPLOAD:
        return

    try:
        env_logs = env.copy()
        env_logs["UPLOAD_MODE"] = "logs_only"
        run_script(["S3_Upload_Single.py"], env=env_logs)
    except Exception as exc:
        print(f"[WARN] Upload S3 des logs impossible : {exc}")


def main():
    flight_number, selected_date_raw, airp_src, airp_dst = parse_input()
    flight_date, hour = normalize_date_and_hour(selected_date_raw)
    delay = "15"

    if not flight_number:
        raise ValueError("flight_number manquant")
    if not flight_date:
        raise ValueError("date / selected_date manquante")
    if not airp_src:
        raise ValueError("departure_airport manquant")

    request_id = build_request_id(flight_number)
    request_dirs = prepare_request_dirs(request_id)
    request_date = datetime.now().strftime("%Y-%m-%d")

    env = os.environ.copy()
    env["REQUEST_ID"] = request_id
    env["REQUEST_DIR"] = str(request_dirs["request_dir"])
    env["REQUEST_OUTPUT_ROOT"] = str(OUTPUT_ROOT)
    env["REQUEST_OUTPUT_SINGLE"] = str(request_dirs["single"])
    env["REQUEST_OUTPUT_GREVES"] = str(request_dirs["greves"])
    env["REQUEST_OUTPUT_METEO"] = str(request_dirs["meteo"])
    env["REQUEST_OUTPUT_JF"] = str(request_dirs["jf"])
    env["RUN_DATE"] = request_date
    env["ENABLE_S3_UPLOAD"] = "1" if ENABLE_S3_UPLOAD else "0"

    print("BASE_DIR:", BASE_DIR)
    print("FLIGHT_SELECTION_PATH:", FLIGHT_SELECTION_PATH)
    print("FLIGHT_DATE:", flight_date)
    print("HOUR:", hour)
    print("AirpSRC:", airp_src)
    print("AirpDST (input):", airp_dst)
    print("Flight_NB:", flight_number)
    print("REQUEST_ID:", request_id)
    print("REQUEST_DIR:", request_dirs["request_dir"])
    print("ENABLE_S3_UPLOAD:", ENABLE_S3_UPLOAD)
    print("DEBUG_NEW_GLOBALRUN_VERSION_RUNNING")

    phase_1_scripts = [
        ["CleanCSV.py"],
        ["aerodatabox_Single_flight.py", flight_number, selected_date_raw, airp_src, airp_dst],
    ]

    for script in phase_1_scripts:
        try:
            run_script(script, env=env)
        except subprocess.CalledProcessError as e:
            print(f"\nErreur dans {script[0]} -> arrêt du pipeline")
            collect_legacy_outputs(request_dirs)

            status_payload = read_request_status(request_dirs)
            if is_blocking_status(status_payload):
                upload_logs_if_needed(env)
                raise RuntimeError(status_payload.get("user_message", "Erreur pendant la recherche du vol.")) from e

            upload_logs_if_needed(env)
            raise RuntimeError(f"Le script {script[0]} a échoué avec le code {e.returncode}") from e
        except Exception as e:
            print(f"\nErreur inattendue dans {script[0]} -> arrêt du pipeline")
            collect_legacy_outputs(request_dirs)
            upload_logs_if_needed(env)
            raise RuntimeError(f"Erreur sur {script[0]} : {str(e)}") from e

    collect_legacy_outputs(request_dirs)

    status_payload = read_request_status(request_dirs)
    if is_blocking_status(status_payload):
        upload_logs_if_needed(env)
        raise RuntimeError(status_payload.get("user_message", "Erreur pendant la recherche du vol."))

    if not airp_dst:
        airp_dst = extract_destination_from_single_flight_csv(request_dirs["single"])

    print(f"[INFO] AirpDST final utilisé pour le pipeline : {airp_dst}")

    phase_3_scripts = [
        ["vols_journaliers_1DayDateAirport.py", flight_date, airp_src],
        ["vols_journaliers_1DayDateAirport.py", flight_date, airp_dst],
        ["greves_aeroports_Single.py", flight_date],
        ["meteo_aeroports_Single.py", flight_date, airp_src],
        ["meteo_aeroports_Single.py", flight_date, airp_dst],
        ["Vacances_et_JoursFeries_Single.py", flight_date],
        ["GlobalCatFiles_Single.py"],
        ["FlightsAndMeteo_Single.py"],
        ["FlightsAndMeteoAndJFVacances_single.py"],
        ["FlightsAndMeteoAndJFVacancesAndGreves_Single.py"],
        ["Signoff_Update_Single.py", delay],
    ]

    for script in phase_3_scripts:
        try:
            run_script(script, env=env)
            collect_legacy_outputs(request_dirs)
        except subprocess.CalledProcessError as e:
            print(f"\nErreur dans {script[0]} -> arrêt du pipeline")
            upload_logs_if_needed(env)
            raise RuntimeError(f"Le script {script[0]} a échoué avec le code {e.returncode}") from e
        except Exception as e:
            print(f"\nErreur inattendue dans {script[0]} -> arrêt du pipeline")
            upload_logs_if_needed(env)
            raise RuntimeError(f"Erreur sur {script[0]} : {str(e)}") from e

    parquet_path = convert_signoff_csv_to_parquet(
        request_dir=request_dirs["request_dir"],
        request_id=request_id,
    )
    print(f"[INFO] Parquet final prêt : {parquet_path}")

    if ENABLE_S3_UPLOAD:
        try:
            run_script(["S3_Upload_Single.py"], env=env)
        except subprocess.CalledProcessError as e:
            print(f"\nErreur dans S3_Upload_Single.py")
            raise RuntimeError(f"Le script S3_Upload_Single.py a échoué avec le code {e.returncode}") from e
        except Exception as e:
            raise RuntimeError(f"Erreur sur S3_Upload_Single.py : {str(e)}") from e

    print("\nPipeline single flight terminé avec succès")
    print(f"[INFO] Dossier final de requête : {request_dirs['request_dir']}")
    print(f"[INFO] Fichier final parquet : {parquet_path}")


if __name__ == "__main__":
    main()