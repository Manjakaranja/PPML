

import subprocess
import glob
import os
import sys
import json

scripts = []


# Extraction des paramètres depuis flight_selection.json
with open("flight_selection.json", "r", encoding="utf-8") as f:
    flight_data = json.load(f)

selected_date = flight_data.get("selected_date", "")
FLIGHT_DATE = selected_date.split(" ")[0] if selected_date else ""
HOUR = selected_date.split(" ")[1] if " " in selected_date else ""

AirpSRC = flight_data.get("departure_airport", "")
AirpDST = flight_data.get("arrival_airport", "")
Flight_NB = flight_data.get("flight_number", "")

DELAY = "15"

print("FLIGHT_DATE:", FLIGHT_DATE)
print("HOUR:", HOUR)
print("AirpSRC:", AirpSRC)
print("AirpDST:", AirpDST)
print("Flight_NB:", Flight_NB)

scripts += [
    ["CleanCSV.py"],                                                                                        ##--     
    ["aerodatabox_Single_flight.py", Flight_NB, FLIGHT_DATE, AirpSRC, AirpDST],                             ##-- 
    
    ["RmOtherFlys.py"]
    
    ["vols_journaliers_1DayDateAirport.py", FLIGHT_DATE , AirpSRC ],                                        ##-- 
    ["vols_journaliers_1DayDateAirport.py", FLIGHT_DATE , AirpDST ],                                        ##-- 
    ["greves_aeroports_Single.py", FLIGHT_DATE],                                                            ##-- 
    ["Meteo_aeroports_Single.py", FLIGHT_DATE , AirpSRC],                                                   ##-- 
    ["Meteo_aeroports_Single.py", FLIGHT_DATE , AirpDST],                                                   ##-- 
    ["Vacances_et_JoursFeries_Single.py", FLIGHT_DATE],                                                     ##-- 
    ["GlobalCatFiles_Single.py"],                                                                           ##-- 
    ["FlightsAndMeteo_Single.py"],                                                                          ##--    
    ["FlightsAndMeteoAndJFVacances_Single.py"],                                                             ##-- 
    ["FlightsAndMeteoAndJFVacancesAndGreves_Single.py"],                                                    ##--
    ["Signoff_Update_Single.py", DELAY],                                                                    ##-- 

    ["LogDiv.py"],                                                                                          ##-- 

    ["S3_Upload_Single.py"],                                                                               ##--
]

for script in scripts:
    # Suppression du fichier d'erreur API avant chaque exécution de script
    err_log = "API_Single_ERR.log"
    if os.path.exists(err_log):
        try:
            os.remove(err_log)
            print(f"[INFO] {err_log} supprimé avant exécution de {script[0]}")
        except Exception as e:
            print(f"[WARN] Impossible de supprimer {err_log} : {e}")
    # Si script est une chaîne, transformer en liste
    if isinstance(script, str):
        cmd = ["python"] + script.split()
    else:
        cmd = ["python"] + script
    try:
        subprocess.run(cmd, check=True)
        print(f"{' '.join(cmd)} terminé avec succès")
        # Vérification après l'exécution de aerodatabox_Single_flight.py (succès)
        if script[0] == "aerodatabox_Single_flight.py":
            err_log = "API_Single_ERR.log"
            if os.path.exists(err_log):
                sys.exit("API ERROR Code generated in API_Single_ERR.log")
    except subprocess.CalledProcessError:
        print(f"Erreur dans {' '.join(cmd)} → arrêt")
        # Vérification après l'exécution de aerodatabox_Single_flight.py (échec)
        if script[0] == "aerodatabox_Single_flight.py":
            err_log = "API_Single_ERR.log"
            if os.path.exists(err_log):
                sys.exit("API ERROR Code generated in API_Single_ERR.log")
        break