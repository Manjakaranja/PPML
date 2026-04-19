

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
    ["CleanCSV.py"],                             ##--     
    ["aerodatabox_Single_flight.py", Flight_NB, FLIGHT_DATE, AirpSRC, AirpDST],                             ##-- 
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
    ["S3_Upload_Single.py"],                                                                                ##--
]

for script in scripts:
    # Si script est une chaîne, transformer en liste
    if isinstance(script, str):
        cmd = ["python"] + script.split()
    else:
        cmd = ["python"] + script
    try:
        subprocess.run(cmd, check=True)
        print(f"{' '.join(cmd)} terminé avec succès")
    except subprocess.CalledProcessError:
        print(f"Erreur dans {' '.join(cmd)} → arrêt")
        break