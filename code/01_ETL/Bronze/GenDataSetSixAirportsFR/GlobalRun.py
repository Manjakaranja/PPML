
import subprocess
import glob
import os
import sys

# Suppression des fichiers .csv sauf ceux dans OutputFlights
for f in glob.glob("*.csv"):
    if not f.startswith("OutputFlights" + os.sep):
        try:
            os.remove(f)
            print(f"Fichier supprimé : {f}")
        except Exception as e:
            print(f"Erreur lors de la suppression de {f} : {e}")

# Suppression des .csv dans OutputDataGreves, OutputDataMeteo, OutputFVacances
for subdir in ["OutputDataGreves", "OutputDataMeteo", "OutputJFVacances"]:
    for f in glob.glob(os.path.join(subdir, "*.csv")):
        try:
            os.remove(f)
            print(f"Fichier supprimé : {f}")
        except Exception as e:
            print(f"Erreur lors de la suppression de {f} : {e}")
if len(sys.argv) > 1 and sys.argv[1].lower() in ("no", "non", "skip", "0"):
    run_first = False
else:
    # Question interactive
    while True:
        answer = input("Run Flights API Call ? Y/N : ").strip().lower()
        if answer in ("y", "Y", "yes", "o", "oui"):
            run_first = True
            break
        elif answer in ("n", "N", "no", "non"):
            run_first = False
            break
        else:
            print("Répondez par Y ou N.")


# Lecture du paramètre CONG_TRIG
if len(sys.argv) > 2 and sys.argv[2].strip():
    CONG_TRIG = sys.argv[2].strip()
else:
    val = input("Rentrez le % Congestion Trig (défaut=10) : ").strip()
    CONG_TRIG = val if val else "10"

# Lecture du paramètre DELAY_TRESH
if len(sys.argv) > 3 and sys.argv[3].strip():
    DELAY_TRESH = sys.argv[3].strip()
else:
    val = input("Rentrez le Delay Threshold en mn (défaut=20) : ").strip()
    DELAY_TRESH = val if val else "20"

## Launch Script: 
#                   GlobalRun.py to Run all the scripts in order and generate the final dataset for the 6 Airports 
#                   in France (CDG, ORLY, NICE, TOULOUSE, LYON, MARSEILLE) with enriched data and ready for ML Modeling. 
#                   It will also ask if you want to run the API calls to get the flight data (past and future) and it will ask for 
#                   the parameters for congestion trigger and delay threshold.

scripts = []
if run_first:
    scripts.append(["aerodatabox_past_flights.py"])                 ##-- arerodatabox_past_flights.py to Get the flights (210 Days Back) data from Aerodatabox API (with retry and backoff)
    scripts.append(["aerodatabox_future_flights.py"])               ##-- arerodatabox_future_flights.py to Get the future (7 Days) flights data from Aerodatabox API (with retry and backoff)   
                            
##-- Then RUN the following scripts in order to enrich the data and prepare the final dataset 

scripts += [
    ["UpdaCong.py", CONG_TRIG],                                     ##-- Generate OutputFlights congestion_WW_updated.csv  -- CONG_TRIG parameter (10% by  default), can be adjusted
    ["greves_aeroports.py"],                                        ##-- Generate OutputDataGreves/greves_aeroports_210_derniers_jours.csv & greves_aeroports_future.csv
    ["Meteo_aeroports.py"],                                         ##-- Generate OutputDataMeteo/meteo_aeroports_210_derniers_jours.csv and OutputDataMeteo/meteo_aeroports_future.csv with hourly weather data for the past 210 and 7 future days for CDG, ORLY, NICE, TOULOUSE, LYON, MARSEILLE airports.    
    ["Vacances_et_JoursFeries.py"],                                 ##-- Generate OutputJFVacances/histo_calendrier_jferies_et_vacances.csv - Calendar data with holidays and school vacations for the past 210 days
    ["FlightsAndMeteo.py"],                                         ##-- Generate FlightsAndMeteo.csv - Merge Flights and Meteo for the last 210 Days  
    ["FlightsAndMeteo_future.py"],                                  ##-- Generate FlightsAndMeteo_future.csv - Merge Flights and Meteo for the future 7 Days
    ["extract_row_to_kv_csv.py", "2000"],                           ##-- FOR DEBUG: ##-- Generate FlightsMeteo_row2000_kv.csv (example with row 2000, can be adjusted) - Extract a specific row (e.g., 2000) from FlightsMeteo.csv - It is a SANITY Check of Flight and Meto Merge Process     
    ["FlightsAndMeteoAndJFVacances.py"],                            ##-- Generate FlightsAndMeteoAndJFVacances.csv - Merge Flights and Meteo with JF et Vacances data for the last 210 Days
    ["FlightsAndMeteoAndJFVacances_future.py"],                     ##-- Generate FlightsAndMeteoAndJFVacances_future.csv - Merge Flights and Meteo with JF et Vacances data for the future 7 Days
    ["FlightsAndMeteoAndJFVacancesAndGreves.py"],                   ##-- Generate FlightsAndMeteoAndJFVacancesAndGreves.csv - Merge Flights and Meteo with JF et Vacances and Greves data for the last 210 Days                
    ["FlightsAndMeteoAndJFVacancesAndGreves_future.py"],            ##-- Generate FlightsAndMeteoAndJFVacancesAndGreves_future.csv - Merge Flights and Meteo with JF et Vacances and Greves data for the future 7 Days
    ["Signoff_Update.py", DELAY_TRESH],                             ##-- DELAY_TRESH (mn) Paramter: Treshsold to Validate Delay in Last Column + Some Cleaning of the final dataset for 210 Days -- Generate SignoffFlightsDataset_YYYYMMDD_HHMMSS_CLEAN.csv
    ["Signoff_Update_future.py", DELAY_TRESH],                      ##-- DELAY_TRESH (mn) Paramter: Treshsold to Validate Delay in Last Column + Some Cleaning of the final dataset for 210 Days -- Generate SignoffFlightsDataset_future_YYYYMMDD_HHMMSS_CLEAN.csv       
    ["extract_line_Signoff_to_kv_csv.py", "2000"],                  ##-- FOR DEBUG: ##-- Generate SignoffLine_LineNumberXXX_YYYYMMDD_HHMMSS.csv (example with line number and timestamp, can be adjusted) - Extract a specific line (e.g., 100) from the final Signoff dataset and save it in a key-value format for easy review and validation.
]


##--- OUTPUT FILES

        ## SignoffFlightsDataset_YYYYMMDD_HHMMSS_CLEAN.csv              6 Airports Dataset with Enriched Data and Cleaned, ready for ML Modeling (210 Days)
        ## SignoffFlightsDataset_future_YYYYMMDD_HHMMSS_CLEAN.csv       6 Airports Dataset with Enriched Data and Cleaned, ready for ML Modeling (Future 7 Days)      


## -- Still to Download Output Files to S3



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