
import subprocess
import glob
import os
import sys


# % Congestion Trig & CONG_TRIG
CONG_TRIG = "10"
DELAY_TRESH = "15"


scripts = []
                            
scripts += [
    ["CleanCSV.py"],                                                ##--     
    ["aerodatabox_past_flights.py"],                                ##-- arerodatabox_past_flights.py to Get the flights (210 Days Back) data from Aerodatabox API (with retry and backoff)
    ["aerodatabox_future_flights.py"],                              ##-- arerodatabox_future_flights.py to Get the future (7 Days) flights data from Aerodatabox API (with retry and backoff)   
    ["UpdaCong.py", CONG_TRIG],                                     ##-- Generate OutputFlights congestion_WW_updated.csv  -- CONG_TRIG parameter (10% by  default), can be adjusted
    ["greves_aeroports.py"],                                        ##-- Generate OutputDataGreves/greves_aeroports_210_derniers_jours.csv & greves_aeroports_future.csv
    ["Meteo_aeroports.py"],                                         ##-- Generate OutputDataMeteo/meteo_aeroports_210_derniers_jours.csv and OutputDataMeteo/meteo_aeroports_future.csv with hourly weather data for the past 210 and 7 future days for CDG, ORLY, NICE, TOULOUSE, LYON, MARSEILLE airports.    
    ["Vacances_et_JoursFeries.py"],                                 ##-- Generate OutputJFVacances/histo_calendrier_jferies_et_vacances.csv - Calendar data with holidays and school vacations for the past 210 days
    ["FlightsAndMeteo.py"],                                         ##-- Generate FlightsAndMeteo.csv - Merge Flights and Meteo for the last 210 Days  
    ["FlightsAndMeteoWWCount.py"],                                  ##-- Generate FlightsAndMeteoWWCount.csv - Merge Flights and Meteo with Flights Count for the last 210 Days   
    ["FlightsAndMeteo_future.py"],                                  ##-- Generate FlightsAndMeteo_future.csv - Merge Flights and Meteo for the future 7 Days
    ["FlightsAndMeteoWWCount_future.py"],                           ##-- Generate FlightsAndMeteoWWCount_future.csv - Merge Flights and Meteo with Flights Count for the last 210 Days   
#    ["extract_row_to_kv_csv.py", "2000"],                           ##-- FOR DEBUG: ##-- Generate FlightsMeteo_row2000_kv.csv (example with row 2000, can be adjusted) - Extract a specific row (e.g., 2000) from FlightsMeteo.csv - It is a SANITY Check of Flight and Meto Merge Process     
    ["FlightsAndMeteoAndJFVacances.py"],                            ##-- Generate FlightsAndMeteoAndJFVacances.csv - Merge Flights and Meteo with JF et Vacances data for the last 210 Days
    ["FlightsAndMeteoAndJFVacances_future.py"],                     ##-- Generate FlightsAndMeteoAndJFVacances_future.csv - Merge Flights and Meteo with JF et Vacances data for the future 7 Days
    ["FlightsAndMeteoAndJFVacancesAndGreves.py"],                   ##-- Generate FlightsAndMeteoAndJFVacancesAndGreves.csv - Merge Flights and Meteo with JF et Vacances and Greves data for the last 210 Days                
    ["FlightsAndMeteoAndJFVacancesAndGreves_future.py"],            ##-- Generate FlightsAndMeteoAndJFVacancesAndGreves_future.csv - Merge Flights and Meteo with JF et Vacances and Greves data for the future 7 Days
    ["Signoff_Update.py", DELAY_TRESH],                             ##-- DELAY_TRESH (mn) Paramter: Treshsold to Validate Delay in Last Column + Some Cleaning of the final dataset for 210 Days -- Generate SignoffFlightsDataset_YYYYMMDD_HHMMSS_CLEAN.csv
    ["Signoff_Update_future.py", DELAY_TRESH],                      ##-- DELAY_TRESH (mn) Paramter: Treshsold to Validate Delay in Last Column + Some Cleaning of the final dataset for 210 Days -- Generate SignoffFlightsDataset_future_YYYYMMDD_HHMMSS_CLEAN.csv       
#    ["extract_line_Signoff_to_kv_csv.py", "2000"],                  ##-- FOR DEBUG: ##-- Generate SignoffLine_LineNumberXXX_YYYYMMDD_HHMMSS.csv (example with line number and timestamp, can be adjusted) - Extract a specific line (e.g., 100) from the final Signoff dataset and save it in a key-value format for easy review and validation.
#    ["CompColumns.py"],                                             ##-- FOR DEBUG: Compare the columns of the final Signoff datasets for past and future and generate a file with all columns for both datasets to facilitate the review and comparison of the two datasets. It will generate All_colList.csv with 2 columns: Hist_Col (columns of historical dataset) and Future_Col (columns of future dataset) aligned by position.
    ["S3_Upload.py"],                                               ##--
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