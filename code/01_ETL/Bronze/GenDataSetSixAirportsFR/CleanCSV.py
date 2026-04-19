
import subprocess
import glob
import os
import sys


# Suppression des .csv et .json dans OutputDataGreves, OutputDataMeteo, OutputJFVacances, OutputFlights, OutputSingleFlight
for subdir in ["OutputDataGreves", "OutputDataMeteo", "OutputJFVacances", "OutputFlights", "OutputSingleFlight"]:
    for ext in ["*.csv", "*.json"]:
        for f in glob.glob(os.path.join(subdir, ext)):
            try:
                os.remove(f)
                print(f"Fichier supprimé : {f}")
            except Exception as e:
                print(f"Erreur lors de la suppression de {f} : {e}")

# Suppression de tous les .csv au même niveau que le script (dossier courant)
for f in glob.glob("*.csv"):
    try:
        os.remove(f)
        print(f"Fichier supprimé : {f}")
    except Exception as e:
        print(f"Erreur lors de la suppression de {f} : {e}")
