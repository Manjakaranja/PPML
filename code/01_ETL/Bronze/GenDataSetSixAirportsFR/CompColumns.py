
import subprocess
import glob
import os
import sys
import pandas as pd
import numpy as np



# Suppression des anciens fichiers de sortie s'ils existent
for f in ['Hist_colList.csv', 'future_colList.csv', 'All_colList.csv']:
    if os.path.exists(f):
        os.remove(f)
        print(f'Supprimé : {f}')





# Recherche du dernier fichier correspondant au motif *SignoffFlightsDataset_Large_*_CLEAN.csv dans le dossier courant
pattern = os.path.join(os.getcwd(), 'SignoffFlightsDataset_Large_*_CLEAN.csv')
file_list = glob.glob(pattern)
if not file_list:
    raise FileNotFoundError('Aucun fichier *SignoffFlightsDataset_Large_*_CLEAN.csv trouvé dans le dossier courant')
latest_file = max(file_list, key=os.path.getmtime)
Hist_data = pd.read_csv(latest_file)
print(f'Set with labels (our train+test) : {Hist_data.shape} | Fichier utilisé : {os.path.basename(latest_file)}')

# generate in colList.csv athe liiust of All Column of Hist_Data on colum pas ligen in csv file
Hist_data.columns.to_series().to_csv('Hist_colList.csv', index=False)        









# Recherche du dernier fichier correspondant au motif *SignoffFlightsDataset_future*_CLEAN.csv dans le dossier courant
pattern = os.path.join(os.getcwd(), 'SignoffFlightsDataset_future*_CLEAN.csv')
file_list = glob.glob(pattern)
if not file_list:
    raise FileNotFoundError('Aucun fichier *SignoffFlightsDataset_future*_CLEAN.csv trouvé dans le dossier courant')
latest_file = max(file_list, key=os.path.getmtime)
future_data = pd.read_csv(latest_file)
print(f'Set with labels (our train+test) : {future_data.shape} | Fichier utilisé : {os.path.basename(latest_file)}')


# generate in colList.csv athe liiust of All Column of future_data on colum pas ligen in csv file
future_data.columns.to_series().to_csv('future_colList.csv', index=False)        





# Fusionne les colonnes des deux fichiers pour avoir dans All_colList.csv :
# colonne 1 = colonne du fichier historique, colonne 2 = colonne du fichier future (alignées par position, pas par nom)
colList_Hist = pd.read_csv('Hist_colList.csv', header=None, names=['Hist_Col'])
colList_Future = pd.read_csv('future_colList.csv', header=None, names=['Future_Col'])
max_len = max(len(colList_Hist), len(colList_Future))
colList_Hist = colList_Hist.reindex(range(max_len))
colList_Future = colList_Future.reindex(range(max_len))
colList_All = pd.concat([colList_Hist, colList_Future], axis=1)
colList_All.to_csv('All_colList.csv', index=False)

