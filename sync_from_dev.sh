#!/bin/bash

echo "🔄 Synchronisation des dossiers de développement vers le repo équipe..."

# Nettoyage préalable
rm -rf api/* streamlit/* training/* 2>/dev/null || true

# ====================== API ======================
echo "→ Copie de l'API (ppml_api_hf)"
mkdir -p api
cp -r ../../ppml_api_hf/api/* api/ 2>/dev/null || true
cp ../../ppml_api_hf/app.py api/ 2>/dev/null || true
cp ../../ppml_api_hf/Dockerfile api/ 2>/dev/null || true
cp ../../ppml_api_hf/requirements.txt api/ 2>/dev/null || true
cp ../../ppml_api_hf/README.md api/ 2>/dev/null || true
cp -r ../../ppml_api_hf/src/* api/src/ 2>/dev/null || true

# ====================== STREAMLIT ======================
echo "→ Copie du Streamlit (streamlit_ppml)"
mkdir -p streamlit
cp -r ../../streamlit_ppml/ppml_streamlit/* streamlit/ 2>/dev/null || true
cp ../../streamlit_ppml/app.py streamlit/ 2>/dev/null || true
cp ../../streamlit_ppml/Dockerfile streamlit/ 2>/dev/null || true
cp ../../streamlit_ppml/requirements.txt streamlit/ 2>/dev/null || true
cp -r ../../streamlit_ppml/.streamlit streamlit/ 2>/dev/null || true
cp ../../streamlit_ppml/README.md streamlit/ 2>/dev/null || true

# ====================== TRAINING ======================
echo "→ Copie du Training (train_ppml)"
mkdir -p training
cp ../../train_ppml/train.py training/ 2>/dev/null || true
cp ../../train_ppml/Dockerfile training/ 2>/dev/null || true
cp ../../train_ppml/requirements.txt training/ 2>/dev/null || true
cp ../../train_ppml/MLproject training/ 2>/dev/null || true
cp ../../train_ppml/conda.yaml training/ 2>/dev/null || true
cp -r ../../train_ppml/config/* training/config/ 2>/dev/null || true

# Nettoyage des éléments indésirables
echo "🧹 Nettoyage des fichiers sensibles et dossiers inutiles..."
rm -f api/secrets.sh streamlit/secrets.sh training/secrets.sh
rm -rf streamlit/.git streamlit/.gitattributes
rm -rf training/mlruns training/models training/data __pycache__ */__pycache__

echo "✅ Synchronisation terminée !"
echo "Maintenant fais : git status"
