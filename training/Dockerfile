# Dockerfile.train - Version ultra-minimale (tout dans train.py)

FROM python:3.11-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Requirements
COPY requirements.txt .
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copie uniquement le fichier train.py et la config
COPY train.py .
COPY config/ config/

# Création des dossiers persistants
RUN mkdir -p data models mlruns predictions

ENV PYTHONUNBUFFERED=1

CMD ["python", "train.py"]
