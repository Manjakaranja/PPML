# Dockerfile.streamlit - Optimisé pour Hugging Face Spaces

FROM python:3.11-slim
# Update all packages and install nano unzip and curl
RUN apt-get update
RUN apt-get install nano unzip curl -y

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
COPY app.py .


# THIS IS SPECIFIC TO HUGGINGFACE AS WELL
# expose port 7860 which is the port used by HuggingFace for Web Application
EXPOSE 8501

# Run streamlit server
CMD ["streamlit", "run", "app.py", "--server.port=8501"]
