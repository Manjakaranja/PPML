FROM python:3.11-slim

# System packages
RUN apt-get update -y && apt-get install -y nano curl && rm -rf /var/lib/apt/lists/*

# Hugging Face user
RUN useradd -m -u 1000 user

USER user

ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

# Copy project
COPY --chown=user . $HOME/app

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose port
EXPOSE 7860

# Launch FastAPI
CMD uvicorn app:app --host 0.0.0.0 --port ${PORT:-7860}