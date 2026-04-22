FROM continuumio/miniconda3

# Install system dependencies + AWS CLI in one layer
RUN apt-get update && apt-get install -y nano unzip curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws/

# HuggingFace: create non-root user
RUN useradd -m -u 1000 user
USER user

ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

COPY requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

COPY --chown=user . $HOME/app

CMD mlflow server -p $PORT \
    --host 0.0.0.0 \
    --backend-store-uri $BACKEND_STORE_URI \
    --default-artifact-root $ARTIFACT_STORE_URI \
    --x-frame-options NONE \
    --allowed-hosts '*' \
    --cors-allowed-origins '*'