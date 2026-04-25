---
title: Fly On Time
emoji: ✈️
colorFrom: red
colorTo: red
sdk: docker
pinned: false
short_description: Projet Jedha FlyOnTime equipe PPML
---

# FlyOnTime Streamlit Space

This Hugging Face Space hosts the user-facing web application.

Streamlit collects the flight information, validates the form inputs, sends the request to the FastAPI Space and displays the prediction result.

## Main files

```text
STREAMLIT/
├── app.py
├── Dockerfile
├── requirements.txt
└── anims/
```

## Current backend target

```python
API_BASE = "https://ppml2026-ppml-fastapi.hf.space"
API_URL = f"{API_BASE}/predict"
```

## Features

- aviation-themed UI;
- Lottie animations;
- flight number validation;
- date and time widgets;
- departure / arrival airport validation;
- loading animation while FastAPI runs;
- delay estimate display;
- risk level display;
- expandable flight details.

## Payload sent to FastAPI

```json
{
  "flight_number": "AF7300",
  "date": "2026-04-22T06:45:00",
  "departure_airport": "CDG - Paris Charles de Gaulle",
  "arrival_airport": "NCE - Nice Côte d'Azur"
}
```

## Recommended variable

The FastAPI URL is currently hardcoded in `app.py`. For a cleaner deployment, it can be moved to a Hugging Face variable:

| Name | Type | Purpose |
|---|---|---|
| `API_BASE` | Variable | Base URL of the FastAPI Space. |
| `PORT` | Variable | Hugging Face runtime port. |

Streamlit should not store AWS credentials, MLflow credentials or external API keys.
