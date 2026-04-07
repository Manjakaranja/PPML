# api/schemas.py

from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime


class SinglePrediction(BaseModel):
    mouvement_id: int
    icao: str
    type: str
    airline: str
    scheduled_hour: int
    scheduled_utc: str | None = None
    flight_number: str | None = None
    terminal_dep: str
    terminal_arr: str
    predicted_delay_minutes: float
    prediction_timestamp: datetime


class PredictionRequest(BaseModel):
    """
    Requête pour la prédiction.
    On envoie une liste de records (1 ou plusieurs vols).
    """

    records: List[Dict[str, Any]]


class PredictionResponse(BaseModel):
    status: str
    message: str
    predictions: List[SinglePrediction]
    total_predictions: int
