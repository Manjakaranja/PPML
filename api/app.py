# app.py - Point d'entrée pour Hugging Face Spaces
import uvicorn
from api.main import app

if __name__ == "__main__":
    uvicorn.run("api.main:app", host="0.0.0.0", port=7860)
