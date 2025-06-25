import os
from fastapi import FastAPI
from pydantic import BaseModel
import chromadb

app = FastAPI()

# Obtén la ruta de la base de datos desde variable de entorno o usa un valor por defecto
CHROMADB_PATH = os.getenv("CHROMADB_PATH", "./my_chroma_db")

# Usa PersistentClient con la ruta especificada
chroma_client = chromadb.PersistentClient(path=CHROMADB_PATH)
collection = chroma_client.get_collection("onboarding_profile_full_20250617_183236")  # Cambia por el nombre de tu colección

class QueryRequest(BaseModel):
    query: str
    n_results: int = 5

@app.post("/search")
def search(request: QueryRequest):
    results = collection.query(
        query_texts=[request.query],
        n_results=request.n_results
    )
    return {
        "ids": results["ids"][0],
        "documents": results["documents"][0],
        "metadatas": results["metadatas"][0],
        "distances": results["distances"][0]
    }