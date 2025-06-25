import os
import chromadb

# Configuración: ruta y nombre de la colección
CHROMADB_PATH = os.getenv("CHROMADB_PATH", "./my_chroma_db")
COLLECTION_NAME = os.getenv("CHROMADB_COLLECTION", "onboarding_profile_full_20250617_183236")

# Conexión a ChromaDB
client = chromadb.PersistentClient(path=CHROMADB_PATH)
collection = client.get_collection(COLLECTION_NAME)

# Solicita el ID al usuario
record_id = input("Introduce el ID del registro: ").strip()

# Obtiene el embedding
results = collection.get(ids=[record_id], include=["embeddings"])
embeddings = results.get("embeddings", [])

if embeddings and embeddings[0] is not None:
    print(f"\nEmbedding numérico para el ID '{record_id}':\n")
    print(embeddings[0])
else:
    print(f"No se encontró embedding para el ID '{record_id}' en la colección '{COLLECTION_NAME}'.") 