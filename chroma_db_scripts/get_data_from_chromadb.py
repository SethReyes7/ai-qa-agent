import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import chromadb
import os

CHROMA_DB_PATH = "./my_chroma_db"

try:
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    print("Cliente de ChromaDB (persistente) inicializado.")
except Exception as e:
    print(f"Error al inicializar el cliente persistente: {e}")
    print("Intentando con un cliente efímero (en memoria)...")
    client = chromadb.EphemeralClient()
    print("Cliente de ChromaDB (efímero) inicializado.")

try:
    print("\nColecciones en la base de datos:")
    collections_list = client.list_collections()
    if not collections_list:
        print("  No hay colecciones en la base de datos.")
    for coll_obj in collections_list:
        print(f"  - Nombre: {coll_obj.name}, ID: {coll_obj.id}, Documentos: {coll_obj.count()}")
except Exception as e:
    print(f"Error al listar colecciones: {e}")
