import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

import chromadb
from chromadb.config import Settings # Importar Settings
import os
import shutil # Para borrar la carpeta si es necesario después del reset

# --- CONFIGURACIÓN ---
# Ruta a la carpeta de tu base de datos ChromaDB persistente
CHROMA_DB_PATH = "./my_chroma_db" 

def borrar_coleccion(db_path):
    """
    Pregunta al usuario el nombre de la colección a borrar. Si existe, la elimina; si no, muestra un mensaje.
    """
    print(f"Colecciones disponibles en la base de datos '{db_path}':")
    try:
        client = chromadb.PersistentClient(path=db_path)
        colecciones = client.list_collections()
        if not colecciones:
            print("  No hay colecciones en la base de datos.")
            return
        for coll in colecciones:
            print(f"  - {coll.name} (ID: {coll.id}, Documentos: {coll.count()})")
    except Exception as e:
        print(f"Error al conectar o listar colecciones: {e}")
        return

    nombre_coleccion = input("\nNombre de la colección que deseas borrar: ").strip()
    if not nombre_coleccion:
        print("No se ingresó ningún nombre de colección. Operación cancelada.")
        return

    # Buscar la colección
    coleccion_obj = None
    for coll in colecciones:
        if coll.name == nombre_coleccion:
            coleccion_obj = coll
            break
    if not coleccion_obj:
        print(f"La colección '{nombre_coleccion}' no existe en la base de datos.")
        return

    confirmacion = input(f"¿Estás seguro de que quieres borrar la colección '{nombre_coleccion}'? (s/N): ")
    if confirmacion.lower() != 's':
        print("Operación de borrado cancelada por el usuario.")
        return

    try:
        client.delete_collection(nombre_coleccion)
        print(f"¡Colección '{nombre_coleccion}' eliminada exitosamente!")
    except Exception as e:
        print(f"Ocurrió un error al intentar borrar la colección: {e}")

if __name__ == "__main__":
    borrar_coleccion(CHROMA_DB_PATH)
