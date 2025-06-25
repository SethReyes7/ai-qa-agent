import chromadb
from chromadb.utils import embedding_functions
import os
import json
import argparse

parser = argparse.ArgumentParser(description="Carga documentos .jsonl a ChromaDB desde una carpeta específica")
parser.add_argument('--documents-dir', type=str, default=None, help='Carpeta de documentos .jsonl a cargar (ej: data/cleaned_data/20240613_xxxx o data/fragmented_data/20240613_xxxx)')
parser.add_argument('--collection', type=str, default=None, help='Nombre de la colección en ChromaDB')
args, unknown = parser.parse_known_args()

documents_folder = args.documents_dir
if not documents_folder:
    documents_folder = input("Nombre de la carpeta de documentos (.jsonl) (deja vacío para 'data/cleaned_data'): ").strip()
    if not documents_folder:
        documents_folder = "data/cleaned_data"
os.makedirs(documents_folder, exist_ok=True)

collection_name = args.collection
if not collection_name:
    while True:
        collection_name = input("Nombre de la colección en ChromaDB (obligatorio): ").strip()
        if collection_name:
            break
        print("[ERROR] Debes ingresar un nombre para la colección. Intenta de nuevo.")

CHROMA_DB_PATH = "./my_chroma_db"

# --- Inicializa el cliente de ChromaDB ---
try:
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    print("Cliente de ChromaDB (persistente) inicializado.")
except Exception as e:
    print(f"Error al inicializar el cliente persistente: {e}")
    print("Intentando con un cliente efímero (en memoria)...")
    client = chromadb.EphemeralClient()
    print("Cliente de ChromaDB (efímero) inicializado.")

# --- Embedding function ---
try:
    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")
    print(f"Función de embedding cargada correctamente.")
except Exception as e:
    print(f"Error al cargar la función de embedding: {e}")
    print("Asegúrate de tener 'sentence-transformers' instalado y conexión a internet la primera vez.")
    exit()

# --- Crear o cargar la colección ---
try:
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=sentence_transformer_ef
    )
    print(f"Colección '{collection_name}' obtenida/creada.")
except Exception as e:
    print(f"Error al obtener o crear la colección '{collection_name}': {e}")
    exit()

# --- Obtener IDs existentes en la colección ---
ids_existentes = set()
try:
    if collection.count() > 0:
        existing_items_ids = collection.get(include=["ids"])['ids']
        if existing_items_ids:
            ids_existentes = set(existing_items_ids)
except Exception as e:
    print(f"Advertencia: No se pudieron obtener los IDs existentes de la colección: {e}")

if not os.path.exists(documents_folder):
    os.makedirs(documents_folder)
    print(f"Carpeta '{documents_folder}' creada. Por favor, añade tus archivos .jsonl allí.")

documentos_para_anadir = []
ids_para_anadir = []
metadatos_para_anadir = []
docs_omitidos = 0
ids_lote = set()
archivos_encontrados = 0
archivos_nuevos_anadidos = 0

print(f"\nBuscando archivos .jsonl en la carpeta '{documents_folder}'...")

for nombre_archivo in os.listdir(documents_folder):
    if not nombre_archivo.lower().endswith(".jsonl"):
        continue
    archivos_encontrados += 1
    ruta_archivo = os.path.join(documents_folder, nombre_archivo)
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    doc = json.loads(line)
                except Exception as e:
                    print(f"  - [WARNING] Línea {line_num} en '{nombre_archivo}' no es JSON válido. Omitiendo.")
                    docs_omitidos += 1
                    continue
                doc_id = doc.get("id", None)
                title = doc.get("title", None)
                content = doc.get("content", None)
                fragment = doc.get("fragment", None)
                total_fragments = doc.get("total_fragments", None)
                full_id = doc_id if not fragment else f"{doc_id}::fragment{fragment}"
                if full_id in ids_existentes:
                    print(f"  - [WARNING] Línea {line_num} en '{nombre_archivo}' tiene un ID ya existente en la colección ('{full_id}'). Omitiendo.")
                    docs_omitidos += 1
                    continue
                if full_id in ids_lote:
                    print(f"  - [WARNING] Línea {line_num} en '{nombre_archivo}' tiene un ID duplicado en el lote ('{full_id}'). Omitiendo.")
                    docs_omitidos += 1
                    continue
                ids_lote.add(full_id)
                documentos_para_anadir.append(f"{title}\n\n{content}")
                ids_para_anadir.append(full_id)
                meta = {"title": title, "source_file": nombre_archivo, "file_type": "jsonl"}
                if fragment:
                    meta["fragment"] = fragment
                if total_fragments:
                    meta["total_fragments"] = total_fragments
                # Agregar todos los campos extra presentes en el registro (excepto id, title, content)
                for k, v in doc.items():
                    if k not in {"id", "title", "content", "fragment", "total_fragments"}:
                        # Convertir listas o dicts a string para compatibilidad con ChromaDB
                        if v is None:
                            meta[k] = ""
                        elif isinstance(v, (list, dict)):
                            meta[k] = json.dumps(v, ensure_ascii=False)
                        else:
                            meta[k] = v
                metadatos_para_anadir.append(meta)
        print(f"  - Archivo JSONL '{nombre_archivo}' procesado.")
    except Exception as e:
        print(f"  - Error al leer el archivo JSONL '{nombre_archivo}': {e}")
        continue

if not archivos_encontrados:
    print(f"No se encontraron archivos .jsonl en la carpeta '{documents_folder}'.")

BATCH_SIZE = 500

if documentos_para_anadir:
    try:
        total_docs = len(documentos_para_anadir)
        print(f"\nAñadiendo {total_docs} nuevos documentos a la colección '{collection_name}' en lotes de {BATCH_SIZE}...")
        for start in range(0, total_docs, BATCH_SIZE):
            end = min(start + BATCH_SIZE, total_docs)
            print(f"  - Añadiendo documentos {start+1} a {end} de {total_docs}...")
            collection.add(
                documents=documentos_para_anadir[start:end],
                ids=ids_para_anadir[start:end],
                metadatas=metadatos_para_anadir[start:end]
            )
        archivos_nuevos_anadidos = total_docs
        print(f"{archivos_nuevos_anadidos} nuevos documentos añadidos exitosamente.")
    except Exception as e:
        print(f"Error al añadir documentos desde archivos: {e}")
else:
    if archivos_encontrados > 0:
        print("\nNo hay nuevos documentos de archivos para añadir a la colección.")
print(f"Total de documentos en la colección '{collection_name}' ahora: {collection.count()}")

print(f"\nResumen: {archivos_nuevos_anadidos} documentos añadidos, {docs_omitidos} documentos omitidos por formato incorrecto.")

try:
    print("\nColecciones en la base de datos:")
    collections_list = client.list_collections()
    if not collections_list:
        print("  No hay colecciones en la base de datos.")
    for coll_obj in collections_list:
        print(f"  - Nombre: {coll_obj.name}, ID: {coll_obj.id}, Documentos: {coll_obj.count()}")
except Exception as e:
    print(f"Error al listar colecciones: {e}")

print("\n¡Proceso de carga de documentos completado!")
