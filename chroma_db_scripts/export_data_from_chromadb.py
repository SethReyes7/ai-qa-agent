import chromadb
import os
import json
import argparse

CHROMA_DB_PATH = "./my_chroma_db"
FRAGMENT_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB

def get_fragmented_output_paths(base_output_file, extension):
    base, _ = os.path.splitext(base_output_file)
    def path(idx):
        return f"{base}_part_{idx}.{extension}"
    return path

def exportar_a_jsonl(data, output_file):
    with open(output_file, 'w', encoding='utf-8') as f:
        for i in range(len(data['ids'])):
            doc = {
                "id": data['ids'][i],
                "content": data['documents'][i] if data.get('documents') and data['documents'][i] else ""
            }
            metadata = data['metadatas'][i] if data.get('metadatas') and data['metadatas'][i] else {}
            # Incluir todos los metadatos relevantes excepto id y content
            for k, v in metadata.items():
                if k not in {"id", "content"}:
                    doc[k] = v
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

def exportar_a_txt(data, output_file):
    path_fn = get_fragmented_output_paths(output_file, 'txt')
    fragment_idx = 1
    buffer = ""
    buffer_size = 0
    for i in range(len(data['ids'])):
        doc_id = data['ids'][i]
        content = data['documents'][i] if data.get('documents') and data['documents'][i] else ""
        metadata = data['metadatas'][i] if data.get('metadatas') and data['metadatas'][i] else {}
        title = metadata.get('title', '') or metadata.get('summary', '')
        doc_txt = f"--- Documento: {doc_id} ---\n"
        if title and not content.strip().startswith(title):
            doc_txt += f"Título: {title}\n"
        for k, v in metadata.items():
            if k not in {"id", "content", "title", "summary"}:
                doc_txt += f"{k}: {v}\n"
        doc_txt += content.strip() + "\n\n"
        doc_bytes = doc_txt.encode('utf-8')
        # Si el documento solo ya excede el fragmento, escríbelo solo
        if len(doc_bytes) > FRAGMENT_SIZE_BYTES:
            if buffer:
                with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
                    f.write(buffer)
                fragment_idx += 1
                buffer = ""
                buffer_size = 0
            with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
                f.write(doc_txt)
            fragment_idx += 1
        else:
            if buffer_size + len(doc_bytes) > FRAGMENT_SIZE_BYTES:
                with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
                    f.write(buffer)
                fragment_idx += 1
                buffer = ""
                buffer_size = 0
            buffer += doc_txt
            buffer_size += len(doc_bytes)
    if buffer:
        with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
            f.write(buffer)

def exportar_a_md(data, output_file):
    path_fn = get_fragmented_output_paths(output_file, 'md')
    fragment_idx = 1
    buffer = ""
    buffer_size = 0
    for i in range(len(data['ids'])):
        doc_id = data['ids'][i]
        content = data['documents'][i] if data.get('documents') and data['documents'][i] else ""
        metadata = data['metadatas'][i] if data.get('metadatas') and data['metadatas'][i] else {}
        title = metadata.get('title', '') or metadata.get('summary', '') or doc_id
        doc_md = f"# {title}\n\n"
        doc_md += f"**ID:** {doc_id}  "
        for k, v in metadata.items():
            if k not in {"id", "content", "title", "summary"}:
                doc_md += f"**{k}:** {v}  "
        doc_md += "\n\n---\n\n"
        doc_md += content.strip() + "\n\n---\n\n"
        doc_bytes = doc_md.encode('utf-8')
        if len(doc_bytes) > FRAGMENT_SIZE_BYTES:
            if buffer:
                with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
                    f.write(buffer)
                fragment_idx += 1
                buffer = ""
                buffer_size = 0
            with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
                f.write(doc_md)
            fragment_idx += 1
        else:
            if buffer_size + len(doc_bytes) > FRAGMENT_SIZE_BYTES:
                with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
                    f.write(buffer)
                fragment_idx += 1
                buffer = ""
                buffer_size = 0
            buffer += doc_md
            buffer_size += len(doc_bytes)
    if buffer:
        with open(path_fn(fragment_idx), 'w', encoding='utf-8') as f:
            f.write(buffer)

def exportar_coleccion(db_path, collection_name, output_file, formato="jsonl"):
    print(f"Intentando conectar a la base de datos ChromaDB en: {db_path}")
    try:
        client = chromadb.PersistentClient(path=db_path)
        print("Cliente de ChromaDB (persistente) inicializado.")
    except Exception as e:
        print(f"Error al inicializar el cliente persistente: {e}")
        return

    try:
        print(f"Intentando obtener la colección: '{collection_name}'...")
        collection = client.get_collection(name=collection_name)
        print(f"Colección '{collection_name}' obtenida. Número de documentos: {collection.count()}")
    except Exception as e:
        print(f"Error al obtener la colección '{collection_name}': {e}")
        return

    if collection.count() == 0:
        print(f"La colección '{collection_name}' está vacía. No hay nada que exportar.")
        with open(output_file, 'w', encoding='utf-8') as f:
            pass
        print(f"Archivo de salida '{output_file}' creado (vacío).")
        return

    print(f"\nExtrayendo documentos de la colección '{collection_name}'...")
    try:
        data = collection.get(
            include=['documents', 'metadatas']
        )
    except Exception as e:
        print(f"Error al extraer datos de la colección: {e}")
        return

    print(f"Exportando {len(data['ids'])} documentos en formato {formato}...")

    if formato == "jsonl":
        exportar_a_jsonl(data, output_file)
    elif formato == "txt":
        exportar_a_txt(data, output_file)
    elif formato == "md":
        exportar_a_md(data, output_file)
    else:
        print(f"Formato '{formato}' no soportado.")
        return

    print(f"\n¡Exportación completada! {len(data['ids'])} documentos guardados en '{output_file}'.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta una colección de ChromaDB a un archivo .jsonl, .txt o .md. Ejemplo de uso: --output-file data/export/20240613_xxxx/mi_coleccion.jsonl")
    parser.add_argument('--output-file', type=str, default=None, help='Ruta del archivo de salida')
    parser.add_argument('--collection', type=str, default=None, help='Nombre de la colección a exportar')
    args, unknown = parser.parse_known_args()

    if not os.path.exists(CHROMA_DB_PATH):
        print(f"Advertencia: La carpeta de la base de datos '{CHROMA_DB_PATH}' no existe.")
    else:
        if args.collection:
            collection_name = args.collection
            formato = 'jsonl'
            if args.output_file and args.output_file.endswith('.txt'):
                formato = 'txt'
            elif args.output_file and args.output_file.endswith('.md'):
                formato = 'md'
            output_file = args.output_file if args.output_file else f"{collection_name}.{formato}"
            output_dir = os.path.dirname(output_file)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
            exportar_coleccion(CHROMA_DB_PATH, collection_name, output_file, formato=formato)
        else:
            # Inicializa el cliente para listar colecciones
            try:
                client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
                colecciones = client.list_collections()
                if not colecciones:
                    print("No se encontraron colecciones en la base de datos.")
                    exit()
                print("Colecciones disponibles en ChromaDB:")
                for idx, col in enumerate(colecciones, 1):
                    print(f"  {idx}. {col.name}")
                seleccion_col = input("Selecciona la colección a exportar (número): ").strip()
                try:
                    idx_col = int(seleccion_col) - 1
                    collection_name = colecciones[idx_col].name
                except (ValueError, IndexError):
                    print("Selección de colección no válida.")
                    exit()
            except Exception as e:
                print(f"Error al conectar o listar colecciones: {e}")
                exit()

            # Selección de formato
            opciones = [("jsonl", "Exportar como JSONL (recomendado para IA)"),
                        ("txt", "Exportar como texto plano (.txt)"),
                        ("md", "Exportar como Markdown (.md)")]
            print("Selecciona el formato de exportación:")
            for idx, (_, desc) in enumerate(opciones, 1):
                print(f"  {idx}. {desc}")
            seleccion = input("Opción (1/2/3): ").strip()
            try:
                idx = int(seleccion) - 1
                formato = opciones[idx][0]
            except (ValueError, IndexError):
                print("Selección no válida. Usa 1, 2 o 3.")
            else:
                if args.output_file:
                    output_file = args.output_file
                else:
                    output_file = f"{collection_name}.{formato}"
                output_dir = os.path.dirname(output_file)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                exportar_coleccion(CHROMA_DB_PATH, collection_name, output_file, formato=formato)