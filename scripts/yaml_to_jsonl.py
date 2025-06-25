import yaml
import json
import argparse
import os

def yaml_to_jsonl(yaml_path, jsonl_path, out_handle=None):
    with open(yaml_path, 'r', encoding='utf-8') as f:
        data = list(yaml.safe_load_all(f))
    if out_handle:
        out = out_handle
    else:
        out = open(jsonl_path, 'w', encoding='utf-8')
    for idx, doc in enumerate(data):
        if not isinstance(doc, dict):
            continue
        record = {
            "id": doc.get("id", f"{os.path.basename(yaml_path)}_{idx+1}"),
            "title": doc.get("title", os.path.basename(yaml_path)),
            "content": doc.get("description") or doc.get("content") or yaml.dump(doc),
            "metadatas": {k: v for k, v in doc.items() if k not in ["id", "title", "description", "content"]}
        }
        out.write(json.dumps(record, ensure_ascii=False) + "\n")
    if not out_handle:
        out.close()

def json_to_jsonl(json_path, out_handle):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    if isinstance(data, list):
        for idx, obj in enumerate(data):
            if not isinstance(obj, dict):
                continue
            record = {
                "id": obj.get("id", f"{os.path.basename(json_path)}_{idx+1}"),
                "title": obj.get("title", os.path.basename(json_path)),
                "content": obj.get("description") or obj.get("content") or json.dumps(obj, ensure_ascii=False),
                "metadatas": {k: v for k, v in obj.items() if k not in ["id", "title", "description", "content"]}
            }
            out_handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    elif isinstance(data, dict):
        record = {
            "id": data.get("id", os.path.basename(json_path)),
            "title": data.get("title", os.path.basename(json_path)),
            "content": data.get("description") or data.get("content") or json.dumps(data, ensure_ascii=False),
            "metadatas": {k: v for k, v in data.items() if k not in ["id", "title", "description", "content"]}
        }
        out_handle.write(json.dumps(record, ensure_ascii=False) + "\n")

def process_folder(input_dir, output_dir=None, combine_path=None):
    if combine_path:
        with open(combine_path, 'w', encoding='utf-8') as out:
            for fname in os.listdir(input_dir):
                lower = fname.lower()
                in_path = os.path.join(input_dir, fname)
                if lower.endswith(('.yaml', '.yml')):
                    print(f"Procesando {fname} (YAML)...")
                    yaml_to_jsonl(in_path, None, out_handle=out)
                elif lower.endswith('.json'):
                    print(f"Procesando {fname} (JSON)...")
                    json_to_jsonl(in_path, out)
        print(f"Todos los documentos combinados en: {combine_path}")
    else:
        os.makedirs(output_dir, exist_ok=True)
        for fname in os.listdir(input_dir):
            lower = fname.lower()
            in_path = os.path.join(input_dir, fname)
            if lower.endswith(('.yaml', '.yml')):
                out_path = os.path.join(output_dir, fname + '.jsonl')
                print(f"Procesando {fname} (YAML)...")
                yaml_to_jsonl(in_path, out_path)
                print(f"  -> {out_path}")
            elif lower.endswith('.json'):
                out_path = os.path.join(output_dir, fname + '.jsonl')
                print(f"Procesando {fname} (JSON)...")
                with open(out_path, 'w', encoding='utf-8') as out:
                    json_to_jsonl(in_path, out)
                print(f"  -> {out_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convierte uno o varios archivos YAML o JSON a JSONL compatible con ChromaDB.")
    parser.add_argument('--input', '-i', help='Archivo YAML o JSON de entrada')
    parser.add_argument('--output', '-o', help='Archivo JSONL de salida')
    parser.add_argument('--input-dir', help='Carpeta con archivos YAML/JSON a convertir')
    parser.add_argument('--output-dir', help='Carpeta de salida para los archivos JSONL')
    parser.add_argument('--combine', help='Ruta de archivo JSONL combinado de salida (opcional, solo con --input-dir)')
    args = parser.parse_args()

    if args.input_dir and args.combine:
        process_folder(args.input_dir, combine_path=args.combine)
    elif args.input_dir and args.output_dir:
        process_folder(args.input_dir, output_dir=args.output_dir)
    elif args.input and args.output:
        lower = args.input.lower()
        if lower.endswith(('.yaml', '.yml')):
            yaml_to_jsonl(args.input, args.output)
        elif lower.endswith('.json'):
            with open(args.output, 'w', encoding='utf-8') as out:
                json_to_jsonl(args.input, out)
        else:
            print("El archivo de entrada debe ser .yaml, .yml o .json")
    else:
        print("Debes especificar --input y --output, o --input-dir y --output-dir, o --input-dir y --combine.") 