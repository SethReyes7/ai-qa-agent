import re
import json
import argparse
from tqdm import tqdm
import os

FRAGMENT_LENGTH = 2000  # Fragmenta automáticamente si el contenido limpio supera este valor

def clean_content(text):
    # Elimina marcas de Atlassian y colores
    text = re.sub(r":(info|check_mark|cross_mark):", "", text)
    text = re.sub(r"atlassian-[\w_-]+", "", text)
    text = re.sub(r"#[A-Fa-f0-9]{6}", "", text)  # colores hexadecimales
    # Elimina líneas que solo tienen caracteres especiales o están vacías
    lines = text.splitlines()
    cleaned_lines = []
    seen = set()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if re.fullmatch(r"[\W_]+", line):
            continue
        if line in seen:
            continue  # deduplicación exacta
        seen.add(line)
        cleaned_lines.append(line)
    # Normaliza espacios múltiples
    cleaned_text = "\n".join(cleaned_lines)
    cleaned_text = re.sub(r"[ \t]+", " ", cleaned_text)
    cleaned_text = re.sub(r"\n{2,}", "\n", cleaned_text)
    return cleaned_text.strip()

def process_jsonl(input_path, output_path):
    with open(input_path, 'r', encoding='utf-8') as fin, open(output_path, 'w', encoding='utf-8') as fout:
        for line in tqdm(fin, desc=f"Limpiando {os.path.basename(input_path)}"):
            record = json.loads(line)
            content = record.get('content', '')
            cleaned = clean_content(content)
            if len(cleaned) > FRAGMENT_LENGTH:
                # Fragmenta el contenido largo automáticamente
                fragments = [cleaned[i:i+FRAGMENT_LENGTH] for i in range(0, len(cleaned), FRAGMENT_LENGTH)]
                for idx, frag in enumerate(fragments):
                    frag_record = record.copy()  # Copia todos los metadatos
                    frag_record['content'] = frag
                    frag_record['fragment'] = idx + 1
                    frag_record['total_fragments'] = len(fragments)
                    fout.write(json.dumps(frag_record, ensure_ascii=False) + '\n')
            else:
                record['content'] = cleaned
                fout.write(json.dumps(record, ensure_ascii=False) + '\n')

def main():
    parser = argparse.ArgumentParser(description="Limpia y optimiza todos los archivos .jsonl en la carpeta de entrada y los guarda en la de salida. Fragmenta automáticamente si el contenido supera 2,000 caracteres.")
    parser.add_argument('--input-dir', type=str, default='data/raw_data', help='Carpeta de entrada')
    parser.add_argument('--output-dir', type=str, default='data/cleaned_data', help='Carpeta de salida')
    args = parser.parse_args()

    raw_dir = args.input_dir
    cleaned_dir = args.output_dir
    os.makedirs(cleaned_dir, exist_ok=True)

    archivos = [f for f in os.listdir(raw_dir) if f.endswith('.jsonl')]
    if not archivos:
        print(f'No se encontraron archivos .jsonl en {raw_dir}')
        return

    for archivo in archivos:
        input_path = os.path.join(raw_dir, archivo)
        output_path = os.path.join(cleaned_dir, archivo)
        process_jsonl(input_path, output_path)
        print(f"Archivo procesado: {archivo} -> {output_path}")

if __name__ == "__main__":
    main()
