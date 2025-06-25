import os
import argparse

def fragment_jsonl_by_size(input_file, output_dir, max_mb=10):
    max_bytes = max_mb * 1024 * 1024
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    part = 1
    current_size = 0
    out_path = os.path.join(output_dir, f"{base_name}_part{part}.jsonl")
    out = open(out_path, "w", encoding="utf-8")
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            line_bytes = line.encode("utf-8")
            if current_size + len(line_bytes) > max_bytes and current_size > 0:
                out.close()
                part += 1
                out_path = os.path.join(output_dir, f"{base_name}_part{part}.jsonl")
                out = open(out_path, "w", encoding="utf-8")
                current_size = 0
            out.write(line)
            current_size += len(line_bytes)
    out.close()
    print(f"Fragmentación completada. {part} archivos creados en '{output_dir}'.")

def main():
    parser = argparse.ArgumentParser(description="Fragmenta un archivo .jsonl en archivos de máximo 10 MB cada uno. Ejemplo: --input data/cleaned_data/20240613_xxxx/archivo.jsonl --output-dir data/fragmented_data/20240613_xxxx/")
    parser.add_argument('--input', '-i', required=True, help='Archivo .jsonl de entrada')
    parser.add_argument('--output-dir', '-o', required=True, help='Carpeta de salida para los fragmentos')
    parser.add_argument('--max-mb', type=int, default=10, help='Tamaño máximo de cada fragmento en MB (default: 10)')
    args = parser.parse_args()
    fragment_jsonl_by_size(args.input, args.output_dir, args.max_mb)

if __name__ == "__main__":
    main()
