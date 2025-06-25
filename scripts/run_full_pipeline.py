#!/usr/bin/env python3
import argparse
import subprocess
import sys
import os
import datetime
import time

def run_step(cmd, desc, log_lines):
    print(f"\n[+] {desc}...")
    start = time.time()
    try:
        subprocess.run(cmd, check=True)
        elapsed = time.time() - start
        print(f"[✔] {desc} completado en {elapsed:.2f} segundos.")
        log_lines.append(f"{desc}: {elapsed:.2f} segundos")
        return elapsed
    except subprocess.CalledProcessError as e:
        print(f"[!] Error en el paso: {desc}")
        log_lines.append(f"ERROR en {desc}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Pipeline completo: extrae, limpia, fragmenta y carga datos de Confluence/JIRA en ChromaDB."
    )
    parser.add_argument('--confluence-spaces', type=str, help='Claves de los espacios de Confluence separadas por coma (ej: DEV,PROD,QA) (opcional)')
    parser.add_argument('--jira-teams', type=str, help='Nombres de los equipos de JIRA separados por coma (ej: BACKEND,FRONTEND) (opcional)')
    parser.add_argument('--collection', type=str, required=True, help='Nombre base de la colección en ChromaDB (se le agregará un identificador único)')
    parser.add_argument('--max-size-mb', type=int, default=10, help='Tamaño máximo de fragmento en MB')
    parser.add_argument('--skip-extract', action='store_true', help='Saltar extracción de datos')
    parser.add_argument('--skip-clean', action='store_true', help='Saltar limpieza de datos')
    parser.add_argument('--skip-fragment', action='store_true', help='Saltar fragmentación')
    parser.add_argument('--skip-load', action='store_true', help='Saltar carga a ChromaDB')
    parser.add_argument('--skip-export', action='store_true', help='Saltar exportación final')
    parser.add_argument('--output-format', type=str, default='jsonl', choices=['jsonl','txt','md'], help='Formato de exportación final')
    args = parser.parse_args()

    # 1. Generar identificador único (timestamp)
    run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    collection_name = f"{args.collection}_{run_id}"

    # 2. Crear subcarpetas
    raw_dir = f"data/raw_data/{run_id}"
    clean_dir = f"data/cleaned_data/{run_id}"
    frag_dir = f"data/fragmented_data/{run_id}"
    export_dir = f"exported_data/{run_id}"
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(clean_dir, exist_ok=True)
    os.makedirs(frag_dir, exist_ok=True)
    os.makedirs(export_dir, exist_ok=True)

    log_lines = []
    total_start = time.time()

    print(f"\n[INFO] Nombre final de la colección en ChromaDB: {collection_name}\n")
    log_lines.append(f"Nombre final de la colección: {collection_name}")

    # 3. Extracción
    if not args.skip_extract:
        if args.confluence_spaces:
            confluence_cmd = [
                "python", "scripts/get_confluence_space_data.py",
                "--spaces", args.confluence_spaces,
                "--output-dir", raw_dir
            ]
            run_step(
                confluence_cmd,
                f"Extracción de Confluence ({args.confluence_spaces})",
                log_lines
            )
        if args.jira_teams:
            for team in [t.strip() for t in args.jira_teams.split(',') if t.strip()]:
                run_step([
                    "python", "scripts/get_jira_team_tickets.py",
                    "--output-dir", raw_dir,
                    "--team", team
                ], f"Extracción de JIRA ({team})", log_lines)

    # 4. Limpieza
    if not args.skip_clean:
        run_step([
            "python", "scripts/data_cleaning.py",
            "--input-dir", raw_dir,
            "--output-dir", clean_dir
        ], f"Limpieza de datos en {raw_dir}", log_lines)

    # 5. Fragmentación
    if not args.skip_fragment:
        for archivo in os.listdir(clean_dir):
            if archivo.endswith('.jsonl'):
                input_path = os.path.join(clean_dir, archivo)
                run_step([
                    "python", "scripts/data_fragmentation.py",
                    "--input", input_path,
                    "--output-dir", frag_dir,
                    "--max-mb", str(args.max_size_mb)
                ], f"Fragmentación de {archivo}", log_lines)

    # 6. Carga a ChromaDB
    if not args.skip_load:
        # Usar fragmented_data si hay archivos, si no cleaned_data
        docs_dir = frag_dir if os.listdir(frag_dir) else clean_dir
        run_step([
            "python", "chroma_db_scripts/add_data_to_chromadb.py",
            "--documents-dir", docs_dir,
            "--collection", collection_name
        ], f"Carga de documentos a ChromaDB en colección {collection_name} desde {docs_dir}", log_lines)

    # 7. Exportación final
    if not args.skip_export:
        output_file = os.path.join(export_dir, f"{collection_name}.{args.output_format}")
        run_step([
            "python", "chroma_db_scripts/export_data_from_chromadb.py",
            "--collection", collection_name,
            "--output-file", output_file
        ], f"Exportación de colección {collection_name} a {output_file}", log_lines)

    total_elapsed = time.time() - total_start
    print(f"\n[✔] Pipeline completado exitosamente en {total_elapsed:.2f} segundos. Archivos de esta ejecución en subcarpetas con ID: {run_id}")
    log_lines.append(f"TOTAL: {total_elapsed:.2f} segundos")

    # Guardar log de ejecución
    log_path = os.path.join(export_dir, "execution_log.txt")
    with open(log_path, "w") as f:
        f.write("Ejecución pipeline - " + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "\n")
        f.write(f"ID de ejecución: {run_id}\n\n")
        for line in log_lines:
            f.write(line + "\n")
        f.write(f"\nNombre final de la colección: {collection_name}\n")
        f.write(f"Archivos generados en: {raw_dir}, {clean_dir}, {frag_dir}, {export_dir}\n")

if __name__ == "__main__":
    main() 