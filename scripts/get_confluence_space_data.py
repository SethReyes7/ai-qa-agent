import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", category=InsecureRequestWarning)

import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import json
from bs4 import BeautifulSoup
from tqdm import tqdm
import base64
import html
import re
import argparse
import sys
from PIL import Image
import pytesseract
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

CONFLUENCE_URL = os.getenv("CONFLUENCE_URL")
CONFLUENCE_USER = os.getenv("CONFLUENCE_USER")
CONFLUENCE_TOKEN = os.getenv("CONFLUENCE_TOKEN_OR_PASS")
SPACE_KEY = os.getenv("CONFLUENCE_SPACE_KEY")
PARENT_PAGE_ID = os.getenv("CONFLUENCE_PARENT_PAGE_ID")

if not all([CONFLUENCE_URL, CONFLUENCE_USER, CONFLUENCE_TOKEN, SPACE_KEY]):
    print("❌ Error: Faltan variables de entorno requeridas. Verifica tu archivo .env.")
    exit()

parser = argparse.ArgumentParser(description="Exporta páginas de Confluence a un archivo .jsonl")
parser.add_argument('--spaces', type=str, required=True, help='Claves de los espacios de Confluence separadas por coma (ej: DEV,PROD,QA)')
parser.add_argument('--output-dir', type=str, default='data/raw_data', help='Carpeta de salida para el archivo exportado')
parser.add_argument('--output-file', type=str, default=None, help='Nombre del archivo de salida (opcional)')
args, unknown = parser.parse_known_args()

output_dir = args.output_dir
os.makedirs(output_dir, exist_ok=True)

space_keys = [s.strip() for s in args.spaces.split(',') if s.strip()]

# Determinar el nombre del archivo de salida combinado
if args.output_file:
    output_file = args.output_file
elif not sys.argv[1:]:  # Si no hay argumentos, modo interactivo
    output_file = input("Nombre del archivo de salida combinado (default: confluence_pages.jsonl): ").strip()
    if not output_file:
        output_file = "confluence_pages.jsonl"
else:
    output_file = "confluence_pages.jsonl"
if not output_file.lower().endswith('.jsonl'):
    output_file += '.jsonl'
output_file = os.path.join(output_dir, output_file)

session = requests.Session()
session.auth = HTTPBasicAuth(CONFLUENCE_USER, CONFLUENCE_TOKEN)

# --- Funciones auxiliares ---
def get_page_details_by_id(page_id, base_url, session):
    api_url = f"{base_url.rstrip('/')}/rest/api/content/{page_id}?expand=version,metadata.labels"
    headers = {'Accept': 'application/json'}
    try:
        response = session.get(api_url, headers=headers, timeout=60, verify=False)
        response.raise_for_status()
        data = response.json()
        version = data.get('version', {})
        created_at = version.get('when')  # No hay created_at directo, pero la primera versión suele ser la creación
        updated_at = version.get('when')
        labels = [l['name'] for l in data.get('metadata', {}).get('labels', {}).get('results', [])] if data.get('metadata', {}).get('labels') else []
        return {
            'id': data.get('id'),
            'title': data.get('title', 'N/A'),
            'parent_id': data.get('_links', {}).get('webui', None),
            'created_at': created_at,
            'updated_at': updated_at,
            'labels': labels
        }
    except Exception as e:
        print(f"Error al obtener detalles de la página {page_id}: {e}")
        return None

def get_page_storage_content(page_id, base_url, session):
    api_url = f"{base_url.rstrip('/')}/rest/api/content/{page_id}?expand=body.storage"
    headers = {'Accept': 'application/json'}
    try:
        response = session.get(api_url, headers=headers, timeout=60, verify=False)
        response.raise_for_status()
        data = response.json()
        return data.get('body', {}).get('storage', {}).get('value', '')
    except Exception as e:
        print(f"Error al obtener storage de la página {page_id}: {e}")
        return ''

def extract_text_from_xhtml(xhtml_content, debug_info=None):
    if not xhtml_content:
        return ""
    # 1. Extraer todos los bloques de código con regex sobre el XHTML completo y reemplazarlos por un marcador único
    code_blocks = []
    marker_template = "__CODEBLOCK_MARKER_{}__"
    def code_block_replacer(match):
        idx = len(code_blocks)
        language = match.group('lang') or ''
        code = match.group('code').strip('\n')
        tag = f"[CÓDIGO{':' + language if language else ''}]"
        code_blocks.append(f"\n{tag}\n{code}\n[/CÓDIGO]\n")
        return marker_template.format(idx)
    pattern = re.compile(
        r'<ac:structured-macro[^>]*ac:name="code"[^>]*>.*?' +
        r'(?:<ac:parameter ac:name="language">(?P<lang>.*?)</ac:parameter>.*?)?' +
        r'<ac:plain-text-body><!\[CDATA\[(?P<code>.*?)\]\]></ac:plain-text-body>.*?</ac:structured-macro>',
        re.DOTALL
    )
    xhtml_with_markers = pattern.sub(code_block_replacer, xhtml_content)
    # 2. Procesar el XHTML con BeautifulSoup para extraer el texto plano y los marcadores
    soup = BeautifulSoup(xhtml_with_markers, 'lxml')
    output_lines = []
    for elem in soup.recursiveChildGenerator():
        if getattr(elem, 'name', None) is None:
            text = str(elem)
            if text.strip():
                # Reemplazar los marcadores por el bloque de código correspondiente
                while True:
                    marker_match = re.search(r'__CODEBLOCK_MARKER_(\d+)__', text)
                    if not marker_match:
                        break
                    idx = int(marker_match.group(1))
                    code_block = code_blocks[idx] if idx < len(code_blocks) else ''
                    text = text.replace(marker_match.group(0), code_block, 1)
                output_lines.append(text.strip())
    return "\n".join(output_lines)

def get_all_pages_in_space(base_url, space_key, session):
    pages_data = []
    current_api_url = f"{base_url.rstrip('/')}/rest/api/space/{space_key}/content/page"
    current_api_params = {'start': 0, 'limit': 50, 'expand': 'title'}
    headers = {'Accept': 'application/json'}
    is_first_request = True
    total_found = 0
    print("Recolectando páginas del espacio (esto puede tardar)...")
    with tqdm(desc="Páginas recolectadas", unit="páginas") as pbar:
        while current_api_url:
            params_for_request = current_api_params if is_first_request else None
            is_first_request = False
            try:
                response = session.get(current_api_url, params=params_for_request, headers=headers, timeout=60, verify=False)
                response.raise_for_status()
                data = response.json()
                current_pages_results = data.get('results', [])
                if not current_pages_results:
                    break
                for page_item in current_pages_results:
                    pages_data.append({'id': page_item.get('id'), 'title': page_item.get('title', 'N/A')})
                    pbar.update(1)
                next_url_from_api = data.get('_links', {}).get('next')
                if next_url_from_api:
                    current_api_url = f"{base_url}{next_url_from_api}" if next_url_from_api.startswith('/') else next_url_from_api
                    current_api_params = None
                else:
                    break
            except Exception as e:
                print(f"Error al obtener páginas del espacio: {e}")
                return []
    return pages_data

def get_direct_children(parent_page_id, base_url, session):
    children_data = []
    current_api_url = f"{base_url.rstrip('/')}/rest/api/content/{parent_page_id}/child/page"
    current_api_params = {'start': 0, 'limit': 50, 'expand': 'title'}
    headers = {'Accept': 'application/json'}
    is_first_request = True
    while current_api_url:
        params_for_request = current_api_params if is_first_request else None
        is_first_request = False
        try:
            response = session.get(current_api_url, params=params_for_request, headers=headers, timeout=60, verify=False)
            response.raise_for_status()
            data = response.json()
            current_children_results = data.get('results', [])
            if not current_children_results:
                break
            for child_item in current_children_results:
                children_data.append({'id': child_item.get('id'), 'title': child_item.get('title', 'N/A')})
            next_url_from_api = data.get('_links', {}).get('next')
            if next_url_from_api:
                current_api_url = f"{base_url}{next_url_from_api}" if next_url_from_api.startswith('/') else next_url_from_api
                current_api_params = None
            else:
                break
        except Exception as e:
            print(f"Error al obtener hijos de la página {parent_page_id}: {e}")
            return []
    return children_data

def collect_all_pages(parent_id, base_url, session, all_pages, hierarchy=None, depth=0, parent=None):
    if hierarchy is None:
        hierarchy = []
    if parent_id in all_pages:
        return
    details = get_page_details_by_id(parent_id, base_url, session)
    if details:
        current_hierarchy = hierarchy + [details['title']]
        children = get_direct_children(parent_id, base_url, session)
        all_pages[parent_id] = {
            'title': details['title'],
            'parent_id': parent,
            'hierarchy': current_hierarchy,
            'depth': depth,
            'children_ids': [child['id'] for child in children],
            'has_children': bool(children),
            'created_at': details['created_at'],
            'updated_at': details['updated_at'],
            'labels': details['labels']
        }
        print(f"{'  '*depth}Recolectando: {details['title']} (ID: {parent_id})")
        for child in children:
            collect_all_pages(child['id'], base_url, session, all_pages, current_hierarchy, depth=depth+1, parent=parent_id)

# --- Flujo principal para múltiples espacios o IDs de página raíz ---
all_pages = {}
for space_key in space_keys:
    print(f"Procesando: {space_key}")
    if space_key.isdigit():
        print(f"  (Detectado como ID de página raíz)")
        collect_all_pages(space_key, CONFLUENCE_URL, session, all_pages)
        print(f"Total de páginas recolectadas desde ID {space_key}: {len([p for p in all_pages.values() if p['hierarchy'][0] == p['title'] and p.get('space_key') == space_key])}")
    else:
        print(f"  (Detectado como clave de espacio)")
        for page in get_all_pages_in_space(CONFLUENCE_URL, space_key, session):
            all_pages[page['id']] = {'title': page['title'], 'parent_id': None, 'hierarchy': [page['title']], 'depth': 0, 'children_ids': [], 'has_children': False, 'created_at': None, 'updated_at': None, 'labels': [], 'space_key': space_key}
        print(f"Total de páginas recolectadas en espacio {space_key}: {len([p for p in all_pages.values() if p['hierarchy'][0] == p['title'] and p.get('space_key') == space_key])}")

print(f"Total de páginas recolectadas en todos los espacios/IDs: {len(all_pages)}")

# --- Paralelizar la descarga del contenido de las páginas ---
def fetch_page_content(page_id):
    return page_id, get_page_storage_content(page_id, CONFLUENCE_URL, session)

print("Descargando contenido de páginas en paralelo...")
with ThreadPoolExecutor(max_workers=8) as executor:
    future_to_page = {executor.submit(fetch_page_content, page_id): page_id for page_id in all_pages}
    for future in tqdm(as_completed(future_to_page), total=len(all_pages), desc="Descargando contenido de páginas"):
        page_id, xhtml_content = future.result()
        all_pages[page_id]['xhtml_content'] = xhtml_content

with open(output_file, "w", encoding="utf-8") as f:
    exportadas = 0
    for page_id, meta in tqdm(all_pages.items(), desc="Exportando páginas"):
        page_title = meta['title']
        parent_id = meta['parent_id']
        hierarchy = meta['hierarchy']
        depth = meta['depth']
        has_children = meta.get('has_children', False)
        space_key = meta.get('space_key', None)
        xhtml_content = meta.get('xhtml_content', '')
        text_content = extract_text_from_xhtml(xhtml_content, debug_info={"id": page_id, "title": page_title})
        # Omitir si el contenido está vacío y no tiene hijos
        if (not text_content or not text_content.strip()) and not has_children:
            continue
        doc = {
            "id": page_id,
            "title": page_title,
            "content": text_content,
            "parent_id": parent_id,
            "hierarchy": hierarchy,
            "depth": depth,
            "has_children": has_children,
            "created_at": meta.get('created_at'),
            "updated_at": meta.get('updated_at'),
            "labels": meta.get('labels', []),
            "space_key": space_key
        }
        f.write(json.dumps(doc, ensure_ascii=False) + "\n")
        exportadas += 1

print(f"\n¡Exportación combinada completada! {exportadas} páginas guardadas en '{output_file}'.")
