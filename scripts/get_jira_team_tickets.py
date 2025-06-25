import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import json
import argparse

load_dotenv()

JIRA_EMAIL = os.getenv("CONFLUENCE_USER")
JIRA_API_TOKEN = os.getenv("CONFLUENCE_TOKEN_OR_PASS")
JIRA_DOMAIN = os.getenv("JIRA_DOMAIN")

if not all([JIRA_EMAIL, JIRA_API_TOKEN, JIRA_DOMAIN]):
    print("❌ Error: Faltan variables de entorno. Verifica tu archivo .env.")
    exit()

parser = argparse.ArgumentParser(description="Exporta tickets de JIRA a un archivo .jsonl")
parser.add_argument('--output-dir', type=str, default='data/raw_data', help='Carpeta de salida para el archivo exportado')
parser.add_argument('--team', type=str, default=None, help='Nombre del equipo de JIRA (Team)')
args, unknown = parser.parse_known_args()

output_dir = args.output_dir
os.makedirs(output_dir, exist_ok=True)

output_file = os.path.join(output_dir, "tickets_equipo_chromadb.jsonl")

# Parámetros de búsqueda
team_name = args.team
if not team_name:
    team_name = input("Nombre del equipo (Team): ").strip()
    if not team_name:
        print("❌ Error: No se ingresó el nombre del equipo.")
        exit()

# Estados y tipos por defecto (no se piden por consola)
estados = ["Done", "In Development", "Code Review", "Regression", "testing", "UAT", "Open", "To Do", "Ready to Deploy", "Blocked"]
tipos = ["Bug", "Story", "Task", "Tech Story"]

# Construir JQL
estados_jql = ", ".join([f'"{e}"' for e in estados])
tipos_jql = ", ".join([f'"{t}"' for t in tipos])
jql = (
    f'project = PRODU AND '
    f'issuetype in ({tipos_jql}) AND '
    f'status in ({estados_jql}) AND '
    f'"team[select list (multiple choices)]" = "{team_name}" '
    'ORDER BY created DESC'
)

url = f"https://{JIRA_DOMAIN}/rest/api/3/search"
headers = {"Accept": "application/json"}

all_issues = []
start_at = 0
max_results = 100

print(f"Consultando tickets con JQL:\n{jql}\n")

while True:
    params = {
        "jql": jql,
        "fields": "summary,description",
        "maxResults": max_results,
        "startAt": start_at
    }
    response = requests.get(url, headers=headers, params=params, auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN))
    if response.status_code != 200:
        print(f"❌ Error: {response.status_code}")
        print(response.text)
        exit()
    data = response.json()
    issues = data.get("issues", [])
    if not issues:
        break
    all_issues.extend(issues)
    if len(issues) < max_results:
        break
    start_at += max_results

print(f"{len(all_issues)} tickets guardados en tickets_equipo_chromadb.jsonl listos para ChromaDB.")

with open(output_file, "w", encoding="utf-8") as f:
    for issue in all_issues:
        fields = issue.get("fields", {})
        titulo = fields.get("summary", "Sin título")
        descripcion = fields.get("description")
        if isinstance(descripcion, str):
            descripcion_texto = descripcion
        else:
            descripcion_texto = "Sin descripción"
        ticket_data = {
            "id": issue.get("key"),
            "title": titulo,
            "content": descripcion_texto,
            "status": fields.get("status", {}).get("name"),
            "created_at": fields.get("created"),
            "updated_at": fields.get("updated"),
            "assignee": fields.get("assignee", {}).get("displayName"),
            "labels": fields.get("labels", []),
            "parent_id": fields.get("parent", {}).get("key"),
            "project": fields.get("project", {}).get("key"),
            "type": fields.get("issuetype", {}).get("name")
        }
        f.write(json.dumps(ticket_data, ensure_ascii=False) + "\n")
