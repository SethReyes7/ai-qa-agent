import os
import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import json

load_dotenv()

JIRA_EMAIL = os.getenv("CONFLUENCE_USER")
JIRA_API_TOKEN = os.getenv("CONFLUENCE_TOKEN_OR_PASS")
JIRA_DOMAIN = os.getenv("JIRA_DOMAIN")

if not all([JIRA_EMAIL, JIRA_API_TOKEN, JIRA_DOMAIN]):
    print("❌ Error: Faltan variables de entorno. Verifica tu archivo .env.")
    exit()

# Solicita la clave del ticket
issue_key = input("Introduce la clave del ticket JIRA (por ejemplo, PRODU-51415): ").strip().upper()
if not issue_key:
    print("❌ Error: No se ingresó ninguna clave de ticket.")
    exit()

url = f"https://{JIRA_DOMAIN}/rest/api/3/issue/{issue_key}"
headers = {"Accept": "application/json"}

print(f"Consultando ticket: {issue_key} en {JIRA_DOMAIN}...\n")

try:
    response = requests.get(url, headers=headers, auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN))
except Exception as e:
    print(f"❌ Error al conectar con la API de Jira: {e}")
    exit()

if response.status_code != 200:
    print(f"❌ Error: {response.status_code}")
    print(response.text)
    exit()

fields = response.json().get("fields", {})

output_filename = f"ticket_fields_{issue_key}.txt"
with open(output_filename, "w", encoding="utf-8") as outfile:
    outfile.write(f"Campos y valores del ticket {issue_key} (id: valor):\n\n")
    for field_id, value in fields.items():
        outfile.write(f"{field_id}: {json.dumps(value, ensure_ascii=False, indent=2)}\n\n")

    # Opcional: nombres amigables
    fields_url = f"https://{JIRA_DOMAIN}/rest/api/3/field"
    fields_response = requests.get(fields_url, headers=headers, auth=HTTPBasicAuth(JIRA_EMAIL, JIRA_API_TOKEN))
    if fields_response.status_code == 200:
        all_fields = fields_response.json()
        id_to_name = {field['id']: field['name'] for field in all_fields}
        outfile.write("\nResumen de campos (id: nombre):\n")
        for field_id in fields.keys():
            outfile.write(f"{field_id}: {id_to_name.get(field_id, 'Desconocido')}\n")
    else:
        outfile.write("No se pudieron obtener los nombres amigables de los campos de Jira.\n")

print(f"Campos exportados a {output_filename}")
