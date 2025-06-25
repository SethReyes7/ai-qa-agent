# Pipeline Automatizado: Confluence/JIRA → ChromaDB

Este script ejecuta todo el flujo de extracción, limpieza, fragmentación, carga y exportación de datos técnicos desde Confluence y/o JIRA hacia ChromaDB, usando subcarpetas por ejecución para aislar los resultados.

---

## Ejecución completa (recomendada)

```bash
python scripts/run_full_pipeline.py \
  --confluence-spaces <CLAVE1,CLAVE2,...> \
  --jira-teams <EQUIPO1,EQUIPO2,...> \
  --collection <NOMBRE_BASE_COLECCION>
```

**Ejemplo real (múltiples espacios y equipos JIRA):**
```bash
python scripts/run_full_pipeline.py \
  --confluence-spaces DEV,PROD,QA \
  --jira-teams BACKEND,FRONTEND \
  --collection org_knowledge
```

Esto ejecuta todas las etapas del pipeline y guarda los archivos de cada paso en subcarpetas con un identificador único (timestamp).

---

## Nombre de la colección en ChromaDB

- El nombre final de la colección será **el nombre base que indiques más un identificador único (timestamp)**.
- Ejemplo: si usas `--collection org_knowledge` y la ejecución es el 7 de junio de 2024 a las 15:30:12, la colección se llamará:
  
  ```
  org_knowledge_20240607_153012
  ```
- El nombre final se muestra en los logs y queda registrado en el archivo `execution_log.txt` dentro de la carpeta de exportación de esa ejecución.
- **No es posible sobrescribir colecciones existentes ni forzar el nombre original.**

---

## Argumentos principales

- `--confluence-spaces`  Claves de los espacios de Confluence **y/o IDs de página raíz** separadas por coma (ej: DEV,PROD,120258649) (opcional)
- `--confluence-parent-page-id` ID de la página raíz (opcional, solo si se usa clave de espacio)
- `--jira-teams`         Nombres de los equipos de JIRA separados por coma (ej: BACKEND,FRONTEND) (opcional)
- `--collection`         Nombre base de la colección en ChromaDB (requerido, se le agregará un identificador único)
- `--max-size-mb`        Tamaño máximo de fragmento en MB (default: 10)
- `--output-format`      Formato de exportación final (`jsonl`, `txt`, `md`)
- `--skip-extract`       Saltar la extracción de datos
- `--skip-clean`         Saltar la limpieza de datos
- `--skip-fragment`      Saltar la fragmentación
- `--skip-load`          Saltar la carga a ChromaDB
- `--skip-export`        Saltar la exportación final

Puedes usar solo Confluence, solo JIRA, o ambos.

---

## Extracción de Confluence: múltiples espacios y/o IDs de página raíz

- Ahora puedes extraer datos de **varios espacios de Confluence y/o IDs de página raíz en una sola ejecución** usando el argumento `--confluence-spaces` con una lista separada por comas (ejemplo: `--confluence-spaces DEV,PROD,120258649`).
- Si un valor es numérico, se trata como ID de página raíz y se extrae todo su árbol; si es alfanumérico, se trata como clave de espacio.
- El archivo de salida combinado se llamará automáticamente `confluence_pages.jsonl` (o el nombre que indiques con `--output-file`).
- Cada documento incluirá el campo `space_key` para identificar de qué espacio o ID proviene.
- Solo si ejecutas el script `get_confluence_space_data.py` de forma manual/interactiva, se te pedirá el nombre del archivo de salida por consola (puedes dejarlo vacío para usar el default).

---

## Extracción de JIRA: nombre del equipo

- **Cuando ejecutas el pipeline maestro, el nombre del equipo de JIRA se pasa automáticamente al script y nunca se pedirá por input.**
- Solo si ejecutas el script `get_jira_team_tickets.py` de forma manual/interactiva, se te pedirá el nombre del equipo por consola.

---

## Carga y exportación de ChromaDB: nombre de la colección

- **Cuando ejecutas el pipeline maestro, el nombre de la colección se pasa automáticamente a los scripts de carga y exportación mediante el argumento `--collection`, por lo que nunca se pedirá por input.**
- Solo si ejecutas los scripts `add_data_to_chromadb.py` o `export_data_from_chromadb.py` de forma manual/interactiva, se te pedirá el nombre de la colección por consola.

---

## Ejecución solo de una fuente

**Solo Confluence (uno o varios espacios):**
```bash
python scripts/run_full_pipeline.py --confluence-spaces DEV,PROD --collection org_knowledge
```

**Solo JIRA (uno o varios equipos):**
```bash
python scripts/run_full_pipeline.py --jira-teams BACKEND,FRONTEND --collection org_knowledge
```

---

## Cambiar formato de exportación

```bash
python scripts/run_full_pipeline.py \
  --confluence-spaces DEV \
  --collection org_knowledge \
  --output-format md
```

---

## Saltar etapas (útil para reintentos o debug)

```bash
python scripts/run_full_pipeline.py \
  --collection org_knowledge \
  --skip-extract --skip-clean --skip-fragment --skip-load
```

---

## Resultados

- Los archivos generados se guardan en subcarpetas de `raw_data/`, `cleaned_data/`, `fragmented_data/`, y `export/` con el identificador de la ejecución (timestamp).
- El archivo exportado final estará en `export/<timestamp>/<NOMBRE_COLECCION_FINAL>.<formato>`
- El nombre final de la colección y los tiempos de ejecución quedan registrados en `export/<timestamp>/execution_log.txt`

---

## Buscar o exportar una colección por identificador

Para buscar o exportar una colección específica en ChromaDB, usa el nombre completo generado (por ejemplo, `org_knowledge_20240607_153012`). Puedes consultar los nombres de colecciones existentes con el script de consulta o revisando el log de ejecución.

---

## Requisitos

- Python 3.8+
- Dependencias indicadas en el proyecto
- Variables de entorno configuradas para acceso a Confluence y JIRA

---

## Notas

- Puedes ejecutar varias veces el pipeline sin sobrescribir resultados anteriores.
- Si solo quieres exportar una colección ya cargada, usa los flags `--skip-*` para saltar etapas.
- Si tienes dudas, revisa los logs que imprime el script en cada paso y el archivo `execution_log.txt`. 