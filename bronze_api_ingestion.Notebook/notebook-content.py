# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# 1) Configuration
import json, time
import requests
from datetime import datetime, timezone
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from delta.tables import DeltaTable

vl = notebookutils.variableLibrary.getLibrary("Variables")

def vl_get(nom, defaut=None):
    try:
        return getattr(vl, nom)
    except Exception:
        return defaut

lakehouse_id = vl.LAKEHOUSE_ID
workspace_id = vl_get("WORKSPACE_ID") or notebookutils.runtime.context["currentWorkspaceId"]
PAGE_LIMIT = int(vl_get("PAGE_LIMIT", 100))
QLIK_TENANT_URL = vl.QLIK_TENANT_URL.rstrip("/")
API_BASE_URL    = f"{QLIK_TENANT_URL}/api/v1"

#get api token

files_root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files"

API_CONFIG_FILE = vl_get("QLIK_API_TOKEN", "Config/secrets.json")   # chemin relatif à Files/

contenu   = notebookutils.fs.head(f"{files_root}/{API_CONFIG_FILE}", 1_000_000)
api_conf  = json.loads(contenu)

api_token = api_conf["QLIK_API_TOKEN"]

tables_root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"

def table_path(schema: str, table: str) -> str:
    return f"{tables_root}/{schema}/{table}"

CFG_METHOD = table_path("api_config", "cfg_api_method")
CFG_FIELDS   = table_path("api_config", "cfg_api_fields")
BRONZE      = "bronze"

# print("Tables :", tables_root)
# print("API    :", API_BASE_URL)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) Lecture de la configuration
methodes = (spark.read.format("delta").load(CFG_METHOD)
            .where(F.col("active") == True)
            .collect())

champs_df = spark.read.format("delta").load(CFG_FIELDS)
champs = {}

for r in champs_df.orderBy("method", "order").collect():
    champs.setdefault(r["method"], []).append((r["field"], r["format"] or "string"))

# Méthode sans champ configuré -> None = tous les champs de l'API
for m in methodes:
    champs.setdefault(m["method"], None)

print(f"{len(methodes)} méthode(s) active(s) :", [m["method"] for m in methodes])

# for m in methodes:
#     f = champs[m["method"]]
#     print(f"  {m['method']} : {'All fields selected' if f is None else f}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3) Appels API : un GET par méthode, avec le paramètre fields
SESSION = requests.Session()
SESSION.headers.update({
    "Authorization": f"Bearer {api_token}",
    "Accept": "application/json",
})

def get_with_retry(url, params=None, tentatives=5):
    """GET avec nouvel essai automatique si l'API est saturée (429) ou en erreur serveur (5xx)."""
    for i in range(tentatives):
        resp = SESSION.get(url, params=params, timeout=60)
        if resp.status_code in (429, 500, 502, 503, 504):
            attente = int(resp.headers.get("Retry-After", 2 ** i))
            print(f"    HTTP {resp.status_code}, nouvel essai dans {attente}s")
            time.sleep(attente)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()

def fetch_all(method, fields=None, limit=PAGE_LIMIT):
    """Appelle une méthode Qlik et suit la pagination (links.next.href)."""
    url = f"{API_BASE_URL}/{method}"

    params = {"limit": limit}
    if fields:
        params["fields"] = ",".join(f for f, _ in fields)

    records, page, deja_vues = [], 0, set()
    while url:
        print(f"    GET {url}" + (f"  params={params}" if page == 0 else ""))
        payload = get_with_retry(url, params if page == 0 else None)
        data = payload.get("data", [])
        records.extend(data)
        page += 1
        deja_vues.add(url)

        next_url = ((payload.get("links") or {}).get("next") or {}).get("href")

        # Arrêts de sécurité
        if not data or len(data) < limit:   # page vide ou incomplète -> c'était la dernière
            break
        if next_url in deja_vues:           # l'API renvoie une URL déjà appelée -> boucle infinie évitée
            break
        url = next_url

    return records, page

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4) Stockage bronze (fichiers JSON) et journal des runs
import uuid
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, LongType

SOURCE      = "qlik"
BRONZE_ROOT = f"{files_root}/bronze/{SOURCE}"
CFG_RUN_LOG = table_path("api_config", "cfg_api_run_log")

# Identifiant unique de ce run : date/heure UTC + suffixe aléatoire
RUN_ID = f"{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}"
print("Run :", RUN_ID)

def bronze_file_path(method, incremental):
    """Un dossier par méthode ; un seul fichier en full, un fichier par run en incrémental."""
    dossier = f"{BRONZE_ROOT}/{method}"
    if incremental:
        return f"{dossier}/{SOURCE}_{method}_{RUN_ID}.json"
    return f"{dossier}/{SOURCE}_{method}.json"

def write_bronze(method, incremental, records, fields):
    """Écrit la réponse brute, entourée de ses métadonnées, dans un fichier JSON."""
    doc = {
        "run_id":           RUN_ID,
        "source":           SOURCE,
        "method":           method,
        "incremental":      incremental,
        "ingestion_ts":     datetime.now(timezone.utc).isoformat(),
        "source_url":       f"{API_BASE_URL}/{method}",
        "requested_fields": [f for f, _ in fields] if fields else None,
        "nb_records":       len(records),
        "data":             records,
    }
    path = bronze_file_path(method, incremental)
    notebookutils.fs.put(path, json.dumps(doc, ensure_ascii=False), True)   # True = écraser si existe
    return path

def compute_watermark(records, field):
    """Plus grande valeur du champ watermark dans le lot (dates ISO -> comparaison texte fiable)."""
    valeurs = [str(r[field]) for r in records if isinstance(r, dict) and r.get(field) is not None]
    return max(valeurs) if valeurs else None

def update_config(method, new_wm=None):
    valeurs = {"date_maj": F.current_timestamp()}
    if new_wm is not None:
        valeurs["lastvalue_watermark"] = F.lit(new_wm)
    DeltaTable.forPath(spark, CFG_METHOD).update(condition=F.col("method") == method, set=valeurs)

RUN_LOG_SCHEMA = StructType([
    StructField("run_id",           StringType()),
    StructField("method",           StringType()),
    StructField("incremental",      BooleanType()),
    StructField("start_ts",         TimestampType()),
    StructField("end_ts",           TimestampType()),
    StructField("status",           StringType()),
    StructField("error",            StringType()),
    StructField("nb_records",       LongType()),
    StructField("nb_pages",         LongType()),
    StructField("requested_fields", StringType()),
    StructField("file_path",        StringType()),
    StructField("watermark_before", StringType()),
    StructField("watermark_after",  StringType()),
])

def write_log(rows):
    """Ajoute les lignes au journal ; la table est créée automatiquement au premier run."""
    spark.createDataFrame(rows, RUN_LOG_SCHEMA).write.format("delta").mode("append").save(CFG_RUN_LOG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 5) Boucle : appel -> stockage -> mise à jour config -> journal
log_rows = []

for m in methodes:
    method      = m["method"]
    incremental = bool(m["incremental"])
    fields      = champs[method]
    wm_field    = m["field_watermark"]
    wm_before   = m["lastvalue_watermark"]

    # En incrémental, le champ watermark doit faire partie des champs demandés
    if incremental and wm_field and fields and wm_field not in [f for f, _ in fields]:
        fields = fields + [(wm_field, "string")]

    print(f"--> {method} ({'incrémental' if incremental else 'full'})")
    start = datetime.now(timezone.utc)
    status, error, nb, pages, path, wm_after = "OK", None, None, None, None, wm_before

    try:
        records, pages = fetch_all(method, fields)
        nb = len(records)

        if incremental and nb == 0:
            print("    aucun nouvel enregistrement, pas de fichier écrit")
        else:
            path = write_bronze(method, incremental, records, fields)
            print(f"    {nb} enregistrement(s) -> {path}")

        if incremental and wm_field:
            wm_after = compute_watermark(records, wm_field) or wm_before
        update_config(method, wm_after if incremental else None)

    except Exception as e:
        status, error = "ERREUR", str(e)[:1000]
        print(f"    ERREUR : {error}")

    log_rows.append((
        RUN_ID, method, incremental, start, datetime.now(timezone.utc), status, error,
        nb, pages, ",".join(f for f, _ in fields) if fields else None, path, wm_before, wm_after,
    ))

write_log(log_rows)
display(spark.read.format("delta").load(CFG_RUN_LOG).where(F.col("run_id") == RUN_ID))

erreurs = [r[1] for r in log_rows if r[5] == "ERREUR"]
if erreurs:
    raise RuntimeError(f"{len(erreurs)} méthode(s) en erreur : {erreurs}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
