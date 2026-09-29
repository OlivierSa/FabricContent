# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import time
import requests
from datetime import datetime, timezone
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

ws_id = notebookutils.runtime.context.get("currentWorkspaceId")

#Get Variables from the Library

try:
    cfg = notebookutils.variableLibrary.getLibrary("Variables")
    QLIK_TENANT_URL = cfg.QLIK_TENANT_URL       
    LAKEHOUSE = notebookutils.lakehouse.get(cfg.LAKEHOUSE)   
    LH_ROOT = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE.id}"    

except Exception as e:
    print(f"Variable Library non lue : {e}")
    QLIK_TENANT_URL = "https://<tenant>.<region>.qlikcloud.com"

# print(f"QLIK_TENANT_URL = {QLIK_TENANT_URL}")  
# print(f"Lakehouse utilisé = {cfg.LAKEHOUSE} ({LH_ROOT})")

#Variables for QLik API

TARGET_TABLE = "qlik_users"
PAGE_SIZE = 100    


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get token from lakehouse config file

secrets = json.loads(notebookutils.fs.head(f"{LH_ROOT}/Files/config/secrets.json"))
QLIK_API_KEY = secrets["QLIK_API_TOKEN"]

print(QLIK_API_KEY)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# CELL 2 - Appel API avec pagination ------------------------------------------
def get_all_users(base_url: str, api_key: str) -> list:
    """Récupère tous les utilisateurs en suivant links.next.href."""
    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    })
 
    url = f"{base_url.rstrip('/')}/api/v1/users"
    params = {"limit": PAGE_SIZE}
    users = []
 
    while url:
        for attempt in range(5):
            resp = session.get(url, params=params, timeout=60)
            if resp.status_code == 429:                       # limite de débit
                wait = int(resp.headers.get("Retry-After", 2 ** attempt))
                time.sleep(wait)
                continue
            resp.raise_for_status()
            break
        else:
            raise RuntimeError(f"Trop de tentatives sur {url}")
 
        payload = resp.json()
        users.extend(payload.get("data", []))
 
        # L'URL "next" contient déjà les paramètres de pagination
        url = (payload.get("links") or {}).get("next", {}).get("href")
        params = None
 
    return users
 
 
users = get_all_users(QLIK_TENANT_URL, QLIK_API_KEY)
# print(f"{users}")
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write result into the lakehouse

schema = StructType([
    StructField("user_id",          StringType()),
    StructField("tenant_id",        StringType()),
    StructField("name",             StringType()),
    StructField("email",            StringType()),
    StructField("subject",          StringType()),
    StructField("status",           StringType()),
    StructField("created_at",       StringType()),
    StructField("last_updated_at",  StringType()),
    StructField("assigned_roles",   StringType()),   # JSON (liste de rôles)
    StructField("assigned_groups",  StringType()),   # JSON (liste de groupes)
    StructField("raw_json",         StringType()),   # réponse complète, pour ne rien perdre
    StructField("load_timestamp",   TimestampType()),
])
 
load_ts = datetime.now(timezone.utc)
 
rows = [
    Row(
        user_id=u.get("id"),
        tenant_id=u.get("tenantId"),
        name=u.get("name"),
        email=u.get("email"),
        subject=u.get("subject"),
        status=u.get("status"),
        created_at=u.get("createdAt"),
        last_updated_at=u.get("lastUpdatedAt"),
        assigned_roles=json.dumps([r.get("name") for r in u.get("assignedRoles", [])]),
        assigned_groups=json.dumps([g.get("name") for g in u.get("assignedGroups", [])]),
        raw_json=json.dumps(u),
        load_timestamp=load_ts,
    )
    for u in users
]
 
df = spark.createDataFrame(rows, schema=schema)
 
(df.write
   .format("delta")
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .save(f"{LH_ROOT}/Tables/{TARGET_TABLE}"))
 
display(df.drop("raw_json"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
