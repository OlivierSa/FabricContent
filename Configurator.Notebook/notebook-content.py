# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, IntegerType
from pyspark.sql import functions as F
from delta.tables import DeltaTable

vl = notebookutils.variableLibrary.getLibrary("Variables")

lakehouse_id = vl.LAKEHOUSE_ID
try:
    workspace_id = vl.WORKSPACE_ID
except Exception:
    workspace_id = notebookutils.runtime.context["currentWorkspaceId"]

SCHEMA = "api_config"
base_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{SCHEMA}"

def table_path(nom_table: str) -> str:
    return f"{base_path}/{nom_table}"

print("Cible :", base_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schemas = {
    # 1) Table des méthodes API
    "cfg_api_method": StructType([
        StructField("method",              StringType(),    False),  # nom de la méthode / endpoint appelé
        StructField("incremental",         BooleanType(),   False),  # TRUE = incrémental, FALSE = full
        StructField("active",              BooleanType(),   True),   # désactiver une méthode sans la supprimer
        StructField("field_watermark",     StringType(),    True),   # champ de filtre incrémental (ex : updated_at)
        StructField("lastvalue_watermark", StringType(),    True),   # dernière valeur chargée
        StructField("date_maj",            TimestampType(), True),
    ]),
    # 2) Table des champs à appeler par méthode
    "cfg_api_fields": StructType([
        StructField("method", StringType(),  False),  # clé vers cfg_api_methode.method
        StructField("field",  StringType(),  False),  # nom du champ dans l'API
        StructField("format", StringType(),  True),   # string, int, decimal(18,2), date, timestamp...
        StructField("order",  IntegerType(), True),   # ordre des champs dans l'appel
    ]),
}

def create_table_if_not_exists(nom_table: str) -> bool:
    """Crée la table Delta vide si elle n'existe pas. Renvoie True si elle vient d'être créée."""
    path = table_path(nom_table)
    if DeltaTable.isDeltaTable(spark, path):
        print(f"{nom_table} : existe déjà")
        return False
    spark.createDataFrame([], schemas[nom_table]).write.format("delta").save(path)
    print(f"{nom_table} : créée")
    return True

creees = {t: create_table_if_not_exists(t) for t in schemas}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if creees["cfg_api_method"]:
    (spark.createDataFrame([
        ("users", False, True, None, None, None),
        ("space", False, True, None, None, None),
    ], schemas["cfg_api_method"])
     .withColumn("date_maj", F.current_timestamp())
     .write.format("delta").mode("append").save(table_path("cfg_api_method")))

if creees["cfg_api_fields"]:
    (spark.createDataFrame([
        ("users", "id",    "string",    1),
        ("users", "name",  "string",    2),
        ("users", "email", "timestamp", 3),
    ], schemas["cfg_api_fields"])
     .write.format("delta").mode("append").save(table_path("cfg_api_fields")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables_root = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables"

for schema in ["bronze", "silver", "gold"]:
    chemin = f"{tables_root}/{schema}"
    if notebookutils.fs.exists(chemin):
        print(f"{schema} : existe déjà")
    else:
        notebookutils.fs.mkdirs(chemin)
        print(f"{schema} : créé")

print("\nContenu de Tables :", [f.name for f in notebookutils.fs.ls(tables_root)])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
