# Databricks notebook source
dbutils.widgets.text("catalog", "")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
import re
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Use a simple catalog name or adapt identifier quoting.")
ns = f"{catalog}.lab_medical"
landing = f"/Volumes/{catalog}/lab_medical/landing"

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {ns}.classified USING DELTA AS
SELECT DISTINCT *, CASE WHEN wait_minutes >= 0 AND visit_minutes > 0 THEN 'accepted' ELSE 'invalid_measure' END AS quality
FROM {ns}.bronze""")
# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {ns}.quarantine USING DELTA AS
SELECT * FROM {ns}.classified WHERE quality <> 'accepted'""")
# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {ns}.silver USING DELTA AS
SELECT event_id, event_date, clinic, wait_minutes, visit_minutes,
CASE WHEN wait_minutes >= 60 THEN 1 ELSE 0 END AS review_flag
FROM {ns}.classified WHERE quality = 'accepted'""")