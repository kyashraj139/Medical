# Databricks notebook source
dbutils.widgets.text("catalog", "")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
import re
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Use a simple catalog name or adapt identifier quoting.")
ns = f"{catalog}.lab_medical"
landing = f"/Volumes/{catalog}/lab_medical/landing"

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE TABLE {ns}.gold USING DELTA AS
SELECT event_date, clinic, COUNT(*) AS records,
SUM(review_flag) AS review_count, AVG(wait_minutes) AS avg_wait_minutes
FROM {ns}.silver GROUP BY event_date, clinic""")
# COMMAND ----------

expected = {'bronze': 248, 'classified': 240, 'quarantine': 6, 'silver': 234, 'gold': 90}
for table, target in expected.items():
    actual = spark.table(f"{ns}.{table}").count()
    assert actual == target, (table, actual, target)
assert spark.sql(f"SELECT COUNT(DISTINCT event_id) FROM {ns}.silver").first()[0] == 234
totals = spark.sql(f"SELECT SUM(records), SUM(review_count) FROM {ns}.gold").first()
assert tuple(totals) == (234, 85), totals
display(spark.sql(f"SELECT * FROM {ns}.gold ORDER BY event_date, clinic"))
