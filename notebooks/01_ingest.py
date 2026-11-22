# Databricks notebook source
dbutils.widgets.text("catalog", "")
catalog = dbutils.widgets.get("catalog").strip() or spark.sql("SELECT current_catalog()").first()[0]
import re
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog):
    raise ValueError("Use a simple catalog name or adapt identifier quoting.")
ns = f"{catalog}.lab_medical"
landing = f"/Volumes/{catalog}/lab_medical/landing"

# COMMAND ----------

import csv
import io
from datetime import date, timedelta

def source_csv():
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["event_id", "event_date", "clinic", "wait_minutes", "visit_minutes"])
    for i in range(1, 241):
        day = date(2026, 1, 1) + timedelta(days=(i-1)//8)
        # Every fortieth event carries an invalid measure.
        row = [f"MED-{i:04d}", day.isoformat(), ['north', 'central', 'east'][(i-1)%3],
               -1 if i%40 == 0 else (i*17)%95, 10+(i*11)%60]
        writer.writerow(row)
        if i%30 == 0:  # Simulate an exact replay.
            writer.writerow(row)
    return buf.getvalue()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ns}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {ns}.landing")
dbutils.fs.put(f"{landing}/snapshot.csv", source_csv(), overwrite=True)
schema = "event_id STRING, event_date DATE, clinic STRING, wait_minutes INT, visit_minutes INT"
raw = (spark.read.option("header", True).option("mode", "FAILFAST")
       .schema(schema).csv(f"{landing}/snapshot.csv"))
raw.write.format("delta").mode("overwrite").saveAsTable(f"{ns}.bronze")
