# Databricks notebook source
# MAGIC %md
# MAGIC # DDL, `gran_concepcion.01_bronce.vulnerabilidad` (Volume)
# MAGIC
# MAGIC Landing zone de carga manual: shapefile IGVUST (`.shp .shx .dbf .prj`)
# MAGIC recortado a las 10 comunas del Gran Concepción, subido a mano una sola
# MAGIC vez (o al actualizar el shapefile) desde `subir_a_volumes/01_bronce_vulnerabilidad/`.
# MAGIC Lo lee `01_bronce/00_carga_manual_poligonos_vulnerabilidad_bronce_python.py`
# MAGIC para poblar `gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv`.
# MAGIC Referencia de solo lectura, el Volume real se crea con
# MAGIC `CREATE VOLUME IF NOT EXISTS`, correr esta celda es idempotente.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.01_bronce")
spark.sql("CREATE VOLUME IF NOT EXISTS gran_concepcion.01_bronce.vulnerabilidad")
