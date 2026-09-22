# Databricks notebook source
# MAGIC %md
# MAGIC # DDL, `gran_concepcion.02_plata.modelos` (Volume)
# MAGIC
# MAGIC Landing zone de carga manual: modelos de imputación de superficie
# MAGIC (Random Forest, `.pkl`) entrenados en el "proyecto original" y subidos
# MAGIC a mano desde `subir_a_volumes/02_plata_modelos/`. Lo lee
# MAGIC `02_plata/05_imputacion_superficie_plata_python.py` (`joblib.load`),
# MAGIC este notebook no entrena, solo carga y predice.
# MAGIC Referencia de solo lectura, el Volume real se crea con
# MAGIC `CREATE VOLUME IF NOT EXISTS`, correr esta celda es idempotente.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.02_plata")
spark.sql("CREATE VOLUME IF NOT EXISTS gran_concepcion.02_plata.modelos")
