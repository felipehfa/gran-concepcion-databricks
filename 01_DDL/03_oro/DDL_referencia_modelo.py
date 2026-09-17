# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.referencia_modelo` (Volume)
# MAGIC
# MAGIC Landing zone de carga manual: dataset histórico congelado, niveles de
# MAGIC barrio, features seleccionadas y BD original (`.csv .json .db`),
# MAGIC subidos a mano desde `subir_a_volumes/03_oro_referencia_modelo/`. Lo lee
# MAGIC `03_oro/00_carga_manual_poblacion_referencia_oro_python.py`, que puebla
# MAGIC `stg_poblacion_referencia` — la población de referencia congelada contra
# MAGIC la que `06_features_oro_sql` puntúa cada aviso nuevo.
# MAGIC Referencia de solo lectura — el Volume real se crea con
# MAGIC `CREATE VOLUME IF NOT EXISTS`, correr esta celda es idempotente.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.03_oro")
spark.sql("CREATE VOLUME IF NOT EXISTS gran_concepcion.03_oro.referencia_modelo")
