# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.modelo_prediccion` (Volume)
# MAGIC
# MAGIC Landing zone de carga manual: ensamble LightGBM vigente (`.pkl .json`)
# MAGIC entrenado en el "proyecto original", subido a mano desde
# MAGIC `subir_a_volumes/03_oro_modelo_prediccion/`. Lo lee
# MAGIC `03_oro/10_prediccion_oro_python.py` (`pickle.load`) — este notebook no
# MAGIC entrena, solo carga y predice.
# MAGIC
# MAGIC Nota: este Volume se renombró desde `gran_concepcion.04_prediccion.modelo`
# MAGIC (la predicción se plegó dentro de Oro, no es una 4ª capa aparte).
# MAGIC Referencia de solo lectura — el Volume real se crea con
# MAGIC `CREATE VOLUME IF NOT EXISTS`, correr esta celda es idempotente.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.03_oro")
spark.sql("CREATE VOLUME IF NOT EXISTS gran_concepcion.03_oro.modelo_prediccion")
