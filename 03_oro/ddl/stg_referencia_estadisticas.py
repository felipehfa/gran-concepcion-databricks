# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.stg_referencia_estadisticas`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` (sección 2)
# MAGIC — referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Respaldos globales (clave/valor: medianas, medias, límites IQR, nivel de
# MAGIC barrio por defecto) usados por `06_features_oro_sql.py` cuando un aviso
# MAGIC nuevo no tiene vecinos válidos en `stg_poblacion_referencia`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_referencia_estadisticas (
        clave STRING NOT NULL,
        valor DOUBLE
    )
""")
