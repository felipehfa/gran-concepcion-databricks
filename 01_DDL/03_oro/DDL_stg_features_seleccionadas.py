# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.stg_features_seleccionadas`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` (sección 2)
# MAGIC — referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Lista de las features que espera el modelo vigente, en el orden exacto
# MAGIC que usa `10_prediccion_oro_python.py` al armar la matriz de entrada.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_features_seleccionadas (
        feature STRING NOT NULL
    )
""")
