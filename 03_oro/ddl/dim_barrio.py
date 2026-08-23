# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_barrio`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` (sección 2)
# MAGIC — referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Única dimensión que no lleva prefijo `stg_`: se consume directo desde
# MAGIC Power BI. Mapa barrio → nivel de precio (1-5), congelado al entrenar el
# MAGIC modelo. `barrio_id` es surrogate key para el modelo dimensional — no
# MAGIC admite `CREATE TABLE ... AS SELECT` con IDENTITY, así que el notebook
# MAGIC fuente la crea vacía y la puebla con `INSERT INTO (barrio, nivel_barrio)`
# MAGIC en su sección 7 (nunca con overwrite de DataFrame). Carga manual, se
# MAGIC sobreescribe completa (`DELETE` + `INSERT`) cada vez que corre el
# MAGIC notebook fuente.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_barrio (
        barrio_id    BIGINT GENERATED ALWAYS AS IDENTITY,
        barrio       STRING NOT NULL,
        nivel_barrio INT NOT NULL
    )
""")
