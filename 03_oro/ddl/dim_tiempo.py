# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_tiempo`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Dimensión de tiempo estándar. `fecha_id = yyyyMMdd` derivado de
# MAGIC `fecha_prediccion` (no de `fecha_publicacion`, que no tiene rol de tiempo
# MAGIC propio en este modelo — queda como atributo directo en `fact_aviso`). Se
# MAGIC puebla incrementalmente (`INSERT`, `NOT EXISTS`) con las fechas nuevas
# MAGIC que aparezcan en `stg_predicciones`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_tiempo (
        fecha_id     BIGINT NOT NULL,
        fecha        DATE NOT NULL,
        anio         INT,
        trimestre    INT,
        mes          INT,
        nombre_mes   STRING,
        dia          INT
    )
""")
