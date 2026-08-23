# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.stg_referencia_estadisticas_por_comuna`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` (sección 2)
# MAGIC — referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Mismos respaldos que `stg_referencia_estadisticas`, agrupados por
# MAGIC comuna — segundo nivel de la cascada de fallback antes de caer a la
# MAGIC media global.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_referencia_estadisticas_por_comuna (
        comuna              STRING NOT NULL,
        mediana_antiguedad  DOUBLE,
        media_rank_nac      DOUBLE,
        media_pob_rsh_uv    DOUBLE,
        media_p_urbano      DOUBLE,
        media_c_ig_com      DOUBLE,
        media_hog_uv        DOUBLE
    )
""")
