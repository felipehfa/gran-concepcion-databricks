# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `01_bronce/00_carga_manual_poligonos_vulnerabilidad_bronce_python.py`
# MAGIC (sección 1) — referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Referencia estática (shapefile IGVUST recortado a las 10 comunas del Gran
# MAGIC Concepción, reproyectado a WGS84 y guardado como WKT). Carga manual, una
# MAGIC sola vez o al actualizar el shapefile — no particionada, no incremental.
# MAGIC Única tabla de Bronce que NO es append-only por fila individual: el
# MAGIC `MERGE` del notebook fuente actualiza una Unidad Vecinal si ya existía.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv (
        uv_rsh          STRING NOT NULL,
        comuna          STRING NOT NULL,
        rank_nac        DOUBLE,
        pob_rsh_uv      DOUBLE,
        p_urbano        DOUBLE,
        c_ig_com        DOUBLE,
        hog_uv          DOUBLE,
        geometria_wkt   STRING NOT NULL,
        _sistema_origen STRING,
        _id_corrida     STRING
    )
""")
