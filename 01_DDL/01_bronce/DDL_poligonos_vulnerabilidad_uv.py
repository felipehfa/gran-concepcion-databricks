# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL, `gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `02_ELT/01_bronce/ELT_00_carga_manual_poligonos_vulnerabilidad_bronce_python`
# MAGIC (sección 1), referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Referencia estática (shapefile IGVUST recortado a las 10 comunas del Gran
# MAGIC Concepción, reproyectado a WGS84 y guardado como WKT). Carga manual, se
# MAGIC actualiza aproximadamente una vez al año cuando sale un shapefile IGVUST
# MAGIC nuevo, no particionada.
# MAGIC
# MAGIC **SCD2 append-only** (2026-09-19): 100% append-only, igual que el resto
# MAGIC de Bronce — nunca se hace `UPDATE` sobre una fila ya insertada. Cada
# MAGIC actualización del shapefile agrega una fila nueva por `uv_rsh` (solo si
# MAGIC algún atributo cambió respecto a la versión vigente), marcada con
# MAGIC `fecha_carga`. La versión vigente de una UV es la de `fecha_carga` más
# MAGIC reciente — se deriva con `QUALIFY ROW_NUMBER() OVER (PARTITION BY uv_rsh
# MAGIC ORDER BY fecha_carga DESC) = 1` en el notebook que la consume
# MAGIC (`ELT_07_vulnerabilidad_oro_python`), nunca se filtra un flag guardado en
# MAGIC la tabla.

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
        fecha_carga     TIMESTAMP NOT NULL,
        _sistema_origen STRING,
        _id_corrida     STRING
    )
""")
