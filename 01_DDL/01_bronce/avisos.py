# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.01_bronce.avisos`
# MAGIC
# MAGIC **Creada por:** `01_bronce/01_scraper_manual_grilla_bronce_python.ipynb`
# MAGIC (sección 3) — referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Append-only: nunca se actualiza una fila insertada. `estado_publicacion`
# MAGIC NO vive acá (vive en `avisos_detalle`, ver
# MAGIC `.claude/rules/metadata-trazabilidad.md`).

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.avisos (
        id_aviso        STRING NOT NULL,
        comuna          STRING,
        tipo_propiedad  STRING,
        operacion       STRING,
        titulo          STRING,
        precio          STRING,
        moneda          STRING,
        ubicacion       STRING,
        dormitorios     STRING,
        banos           STRING,
        superficie_m2   STRING,
        url             STRING,
        first_seen      STRING,
        _sistema_origen STRING,
        _id_corrida     STRING
    )
    PARTITIONED BY (first_seen)
""")
