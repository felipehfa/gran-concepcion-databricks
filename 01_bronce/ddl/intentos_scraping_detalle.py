# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.01_bronce.intentos_scraping_detalle`
# MAGIC
# MAGIC **Creada por:** `01_bronce/02_scraper_manual_detalle_bronce_python.ipynb`
# MAGIC (sección 2) — referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Log append-only: una fila nueva por cada intento de scrapeo de detalle
# MAGIC (nuevo o reintento entre corridas), nunca se actualiza una fila
# MAGIC existente. Reemplaza los contadores mutables que en el proyecto original
# MAGIC vivían como columnas en `avisos` — la cola de "pendientes" (qué
# MAGIC reintentar, qué ya se sabe inalcanzable) se deriva con una query contra
# MAGIC este log, ver `.claude/rules/metadata-trazabilidad.md`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.intentos_scraping_detalle (
        id_aviso      STRING NOT NULL,
        resultado     STRING NOT NULL,
        motivo        STRING,
        fecha_intento TIMESTAMP NOT NULL
    )
""")
