# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.01_bronce.control`
# MAGIC
# MAGIC **Creada por:** `01_bronce/01_scraper_manual_grilla_bronce_python.ipynb`
# MAGIC (sección 3) — referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Clave/valor genérica para estado interno de los scrapers de Bronce (ej.
# MAGIC cooldown tras CAPTCHA en `02_scraper_manual_detalle_bronce`).
# MAGIC Infraestructura operativa del pipeline, no una entidad de negocio — queda
# MAGIC fuera de la convención de inmutabilidad append-only del resto de Bronce.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.control (
        clave STRING NOT NULL,
        valor STRING
    )
""")
