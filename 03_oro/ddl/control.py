# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.control`
# MAGIC
# MAGIC **Creada por:** `03_oro/09_actualizacion_estado_avisos_oro_python.py`
# MAGIC (sección 3) — referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Clave/valor genérica para estado interno de Oro (cooldown tras CAPTCHA o
# MAGIC muro de verificación durante el re-chequeo de estado). Propia de Oro — no
# MAGIC se lee ni se escribe la tabla `control` de Bronce, cada capa mantiene la
# MAGIC suya. Fuera del esquema dimensional (`stg_`/`dim_`/`fact_`): es metadata
# MAGIC operativa del pipeline, no un dato de negocio.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.control (
        clave STRING NOT NULL,
        valor STRING
    )
""")
