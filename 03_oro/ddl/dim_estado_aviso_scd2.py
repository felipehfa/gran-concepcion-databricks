# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_estado_aviso_scd2`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC SCD2: histórico de cómo cambió `estado_publicacion` de un aviso en el
# MAGIC tiempo (activo → pausado → finalizado/no_disponible). El notebook fuente
# MAGIC cierra (`valid_to`, `is_current = false`) la fila vigente ANTES de
# MAGIC insertar la nueva — nunca al revés. `fact_aviso.estado_aviso_scd_id`
# MAGIC apunta siempre a la fila `is_current = true`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_estado_aviso_scd2 (
        estado_aviso_scd_id BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso            STRING NOT NULL,
        estado_publicacion  STRING NOT NULL,
        valid_from          TIMESTAMP NOT NULL,
        valid_to            TIMESTAMP,
        is_current          BOOLEAN NOT NULL
    )
""")
