# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_prediccion_scd2`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC SCD2: histórico de cómo cambió la evaluación del modelo para un aviso
# MAGIC (típicamente entre reentrenamientos) — costo predicho, z-score, etiqueta,
# MAGIC confianza. Guarda measures (no solo atributos categóricos) a propósito:
# MAGIC son necesarias para reconstruir "qué decía el modelo en el momento X",
# MAGIC algo que `stg_predicciones` no responde directo sin filtrar por versión.
# MAGIC Mismo patrón cerrar-antes-de-insertar que `dim_estado_aviso_scd2`.
# MAGIC `fact_aviso.prediccion_scd_id` apunta siempre a la fila
# MAGIC `is_current = true`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_prediccion_scd2 (
        prediccion_scd_id     BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso              STRING NOT NULL,
        version_modelo        STRING NOT NULL,
        costo_total_predicho  DOUBLE NOT NULL,
        z_robusto             DOUBLE,
        cv_ensamble           DOUBLE,
        decil_precio          INT,
        etiqueta              STRING,
        nivel_confianza       STRING,
        fecha_prediccion      TIMESTAMP,
        valid_from            TIMESTAMP NOT NULL,
        valid_to              TIMESTAMP,
        is_current            BOOLEAN NOT NULL
    )
""")
