# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.stg_predicciones`
# MAGIC
# MAGIC **Creada por:** `03_oro/10_prediccion_oro_python.py` (sección 2) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Una fila por (`id_aviso`, `version_modelo`) — histórico completo, nunca
# MAGIC se pisa entre reentrenamientos. Fuente de `dim_prediccion_scd2` y de las
# MAGIC medidas reales/predichas de `fact_aviso`
# MAGIC (`11_modelo_dimensional_oro_sql.py`). Particionada por
# MAGIC `version_modelo`: la dimensión que más se filtra en las queries de este
# MAGIC notebook y del visualizador.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_predicciones (
        id_aviso              STRING NOT NULL,
        version_modelo        STRING NOT NULL,
        fecha_prediccion      TIMESTAMP NOT NULL,
        costo_total_predicho  DOUBLE NOT NULL,
        z_robusto             DOUBLE,
        decil_precio          INT,
        etiqueta              STRING,
        nivel_confianza       STRING,
        cv_ensamble           DOUBLE
    )
    PARTITIONED BY (version_modelo)
""")
