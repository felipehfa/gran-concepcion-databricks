# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.stg_poblacion_referencia`
# MAGIC
# MAGIC **Creada por:**
# MAGIC `03_oro/00_carga_manual_poblacion_referencia_oro_python.py` (sección 2)
# MAGIC — referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Población de entrenamiento congelada del modelo vigente —
# MAGIC `06_features_oro_sql.py` puntúa cada aviso nuevo contra ESTA tabla (nunca
# MAGIC contra el resto del catálogo), para que reciba exactamente las mismas
# MAGIC features que en el proyecto original. Carga manual, una sola vez (o al
# MAGIC reentrenar con un dataset de referencia distinto) — se sobreescribe
# MAGIC completa cada vez.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_poblacion_referencia (
        id_aviso          STRING NOT NULL,
        comuna            STRING,
        latitud           DOUBLE,
        longitud          DOUBLE,
        antiguedad_anos   DOUBLE,
        piso_unidad       DOUBLE,
        precio_m2         DOUBLE,
        precio_m2_valido  BOOLEAN,
        rank_nac          DOUBLE,
        pob_rsh_uv        DOUBLE,
        p_urbano          DOUBLE,
        c_ig_com          DOUBLE,
        hog_uv            DOUBLE
    )
""")
