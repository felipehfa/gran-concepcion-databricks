# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_ubicacion`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Dimensión satélite 1:1 con el aviso — comuna, coordenadas, distancias y
# MAGIC vulnerabilidad socioterritorial. `barrio_id` es FK a `dim_barrio`
# MAGIC (Snowflake deliberado: barrio se comparte entre muchos avisos). Se
# MAGIC refresca (`MERGE`, SCD1) desde `stg_avisos_features` en cada corrida —
# MAGIC `barrio_id` puede quedar NULL si el barrio del aviso no está en
# MAGIC `dim_barrio`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_ubicacion (
        ubicacion_id                   BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso                       STRING NOT NULL,
        barrio_id                      BIGINT,
        comuna                         STRING,
        latitud                        DOUBLE,
        longitud                       DOUBLE,
        distancia_centro_comuna_m      DOUBLE,
        distancia_centro_concepcion_m  DOUBLE,
        rank_nac                       DOUBLE,
        pob_rsh_uv                     DOUBLE,
        p_urbano                       DOUBLE,
        c_ig_com                       DOUBLE,
        hog_uv                         DOUBLE,
        uv_rsh                         STRING
    )
""")
