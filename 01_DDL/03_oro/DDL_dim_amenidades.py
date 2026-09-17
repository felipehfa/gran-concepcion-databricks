# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_amenidades`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Dimensión satélite 1:1 con el aviso — puntos de interés cercanos (radio
# MAGIC 500m), 11 categorías, cada una con cantidad + distancia al más cercano.
# MAGIC Se refresca (`MERGE`, SCD1) desde `stg_avisos_features` en cada corrida.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_amenidades (
        amenidades_id                        BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso                             STRING NOT NULL,
        cantidad_paraderos                   INT,
        distancia_min_m_paraderos            DOUBLE,
        cantidad_estaciones_metro            INT,
        distancia_min_m_estaciones_metro     DOUBLE,
        cantidad_jardines_infantiles         INT,
        distancia_min_m_jardines_infantiles  DOUBLE,
        cantidad_colegios                    INT,
        distancia_min_m_colegios             DOUBLE,
        cantidad_universidades               INT,
        distancia_min_m_universidades        DOUBLE,
        cantidad_plazas                      INT,
        distancia_min_m_plazas               DOUBLE,
        cantidad_supermercados               INT,
        distancia_min_m_supermercados        DOUBLE,
        cantidad_farmacias                   INT,
        distancia_min_m_farmacias            DOUBLE,
        cantidad_centros_comerciales         INT,
        distancia_min_m_centros_comerciales  DOUBLE,
        cantidad_hospitales                  INT,
        distancia_min_m_hospitales           DOUBLE,
        cantidad_clinicas                    INT,
        distancia_min_m_clinicas             DOUBLE
    )
""")
