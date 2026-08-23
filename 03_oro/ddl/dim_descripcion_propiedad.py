# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.dim_descripcion_propiedad`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Dimensión satélite 1:1 con el aviso — descripción física de la
# MAGIC propiedad. Se refresca (`MERGE`, SCD1) desde `stg_avisos_features` en
# MAGIC cada corrida. `amoblado`/`ascensor`/etc. quedan `INT` (0/1), no
# MAGIC `BOOLEAN`: así vienen tipados desde `04_limpieza_plata_sql.py`, y castear
# MAGIC acá metería una conversión de tipo innecesaria al `MERGE`.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_descripcion_propiedad (
        descripcion_id       BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso             STRING NOT NULL,
        tipo_propiedad       STRING,
        dormitorios          INT,
        banos                INT,
        estacionamientos     INT,
        bodegas              INT,
        superficie_util_m2   DOUBLE,
        superficie_total_m2  DOUBLE,
        ratio_total_util     DOUBLE,
        antiguedad_anos      DOUBLE,
        piso_unidad          DOUBLE,
        amoblado             INT,
        ascensor             INT,
        piscina              INT,
        conserjeria          INT,
        condominio_cerrado   INT,
        admite_mascotas      INT
    )
""")
