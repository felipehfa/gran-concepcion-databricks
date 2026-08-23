# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.fact_aviso`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 1) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Hecho snapshot — grano 1 fila por aviso, se sobreescribe con el estado
# MAGIC vigente (no acumula una fila por versión de modelo, a diferencia de
# MAGIC `stg_predicciones`/`dim_prediccion_scd2`). Concentra las 6 FK directo,
# MAGIC sin hub intermedio: `dim_propiedad` (solo llaves, sin atributos propios)
# MAGIC se descartó del diseño por no cumplir función en Kimball.
# MAGIC
# MAGIC Solo incluye avisos que ya tienen al menos una predicción vigente en
# MAGIC `dim_prediccion_scd2` (`JOIN`, no `LEFT JOIN`, en el `MERGE` que puebla
# MAGIC esta tabla) — sin eso no hay nada que comparar.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.fact_aviso (
        id_aviso                     STRING NOT NULL,
        descripcion_id               BIGINT,
        amenidades_id                BIGINT,
        ubicacion_id                 BIGINT,
        fecha_id                     BIGINT,
        estado_aviso_scd_id          BIGINT,
        prediccion_scd_id            BIGINT,
        url                          STRING,
        fecha_publicacion_precision  STRING,
        precio_clp_real              DOUBLE,
        gastos_comunes_real          DOUBLE,
        costo_total_real             DOUBLE
    )
""")
