# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.02_plata.valores_pesos`
# MAGIC
# MAGIC **Creada por:** `02_plata/03_tasas_historicas_plata_python.py` (sección
# MAGIC 1) — referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente; no
# MAGIC reemplaza esa lógica, sirve para recrear el esquema en un workspace nuevo
# MAGIC o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC Caché de valores de UF/USD por fecha (mindicador.cl), para convertir
# MAGIC precios a CLP con la tasa vigente el día exacto de publicación del
# MAGIC aviso, no la más reciente disponible. Se cachea solo lo que hace falta,
# MAGIC nunca se vuelve a consultar una fecha ya guardada.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.02_plata.valores_pesos (
        fecha_valor     DATE,
        valor_uf_clp    DOUBLE,
        valor_dolar_clp DOUBLE,
        fecha_consulta  DATE
    )
""")
