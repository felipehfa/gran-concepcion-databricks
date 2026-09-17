# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista — `gran_concepcion.04_capa_semantica.vw_buscador_valor_m2_diario`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 10) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE OR REPLACE VIEW`. Idempotente, sin datos propios.
# MAGIC
# MAGIC **Serie diaria del valor por m² útil (CLP)** de los avisos que estaban
# MAGIC `activo` cada día, según `dim_estado_aviso_scd2`: media, mediana y
# MAGIC desviación estándar de `costo_total_real / superficie_util_m2`. Alimenta
# MAGIC el gráfico "Valor por m² útil" de la página "📊 Estadísticas diarias"
# MAGIC (dataset `ds_valor_m2`).
# MAGIC
# MAGIC Nota: el valor/m² usa el `costo_total_real` **actual** de cada aviso
# MAGIC (`fact_aviso` es snapshot), no el precio histórico de cada día — la serie
# MAGIC refleja qué avisos estaban activos, no cómo cambió el precio de un aviso
# MAGIC dado en el tiempo.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.04_capa_semantica.vw_buscador_valor_m2_diario AS
    WITH fechas AS (
      SELECT explode(sequence(
        (SELECT DATE(MIN(valid_from)) FROM gran_concepcion.03_oro.dim_estado_aviso_scd2),
        CURRENT_DATE(),
        INTERVAL 1 DAY
      )) AS fecha
    ),
    activos AS (
      SELECT fc.fecha, e.id_aviso FROM fechas fc
      JOIN gran_concepcion.03_oro.dim_estado_aviso_scd2 e
        ON e.estado_publicacion = 'activo'
       AND fc.fecha >= DATE(e.valid_from)
       AND fc.fecha < COALESCE(DATE(e.valid_to), DATE_ADD(CURRENT_DATE(), 1))
    ),
    valor_actual AS (
      SELECT f.id_aviso, f.costo_total_real / d.superficie_util_m2 AS valor_m2
      FROM gran_concepcion.03_oro.fact_aviso f
      JOIN gran_concepcion.03_oro.dim_descripcion_propiedad d ON f.descripcion_id = d.descripcion_id
      WHERE d.superficie_util_m2 > 0
    )
    SELECT a.fecha,
      AVG(v.valor_m2) AS media,
      PERCENTILE(v.valor_m2, 0.5) AS mediana,
      STDDEV(v.valor_m2) AS desviacion_estandar
    FROM activos a
    JOIN valor_actual v ON v.id_aviso = a.id_aviso
    GROUP BY a.fecha
    ORDER BY a.fecha
""")
