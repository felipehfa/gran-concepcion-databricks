# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista — `gran_concepcion.03_oro.vw_buscador_historial_diario`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 10) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE OR REPLACE VIEW`. Idempotente, sin datos propios.
# MAGIC
# MAGIC **Serie diaria de disponibilidad de avisos**, derivada de la SCD2
# MAGIC `dim_estado_aviso_scd2`: para cada día entre la primera fecha registrada y
# MAGIC hoy, cuántos avisos estaban `activo`, cuántos `activo` o `pausado`, y
# MAGIC cuántos entraron/salieron del estado `activo` respecto al día anterior.
# MAGIC Alimenta la página "📊 Estadísticas diarias" del dashboard (dataset
# MAGIC `ds_historial_diario`).
# MAGIC
# MAGIC Vista plana: usa `CURRENT_DATE()`, así el dashboard siempre llega hasta
# MAGIC hoy sin refresh. El cross join fecha × aviso es trivial al volumen actual
# MAGIC (miles de avisos × cientos de días); si el histórico crece a años,
# MAGIC evaluar materializarla como tabla incremental estilo `stg_`.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.03_oro.vw_buscador_historial_diario AS
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
    pausados AS (
      SELECT fc.fecha, e.id_aviso FROM fechas fc
      JOIN gran_concepcion.03_oro.dim_estado_aviso_scd2 e
        ON e.estado_publicacion = 'pausado'
       AND fc.fecha >= DATE(e.valid_from)
       AND fc.fecha < COALESCE(DATE(e.valid_to), DATE_ADD(CURRENT_DATE(), 1))
    ),
    hoy_ayer AS (
      SELECT h.fecha, h.id_aviso, (a.id_aviso IS NOT NULL) AS activo_hoy, (y.id_aviso IS NOT NULL) AS activo_ayer
      FROM (SELECT DISTINCT fecha, id_aviso FROM activos UNION SELECT DATE_ADD(fecha,1), id_aviso FROM activos) h
      LEFT JOIN activos a ON a.fecha = h.fecha AND a.id_aviso = h.id_aviso
      LEFT JOIN activos y ON y.fecha = DATE_SUB(h.fecha,1) AND y.id_aviso = h.id_aviso
    ),
    entran_salen AS (
      SELECT fecha,
        SUM(CASE WHEN activo_hoy AND NOT activo_ayer THEN 1 ELSE 0 END) AS entran,
        SUM(CASE WHEN activo_ayer AND NOT activo_hoy THEN 1 ELSE 0 END) AS salen
      FROM hoy_ayer GROUP BY fecha
    ),
    totales_activo AS (
      SELECT fecha, COUNT(DISTINCT id_aviso) AS total_activo FROM activos GROUP BY fecha
    ),
    totales_todos AS (
      SELECT fecha, COUNT(DISTINCT id_aviso) AS total_activo_o_pausado
      FROM (SELECT fecha, id_aviso FROM activos UNION SELECT fecha, id_aviso FROM pausados)
      GROUP BY fecha
    )
    SELECT fc.fecha,
      COALESCE(ta.total_activo,0) AS total_activo,
      COALESCE(tt.total_activo_o_pausado,0) AS total_activo_o_pausado,
      es.entran, es.salen
    FROM fechas fc
    LEFT JOIN totales_activo ta ON ta.fecha = fc.fecha
    LEFT JOIN totales_todos tt ON tt.fecha = fc.fecha
    LEFT JOIN entran_salen es ON es.fecha = fc.fecha
    ORDER BY fc.fecha
""")
