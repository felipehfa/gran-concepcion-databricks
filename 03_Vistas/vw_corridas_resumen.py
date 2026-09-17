# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista -- gran_concepcion.04_capa_semantica.vw_corridas_resumen
# MAGIC
# MAGIC **Creada por:** 03_oro/12_snapshot_historial_tablas_oro_python.py
# MAGIC (CREATE OR REPLACE VIEW inline). Referencia de solo lectura, idempotente,
# MAGIC sin datos propios.
# MAGIC
# MAGIC Fila unica de KPIs para los contadores de la pagina "Corridas": ultimo
# MAGIC estado, hace cuanto (min), corridas ultimas 24 h, fallidas ultimos 7 d,
# MAGIC avisos nuevos y cambios de estado de hoy, duracion media (min).

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.04_capa_semantica.vw_corridas_resumen AS
    SELECT
        MAX_BY(estado, inicio) AS ultimo_estado,
        MAX(inicio)            AS ultima_corrida,
        timestampdiff(MINUTE, MAX(inicio), current_timestamp()) AS hace_minutos,
        COUNT_IF(inicio >= current_timestamp() - INTERVAL 24 HOURS) AS corridas_24h,
        COUNT_IF(NOT exito AND inicio >= current_timestamp() - INTERVAL 7 DAYS) AS fallidas_7d,
        SUM(IF(CAST(inicio AS DATE) = current_date(), avisos_nuevos, 0))  AS avisos_nuevos_hoy,
        SUM(IF(CAST(inicio AS DATE) = current_date(), cambios_estado, 0)) AS cambios_estado_hoy,
        ROUND(AVG(duracion_min), 1) AS duracion_media_min
    FROM gran_concepcion.04_capa_semantica.vw_corridas
""")
