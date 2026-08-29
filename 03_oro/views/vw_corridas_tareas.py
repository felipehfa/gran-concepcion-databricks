# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista -- gran_concepcion.03_oro.vw_corridas_tareas
# MAGIC
# MAGIC **Creada por:** 03_oro/12_snapshot_historial_tablas_oro_python.py
# MAGIC (CREATE OR REPLACE VIEW inline). Referencia de solo lectura, idempotente,
# MAGIC sin datos propios.
# MAGIC
# MAGIC Una fila por (corrida, tarea): orden en el pipeline, capa, inicio/fin,
# MAGIC duracion y estado de cada notebook del Job gran_concepcion_pipeline
# MAGIC (id 1060179417531534), desde system.lakeflow.job_task_run_timeline.

# COMMAND ----------

JOB_ID = "1060179417531534"

spark.sql(f"""
    CREATE OR REPLACE VIEW gran_concepcion.03_oro.vw_corridas_tareas AS
    SELECT
        t.job_run_id AS run_id,
        t.task_key,
        CASE t.task_key
            WHEN 'scraper_grilla' THEN 1 WHEN 'scraper_detalle' THEN 2
            WHEN 'tasas_historicas' THEN 3 WHEN 'limpieza_plata' THEN 4
            WHEN 'imputacion_superficie' THEN 5 WHEN 'features_oro' THEN 6
            WHEN 'vulnerabilidad_oro' THEN 7 WHEN 'actualizacion_estado' THEN 8
            WHEN 'prediccion_oro' THEN 9 WHEN 'modelo_dimensional' THEN 10
            WHEN 'snapshot_historial' THEN 11 ELSE 99
        END AS orden,
        CASE
            WHEN t.task_key LIKE 'scraper%' THEN 'bronce'
            WHEN t.task_key IN ('tasas_historicas','limpieza_plata','imputacion_superficie') THEN 'plata'
            ELSE 'oro'
        END AS capa,
        MIN(t.period_start_time) AS inicio,
        MAX(t.period_end_time)   AS fin,
        MAX(t.execution_duration_seconds) AS duracion_seg,
        MAX_BY(t.result_state, t.period_end_time) AS estado
    FROM system.lakeflow.job_task_run_timeline t
    WHERE t.job_id = '{JOB_ID}'
    GROUP BY t.job_run_id, t.task_key
""")
