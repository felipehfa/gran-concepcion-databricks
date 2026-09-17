# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista -- gran_concepcion.04_capa_semantica.vw_corridas
# MAGIC
# MAGIC **Creada por:** 03_oro/12_snapshot_historial_tablas_oro_python.py
# MAGIC (CREATE OR REPLACE VIEW inline). Referencia de solo lectura, idempotente,
# MAGIC sin datos propios.
# MAGIC
# MAGIC Una fila por corrida del Job `gran_concepcion_pipeline` (id
# MAGIC 1060179417531534): inicio, fin, duracion, estado (`SUCCEEDED` / `FAILED` /
# MAGIC `EN_CURSO` ...), `exito`, `termination_code`, `trigger_type`, tareas no OK,
# MAGIC y las metricas de negocio **por corrida** — avisos nuevos, rechequeos de
# MAGIC estado, cambios de estado, predicciones nuevas.
# MAGIC
# MAGIC Las metricas de negocio NO estan instrumentadas en los notebooks: se
# MAGIC reconstruyen cruzando los timestamps por fila de Oro
# MAGIC (`fecha_creacion_oro`, `fecha_chequeo_estado_oro`,
# MAGIC `dim_estado_aviso_scd2.valid_from`, `stg_predicciones.fecha_prediccion`)
# MAGIC contra la ventana `[inicio_tarea, inicio_misma_tarea_corrida_siguiente)`
# MAGIC de `system.lakeflow.job_task_run_timeline`.

# COMMAND ----------

JOB_ID = "1060179417531534"

spark.sql(f"""
    CREATE OR REPLACE VIEW gran_concepcion.04_capa_semantica.vw_corridas AS
    WITH runs AS (
        SELECT r.run_id,
            MIN(r.period_start_time) AS inicio,
            MAX(r.period_end_time)   AS fin,
            timestampdiff(SECOND, MIN(r.period_start_time), MAX(r.period_end_time)) AS duracion_seg,
            MAX_BY(r.result_state, r.period_end_time)     AS estado,
            MAX_BY(r.termination_code, r.period_end_time) AS termination_code,
            MAX_BY(r.trigger_type, r.period_end_time)     AS trigger_type
        FROM system.lakeflow.job_run_timeline r
        WHERE r.job_id = '{JOB_ID}'
        GROUP BY r.run_id
    ),
    tareas AS (
        SELECT job_run_id AS run_id, task_key,
            MIN(period_start_time) AS inicio,
            MAX_BY(result_state, period_end_time) AS estado
        FROM system.lakeflow.job_task_run_timeline
        WHERE job_id = '{JOB_ID}'
        GROUP BY job_run_id, task_key
    ),
    tareas_win AS (
        SELECT run_id, task_key, estado, inicio,
            LEAD(inicio) OVER (PARTITION BY task_key ORDER BY inicio) AS inicio_siguiente
        FROM tareas
    ),
    avisos_nuevos AS (
        SELECT w.run_id, COUNT(*) AS n FROM tareas_win w
        JOIN gran_concepcion.03_oro.stg_avisos_features s
          ON w.task_key = 'features_oro'
         AND s.fecha_creacion_oro >= w.inicio
         AND s.fecha_creacion_oro <  COALESCE(w.inicio_siguiente, current_timestamp())
        GROUP BY w.run_id
    ),
    rechequeos AS (
        SELECT w.run_id, COUNT(*) AS n FROM tareas_win w
        JOIN gran_concepcion.03_oro.stg_avisos_features s
          ON w.task_key = 'actualizacion_estado'
         AND s.fecha_chequeo_estado_oro >= w.inicio
         AND s.fecha_chequeo_estado_oro <  COALESCE(w.inicio_siguiente, current_timestamp())
        GROUP BY w.run_id
    ),
    predicciones AS (
        SELECT w.run_id, COUNT(*) AS n FROM tareas_win w
        JOIN gran_concepcion.03_oro.stg_predicciones p
          ON w.task_key = 'prediccion_oro'
         AND p.fecha_prediccion >= w.inicio
         AND p.fecha_prediccion <  COALESCE(w.inicio_siguiente, current_timestamp())
        GROUP BY w.run_id
    ),
    estado_ranked AS (
        SELECT id_aviso, valid_from,
               ROW_NUMBER() OVER (PARTITION BY id_aviso ORDER BY valid_from) AS rn
        FROM gran_concepcion.03_oro.dim_estado_aviso_scd2
    ),
    cambios_estado AS (
        SELECT w.run_id, COUNT(*) AS n FROM tareas_win w
        JOIN estado_ranked e
          ON w.task_key = 'modelo_dimensional'
         AND e.rn > 1
         AND e.valid_from >= w.inicio
         AND e.valid_from <  COALESCE(w.inicio_siguiente, current_timestamp())
        GROUP BY w.run_id
    ),
    tarea_fallida AS (
        SELECT run_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_LIST(task_key))) AS tareas_no_ok
        FROM tareas WHERE estado IS NOT NULL AND estado <> 'SUCCEEDED'
        GROUP BY run_id
    )
    SELECT
        r.run_id, r.inicio, r.fin, r.duracion_seg,
        ROUND(r.duracion_seg / 60.0, 1) AS duracion_min,
        COALESCE(r.estado, 'EN_CURSO') AS estado,
        (r.estado = 'SUCCEEDED') AS exito,
        r.termination_code, r.trigger_type,
        tf.tareas_no_ok,
        COALESCE(an.n, 0) AS avisos_nuevos,
        COALESCE(rc.n, 0) AS avisos_rechequeados,
        COALESCE(ce.n, 0) AS cambios_estado,
        COALESCE(pr.n, 0) AS predicciones_nuevas
    FROM runs r
    LEFT JOIN avisos_nuevos  an ON an.run_id = r.run_id
    LEFT JOIN rechequeos     rc ON rc.run_id = r.run_id
    LEFT JOIN predicciones   pr ON pr.run_id = r.run_id
    LEFT JOIN cambios_estado ce ON ce.run_id = r.run_id
    LEFT JOIN tarea_fallida  tf ON tf.run_id = r.run_id
""")
