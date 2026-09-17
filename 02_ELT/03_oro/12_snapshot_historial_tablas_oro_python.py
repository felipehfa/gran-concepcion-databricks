# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 12 — Snapshot del historial de tablas (Oro)
# MAGIC
# MAGIC Recorre `DESCRIBE HISTORY` de todas las tablas Delta de las 3 capas
# MAGIC (`01_bronce`, `02_plata`, `03_oro`) y persiste cada commit — con sus
# MAGIC métricas de filas (insertadas / actualizadas / borradas) — en
# MAGIC `gran_concepcion.03_oro.historial_tablas`.
# MAGIC
# MAGIC **Por qué existe:** el historial Delta se poda a ~30 días
# MAGIC (`delta.logRetentionDuration`) y `DESCRIBE HISTORY` no se puede usar
# MAGIC dentro de una vista. Esta tabla persistente es lo que consume la página
# MAGIC "⚙️ Corridas" del dashboard (vía `vw_corridas_cambios_tablas`) para
# MAGIC mostrar qué cambió en cada tabla por corrida.
# MAGIC
# MAGIC **Append-only** (mismo criterio que `01_bronce.intentos_scraping_detalle`):
# MAGIC cada corrida agrega solo las versiones de tabla que todavía no están
# MAGIC registradas (`LEFT ANTI JOIN` por `tabla + version`). Nunca actualiza una
# MAGIC fila existente.
# MAGIC
# MAGIC **Última tarea del Job `gran_concepcion_pipeline`** — corre después de
# MAGIC `modelo_dimensional`, así captura los commits de la corrida completa.

# COMMAND ----------

from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

SCHEMAS = {
    "01_bronce": "bronce",
    "02_plata": "plata",
    "03_oro": "oro",
}
CATALOGO = "gran_concepcion"

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.historial_tablas (
        capa               STRING,
        tabla              STRING NOT NULL,
        version            BIGINT NOT NULL,
        timestamp          TIMESTAMP,
        operation          STRING,
        num_output_rows    BIGINT,
        num_inserted_rows  BIGINT,
        num_updated_rows   BIGINT,
        num_deleted_rows   BIGINT,
        num_copied_rows    BIGINT,
        user_name          STRING,
        job_run_id         STRING,
        cluster_id         STRING,
        _snapshot_ts       TIMESTAMP
    )
""")

print("Tabla historial_tablas verificada/creada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vistas de observabilidad de corridas
# MAGIC `CREATE OR REPLACE VIEW` (idempotente) de las 4 vistas que consume la
# MAGIC página "⚙️ Corridas" del dashboard. Leen `system.lakeflow.job_run_timeline`
# MAGIC / `job_task_run_timeline` del Job `gran_concepcion_pipeline`
# MAGIC (id `1060179417531534`) y las cruzan por ventana de tiempo contra los
# MAGIC timestamps por fila de Oro para reconstruir avisos nuevos / rechequeos /
# MAGIC cambios de estado / predicciones **por corrida** (sin instrumentar los
# MAGIC notebooks). Viven en `gran_concepcion.04_capa_semantica` (mismo schema
# MAGIC que las vistas del buscador — ver nb 11 sección 10), aunque leen tablas
# MAGIC de `03_oro` como `historial_tablas` y `dim_estado_aviso_scd2`. Definición
# MAGIC standalone en `04_capa_semantica/views/vw_corridas*.py`.

# COMMAND ----------

JOB_ID = "1060179417531534"

spark.sql("CREATE SCHEMA IF NOT EXISTS gran_concepcion.04_capa_semantica")

spark.sql(f"""
    CREATE OR REPLACE VIEW gran_concepcion.04_capa_semantica.vw_corridas_tareas AS
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

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.04_capa_semantica.vw_corridas_cambios_tablas AS
    WITH run_win AS (
        SELECT run_id, inicio,
            LEAD(inicio) OVER (ORDER BY inicio) AS inicio_siguiente
        FROM (SELECT run_id, MIN(inicio) AS inicio FROM gran_concepcion.04_capa_semantica.vw_corridas_tareas GROUP BY run_id)
    )
    SELECT w.run_id, h.capa, h.tabla,
        element_at(split(h.tabla, '[.]'), -1) AS tabla_corta,
        h.operation,
        COUNT(*) AS commits,
        SUM(COALESCE(h.num_output_rows, 0))   AS filas_escritas,
        SUM(COALESCE(h.num_inserted_rows, 0)) AS filas_insertadas,
        SUM(COALESCE(h.num_updated_rows, 0))  AS filas_actualizadas,
        SUM(COALESCE(h.num_deleted_rows, 0))  AS filas_borradas,
        MIN(h.timestamp) AS primer_commit,
        MAX(h.timestamp) AS ultimo_commit
    FROM gran_concepcion.03_oro.historial_tablas h
    JOIN run_win w
      ON h.timestamp >= w.inicio
     AND h.timestamp <  COALESCE(w.inicio_siguiente, current_timestamp())
    WHERE h.operation NOT IN ('OPTIMIZE','SET TBLPROPERTIES','ADD COLUMNS','COMPUTE STATS','CREATE TABLE')
      AND h.tabla <> 'gran_concepcion.03_oro.historial_tablas'
    GROUP BY w.run_id, h.capa, h.tabla, h.operation
""")

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

print("Vistas vw_corridas* creadas/reemplazadas.")

# COMMAND ----------

ESQUEMA_FILA = StructType([
    StructField("capa", StringType()),
    StructField("tabla", StringType(), False),
    StructField("version", LongType(), False),
    StructField("timestamp", TimestampType()),
    StructField("operation", StringType()),
    StructField("num_output_rows", LongType()),
    StructField("num_inserted_rows", LongType()),
    StructField("num_updated_rows", LongType()),
    StructField("num_deleted_rows", LongType()),
    StructField("num_copied_rows", LongType()),
    StructField("user_name", StringType()),
    StructField("job_run_id", StringType()),
    StructField("cluster_id", StringType()),
    StructField("_snapshot_ts", TimestampType()),
])


def _int(metricas, clave):
    valor = (metricas or {}).get(clave)
    if valor is None:
        return None
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


snapshot_ts = datetime.now(timezone.utc)
filas = []

for schema, capa in SCHEMAS.items():
    tablas = [
        r["table_name"]
        for r in spark.sql(f"""
            SELECT table_name
            FROM {CATALOGO}.information_schema.tables
            WHERE table_schema = '{schema}' AND table_type <> 'VIEW'
        """).collect()
    ]
    for t in tablas:
        if t == "historial_tablas":
            continue  # no se registra a sí misma
        fqn = f"{CATALOGO}.{schema}.{t}"
        try:
            historia = spark.sql(f"DESCRIBE HISTORY {fqn}").collect()
        except Exception as e:  # tabla sin historial Delta / permisos / etc.
            print(f"  skip {fqn}: {e}")
            continue
        for h in historia:
            m = h["operationMetrics"] or {}
            filas.append(Row(
                capa=capa,
                tabla=fqn,
                version=int(h["version"]),
                timestamp=h["timestamp"],
                operation=h["operation"],
                num_output_rows=_int(m, "numOutputRows"),
                num_inserted_rows=_int(m, "numTargetRowsInserted"),
                num_updated_rows=_int(m, "numTargetRowsUpdated") if "numTargetRowsUpdated" in m else _int(m, "numUpdatedRows"),
                num_deleted_rows=_int(m, "numTargetRowsDeleted") if "numTargetRowsDeleted" in m else _int(m, "numDeletedRows"),
                num_copied_rows=_int(m, "numTargetRowsCopied"),
                user_name=h["userName"],
                job_run_id=None,
                cluster_id=h["clusterId"],
                _snapshot_ts=snapshot_ts,
            ))

print(f"{len(filas)} commits leídos de {len(SCHEMAS)} esquemas.")
spark.createDataFrame(filas, ESQUEMA_FILA).createOrReplaceTempView("_hist_nuevo")

# COMMAND ----------

spark.sql("""
    INSERT INTO gran_concepcion.03_oro.historial_tablas
    SELECT n.*
    FROM _hist_nuevo n
    LEFT ANTI JOIN gran_concepcion.03_oro.historial_tablas h
      ON h.tabla = n.tabla AND h.version = n.version
""")

print("historial_tablas actualizada (append-only).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT capa,
# MAGIC        COUNT(DISTINCT tabla) AS tablas,
# MAGIC        COUNT(*)             AS commits,
# MAGIC        MAX(timestamp)       AS ultimo_commit
# MAGIC FROM gran_concepcion.03_oro.historial_tablas
# MAGIC GROUP BY capa
# MAGIC ORDER BY capa

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compactar (OPTIMIZE)

# COMMAND ----------

spark.sql("OPTIMIZE gran_concepcion.03_oro.historial_tablas ZORDER BY (tabla)")
