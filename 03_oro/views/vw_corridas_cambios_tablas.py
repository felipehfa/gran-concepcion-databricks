# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista -- gran_concepcion.03_oro.vw_corridas_cambios_tablas
# MAGIC
# MAGIC **Creada por:** 03_oro/12_snapshot_historial_tablas_oro_python.py
# MAGIC (CREATE OR REPLACE VIEW inline). Referencia de solo lectura, idempotente,
# MAGIC sin datos propios.
# MAGIC
# MAGIC Una fila por (corrida, tabla, operacion): commits y filas
# MAGIC escritas/insertadas/actualizadas/borradas en cada tabla Delta de las 3
# MAGIC capas durante esa corrida. Sale de `historial_tablas` (snapshot de
# MAGIC DESCRIBE HISTORY), atribuido a la corrida por ventana de tiempo entre el
# MAGIC inicio de una corrida y el de la siguiente (`vw_corridas_tareas`).
# MAGIC Excluye OPTIMIZE y mantenimiento, y la propia `historial_tablas`.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.03_oro.vw_corridas_cambios_tablas AS
    WITH run_win AS (
        SELECT run_id, inicio,
            LEAD(inicio) OVER (ORDER BY inicio) AS inicio_siguiente
        FROM (SELECT run_id, MIN(inicio) AS inicio FROM gran_concepcion.03_oro.vw_corridas_tareas GROUP BY run_id)
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
