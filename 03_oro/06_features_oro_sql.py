# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 — Ingeniería de variables (Oro), incremental
# MAGIC
# MAGIC Toma `gran_concepcion.02_plata.avisos_limpios` y agrega las features
# MAGIC finales para el modelo: distancias Haversine a los centros de comuna y de
# MAGIC Concepción, `ratio_total_util`, `nivel_barrio` (quintil de precio/m² por
# MAGIC barrio, ponderado por cantidad de avisos) y `precio_m2_sector_departamento`
# MAGIC (mediana de precio/m² de los vecinos dentro de 300m, con filtro de
# MAGIC outliers y fallback a la mediana global).
# MAGIC
# MAGIC Procesa **solo** los avisos de Plata que todavía no existen en
# MAGIC `gran_concepcion.03_oro.avisos_features` — no se recalcula ni se toca
# MAGIC ningún aviso ya procesado en corridas anteriores. Cada fila insertada
# MAGIC queda marcada con `fecha_creacion_oro`.
# MAGIC
# MAGIC **IMPORTANTE — primera corrida:** si `avisos_features` todavía no existe,
# MAGIC hay una celda marcada "PRIMERA CORRIDA" que hay que ajustar temporalmente.
# MAGIC
# MAGIC **Requisito previo:** `gran_concepcion.02_plata.avisos_limpios` ya
# MAGIC generada (notebooks 03, 04 y 05 ya corridos).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Crear el esquema de Oro (si no existe)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gran_concepcion.03_oro

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Identificar avisos pendientes de procesar
# MAGIC Avisos que ya están limpios en Plata, pero todavía no están en
# MAGIC `avisos_features`. La consulta se adapta automáticamente: si la tabla
# MAGIC `avisos_features` no existe todavía (primera corrida), todos los avisos de
# MAGIC Plata se consideran pendientes; si ya existe, se filtra con `NOT EXISTS`
# MAGIC para traer solo los que faltan. Mismo código para cualquier corrida.

# COMMAND ----------

tabla_oro = "gran_concepcion.03_oro.avisos_features"

if spark.catalog.tableExists(tabla_oro):
    condicion_pendientes = f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM {tabla_oro} f
            WHERE f.id_aviso = p.id_aviso
        )
    """
else:
    condicion_pendientes = ""  # tabla no existe todavía -> todo Plata es "pendiente"

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW plata_pendiente AS
    SELECT *
    FROM gran_concepcion.02_plata.avisos_limpios p
    {condicion_pendientes}
""")

print(f"{spark.table('plata_pendiente').count()} avisos pendientes de procesar en Oro.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Centros de referencia (comuna y Concepción)
# MAGIC Coordenadas aproximadas del centro de cada comuna, usadas para calcular
# MAGIC distancias. El centro de Concepción es fijo, independiente de la comuna
# MAGIC del aviso.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW centros_comuna AS
# MAGIC SELECT * FROM VALUES
# MAGIC     ('concepcion-biobio',          -36.8265, -73.0524),
# MAGIC     ('talcahuano-biobio',          -36.7249, -73.1149),
# MAGIC     ('hualpen-biobio',             -36.7690, -73.1000),
# MAGIC     ('san-pedro-de-la-paz-biobio', -36.8380, -73.0970),
# MAGIC     ('chiguayante-biobio',         -36.9280, -73.0230),
# MAGIC     ('penco-biobio',               -36.7420, -72.9970),
# MAGIC     ('tome-biobio',                -36.6180, -72.9570),
# MAGIC     ('coronel-biobio',             -37.0270, -73.1370),
# MAGIC     ('hualqui-biobio',             -36.9670, -72.9420),
# MAGIC     ('lota-biobio',                -37.0920, -73.1600)
# MAGIC AS t(comuna, lat_centro, lon_centro)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Distancias Haversine + ratio_total_util
# MAGIC Distancia en metros al centro de la propia comuna y al centro de
# MAGIC Concepción (fijo: `-36.8265, -73.0524`, mismo punto que `concepcion-biobio`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_distancias AS
# MAGIC SELECT
# MAGIC     p.*,
# MAGIC     2 * 6371000 * ASIN(SQRT(
# MAGIC         POWER(SIN(RADIANS(cc.lat_centro - p.latitud) / 2), 2) +
# MAGIC         COS(RADIANS(p.latitud)) * COS(RADIANS(cc.lat_centro)) *
# MAGIC         POWER(SIN(RADIANS(cc.lon_centro - p.longitud) / 2), 2)
# MAGIC     )) AS distancia_centro_comuna_m,
# MAGIC
# MAGIC     2 * 6371000 * ASIN(SQRT(
# MAGIC         POWER(SIN(RADIANS(-36.8265 - p.latitud) / 2), 2) +
# MAGIC         COS(RADIANS(p.latitud)) * COS(RADIANS(-36.8265)) *
# MAGIC         POWER(SIN(RADIANS(-73.0524 - p.longitud) / 2), 2)
# MAGIC     )) AS distancia_centro_concepcion_m,
# MAGIC
# MAGIC     CASE WHEN p.superficie_util_m2 > 0
# MAGIC          THEN p.superficie_total_m2 / p.superficie_util_m2
# MAGIC     END AS ratio_total_util,
# MAGIC
# MAGIC     CASE WHEN p.superficie_util_m2 > 0
# MAGIC          THEN p.precio_clp / p.superficie_util_m2
# MAGIC     END AS precio_m2
# MAGIC FROM plata_pendiente p
# MAGIC LEFT JOIN centros_comuna cc ON p.comuna = cc.comuna

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. `nivel_barrio`: precio/m² promedio por barrio, suavizado
# MAGIC Se calcula sobre TODO Plata (no solo los pendientes), para que el nivel de
# MAGIC cada barrio sea consistente y no dependa de si el aviso es nuevo o viejo.
# MAGIC El promedio de cada barrio se suaviza hacia la media general (más peso a
# MAGIC la media general cuantos menos avisos tenga el barrio), y luego se agrupa
# MAGIC en 5 niveles mediante un cuantil ponderado por cantidad de avisos.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW stats_barrio AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         barrio,
# MAGIC         precio_clp / NULLIF(superficie_util_m2, 0) AS precio_m2
# MAGIC     FROM gran_concepcion.02_plata.avisos_limpios
# MAGIC     WHERE barrio IS NOT NULL
# MAGIC       AND superficie_util_m2 IS NOT NULL AND superficie_util_m2 > 0
# MAGIC       AND precio_clp IS NOT NULL
# MAGIC ),
# MAGIC global_stats AS (
# MAGIC     SELECT AVG(precio_m2) AS media_global FROM base
# MAGIC ),
# MAGIC por_barrio AS (
# MAGIC     SELECT
# MAGIC         barrio,
# MAGIC         COUNT(*) AS n_avisos,
# MAGIC         AVG(precio_m2) AS media_barrio
# MAGIC     FROM base
# MAGIC     GROUP BY barrio
# MAGIC )
# MAGIC SELECT
# MAGIC     b.barrio,
# MAGIC     b.n_avisos,
# MAGIC     -- suavizado: k=5 -> barrios con pocos avisos se acercan más a la media global
# MAGIC     (b.n_avisos * b.media_barrio + 5 * g.media_global) / (b.n_avisos + 5) AS media_suavizada
# MAGIC FROM por_barrio b
# MAGIC CROSS JOIN global_stats g

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW niveles_barrio AS
# MAGIC WITH ordenado AS (
# MAGIC     SELECT
# MAGIC         barrio,
# MAGIC         n_avisos,
# MAGIC         media_suavizada,
# MAGIC         SUM(n_avisos) OVER (ORDER BY media_suavizada ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS peso_acumulado,
# MAGIC         SUM(n_avisos) OVER () AS peso_total
# MAGIC     FROM stats_barrio
# MAGIC )
# MAGIC SELECT
# MAGIC     barrio,
# MAGIC     LEAST(5, GREATEST(1, CEIL((peso_acumulado / peso_total) * 5))) AS nivel_barrio
# MAGIC FROM ordenado

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Asignar `nivel_barrio` a los avisos pendientes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_nivel_barrio AS
# MAGIC SELECT
# MAGIC     pd.*,
# MAGIC     nb.nivel_barrio
# MAGIC FROM pendientes_distancias pd
# MAGIC LEFT JOIN niveles_barrio nb ON pd.barrio = nb.barrio

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. `precio_m2_sector_departamento`: mediana de vecinos dentro de 300m
# MAGIC Para cada aviso pendiente, se buscan vecinos (cualquier aviso de Plata,
# MAGIC viejo o nuevo, excluyéndose a sí mismo) dentro de 300 metros. Se filtran
# MAGIC outliers con un criterio IQR ×3 sobre el precio/m² de esos vecinos, y se
# MAGIC toma la mediana de los que quedan. Si no hay vecinos válidos, se marca
# MAGIC `tiene_comparables_cercanos = false` y se usa la mediana global como
# MAGIC fallback.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW candidatos_sector AS
# MAGIC SELECT
# MAGIC     a.id_aviso,
# MAGIC     b.precio_m2 AS precio_m2_vecino
# MAGIC FROM pendientes_nivel_barrio a
# MAGIC INNER JOIN (
# MAGIC     SELECT
# MAGIC         id_aviso, latitud, longitud,
# MAGIC         precio_clp / NULLIF(superficie_util_m2, 0) AS precio_m2
# MAGIC     FROM gran_concepcion.02_plata.avisos_limpios
# MAGIC     WHERE superficie_util_m2 IS NOT NULL AND superficie_util_m2 > 0
# MAGIC       AND precio_clp IS NOT NULL
# MAGIC       AND latitud IS NOT NULL AND longitud IS NOT NULL
# MAGIC ) b
# MAGIC ON a.id_aviso != b.id_aviso
# MAGIC AND a.latitud IS NOT NULL AND a.longitud IS NOT NULL
# MAGIC AND (
# MAGIC     2 * 6371000 * ASIN(SQRT(
# MAGIC         POWER(SIN(RADIANS(b.latitud - a.latitud) / 2), 2) +
# MAGIC         COS(RADIANS(a.latitud)) * COS(RADIANS(b.latitud)) *
# MAGIC         POWER(SIN(RADIANS(b.longitud - a.longitud) / 2), 2)
# MAGIC     ))
# MAGIC ) <= 300

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sector_percentiles AS
# MAGIC SELECT
# MAGIC     id_aviso,
# MAGIC     PERCENTILE_APPROX(precio_m2_vecino, 0.25) OVER (PARTITION BY id_aviso) AS q1,
# MAGIC     PERCENTILE_APPROX(precio_m2_vecino, 0.75) OVER (PARTITION BY id_aviso) AS q3,
# MAGIC     precio_m2_vecino
# MAGIC FROM candidatos_sector

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sector_filtrado AS
# MAGIC SELECT
# MAGIC     id_aviso,
# MAGIC     precio_m2_vecino
# MAGIC FROM sector_percentiles
# MAGIC WHERE precio_m2_vecino BETWEEN (q1 - 3 * (q3 - q1)) AND (q3 + 3 * (q3 - q1))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sector_mediana AS
# MAGIC SELECT
# MAGIC     id_aviso,
# MAGIC     PERCENTILE_APPROX(precio_m2_vecino, 0.5) AS precio_m2_sector_departamento,
# MAGIC     true AS tiene_comparables_cercanos
# MAGIC FROM sector_filtrado
# MAGIC GROUP BY id_aviso

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Mediana global (fallback para avisos sin comparables cercanos)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW mediana_global_precio_m2 AS
# MAGIC SELECT PERCENTILE_APPROX(precio_clp / NULLIF(superficie_util_m2, 0), 0.5) AS mediana_global
# MAGIC FROM gran_concepcion.02_plata.avisos_limpios
# MAGIC WHERE superficie_util_m2 IS NOT NULL AND superficie_util_m2 > 0
# MAGIC   AND precio_clp IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Ensamblar la tabla final de Oro
# MAGIC Se agrega `fecha_creacion_oro` con el momento exacto del procesamiento —
# MAGIC como esta vista solo cubre avisos NUEVOS, cada fila queda con la fecha en
# MAGIC que efectivamente entró a Oro.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_oro AS
# MAGIC SELECT
# MAGIC     pnb.*,
# MAGIC     COALESCE(sm.precio_m2_sector_departamento, mg.mediana_global) AS precio_m2_sector_departamento,
# MAGIC     COALESCE(sm.tiene_comparables_cercanos, false) AS tiene_comparables_cercanos,
# MAGIC     CURRENT_TIMESTAMP() AS fecha_creacion_oro
# MAGIC FROM pendientes_nivel_barrio pnb
# MAGIC LEFT JOIN sector_mediana sm ON pnb.id_aviso = sm.id_aviso
# MAGIC CROSS JOIN mediana_global_precio_m2 mg

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Crear la tabla de Oro vacía (SOLO la primera corrida)
# MAGIC Si `avisos_features` ya existe, esta celda no hace nada (`IF NOT EXISTS`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.avisos_features AS
# MAGIC SELECT * FROM pendientes_oro WHERE 1=0

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Insertar solo los avisos nuevos en Oro

# COMMAND ----------

tabla_destino = "gran_concepcion.03_oro.avisos_features"

if spark.catalog.tableExists(tabla_destino):
    columnas = spark.table("pendientes_oro").columns
    lista_columnas = ", ".join(columnas)

    spark.sql(f"""
        INSERT INTO {tabla_destino} ({lista_columnas})
        SELECT {lista_columnas} FROM pendientes_oro
    """)
else:
    spark.sql(f"""
        CREATE TABLE {tabla_destino} AS
        SELECT * FROM pendientes_oro
    """)

print(f"Procesadas {spark.table('pendientes_oro').count()} filas.")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Eliminar duplicados

# COMMAND ----------

columnas_actuales = spark.table("gran_concepcion.03_oro.avisos_features").columns

if "rn" in columnas_actuales:
    seleccion_base = "* EXCEPT (rn)"
else:
    seleccion_base = "*"

spark.sql(f"""
    CREATE OR REPLACE TABLE gran_concepcion.03_oro.avisos_features AS
    SELECT * FROM (
        SELECT {seleccion_base},
            ROW_NUMBER() OVER (PARTITION BY id_aviso ORDER BY fecha_creacion_oro DESC) AS rn
        FROM gran_concepcion.03_oro.avisos_features
    )
    WHERE rn = 1
""")

print("Duplicados eliminados. Columna 'rn' ya no debería persistir en la tabla.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_filas FROM gran_concepcion.03_oro.avisos_features

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id_aviso, comuna, barrio, nivel_barrio,
# MAGIC     distancia_centro_comuna_m, distancia_centro_concepcion_m,
# MAGIC     ratio_total_util, precio_m2_sector_departamento, tiene_comparables_cercanos,
# MAGIC     fecha_creacion_oro
# MAGIC FROM gran_concepcion.03_oro.avisos_features
# MAGIC ORDER BY fecha_creacion_oro DESC
# MAGIC LIMIT 10