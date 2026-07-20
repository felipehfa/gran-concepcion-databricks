# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # 04 — Limpieza SQL (Plata), incremental
# MAGIC
# MAGIC Procesa **solo** los avisos de Bronce que todavía no existen en
# MAGIC `gran_concepcion.02_plata.avisos_limpios` (comparando por `id_aviso`), y
# MAGIC los agrega vía INSERT — sin recalcular ni tocar los avisos ya procesados
# MAGIC en corridas anteriores. Cada fila insertada queda marcada con
# MAGIC `fecha_limpieza_plata`, la fecha/hora exacta en que se procesó.
# MAGIC
# MAGIC **Nota sobre la imputación de antigüedad:** para buscar el vecino
# MAGIC geográfico más cercano, cada aviso nuevo se compara contra el conjunto
# MAGIC completo de avisos con antigüedad conocida — tanto los ya existentes en
# MAGIC Plata como los nuevos de esta corrida — para no perder un vecino más
# MAGIC cercano que ya estuviera guardado. Solo los avisos nuevos se insertan al
# MAGIC final.
# MAGIC
# MAGIC **IMPORTANTE — primera corrida:** si `gran_concepcion.02_plata.avisos_limpios`
# MAGIC todavía no existe, hay dos celdas marcadas más abajo ("PRIMERA CORRIDA")
# MAGIC que hay que ajustar temporalmente (comentar la referencia a la tabla que
# MAGIC todavía no existe). Desde la segunda corrida en adelante, se usan tal cual
# MAGIC están escritas.
# MAGIC
# MAGIC **Requisito previo:** `avisos`, `avisos_detalle` cargadas en Bronce, y
# MAGIC el notebook `03_tasas_historicas_plata` ya corrido, con
# MAGIC `gran_concepcion.02_plata.valores_pesos` conteniendo las fechas
# MAGIC necesarias.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Crear el esquema de Plata (si no existe)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gran_concepcion.02_plata

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Identificar avisos pendientes de limpiar
# MAGIC Avisos que ya tienen detalle en Bronce, pero todavía no están en
# MAGIC `avisos_limpios`.
# MAGIC
# MAGIC **PRIMERA CORRIDA:** si `avisos_limpios` no existe todavía, borrar el
# MAGIC bloque `WHERE NOT EXISTS (...)` completo (dejar el SELECT sin ese filtro).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW base_avisos_pendientes AS
# MAGIC SELECT
# MAGIC     a.id_aviso,
# MAGIC     a.comuna,
# MAGIC     a.tipo_propiedad,
# MAGIC     a.operacion,
# MAGIC     a.titulo,
# MAGIC     a.precio        AS precio_texto,
# MAGIC     a.moneda,
# MAGIC     a.ubicacion,
# MAGIC     a.first_seen,
# MAGIC     d.descripcion,
# MAGIC     d.fecha_publicacion_texto,
# MAGIC     d.fecha_publicacion_aprox,
# MAGIC     d.fecha_publicacion_precision,
# MAGIC     d.superficie_total_m2   AS superficie_total_m2_texto,
# MAGIC     d.superficie_util_m2    AS superficie_util_m2_texto,
# MAGIC     d.dormitorios           AS dormitorios_texto,
# MAGIC     d.banos                 AS banos_texto,
# MAGIC     d.estacionamientos      AS estacionamientos_texto,
# MAGIC     d.antiguedad_anos       AS antiguedad_anos_texto,
# MAGIC     d.amoblado              AS amoblado_texto,
# MAGIC     d.admite_mascotas       AS admite_mascotas_texto,
# MAGIC     d.condominio_cerrado    AS condominio_cerrado_texto,
# MAGIC     d.bodegas               AS bodegas_texto,
# MAGIC     d.gastos_comunes        AS gastos_comunes_texto,
# MAGIC     d.estacionamiento_visitas AS estacionamiento_visitas_texto,
# MAGIC     d.solo_familias         AS solo_familias_texto,
# MAGIC     d.max_habitantes        AS max_habitantes_texto,
# MAGIC     d.piscina               AS piscina_texto,
# MAGIC     d.quincho               AS quincho_texto,
# MAGIC     d.conserjeria           AS conserjeria_texto,
# MAGIC     d.ascensor              AS ascensor_texto,
# MAGIC     d.piso_unidad           AS piso_unidad_texto,
# MAGIC     d.deptos_por_piso       AS deptos_por_piso_texto,
# MAGIC     d.barrio,
# MAGIC     d.latitud   AS latitud_texto,
# MAGIC     d.longitud  AS longitud_texto,
# MAGIC     d.estado_publicacion,
# MAGIC     d.cantidad_paraderos, d.distancia_min_m_paraderos,
# MAGIC     d.cantidad_estaciones_metro, d.distancia_min_m_estaciones_metro,
# MAGIC     d.cantidad_jardines_infantiles, d.distancia_min_m_jardines_infantiles,
# MAGIC     d.cantidad_colegios, d.distancia_min_m_colegios,
# MAGIC     d.cantidad_universidades, d.distancia_min_m_universidades,
# MAGIC     d.cantidad_plazas, d.distancia_min_m_plazas,
# MAGIC     d.cantidad_supermercados, d.distancia_min_m_supermercados,
# MAGIC     d.cantidad_farmacias, d.distancia_min_m_farmacias,
# MAGIC     d.cantidad_centros_comerciales, d.distancia_min_m_centros_comerciales,
# MAGIC     d.cantidad_hospitales, d.distancia_min_m_hospitales,
# MAGIC     d.cantidad_clinicas, d.distancia_min_m_clinicas
# MAGIC FROM gran_concepcion.01_bronce.avisos a
# MAGIC INNER JOIN gran_concepcion.01_bronce.avisos_detalle d ON a.id_aviso = d.id_aviso

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Parsear números en formato chileno (solo pendientes)
# MAGIC `.` como separador de miles, `,` como separador decimal. `TRY_CAST` se usa
# MAGIC en vez de `CAST` para tolerar strings vacíos o mal formados sin romper la
# MAGIC ejecución (devuelve NULL en esos casos).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_numeros_parseados AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     TRY_CAST(regexp_replace(precio_texto, '\\.', '') AS DOUBLE) AS precio,
# MAGIC     TRY_CAST(
# MAGIC         regexp_replace(regexp_replace(superficie_total_m2_texto, '\\.', ''), ',', '.')
# MAGIC         AS DOUBLE
# MAGIC     ) AS superficie_total_m2,
# MAGIC     TRY_CAST(
# MAGIC         regexp_replace(regexp_replace(superficie_util_m2_texto, '\\.', ''), ',', '.')
# MAGIC         AS DOUBLE
# MAGIC     ) AS superficie_util_m2,
# MAGIC     TRY_CAST(dormitorios_texto AS DOUBLE)      AS dormitorios,
# MAGIC     TRY_CAST(banos_texto AS DOUBLE)            AS banos,
# MAGIC     TRY_CAST(estacionamientos_texto AS DOUBLE) AS estacionamientos,
# MAGIC     TRY_CAST(antiguedad_anos_texto AS DOUBLE)  AS antiguedad_anos,
# MAGIC     TRY_CAST(bodegas_texto AS DOUBLE)          AS bodegas,
# MAGIC     TRY_CAST(max_habitantes_texto AS DOUBLE)   AS max_habitantes,
# MAGIC     TRY_CAST(piso_unidad_texto AS DOUBLE)      AS piso_unidad,
# MAGIC     TRY_CAST(deptos_por_piso_texto AS DOUBLE)  AS deptos_por_piso,
# MAGIC     TRY_CAST(latitud_texto AS DOUBLE)  AS latitud,
# MAGIC     TRY_CAST(longitud_texto AS DOUBLE) AS longitud,
# MAGIC     CASE WHEN lower(amoblado_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(amoblado_texto) = 'no' THEN 0 END AS amoblado,
# MAGIC     CASE WHEN lower(admite_mascotas_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(admite_mascotas_texto) = 'no' THEN 0 END AS admite_mascotas,
# MAGIC     CASE WHEN lower(condominio_cerrado_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(condominio_cerrado_texto) = 'no' THEN 0 END AS condominio_cerrado,
# MAGIC     CASE WHEN lower(estacionamiento_visitas_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(estacionamiento_visitas_texto) = 'no' THEN 0 END AS estacionamiento_visitas,
# MAGIC     CASE WHEN lower(solo_familias_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(solo_familias_texto) = 'no' THEN 0 END AS solo_familias,
# MAGIC     CASE WHEN lower(piscina_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(piscina_texto) = 'no' THEN 0 END AS piscina,
# MAGIC     CASE WHEN lower(quincho_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(quincho_texto) = 'no' THEN 0 END AS quincho,
# MAGIC     CASE WHEN lower(conserjeria_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(conserjeria_texto) = 'no' THEN 0 END AS conserjeria,
# MAGIC     CASE WHEN lower(ascensor_texto) IN ('sí', 'si') THEN 1
# MAGIC          WHEN lower(ascensor_texto) = 'no' THEN 0 END AS ascensor
# MAGIC FROM base_avisos_pendientes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Corregir `gastos_comunes` (3 casos)
# MAGIC - Con punto (`"82.000"`): miles real, se parsea normal.
# MAGIC - Dígito suelto sin punto y < 1.000 (ej. `"1"`, `"10"`): placeholder de
# MAGIC   "incluido en el arriendo" → se guarda como `0.0`.
# MAGIC - Sin punto y ≥ 500.000 (ej. `"1111111"`): outlier implausible → `NULL`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_gastos_comunes AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CASE
# MAGIC         WHEN gastos_comunes_texto LIKE '%.%'
# MAGIC             THEN TRY_CAST(regexp_replace(gastos_comunes_texto, '\\.', '') AS DOUBLE)
# MAGIC         WHEN gastos_comunes_texto IS NOT NULL AND gastos_comunes_texto != ''
# MAGIC              AND TRY_CAST(gastos_comunes_texto AS DOUBLE) < 1000
# MAGIC             THEN 0.0
# MAGIC         WHEN gastos_comunes_texto IS NOT NULL AND gastos_comunes_texto != ''
# MAGIC              AND TRY_CAST(gastos_comunes_texto AS DOUBLE) >= 500000
# MAGIC             THEN NULL
# MAGIC         WHEN gastos_comunes_texto IS NOT NULL AND gastos_comunes_texto != ''
# MAGIC             THEN TRY_CAST(gastos_comunes_texto AS DOUBLE)
# MAGIC         ELSE NULL
# MAGIC     END AS gastos_comunes
# MAGIC FROM pendientes_numeros_parseados

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Filtros de valores imposibles
# MAGIC Descarta (deja en NULL, no borra la fila) valores fuera de rango razonable
# MAGIC para dormitorios, baños, estacionamientos y precio.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_filtrados AS
# MAGIC SELECT
# MAGIC     * EXCEPT (dormitorios, banos, estacionamientos, precio),
# MAGIC     CASE WHEN dormitorios BETWEEN 0 AND 10 THEN dormitorios END AS dormitorios,
# MAGIC     CASE WHEN banos BETWEEN 0 AND 10 THEN banos END AS banos,
# MAGIC     CASE WHEN estacionamientos BETWEEN 0 AND 5 THEN estacionamientos END AS estacionamientos,
# MAGIC     CASE WHEN precio <= 8000000 THEN precio END AS precio
# MAGIC FROM pendientes_gastos_comunes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Conversión de precio a CLP
# MAGIC El precio puede venir en CLP, UF o USD (columna `moneda`). Se convierte a
# MAGIC CLP usando la tasa vigente el día de la publicación del aviso (fecha
# MAGIC exacta, no "la más reciente disponible").

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_precio_clp AS
# MAGIC SELECT
# MAGIC     av.*,
# MAGIC     tasas.valor_uf_clp,
# MAGIC     tasas.valor_dolar_clp,
# MAGIC     CASE
# MAGIC         WHEN lower(av.moneda) = 'uf' THEN av.precio * tasas.valor_uf_clp
# MAGIC         WHEN lower(av.moneda) IN ('us$', 'usd', 'usd$') THEN av.precio * tasas.valor_dolar_clp
# MAGIC         ELSE av.precio
# MAGIC     END AS precio_clp
# MAGIC FROM pendientes_filtrados av
# MAGIC LEFT JOIN gran_concepcion.02_plata.valores_pesos tasas
# MAGIC     ON tasas.fecha_valor = COALESCE(av.fecha_publicacion_aprox, av.first_seen)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Imputación de antigüedad: vecinos desde Plata existente + pendientes
# MAGIC Cada aviso pendiente busca su vecino más cercano entre TODOS los avisos con
# MAGIC antigüedad conocida — los ya guardados en Plata, y los pendientes de esta
# MAGIC misma corrida — para no perder un vecino mejor que ya existiera.
# MAGIC
# MAGIC **PRIMERA CORRIDA:** si `avisos_limpios` no existe todavía, borrar el
# MAGIC primer SELECT de la UNION ALL (el que lee de `avisos_limpios`) y dejar
# MAGIC solo el segundo (el que lee de `pendientes_precio_clp`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW candidatos_antiguedad_conocida AS
# MAGIC SELECT id_aviso, tipo_propiedad, comuna, antiguedad_anos, latitud, longitud
# MAGIC FROM pendientes_precio_clp
# MAGIC WHERE antiguedad_anos IS NOT NULL AND latitud IS NOT NULL AND longitud IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW candidatos_vecino_antiguedad AS
# MAGIC SELECT
# MAGIC     a.id_aviso,
# MAGIC     b.antiguedad_anos AS antiguedad_vecino,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC         PARTITION BY a.id_aviso
# MAGIC         ORDER BY 2 * 6371000 * ASIN(SQRT(
# MAGIC             POWER(SIN(RADIANS(b.latitud - a.latitud) / 2), 2) +
# MAGIC             COS(RADIANS(a.latitud)) * COS(RADIANS(b.latitud)) *
# MAGIC             POWER(SIN(RADIANS(b.longitud - a.longitud) / 2), 2)
# MAGIC         )) ASC
# MAGIC     ) AS ranking
# MAGIC FROM pendientes_precio_clp a
# MAGIC INNER JOIN candidatos_antiguedad_conocida b
# MAGIC     ON a.tipo_propiedad = b.tipo_propiedad
# MAGIC     AND a.id_aviso != b.id_aviso
# MAGIC WHERE a.antiguedad_anos IS NULL
# MAGIC   AND a.latitud IS NOT NULL AND a.longitud IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Medianas de fallback (calculadas sobre Plata + pendientes)
# MAGIC Se usan cuando no hay ningún vecino geográfico disponible: mediana por
# MAGIC tipo de propiedad, luego por comuna, luego global.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW medianas_antiguedad AS
# MAGIC SELECT
# MAGIC     tipo_propiedad,
# MAGIC     comuna,
# MAGIC     PERCENTILE_APPROX(antiguedad_anos, 0.5) OVER (PARTITION BY tipo_propiedad) AS mediana_tipo,
# MAGIC     PERCENTILE_APPROX(antiguedad_anos, 0.5) OVER (PARTITION BY comuna) AS mediana_comuna,
# MAGIC     PERCENTILE_APPROX(antiguedad_anos, 0.5) OVER () AS mediana_global
# MAGIC FROM candidatos_antiguedad_conocida

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Aplicar la cascada de imputación sobre los pendientes
# MAGIC Vecino más cercano → mediana por tipo → mediana por comuna → mediana
# MAGIC global. Se agrega `fecha_limpieza_plata` con el momento exacto del
# MAGIC procesamiento — como esta vista solo cubre avisos NUEVOS (nunca ya
# MAGIC existentes), cada fila queda con la fecha en que efectivamente entró a
# MAGIC Plata.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_antiguedad_imputada AS
# MAGIC SELECT
# MAGIC     av.* EXCEPT (antiguedad_anos),
# MAGIC     COALESCE(
# MAGIC         av.antiguedad_anos,
# MAGIC         vecino.antiguedad_vecino,
# MAGIC         med.mediana_tipo,
# MAGIC         med.mediana_comuna,
# MAGIC         med.mediana_global
# MAGIC     ) AS antiguedad_anos,
# MAGIC     CASE WHEN av.antiguedad_anos IS NULL THEN true ELSE false END AS antiguedad_imputada,
# MAGIC     CURRENT_TIMESTAMP() AS fecha_limpieza_plata
# MAGIC FROM pendientes_precio_clp av
# MAGIC LEFT JOIN candidatos_vecino_antiguedad vecino
# MAGIC     ON av.id_aviso = vecino.id_aviso AND vecino.ranking = 1
# MAGIC LEFT JOIN (SELECT DISTINCT tipo_propiedad, comuna, mediana_tipo, mediana_comuna, mediana_global FROM medianas_antiguedad) med
# MAGIC     ON av.tipo_propiedad = med.tipo_propiedad AND av.comuna = med.comuna

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Crear la tabla de Plata vacía (SOLO la primera corrida)
# MAGIC Si `avisos_limpios` ya existe, esta celda no hace nada (`IF NOT EXISTS`).
# MAGIC El `WHERE 1=0` asegura que se cree el esquema correcto (incluida
# MAGIC `fecha_limpieza_plata`) sin insertar filas todavía — el INSERT real
# MAGIC ocurre en la celda siguiente.
# MAGIC
# MAGIC ### 11. Insertar solo los avisos nuevos en Plata
# MAGIC No se recalcula ni se toca ningún aviso ya existente — se corre en cada
# MAGIC corrida, incluida la primera.

# COMMAND ----------

tabla_destino = "gran_concepcion.02_plata.avisos_limpios"

if spark.catalog.tableExists(tabla_destino):
    columnas = spark.table("pendientes_antiguedad_imputada").columns
    lista_columnas = ", ".join(columnas)

    spark.sql(f"""
        INSERT INTO {tabla_destino} ({lista_columnas})
        SELECT {lista_columnas} FROM pendientes_antiguedad_imputada
    """)
else:
    spark.sql(f"""
        CREATE TABLE {tabla_destino} AS
        SELECT * FROM pendientes_antiguedad_imputada
    """)

print(f"Procesadas {spark.table('pendientes_antiguedad_imputada').count()} filas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_filas FROM gran_concepcion.02_plata.avisos_limpios

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_aviso, fecha_limpieza_plata FROM gran_concepcion.02_plata.avisos_limpios
# MAGIC ORDER BY fecha_limpieza_plata DESC LIMIT 10