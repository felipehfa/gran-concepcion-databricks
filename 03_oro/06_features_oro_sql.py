# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 — Ingeniería de variables (Oro), incremental
# MAGIC
# MAGIC Toma `gran_concepcion.02_plata.avisos_limpios` y agrega las features
# MAGIC finales para el modelo, puntuando cada aviso nuevo contra la **población
# MAGIC de referencia congelada** que carga `00_carga_manual_poblacion_referencia_oro_python`
# MAGIC (el mismo dataset con el que se entrenó el modelo vigente), en vez de
# MAGIC contra el resto de avisos que vayan llegando. Así el vector de features de
# MAGIC un aviso no cambia según cuándo se corra el pipeline ni según qué otros
# MAGIC avisos hayan llegado antes o después — igual que en el proyecto original.
# MAGIC
# MAGIC Calcula: distancias Haversine a los centros de comuna y de Concepción,
# MAGIC `ratio_total_util`, `amoblado` (desde el título, no desde el campo de
# MAGIC detalle), `nivel_barrio` (lookup contra el diccionario congelado, con
# MAGIC nivel por defecto para barrios no vistos al entrenar),
# MAGIC `precio_m2_sector_departamento` (mediana de precio/m2 de los vecinos
# MAGIC dentro de 300m EN LA POBLACIÓN DE REFERENCIA, con el mismo filtro de
# MAGIC outliers IQR que se calculó una sola vez al cargar esa población),
# MAGIC `antiguedad_anos` (si falta: mediana de vecinos dentro de 200m en la
# MAGIC población de referencia, luego mediana por comuna, luego mediana global),
# MAGIC y el respaldo de vulnerabilidad socioterritorial (`rank_nac`,
# MAGIC `pob_rsh_uv`, `p_urbano`, `c_ig_com`, `hog_uv`: media de la comuna en la
# MAGIC población de referencia mientras el cruce punto-en-polígono de
# MAGIC `07_vulnerabilidad_oro_python` no los haya resuelto todavía).
# MAGIC
# MAGIC Procesa **solo** los avisos de Plata que todavía no existen en
# MAGIC `gran_concepcion.03_oro.avisos_features`, y descarta (no incluye en Oro,
# MAGIC se reintentan en la próxima corrida) los que no pasan el mismo filtro de
# MAGIC sanidad que usa el proyecto original antes de calcular features: sin
# MAGIC dormitorios o baños, o sin ningún dato de superficie. Cada fila insertada
# MAGIC queda marcada con `fecha_creacion_oro`.
# MAGIC
# MAGIC **Requisito previo:**
# MAGIC - `gran_concepcion.02_plata.avisos_limpios` ya generada (notebooks 03, 04
# MAGIC   y 05 ya corridos).
# MAGIC - `00_carga_manual_poblacion_referencia_oro_python` ya corrido al menos
# MAGIC   una vez (población de referencia, niveles de barrio y estadísticas de
# MAGIC   respaldo ya cargadas).

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
# MAGIC `avisos_features`. Si `avisos_features` no existe todavía (primera
# MAGIC corrida), todos los avisos de Plata se consideran pendientes.

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
# MAGIC ### 3. Filtro de sanidad (mismo criterio que `aplicar_filtros_sanidad` del original)
# MAGIC Descarta de esta corrida (no se insertan en Oro, se reintentan en la
# MAGIC próxima) los avisos sin dormitorios o baños, o sin ningún dato de
# MAGIC superficie (ni útil, ni total, ni la superficie cruda de la grilla). Los
# MAGIC casos de dormitorios/baños/estacionamientos extremos ya quedaron en NULL
# MAGIC desde Plata (paso 5 de `04_limpieza_plata_sql`), así que ya caen en el
# MAGIC primer filtro.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_validos AS
# MAGIC SELECT *
# MAGIC FROM plata_pendiente
# MAGIC WHERE dormitorios IS NOT NULL
# MAGIC   AND banos IS NOT NULL
# MAGIC   AND NOT (superficie_util_m2 IS NULL AND superficie_total_m2 IS NULL AND superficie_m2 IS NULL)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Centros de referencia (comuna y Concepción)
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
# MAGIC ### 5. Distancias Haversine + `ratio_total_util` + `amoblado`
# MAGIC Distancia en metros al centro de la propia comuna y al centro de
# MAGIC Concepción (fijo: `-36.8265, -73.0524`, mismo punto que
# MAGIC `concepcion-biobio`). `amoblado` se deriva del TÍTULO del aviso (mismo
# MAGIC criterio que el modelo original), no del campo "Amoblado" de la página de
# MAGIC detalle — se reemplaza el `amoblado` que ya trae Plata (de otra fuente)
# MAGIC por este.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_distancias AS
# MAGIC SELECT
# MAGIC     p.* EXCEPT (amoblado),
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
# MAGIC     CAST(regexp_like(p.titulo, '(?i)amoblad[oa]|amueblad[oa]') AS INT) AS amoblado
# MAGIC FROM pendientes_validos p
# MAGIC LEFT JOIN centros_comuna cc ON p.comuna = cc.comuna

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Rellenar con 0 los amenities/POIs sin dato
# MAGIC Ausencia de dato equivale a "no tiene"/"no hay ninguno cerca", mismo
# MAGIC criterio que `normalizar_columnas_nuevas` del original — reemplaza los
# MAGIC valores NULL que trae Plata por 0, en vez de dejarlos sin resolver.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_normalizados AS
# MAGIC SELECT
# MAGIC     * EXCEPT (
# MAGIC         estacionamientos, bodegas, conserjeria, estacionamiento_visitas,
# MAGIC         condominio_cerrado, piscina, ascensor, gastos_comunes,
# MAGIC         cantidad_paraderos, cantidad_jardines_infantiles, cantidad_colegios,
# MAGIC         cantidad_universidades, cantidad_plazas, cantidad_supermercados,
# MAGIC         cantidad_farmacias, cantidad_centros_comerciales, cantidad_clinicas
# MAGIC     ),
# MAGIC     COALESCE(estacionamientos, 0)            AS estacionamientos,
# MAGIC     COALESCE(bodegas, 0)                     AS bodegas,
# MAGIC     COALESCE(conserjeria, 0)                 AS conserjeria,
# MAGIC     COALESCE(estacionamiento_visitas, 0)     AS estacionamiento_visitas,
# MAGIC     COALESCE(condominio_cerrado, 0)          AS condominio_cerrado,
# MAGIC     COALESCE(piscina, 0)                     AS piscina,
# MAGIC     COALESCE(ascensor, 0)                    AS ascensor,
# MAGIC     COALESCE(gastos_comunes, 0)              AS gastos_comunes,
# MAGIC     COALESCE(cantidad_paraderos, 0)          AS cantidad_paraderos,
# MAGIC     COALESCE(cantidad_jardines_infantiles, 0) AS cantidad_jardines_infantiles,
# MAGIC     COALESCE(cantidad_colegios, 0)           AS cantidad_colegios,
# MAGIC     COALESCE(cantidad_universidades, 0)      AS cantidad_universidades,
# MAGIC     COALESCE(cantidad_plazas, 0)              AS cantidad_plazas,
# MAGIC     COALESCE(cantidad_supermercados, 0)      AS cantidad_supermercados,
# MAGIC     COALESCE(cantidad_farmacias, 0)          AS cantidad_farmacias,
# MAGIC     COALESCE(cantidad_centros_comerciales, 0) AS cantidad_centros_comerciales,
# MAGIC     COALESCE(cantidad_clinicas, 0)           AS cantidad_clinicas
# MAGIC FROM pendientes_distancias

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Estadísticas de respaldo, en columnas (una sola fila)
# MAGIC Pivotea `referencia_estadisticas` (clave/valor) a columnas, para poder
# MAGIC hacer `CROSS JOIN` contra ella en los pasos siguientes sin subconsultas
# MAGIC repetidas.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW referencia_escalares AS
# MAGIC SELECT
# MAGIC     MAX(CASE WHEN clave = 'mediana_antiguedad_global' THEN valor END) AS mediana_antiguedad_global,
# MAGIC     MAX(CASE WHEN clave = 'piso_promedio' THEN valor END) AS piso_promedio,
# MAGIC     MAX(CASE WHEN clave = 'media_rank_nac_global' THEN valor END) AS media_rank_nac_global,
# MAGIC     MAX(CASE WHEN clave = 'media_pob_rsh_uv_global' THEN valor END) AS media_pob_rsh_uv_global,
# MAGIC     MAX(CASE WHEN clave = 'media_p_urbano_global' THEN valor END) AS media_p_urbano_global,
# MAGIC     MAX(CASE WHEN clave = 'media_c_ig_com_global' THEN valor END) AS media_c_ig_com_global,
# MAGIC     MAX(CASE WHEN clave = 'media_hog_uv_global' THEN valor END) AS media_hog_uv_global,
# MAGIC     MAX(CASE WHEN clave = 'mediana_precio_m2_fallback' THEN valor END) AS mediana_precio_m2_fallback,
# MAGIC     MAX(CASE WHEN clave = 'nivel_barrio_default' THEN valor END) AS nivel_barrio_default
# MAGIC FROM gran_concepcion.03_oro.referencia_estadisticas

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. `nivel_barrio`: lookup contra el diccionario congelado
# MAGIC A diferencia del cálculo dinámico, acá se busca el nivel ya calculado al
# MAGIC entrenar (`niveles_barrio_referencia`); un barrio no visto en ese momento
# MAGIC cae al nivel por defecto.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_nivel_barrio AS
# MAGIC SELECT
# MAGIC     p.*,
# MAGIC     COALESCE(nb.nivel_barrio, CAST(e.nivel_barrio_default AS INT)) AS nivel_barrio
# MAGIC FROM pendientes_normalizados p
# MAGIC LEFT JOIN gran_concepcion.03_oro.niveles_barrio_referencia nb ON p.barrio = nb.barrio
# MAGIC CROSS JOIN referencia_escalares e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Respaldo de vulnerabilidad socioterritorial (media de la comuna, en la referencia)
# MAGIC `07_vulnerabilidad_oro_python` corre DESPUÉS de este notebook y resuelve
# MAGIC estas columnas por cruce punto-en-polígono, sobreescribiendo el respaldo
# MAGIC calculado acá en cuanto lo logre — mismo orden de prioridad que el
# MAGIC original (valor real del cruce > media de la comuna > media global).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_vulnerabilidad AS
# MAGIC SELECT
# MAGIC     p.* EXCEPT (rank_nac, pob_rsh_uv, p_urbano, c_ig_com, hog_uv),
# MAGIC     COALESCE(p.rank_nac, cs.media_rank_nac, e.media_rank_nac_global) AS rank_nac,
# MAGIC     COALESCE(p.pob_rsh_uv, cs.media_pob_rsh_uv, e.media_pob_rsh_uv_global) AS pob_rsh_uv,
# MAGIC     COALESCE(p.p_urbano, cs.media_p_urbano, e.media_p_urbano_global) AS p_urbano,
# MAGIC     COALESCE(p.c_ig_com, cs.media_c_ig_com, e.media_c_ig_com_global) AS c_ig_com,
# MAGIC     COALESCE(p.hog_uv, cs.media_hog_uv, e.media_hog_uv_global) AS hog_uv
# MAGIC FROM pendientes_nivel_barrio p
# MAGIC LEFT JOIN gran_concepcion.03_oro.referencia_estadisticas_por_comuna cs ON p.comuna = cs.comuna
# MAGIC CROSS JOIN referencia_escalares e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. `piso_unidad`: sin dato -> 1er piso; > 30 -> promedio de la referencia

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_piso AS
# MAGIC SELECT
# MAGIC     p.* EXCEPT (piso_unidad),
# MAGIC     CASE
# MAGIC         WHEN p.piso_unidad IS NULL THEN 1.0
# MAGIC         WHEN p.piso_unidad > 30 THEN e.piso_promedio
# MAGIC         ELSE p.piso_unidad
# MAGIC     END AS piso_unidad
# MAGIC FROM pendientes_vulnerabilidad p
# MAGIC CROSS JOIN referencia_escalares e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. `antiguedad_anos`: vecinos dentro de 200m EN LA POBLACIÓN DE REFERENCIA
# MAGIC Si el aviso ya trae antigüedad, se deja tal cual. Si no, se busca la
# MAGIC mediana de los avisos de `poblacion_referencia` (no de Plata) dentro de
# MAGIC 200 metros; si no hay ninguno, mediana por comuna de la referencia; si
# MAGIC tampoco, mediana global de la referencia. Cascada calculada sobre los
# MAGIC valores ORIGINALES de la referencia (ya vienen sin nulos: la población de
# MAGIC referencia se cargó ya imputada).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW candidatos_antiguedad AS
# MAGIC SELECT
# MAGIC     p.id_aviso,
# MAGIC     r.antiguedad_anos AS antiguedad_vecino
# MAGIC FROM pendientes_piso p
# MAGIC INNER JOIN gran_concepcion.03_oro.poblacion_referencia r
# MAGIC     ON p.latitud IS NOT NULL AND p.longitud IS NOT NULL
# MAGIC     AND (
# MAGIC         2 * 6371000 * ASIN(SQRT(
# MAGIC             POWER(SIN(RADIANS(r.latitud - p.latitud) / 2), 2) +
# MAGIC             COS(RADIANS(p.latitud)) * COS(RADIANS(r.latitud)) *
# MAGIC             POWER(SIN(RADIANS(r.longitud - p.longitud) / 2), 2)
# MAGIC         )) <= 200
# MAGIC     )
# MAGIC WHERE p.antiguedad_anos IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW antiguedad_vecinos AS
# MAGIC SELECT id_aviso, PERCENTILE_APPROX(antiguedad_vecino, 0.5) AS antiguedad_vecinos_mediana
# MAGIC FROM candidatos_antiguedad
# MAGIC GROUP BY id_aviso

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_antiguedad AS
# MAGIC SELECT
# MAGIC     p.* EXCEPT (antiguedad_anos),
# MAGIC     COALESCE(
# MAGIC         p.antiguedad_anos,
# MAGIC         av.antiguedad_vecinos_mediana,
# MAGIC         cs.mediana_antiguedad,
# MAGIC         e.mediana_antiguedad_global
# MAGIC     ) AS antiguedad_anos
# MAGIC FROM pendientes_piso p
# MAGIC LEFT JOIN antiguedad_vecinos av ON p.id_aviso = av.id_aviso
# MAGIC LEFT JOIN gran_concepcion.03_oro.referencia_estadisticas_por_comuna cs ON p.comuna = cs.comuna
# MAGIC CROSS JOIN referencia_escalares e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 12. `precio_m2_sector_departamento`: vecinos dentro de 300m EN LA POBLACIÓN DE REFERENCIA
# MAGIC Solo contra los avisos de referencia marcados `precio_m2_valido = true`
# MAGIC (el filtro IQR ya se calculó UNA vez, de forma global, al cargar la
# MAGIC población — no un IQR distinto por cada aviso o vecindario). Si no hay
# MAGIC vecinos válidos, cae a la mediana de respaldo y queda
# MAGIC `tiene_comparables_cercanos = false`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW candidatos_sector AS
# MAGIC SELECT
# MAGIC     p.id_aviso,
# MAGIC     r.precio_m2 AS precio_m2_vecino
# MAGIC FROM pendientes_antiguedad p
# MAGIC INNER JOIN gran_concepcion.03_oro.poblacion_referencia r
# MAGIC     ON r.precio_m2_valido = true
# MAGIC     AND p.latitud IS NOT NULL AND p.longitud IS NOT NULL
# MAGIC     AND (
# MAGIC         2 * 6371000 * ASIN(SQRT(
# MAGIC             POWER(SIN(RADIANS(r.latitud - p.latitud) / 2), 2) +
# MAGIC             COS(RADIANS(p.latitud)) * COS(RADIANS(r.latitud)) *
# MAGIC             POWER(SIN(RADIANS(r.longitud - p.longitud) / 2), 2)
# MAGIC         )) <= 300
# MAGIC     )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW sector_mediana AS
# MAGIC SELECT
# MAGIC     id_aviso,
# MAGIC     PERCENTILE_APPROX(precio_m2_vecino, 0.5) AS precio_m2_sector_vecinos,
# MAGIC     true AS tiene_comparables_cercanos
# MAGIC FROM candidatos_sector
# MAGIC GROUP BY id_aviso

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_sector AS
# MAGIC SELECT
# MAGIC     p.*,
# MAGIC     COALESCE(sm.precio_m2_sector_vecinos, e.mediana_precio_m2_fallback) AS precio_m2_sector_departamento,
# MAGIC     COALESCE(sm.tiene_comparables_cercanos, false) AS tiene_comparables_cercanos
# MAGIC FROM pendientes_antiguedad p
# MAGIC LEFT JOIN sector_mediana sm ON p.id_aviso = sm.id_aviso
# MAGIC CROSS JOIN referencia_escalares e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 13. Ensamblar la tabla final de Oro
# MAGIC Se agrega `fecha_creacion_oro` con el momento exacto del procesamiento —
# MAGIC como esta vista solo cubre avisos NUEVOS, cada fila queda con la fecha en
# MAGIC que efectivamente entró a Oro. Se conservan todas las columnas de Plata
# MAGIC (informativas) además de las 30 features del modelo.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_oro AS
# MAGIC SELECT
# MAGIC     *,
# MAGIC     CURRENT_TIMESTAMP() AS fecha_creacion_oro
# MAGIC FROM pendientes_sector

# COMMAND ----------

# MAGIC %md
# MAGIC ### 14. Crear la tabla de Oro (si no existe) e insertar los avisos nuevos

# COMMAND ----------

if spark.catalog.tableExists(tabla_oro):
    columnas = spark.table("pendientes_oro").columns
    lista_columnas = ", ".join(columnas)

    spark.sql(f"""
        INSERT INTO {tabla_oro} ({lista_columnas})
        SELECT {lista_columnas} FROM pendientes_oro
    """)
else:
    spark.sql(f"""
        CREATE TABLE {tabla_oro} AS
        SELECT * FROM pendientes_oro
    """)

print(f"Procesadas {spark.table('pendientes_oro').count()} filas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 15. Eliminar duplicados
# MAGIC Red de seguridad ante una corrida repetida sobre el mismo aviso (ej. dos
# MAGIC corridas manuales muy seguidas): se queda con la fila más reciente por
# MAGIC `id_aviso`.

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
# MAGIC ### 16. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_filas FROM gran_concepcion.03_oro.avisos_features

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     id_aviso, comuna, barrio, nivel_barrio, amoblado,
# MAGIC     distancia_centro_comuna_m, distancia_centro_concepcion_m,
# MAGIC     ratio_total_util, antiguedad_anos,
# MAGIC     precio_m2_sector_departamento, tiene_comparables_cercanos,
# MAGIC     rank_nac, pob_rsh_uv, p_urbano, c_ig_com, hog_uv,
# MAGIC     fecha_creacion_oro
# MAGIC FROM gran_concepcion.03_oro.avisos_features
# MAGIC ORDER BY fecha_creacion_oro DESC
# MAGIC LIMIT 10
