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
# MAGIC los agrega vía INSERT, sin recalcular ni tocar los avisos ya procesados
# MAGIC en corridas anteriores. Cada fila insertada queda marcada con
# MAGIC `fecha_limpieza_plata`, la fecha/hora exacta en que se procesó.
# MAGIC
# MAGIC Esta capa solo parsea y tipa los datos crudos de Bronce (números en
# MAGIC formato chileno, booleanos "Sí"/"No", conversión de precio a CLP). No
# MAGIC imputa ni descarta nada todavía: eso ocurre después, en la etapa de
# MAGIC features de Oro, contra la población de referencia congelada del modelo
# MAGIC (ver `03_oro/06_features_oro_sql.py`) — así el aviso que llega a Oro se
# MAGIC puntúa exactamente igual que en el proyecto original, sea cual sea el
# MAGIC estado del catálogo en el momento de la corrida.
# MAGIC
# MAGIC **Primera corrida o corridas siguientes: mismo código.** La condición de
# MAGIC "avisos pendientes" y la creación de la tabla se resuelven solas según si
# MAGIC `avisos_limpios` ya existe (`spark.catalog.tableExists`), sin ningún paso
# MAGIC manual que ajustar.
# MAGIC
# MAGIC **Requisito previo:** `avisos`, `avisos_detalle` cargadas en Bronce, y el
# MAGIC notebook `03_tasas_historicas_plata` ya corrido, con
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
# MAGIC `avisos_limpios`. Si la tabla no existe todavía (primera corrida), todos
# MAGIC los avisos de Bronce con detalle se consideran pendientes.

# COMMAND ----------

tabla_plata = "gran_concepcion.02_plata.avisos_limpios"

if spark.catalog.tableExists(tabla_plata):
    condicion_pendientes = f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM {tabla_plata} p
            WHERE p.id_aviso = a.id_aviso
        )
    """
else:
    condicion_pendientes = ""  # tabla no existe todavía -> todo Bronce con detalle es "pendiente"

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW base_avisos_pendientes AS
    SELECT
        a.id_aviso,
        a.comuna,
        a.tipo_propiedad,
        a.operacion,
        a.titulo,
        a.precio        AS precio_texto,
        a.moneda,
        a.ubicacion,
        a.first_seen,
        a.superficie_m2 AS superficie_m2_texto,
        d.descripcion,
        d.fecha_publicacion_texto,
        d.fecha_publicacion_aprox,
        d.fecha_publicacion_precision,
        d.superficie_total_m2   AS superficie_total_m2_texto,
        d.superficie_util_m2    AS superficie_util_m2_texto,
        d.dormitorios           AS dormitorios_texto,
        d.banos                 AS banos_texto,
        d.estacionamientos      AS estacionamientos_texto,
        d.antiguedad_anos       AS antiguedad_anos_texto,
        d.amoblado              AS amoblado_texto,
        d.admite_mascotas       AS admite_mascotas_texto,
        d.condominio_cerrado    AS condominio_cerrado_texto,
        d.bodegas               AS bodegas_texto,
        d.gastos_comunes        AS gastos_comunes_texto,
        d.estacionamiento_visitas AS estacionamiento_visitas_texto,
        d.solo_familias         AS solo_familias_texto,
        d.max_habitantes        AS max_habitantes_texto,
        d.piscina               AS piscina_texto,
        d.quincho               AS quincho_texto,
        d.conserjeria           AS conserjeria_texto,
        d.ascensor              AS ascensor_texto,
        d.piso_unidad           AS piso_unidad_texto,
        d.deptos_por_piso       AS deptos_por_piso_texto,
        d.barrio,
        d.latitud   AS latitud_texto,
        d.longitud  AS longitud_texto,
        a.estado_publicacion,
        d.cantidad_paraderos, d.distancia_min_m_paraderos,
        d.cantidad_estaciones_metro, d.distancia_min_m_estaciones_metro,
        d.cantidad_jardines_infantiles, d.distancia_min_m_jardines_infantiles,
        d.cantidad_colegios, d.distancia_min_m_colegios,
        d.cantidad_universidades, d.distancia_min_m_universidades,
        d.cantidad_plazas, d.distancia_min_m_plazas,
        d.cantidad_supermercados, d.distancia_min_m_supermercados,
        d.cantidad_farmacias, d.distancia_min_m_farmacias,
        d.cantidad_centros_comerciales, d.distancia_min_m_centros_comerciales,
        d.cantidad_hospitales, d.distancia_min_m_hospitales,
        d.cantidad_clinicas, d.distancia_min_m_clinicas
    FROM gran_concepcion.01_bronce.avisos a
    INNER JOIN gran_concepcion.01_bronce.avisos_detalle d ON a.id_aviso = d.id_aviso
    {condicion_pendientes}
""")

print(f"{spark.table('base_avisos_pendientes').count()} avisos pendientes de limpiar en Plata.")

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
# MAGIC     TRY_CAST(
# MAGIC         regexp_replace(regexp_replace(superficie_m2_texto, '\\.', ''), ',', '.')
# MAGIC         AS DOUBLE
# MAGIC     ) AS superficie_m2,
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
# MAGIC   Mismo umbral que usa el proyecto original para descartar este caso.

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
# MAGIC ### 5. Filtro de valores imposibles (dormitorios, baños, estacionamientos)
# MAGIC Deja en NULL (no borra la fila) los valores fuera de rango razonable,
# MAGIC mismos umbrales que usa el proyecto original para descartar avisos en la
# MAGIC etapa de features (`aplicar_filtros_sanidad`): dormitorios > 6, baños > 5,
# MAGIC estacionamientos > 15.
# MAGIC
# MAGIC **El filtro de precio máximo NO va acá.** El proyecto original lo aplica
# MAGIC sobre `precio_clp` (ya convertido a CLP) recién en la etapa de predicción,
# MAGIC no sobre el monto crudo en la moneda original — un aviso en UF nunca
# MAGIC dispararía un filtro de "precio > 8.000.000" antes de convertir (las UF
# MAGIC son números de 3 cifras). Ver `04_prediccion/08_prediccion_oro_python.py`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW pendientes_filtrados AS
# MAGIC SELECT
# MAGIC     * EXCEPT (dormitorios, banos, estacionamientos),
# MAGIC     CASE WHEN dormitorios <= 6 THEN dormitorios END AS dormitorios,
# MAGIC     CASE WHEN banos <= 5 THEN banos END AS banos,
# MAGIC     CASE WHEN estacionamientos <= 15 THEN estacionamientos END AS estacionamientos
# MAGIC FROM pendientes_gastos_comunes

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Conversión de precio a CLP
# MAGIC El precio puede venir en CLP, UF o USD (columna `moneda`). Se convierte a
# MAGIC CLP usando la tasa vigente el día de la publicación del aviso (fecha
# MAGIC exacta, no "la más reciente disponible"). Se agrega acá mismo
# MAGIC `fecha_limpieza_plata`, el momento exacto en que el aviso entró a Plata.

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
# MAGIC     END AS precio_clp,
# MAGIC     CURRENT_TIMESTAMP() AS fecha_limpieza_plata
# MAGIC FROM pendientes_filtrados av
# MAGIC LEFT JOIN gran_concepcion.02_plata.valores_pesos tasas
# MAGIC     ON tasas.fecha_valor = COALESCE(av.fecha_publicacion_aprox, av.first_seen)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. Crear la tabla de Plata (si no existe) e insertar los avisos nuevos
# MAGIC Igual patrón que el resto del pipeline: si `avisos_limpios` no existe
# MAGIC todavía, esta misma celda la crea con el esquema correcto a partir de los
# MAGIC pendientes; si ya existe, inserta solo las filas nuevas.

# COMMAND ----------

if spark.catalog.tableExists(tabla_plata):
    columnas = spark.table("pendientes_precio_clp").columns
    lista_columnas = ", ".join(columnas)

    spark.sql(f"""
        INSERT INTO {tabla_plata} ({lista_columnas})
        SELECT {lista_columnas} FROM pendientes_precio_clp
    """)
else:
    spark.sql(f"""
        CREATE TABLE {tabla_plata} AS
        SELECT * FROM pendientes_precio_clp
    """)

print(f"Procesadas {spark.table('pendientes_precio_clp').count()} filas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_filas FROM gran_concepcion.02_plata.avisos_limpios

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id_aviso, fecha_limpieza_plata FROM gran_concepcion.02_plata.avisos_limpios
# MAGIC ORDER BY fecha_limpieza_plata DESC LIMIT 10
