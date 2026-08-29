# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 11 — Modelo dimensional (Oro), incremental
# MAGIC
# MAGIC Construye la capa de consumo para Power BI a partir de las tablas de
# MAGIC staging que ya arma el resto de Oro (`stg_avisos_features`,
# MAGIC `stg_predicciones`) — comparar precio real vs. predicción del modelo e
# MAGIC identificar avisos activos sobre/bajo precio de mercado, sin que el
# MAGIC reporte tenga que leer directo el OBT ancho ni entender el detalle de
# MAGIC cómo se calculan las features.
# MAGIC
# MAGIC **Esquema** (ver diseño completo en la conversación que originó este
# MAGIC notebook): cuatro dimensiones satélite 1:1 con el aviso
# MAGIC (`dim_descripcion_propiedad`, `dim_amenidades`, `dim_ubicacion` — con FK
# MAGIC a `dim_barrio` —, más `dim_tiempo` y dos SCD2 —
# MAGIC `dim_estado_aviso_scd2`, `dim_prediccion_scd2` —), y `fact_aviso`: un
# MAGIC hecho snapshot (se sobreescribe, no acumula versiones) con grano 1 fila
# MAGIC por aviso, que concentra las 6 FK directo sin hub intermedio.
# MAGIC
# MAGIC **`fact_aviso` solo incluye avisos que ya tienen al menos una
# MAGIC predicción** — sin eso no hay nada que comparar. Un aviso recién
# MAGIC scrapeado que todavía no pasó por `10_prediccion_oro_python.py` no
# MAGIC aparece acá todavía; entra solo en la corrida en que ya tenga una fila
# MAGIC vigente en `dim_prediccion_scd2`.
# MAGIC
# MAGIC **Requisito previo:** `06_features_oro_sql.py`, `07_vulnerabilidad_oro_python.py`,
# MAGIC `09_actualizacion_estado_avisos_oro_python.py` y `10_prediccion_oro_python.py`
# MAGIC ya corridos al menos una vez.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Crear las tablas dimensionales (si no existen)
# MAGIC `dim_barrio` no se crea acá — la puebla `00_carga_manual_poblacion_referencia_oro_python.py`,
# MAGIC porque es carga manual de referencia estática, no incremental.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_descripcion_propiedad (
        descripcion_id       BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso             STRING NOT NULL,
        tipo_propiedad       STRING,
        dormitorios          INT,
        banos                INT,
        estacionamientos     INT,
        bodegas              INT,
        superficie_util_m2   DOUBLE,
        superficie_total_m2  DOUBLE,
        ratio_total_util     DOUBLE,
        antiguedad_anos      DOUBLE,
        piso_unidad          DOUBLE,
        amoblado             INT,
        ascensor             INT,
        piscina              INT,
        conserjeria          INT,
        condominio_cerrado   INT,
        admite_mascotas      INT
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_amenidades (
        amenidades_id                        BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso                             STRING NOT NULL,
        cantidad_paraderos                   INT,
        distancia_min_m_paraderos            DOUBLE,
        cantidad_estaciones_metro            INT,
        distancia_min_m_estaciones_metro     DOUBLE,
        cantidad_jardines_infantiles         INT,
        distancia_min_m_jardines_infantiles  DOUBLE,
        cantidad_colegios                    INT,
        distancia_min_m_colegios             DOUBLE,
        cantidad_universidades               INT,
        distancia_min_m_universidades        DOUBLE,
        cantidad_plazas                      INT,
        distancia_min_m_plazas               DOUBLE,
        cantidad_supermercados               INT,
        distancia_min_m_supermercados        DOUBLE,
        cantidad_farmacias                   INT,
        distancia_min_m_farmacias            DOUBLE,
        cantidad_centros_comerciales         INT,
        distancia_min_m_centros_comerciales  DOUBLE,
        cantidad_hospitales                  INT,
        distancia_min_m_hospitales           DOUBLE,
        cantidad_clinicas                    INT,
        distancia_min_m_clinicas             DOUBLE
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_ubicacion (
        ubicacion_id                    BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso                        STRING NOT NULL,
        barrio_id                       BIGINT,
        comuna                          STRING,
        latitud                         DOUBLE,
        longitud                        DOUBLE,
        distancia_centro_comuna_m       DOUBLE,
        distancia_centro_concepcion_m   DOUBLE,
        rank_nac                        DOUBLE,
        pob_rsh_uv                      DOUBLE,
        p_urbano                        DOUBLE,
        c_ig_com                        DOUBLE,
        hog_uv                          DOUBLE,
        uv_rsh                          STRING
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_tiempo (
        fecha_id     BIGINT NOT NULL,
        fecha        DATE NOT NULL,
        anio         INT,
        trimestre    INT,
        mes          INT,
        nombre_mes   STRING,
        dia          INT
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_estado_aviso_scd2 (
        estado_aviso_scd_id   BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso              STRING NOT NULL,
        estado_publicacion    STRING NOT NULL,
        valid_from            TIMESTAMP NOT NULL,
        valid_to              TIMESTAMP,
        is_current            BOOLEAN NOT NULL
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_prediccion_scd2 (
        prediccion_scd_id     BIGINT GENERATED ALWAYS AS IDENTITY,
        id_aviso              STRING NOT NULL,
        version_modelo        STRING NOT NULL,
        costo_total_predicho  DOUBLE NOT NULL,
        z_robusto             DOUBLE,
        cv_ensamble           DOUBLE,
        decil_precio          INT,
        etiqueta              STRING,
        nivel_confianza       STRING,
        fecha_prediccion      TIMESTAMP,
        valid_from            TIMESTAMP NOT NULL,
        valid_to              TIMESTAMP,
        is_current            BOOLEAN NOT NULL
    )
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.fact_aviso (
        id_aviso                     STRING NOT NULL,
        descripcion_id               BIGINT,
        amenidades_id                BIGINT,
        ubicacion_id                 BIGINT,
        fecha_id                     BIGINT,
        estado_aviso_scd_id          BIGINT,
        prediccion_scd_id            BIGINT,
        url                          STRING,
        fecha_publicacion_precision  STRING,
        precio_clp_real              DOUBLE,
        gastos_comunes_real          DOUBLE,
        costo_total_real             DOUBLE
    )
""")

print("Tablas dimensionales verificadas/creadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. `dim_descripcion_propiedad` — MERGE desde `stg_avisos_features`
# MAGIC SCD1: refresca los atributos si cambian (ej. una imputación de superficie
# MAGIC que llegó tarde), inserta si es un aviso nuevo. `amoblado`/`ascensor`/etc.
# MAGIC quedan como `INT` (0/1), no `BOOLEAN`: así vienen tipados desde
# MAGIC `04_limpieza_plata_sql.py`, y castear acá metería una conversión de tipo
# MAGIC innecesaria al `MERGE`.
# MAGIC
# MAGIC (Hasta la corrida que corrigió el schema drift de `avisos_limpios`/
# MAGIC `stg_avisos_features`, `ascensor`/`piscina`/`conserjeria`/
# MAGIC `condominio_cerrado`/`estacionamiento_visitas`/`solo_familias` llegaban
# MAGIC en realidad como `DOUBLE` desde la fuente, y este `MERGE` funcionaba solo
# MAGIC porque Spark angostaba el tipo en silencio al insertar contra la columna
# MAGIC `INT` del target — ver `02_plata/ddl/avisos_limpios.py` para el detalle.
# MAGIC Ya corregido en la fuente, este `MERGE` ya no depende de ese cast
# MAGIC implícito.)

# COMMAND ----------

spark.sql("""
    MERGE INTO gran_concepcion.03_oro.dim_descripcion_propiedad AS d
    USING (
        SELECT
            id_aviso, tipo_propiedad, dormitorios, banos, estacionamientos, bodegas,
            superficie_util_m2, superficie_total_m2, ratio_total_util, antiguedad_anos,
            piso_unidad, amoblado, ascensor, piscina, conserjeria, condominio_cerrado,
            admite_mascotas
        FROM gran_concepcion.03_oro.stg_avisos_features
    ) AS s
    ON d.id_aviso = s.id_aviso
    WHEN MATCHED THEN UPDATE SET
        tipo_propiedad = s.tipo_propiedad, dormitorios = s.dormitorios, banos = s.banos,
        estacionamientos = s.estacionamientos, bodegas = s.bodegas,
        superficie_util_m2 = s.superficie_util_m2, superficie_total_m2 = s.superficie_total_m2,
        ratio_total_util = s.ratio_total_util, antiguedad_anos = s.antiguedad_anos,
        piso_unidad = s.piso_unidad, amoblado = s.amoblado, ascensor = s.ascensor,
        piscina = s.piscina, conserjeria = s.conserjeria, condominio_cerrado = s.condominio_cerrado,
        admite_mascotas = s.admite_mascotas
    WHEN NOT MATCHED THEN INSERT (
        id_aviso, tipo_propiedad, dormitorios, banos, estacionamientos, bodegas,
        superficie_util_m2, superficie_total_m2, ratio_total_util, antiguedad_anos,
        piso_unidad, amoblado, ascensor, piscina, conserjeria, condominio_cerrado,
        admite_mascotas
    ) VALUES (
        s.id_aviso, s.tipo_propiedad, s.dormitorios, s.banos, s.estacionamientos, s.bodegas,
        s.superficie_util_m2, s.superficie_total_m2, s.ratio_total_util, s.antiguedad_anos,
        s.piso_unidad, s.amoblado, s.ascensor, s.piscina, s.conserjeria, s.condominio_cerrado,
        s.admite_mascotas
    )
""")

print("dim_descripcion_propiedad actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. `dim_amenidades` — MERGE desde `stg_avisos_features`

# COMMAND ----------

spark.sql("""
    MERGE INTO gran_concepcion.03_oro.dim_amenidades AS d
    USING (
        SELECT
            id_aviso,
            cantidad_paraderos, distancia_min_m_paraderos,
            cantidad_estaciones_metro, distancia_min_m_estaciones_metro,
            cantidad_jardines_infantiles, distancia_min_m_jardines_infantiles,
            cantidad_colegios, distancia_min_m_colegios,
            cantidad_universidades, distancia_min_m_universidades,
            cantidad_plazas, distancia_min_m_plazas,
            cantidad_supermercados, distancia_min_m_supermercados,
            cantidad_farmacias, distancia_min_m_farmacias,
            cantidad_centros_comerciales, distancia_min_m_centros_comerciales,
            cantidad_hospitales, distancia_min_m_hospitales,
            cantidad_clinicas, distancia_min_m_clinicas
        FROM gran_concepcion.03_oro.stg_avisos_features
    ) AS s
    ON d.id_aviso = s.id_aviso
    WHEN MATCHED THEN UPDATE SET
        cantidad_paraderos = s.cantidad_paraderos, distancia_min_m_paraderos = s.distancia_min_m_paraderos,
        cantidad_estaciones_metro = s.cantidad_estaciones_metro,
        distancia_min_m_estaciones_metro = s.distancia_min_m_estaciones_metro,
        cantidad_jardines_infantiles = s.cantidad_jardines_infantiles,
        distancia_min_m_jardines_infantiles = s.distancia_min_m_jardines_infantiles,
        cantidad_colegios = s.cantidad_colegios, distancia_min_m_colegios = s.distancia_min_m_colegios,
        cantidad_universidades = s.cantidad_universidades,
        distancia_min_m_universidades = s.distancia_min_m_universidades,
        cantidad_plazas = s.cantidad_plazas, distancia_min_m_plazas = s.distancia_min_m_plazas,
        cantidad_supermercados = s.cantidad_supermercados,
        distancia_min_m_supermercados = s.distancia_min_m_supermercados,
        cantidad_farmacias = s.cantidad_farmacias, distancia_min_m_farmacias = s.distancia_min_m_farmacias,
        cantidad_centros_comerciales = s.cantidad_centros_comerciales,
        distancia_min_m_centros_comerciales = s.distancia_min_m_centros_comerciales,
        cantidad_hospitales = s.cantidad_hospitales, distancia_min_m_hospitales = s.distancia_min_m_hospitales,
        cantidad_clinicas = s.cantidad_clinicas, distancia_min_m_clinicas = s.distancia_min_m_clinicas
    WHEN NOT MATCHED THEN INSERT (
        id_aviso, cantidad_paraderos, distancia_min_m_paraderos,
        cantidad_estaciones_metro, distancia_min_m_estaciones_metro,
        cantidad_jardines_infantiles, distancia_min_m_jardines_infantiles,
        cantidad_colegios, distancia_min_m_colegios,
        cantidad_universidades, distancia_min_m_universidades,
        cantidad_plazas, distancia_min_m_plazas,
        cantidad_supermercados, distancia_min_m_supermercados,
        cantidad_farmacias, distancia_min_m_farmacias,
        cantidad_centros_comerciales, distancia_min_m_centros_comerciales,
        cantidad_hospitales, distancia_min_m_hospitales,
        cantidad_clinicas, distancia_min_m_clinicas
    ) VALUES (
        s.id_aviso, s.cantidad_paraderos, s.distancia_min_m_paraderos,
        s.cantidad_estaciones_metro, s.distancia_min_m_estaciones_metro,
        s.cantidad_jardines_infantiles, s.distancia_min_m_jardines_infantiles,
        s.cantidad_colegios, s.distancia_min_m_colegios,
        s.cantidad_universidades, s.distancia_min_m_universidades,
        s.cantidad_plazas, s.distancia_min_m_plazas,
        s.cantidad_supermercados, s.distancia_min_m_supermercados,
        s.cantidad_farmacias, s.distancia_min_m_farmacias,
        s.cantidad_centros_comerciales, s.distancia_min_m_centros_comerciales,
        s.cantidad_hospitales, s.distancia_min_m_hospitales,
        s.cantidad_clinicas, s.distancia_min_m_clinicas
    )
""")

print("dim_amenidades actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. `dim_ubicacion` — MERGE desde `stg_avisos_features`, con FK a `dim_barrio`
# MAGIC `LEFT JOIN` contra `dim_barrio` por nombre de barrio: un aviso con barrio
# MAGIC no reconocido en el diccionario congelado (`06_features_oro_sql.py` ya le
# MAGIC asigna `nivel_barrio_default`, pero el nombre de barrio en sí puede no
# MAGIC estar en `dim_barrio`) queda con `barrio_id = NULL` en vez de romper el
# MAGIC `MERGE`.

# COMMAND ----------

spark.sql("""
    MERGE INTO gran_concepcion.03_oro.dim_ubicacion AS d
    USING (
        SELECT
            f.id_aviso, b.barrio_id, f.comuna, f.latitud, f.longitud,
            f.distancia_centro_comuna_m, f.distancia_centro_concepcion_m,
            f.rank_nac, f.pob_rsh_uv, f.p_urbano, f.c_ig_com, f.hog_uv, f.uv_rsh
        FROM gran_concepcion.03_oro.stg_avisos_features f
        LEFT JOIN gran_concepcion.03_oro.dim_barrio b ON f.barrio = b.barrio
    ) AS s
    ON d.id_aviso = s.id_aviso
    WHEN MATCHED THEN UPDATE SET
        barrio_id = s.barrio_id, comuna = s.comuna, latitud = s.latitud, longitud = s.longitud,
        distancia_centro_comuna_m = s.distancia_centro_comuna_m,
        distancia_centro_concepcion_m = s.distancia_centro_concepcion_m,
        rank_nac = s.rank_nac, pob_rsh_uv = s.pob_rsh_uv, p_urbano = s.p_urbano,
        c_ig_com = s.c_ig_com, hog_uv = s.hog_uv, uv_rsh = s.uv_rsh
    WHEN NOT MATCHED THEN INSERT (
        id_aviso, barrio_id, comuna, latitud, longitud,
        distancia_centro_comuna_m, distancia_centro_concepcion_m,
        rank_nac, pob_rsh_uv, p_urbano, c_ig_com, hog_uv, uv_rsh
    ) VALUES (
        s.id_aviso, s.barrio_id, s.comuna, s.latitud, s.longitud,
        s.distancia_centro_comuna_m, s.distancia_centro_concepcion_m,
        s.rank_nac, s.pob_rsh_uv, s.p_urbano, s.c_ig_com, s.hog_uv, s.uv_rsh
    )
""")

print("dim_ubicacion actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. `dim_tiempo` — puebla las fechas de predicción que todavía no estén
# MAGIC `fecha_id = yyyyMMdd` de `fecha_prediccion`. Calendario estándar, sin
# MAGIC dependencia de ningún otro rol de fecha (`fecha_publicacion` no tiene
# MAGIC dimensión de tiempo propia — se dejó como atributo directo en `fact_aviso`,
# MAGIC ver diseño).

# COMMAND ----------

spark.sql("""
    INSERT INTO gran_concepcion.03_oro.dim_tiempo
    SELECT
        CAST(DATE_FORMAT(fecha, 'yyyyMMdd') AS BIGINT) AS fecha_id,
        fecha,
        YEAR(fecha) AS anio,
        QUARTER(fecha) AS trimestre,
        MONTH(fecha) AS mes,
        DATE_FORMAT(fecha, 'MMMM') AS nombre_mes,
        DAY(fecha) AS dia
    FROM (
        SELECT DISTINCT CAST(fecha_prediccion AS DATE) AS fecha
        FROM gran_concepcion.03_oro.stg_predicciones
    ) nuevas
    WHERE NOT EXISTS (
        SELECT 1 FROM gran_concepcion.03_oro.dim_tiempo t
        WHERE t.fecha_id = CAST(DATE_FORMAT(nuevas.fecha, 'yyyyMMdd') AS BIGINT)
    )
""")

print("dim_tiempo actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. `dim_estado_aviso_scd2` — SCD2, cerrar antes de insertar
# MAGIC Compara `estado_publicacion` vigente en `stg_avisos_features` (que
# MAGIC `09_actualizacion_estado_avisos_oro_python.py` mantiene al día) contra la
# MAGIC fila `is_current = true` de esta tabla. Si cambió (o el aviso todavía no
# MAGIC tiene fila), se cierra la vigente y se inserta la nueva — en ese orden:
# MAGIC si se insertara primero, el `UPDATE` de cierre alcanzaría a la fila recién
# MAGIC insertada y la cerraría por error.

# COMMAND ----------

spark.sql("""
    UPDATE gran_concepcion.03_oro.dim_estado_aviso_scd2 AS d
    SET valid_to = current_timestamp(), is_current = false
    WHERE is_current = true
      AND EXISTS (
          SELECT 1 FROM gran_concepcion.03_oro.stg_avisos_features f
          WHERE f.id_aviso = d.id_aviso
            AND f.estado_publicacion <> d.estado_publicacion
      )
""")

spark.sql("""
    INSERT INTO gran_concepcion.03_oro.dim_estado_aviso_scd2
        (id_aviso, estado_publicacion, valid_from, valid_to, is_current)
    SELECT f.id_aviso, f.estado_publicacion, current_timestamp(), NULL, true
    FROM gran_concepcion.03_oro.stg_avisos_features f
    LEFT JOIN gran_concepcion.03_oro.dim_estado_aviso_scd2 d
        ON f.id_aviso = d.id_aviso AND d.is_current = true
    WHERE d.id_aviso IS NULL
""")

print("dim_estado_aviso_scd2 actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7. `dim_prediccion_scd2` — SCD2, mismo patrón cierre-antes-de-insertar
# MAGIC `stg_predicciones` acumula una fila por `(id_aviso, version_modelo)` sin
# MAGIC pisar nada — acá se toma la más reciente por aviso (`ROW_NUMBER` por
# MAGIC `fecha_prediccion` descendente) como "lo que el modelo dice hoy", y se
# MAGIC compara contra la fila vigente de la SCD2.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE TEMP VIEW prediccion_vigente_stg AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id_aviso ORDER BY fecha_prediccion DESC) AS rn
        FROM gran_concepcion.03_oro.stg_predicciones
    )
    WHERE rn = 1
""")

spark.sql("""
    UPDATE gran_concepcion.03_oro.dim_prediccion_scd2 AS d
    SET valid_to = current_timestamp(), is_current = false
    WHERE is_current = true
      AND EXISTS (
          SELECT 1 FROM prediccion_vigente_stg s
          WHERE s.id_aviso = d.id_aviso
            AND (
                s.version_modelo <> d.version_modelo
                OR s.etiqueta <> d.etiqueta
                OR s.costo_total_predicho <> d.costo_total_predicho
            )
      )
""")

spark.sql("""
    INSERT INTO gran_concepcion.03_oro.dim_prediccion_scd2 (
        id_aviso, version_modelo, costo_total_predicho, z_robusto, cv_ensamble,
        decil_precio, etiqueta, nivel_confianza, fecha_prediccion,
        valid_from, valid_to, is_current
    )
    SELECT
        s.id_aviso, s.version_modelo, s.costo_total_predicho, s.z_robusto, s.cv_ensamble,
        s.decil_precio, s.etiqueta, s.nivel_confianza, s.fecha_prediccion,
        current_timestamp(), NULL, true
    FROM prediccion_vigente_stg s
    LEFT JOIN gran_concepcion.03_oro.dim_prediccion_scd2 d
        ON s.id_aviso = d.id_aviso AND d.is_current = true
    WHERE d.id_aviso IS NULL
""")

print("dim_prediccion_scd2 actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. `fact_aviso` — snapshot, se sobreescribe con el estado vigente
# MAGIC `JOIN` (no `LEFT JOIN`) contra `dim_prediccion_scd2`: un aviso sin
# MAGIC predicción todavía no tiene nada que comparar, así que no entra a
# MAGIC `fact_aviso` en esta corrida — entrará en la primera corrida donde ya
# MAGIC tenga una fila vigente ahí. `costo_total_real` se recalcula acá mismo
# MAGIC (`precio_clp + gastos_comunes`), igual fórmula que usa
# MAGIC `10_prediccion_oro_python.py` para comparar contra la predicción.

# COMMAND ----------

spark.sql("""
    MERGE INTO gran_concepcion.03_oro.fact_aviso AS fa
    USING (
        SELECT
            f.id_aviso,
            dp.descripcion_id,
            da.amenidades_id,
            du.ubicacion_id,
            t.fecha_id,
            es.estado_aviso_scd_id,
            pr.prediccion_scd_id,
            f.url,
            f.fecha_publicacion_precision,
            f.precio_clp AS precio_clp_real,
            f.gastos_comunes AS gastos_comunes_real,
            f.precio_clp + f.gastos_comunes AS costo_total_real
        FROM gran_concepcion.03_oro.stg_avisos_features f
        JOIN gran_concepcion.03_oro.dim_descripcion_propiedad dp ON f.id_aviso = dp.id_aviso
        JOIN gran_concepcion.03_oro.dim_amenidades da ON f.id_aviso = da.id_aviso
        JOIN gran_concepcion.03_oro.dim_ubicacion du ON f.id_aviso = du.id_aviso
        JOIN gran_concepcion.03_oro.dim_prediccion_scd2 pr
            ON f.id_aviso = pr.id_aviso AND pr.is_current = true
        JOIN gran_concepcion.03_oro.dim_tiempo t ON t.fecha = CAST(pr.fecha_prediccion AS DATE)
        LEFT JOIN gran_concepcion.03_oro.dim_estado_aviso_scd2 es
            ON f.id_aviso = es.id_aviso AND es.is_current = true
    ) AS s
    ON fa.id_aviso = s.id_aviso
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("fact_aviso actualizada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Compactar (OPTIMIZE)
# MAGIC `fact_aviso` es la tabla que más `MERGE`s recibe de este notebook (cada
# MAGIC corrida la sobreescribe entera, fila por fila cambiada); se compacta
# MAGIC igual que el resto de tablas incrementales del pipeline.

# COMMAND ----------

spark.sql("OPTIMIZE gran_concepcion.03_oro.fact_aviso ZORDER BY (id_aviso)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10. Capa de consumo — vistas semánticas
# MAGIC `CREATE OR REPLACE VIEW` (idempotente, sin datos propios) de las tres
# MAGIC vistas que consume el dashboard AI/BI "Buscador de Arriendos - Gran
# MAGIC Concepcion". El dashboard lee `SELECT * FROM` estas vistas en vez de
# MAGIC repetir los joins y la lógica de negocio en el JSON de cada dataset —
# MAGIC así la definición queda versionada acá. Definición standalone de cada
# MAGIC una (para recrear en un workspace nuevo o inspeccionar sin abrir este
# MAGIC notebook) en `03_oro/views/vw_buscador_*.py`.
# MAGIC
# MAGIC - `vw_buscador_avisos` — 1 fila por aviso publicado con predicción vigente.
# MAGIC - `vw_buscador_historial_diario` — serie diaria de avisos activos / entran / salen.
# MAGIC - `vw_buscador_valor_m2_diario` — serie diaria de valor por m² útil (media/mediana/desv).

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.03_oro.vw_buscador_avisos AS
    SELECT
        f.id_aviso,
        s.titulo,
        f.url,
        f.precio_clp_real AS precio,
        f.gastos_comunes_real AS gastos_comunes,
        f.costo_total_real AS costo_total,
        u.comuna,
        b.barrio,
        b.nivel_barrio,
        u.latitud,
        u.longitud,
        d.dormitorios,
        d.banos,
        d.superficie_util_m2,
        d.superficie_total_m2,
        d.antiguedad_anos,
        CAST(d.amoblado AS BOOLEAN)    AS amoblado,
        CAST(d.piscina AS BOOLEAN)     AS piscina,
        CAST(d.ascensor AS BOOLEAN)    AS ascensor,
        CAST(d.conserjeria AS BOOLEAN) AS conserjeria,
        d.estacionamientos,
        p.costo_total_predicho,
        p.z_robusto,
        CASE p.etiqueta
            WHEN 'oportunidad'       THEN 'Oportunidad'
            WHEN 'precio_de_mercado' THEN 'Precio de mercado'
            WHEN 'caro'              THEN 'Caro'
            ELSE p.etiqueta
        END AS etiqueta,
        CASE p.nivel_confianza
            WHEN 'alta confianza'  THEN 'Alta'
            WHEN 'confianza media' THEN 'Media'
            WHEN 'baja confianza'  THEN 'Baja'
            ELSE p.nivel_confianza
        END AS nivel_confianza,
        e.estado_publicacion,
        f.fecha_publicacion_precision
    FROM gran_concepcion.03_oro.fact_aviso f
    JOIN gran_concepcion.03_oro.dim_descripcion_propiedad d ON f.descripcion_id = d.descripcion_id
    JOIN gran_concepcion.03_oro.dim_ubicacion u ON f.ubicacion_id = u.ubicacion_id
    LEFT JOIN gran_concepcion.03_oro.dim_barrio b ON u.barrio_id = b.barrio_id
    JOIN gran_concepcion.03_oro.dim_estado_aviso_scd2 e ON f.estado_aviso_scd_id = e.estado_aviso_scd_id
    JOIN gran_concepcion.03_oro.dim_prediccion_scd2 p ON f.prediccion_scd_id = p.prediccion_scd_id
    LEFT JOIN gran_concepcion.03_oro.stg_avisos_features s ON f.id_aviso = s.id_aviso
    WHERE e.estado_publicacion IN ('activo', 'pausado')
      AND u.latitud IS NOT NULL AND u.longitud IS NOT NULL
""")

print("vw_buscador_avisos creada/reemplazada.")

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

print("vw_buscador_historial_diario creada/reemplazada.")

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.03_oro.vw_buscador_valor_m2_diario AS
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
    valor_actual AS (
      SELECT f.id_aviso, f.costo_total_real / d.superficie_util_m2 AS valor_m2
      FROM gran_concepcion.03_oro.fact_aviso f
      JOIN gran_concepcion.03_oro.dim_descripcion_propiedad d ON f.descripcion_id = d.descripcion_id
      WHERE d.superficie_util_m2 > 0
    )
    SELECT a.fecha,
      AVG(v.valor_m2) AS media,
      PERCENTILE(v.valor_m2, 0.5) AS mediana,
      STDDEV(v.valor_m2) AS desviacion_estandar
    FROM activos a
    JOIN valor_actual v ON v.id_aviso = a.id_aviso
    GROUP BY a.fecha
    ORDER BY a.fecha
""")

print("vw_buscador_valor_m2_diario creada/reemplazada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 11. Verificar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.dim_descripcion_propiedad) AS descripcion,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.dim_amenidades) AS amenidades,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.dim_ubicacion) AS ubicacion,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.dim_tiempo) AS tiempo,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.dim_estado_aviso_scd2 WHERE is_current) AS estado_vigentes,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.dim_prediccion_scd2 WHERE is_current) AS prediccion_vigentes,
# MAGIC     (SELECT COUNT(*) FROM gran_concepcion.03_oro.fact_aviso) AS fact_aviso

# COMMAND ----------

# MAGIC %md
# MAGIC #### Chequeo de integridad (inspección manual)
# MAGIC `fact_aviso` no debería tener FKs nulas hacia las 4 dimensiones satélite
# MAGIC (siempre están disponibles, se refrescan en las secciones 2-4 antes del
# MAGIC `MERGE` de la sección 8). `estado_aviso_scd_id` sí puede ser NULL en teoría
# MAGIC (LEFT JOIN) aunque en la práctica nunca debería pasar, dado que la
# MAGIC sección 6 corre antes y cubre el 100% de `stg_avisos_features`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS filas_con_fk_nula
# MAGIC FROM gran_concepcion.03_oro.fact_aviso
# MAGIC WHERE descripcion_id IS NULL OR amenidades_id IS NULL OR ubicacion_id IS NULL
