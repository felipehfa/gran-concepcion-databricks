# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.03_oro.stg_avisos_features`
# MAGIC
# MAGIC **Creada por:** `03_oro/06_features_oro_sql.py` (sección 14,
# MAGIC `CREATE TABLE ... AS SELECT` a partir de `pendientes_oro`) — el notebook
# MAGIC fuente NO tiene un DDL explícito, el esquema lo infiere Spark de la
# MAGIC cadena de vistas temporales (misma situación que `avisos_limpios` en
# MAGIC Plata). Columnas agregadas después vía `ALTER TABLE ADD COLUMNS`:
# MAGIC `uv_rsh`/`fecha_vulnerabilidad_oro` (`07_vulnerabilidad_oro_python.py`) y
# MAGIC `fecha_chequeo_estado_oro`/`intentos_fallidos_chequeo_estado_oro`
# MAGIC (`09_actualizacion_estado_avisos_oro_python.py`).
# MAGIC
# MAGIC Esta celda se generó leyendo el esquema real de la tabla en el catálogo
# MAGIC (`DESCRIBE TABLE`), no transcribiendo el `SELECT` a mano. Es referencia
# MAGIC de solo lectura: correrla no reemplaza el `CREATE TABLE ... AS SELECT`
# MAGIC real del notebook fuente.
# MAGIC
# MAGIC OBT — el feature store completo: hereda ~90 columnas de
# MAGIC `avisos_limpios` (texto crudo + parseado) y agrega las ~15 features
# MAGIC calculadas por Oro. Fuente de las 4 dimensiones satélite
# MAGIC (`dim_descripcion_propiedad`, `dim_amenidades`, `dim_ubicacion` vía
# MAGIC `dim_barrio`, y `dim_estado_aviso_scd2`) que puebla
# MAGIC `11_modelo_dimensional_oro_sql.py`.
# MAGIC
# MAGIC Notas sobre el estado real observado al momento de escribir esta celda:
# MAGIC - `fecha_chequeo_estado_oro`/`intentos_fallidos_chequeo_estado_oro` están
# MAGIC   declaradas abajo como parte del esquema completo del diseño, pero
# MAGIC   `09_actualizacion_estado_avisos_oro_python.py` todavía no había
# MAGIC   corrido contra este catálogo — esas dos columnas pueden no existir
# MAGIC   todavía en la tabla real hasta la primera corrida de ese notebook.
# MAGIC - `rn` (BIGINT/INT) aparece al final de la tabla real, sobrante del
# MAGIC   dedup de la sección 15 (`ROW_NUMBER() ... AS rn`, `WHERE rn = 1`,
# MAGIC   `CREATE OR REPLACE TABLE ... AS SELECT`). Es un bug latente del
# MAGIC   notebook original: `seleccion_base` solo excluye `rn` de las columnas
# MAGIC   ORIGINALES, pero el `SELECT` siempre vuelve a agregar `rn` al final —
# MAGIC   nunca se limpia. No se declara en el DDL de abajo (no debería existir
# MAGIC   como columna permanente); si aparece en `DESCRIBE TABLE`, es la señal
# MAGIC   de que ese bug sigue sin corregirse en `06_features_oro_sql.py`.
# MAGIC - `solo_familias_texto` puede reportar tipo `void` en `DESCRIBE TABLE` si
# MAGIC   el 100% de las filas actuales tienen NULL ahí — se declara `STRING`
# MAGIC   abajo, el tipo que le corresponde.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_avisos_features (
        id_aviso                                    STRING,
        comuna                                       STRING,
        tipo_propiedad                               STRING,
        operacion                                    STRING,
        titulo                                       STRING,
        precio_texto                                 STRING,
        moneda                                       STRING,
        ubicacion                                    STRING,
        first_seen                                   STRING,
        url                                          STRING,
        superficie_m2_texto                          STRING,
        descripcion                                  STRING,
        fecha_publicacion_texto                      STRING,
        fecha_publicacion_aprox                      STRING,
        fecha_publicacion_precision                  STRING,
        superficie_total_m2_texto                    STRING,
        superficie_util_m2_texto                     STRING,
        dormitorios_texto                            STRING,
        banos_texto                                  STRING,
        estacionamientos_texto                       STRING,
        antiguedad_anos_texto                        STRING,
        amoblado_texto                               STRING,
        admite_mascotas_texto                        STRING,
        condominio_cerrado_texto                     STRING,
        bodegas_texto                                STRING,
        gastos_comunes_texto                         STRING,
        estacionamiento_visitas_texto                STRING,
        solo_familias_texto                          STRING,
        max_habitantes_texto                         STRING,
        piscina_texto                                STRING,
        quincho_texto                                STRING,
        conserjeria_texto                            STRING,
        ascensor_texto                               STRING,
        piso_unidad_texto                            STRING,
        deptos_por_piso_texto                        STRING,
        barrio                                       STRING,
        latitud_texto                                STRING,
        longitud_texto                               STRING,
        estado_publicacion                           STRING,
        _sistema_origen                              STRING,
        _id_corrida                                  STRING,
        cantidad_paraderos_texto                     STRING,
        distancia_min_m_paraderos_texto              STRING,
        cantidad_estaciones_metro_texto              STRING,
        distancia_min_m_estaciones_metro_texto       STRING,
        cantidad_jardines_infantiles_texto           STRING,
        distancia_min_m_jardines_infantiles_texto    STRING,
        cantidad_colegios_texto                      STRING,
        distancia_min_m_colegios_texto               STRING,
        cantidad_universidades_texto                 STRING,
        distancia_min_m_universidades_texto          STRING,
        cantidad_plazas_texto                        STRING,
        distancia_min_m_plazas_texto                 STRING,
        cantidad_supermercados_texto                 STRING,
        distancia_min_m_supermercados_texto          STRING,
        cantidad_farmacias_texto                     STRING,
        distancia_min_m_farmacias_texto              STRING,
        cantidad_centros_comerciales_texto           STRING,
        distancia_min_m_centros_comerciales_texto    STRING,
        cantidad_hospitales_texto                    STRING,
        distancia_min_m_hospitales_texto             STRING,
        cantidad_clinicas_texto                      STRING,
        distancia_min_m_clinicas_texto               STRING,
        precio                                       DOUBLE,
        superficie_total_m2                          DOUBLE,
        superficie_util_m2                           DOUBLE,
        superficie_m2                                DOUBLE,
        max_habitantes                               DOUBLE,
        deptos_por_piso                              DOUBLE,
        latitud                                      DOUBLE,
        longitud                                     DOUBLE,
        distancia_min_m_paraderos                    DOUBLE,
        distancia_min_m_estaciones_metro             DOUBLE,
        distancia_min_m_jardines_infantiles          DOUBLE,
        distancia_min_m_colegios                     DOUBLE,
        distancia_min_m_universidades                DOUBLE,
        distancia_min_m_plazas                       DOUBLE,
        distancia_min_m_supermercados                DOUBLE,
        distancia_min_m_farmacias                    DOUBLE,
        distancia_min_m_centros_comerciales          DOUBLE,
        distancia_min_m_hospitales                   DOUBLE,
        distancia_min_m_clinicas                     DOUBLE,
        admite_mascotas                              INT,
        solo_familias                                DOUBLE,
        quincho                                      DOUBLE,
        dormitorios                                  DOUBLE,
        banos                                        DOUBLE,
        valor_uf_clp                                 DOUBLE,
        valor_dolar_clp                              DOUBLE,
        precio_clp                                   DOUBLE,
        fecha_limpieza_plata                         TIMESTAMP,
        superficie_util_imputada                     BOOLEAN,
        superficie_total_imputada                    BOOLEAN,
        fecha_imputacion_util                        TIMESTAMP,
        fecha_imputacion_total                       TIMESTAMP,
        distancia_centro_comuna_m                    DOUBLE,
        distancia_centro_concepcion_m                DOUBLE,
        ratio_total_util                             DOUBLE,
        amoblado                                     INT,
        estacionamientos                             DOUBLE,
        bodegas                                      DOUBLE,
        conserjeria                                  DOUBLE,
        estacionamiento_visitas                      DOUBLE,
        condominio_cerrado                           DOUBLE,
        piscina                                      DOUBLE,
        ascensor                                     DOUBLE,
        gastos_comunes                               DOUBLE,
        cantidad_paraderos                           INT,
        cantidad_estaciones_metro                    INT,
        cantidad_jardines_infantiles                 INT,
        cantidad_colegios                            INT,
        cantidad_universidades                       INT,
        cantidad_plazas                              INT,
        cantidad_supermercados                       INT,
        cantidad_farmacias                           INT,
        cantidad_centros_comerciales                 INT,
        cantidad_hospitales                          INT,
        cantidad_clinicas                            INT,
        nivel_barrio                                 INT,
        rank_nac                                     DOUBLE,
        pob_rsh_uv                                   DOUBLE,
        p_urbano                                     DOUBLE,
        c_ig_com                                     DOUBLE,
        hog_uv                                       DOUBLE,
        piso_unidad                                  DOUBLE,
        antiguedad_anos                              DOUBLE,
        precio_m2_sector_departamento                DOUBLE,
        tiene_comparables_cercanos                   BOOLEAN,
        fecha_creacion_oro                           TIMESTAMP,
        uv_rsh                                       STRING,
        fecha_vulnerabilidad_oro                     TIMESTAMP,
        fecha_chequeo_estado_oro                     TIMESTAMP,
        intentos_fallidos_chequeo_estado_oro         INT
    )
    PARTITIONED BY (fecha_publicacion_aprox)
""")
