# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # DDL — `gran_concepcion.01_bronce.avisos_detalle`
# MAGIC
# MAGIC **Creada por:** `01_bronce/02_scraper_manual_detalle_bronce_python.ipynb`
# MAGIC (sección 2) — referencia de solo lectura, el notebook fuente la crea
# MAGIC inline con `CREATE TABLE IF NOT EXISTS`. Correr esta celda es idempotente;
# MAGIC no reemplaza esa lógica, sirve para recrear el esquema en un workspace
# MAGIC nuevo o inspeccionar la definición sin abrir el notebook fuente.
# MAGIC
# MAGIC `estado_publicacion` es la captura cruda de la PRIMERA visita al aviso,
# MAGIC nunca se actualiza después (el re-chequeo periódico vive en Oro, ver
# MAGIC `03_oro/09_actualizacion_estado_avisos_oro_python.py`). 100% append-only:
# MAGIC no hay ningún `UPDATE` sobre esta tabla en todo el pipeline.

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.avisos_detalle (
        id_aviso                              STRING NOT NULL,
        descripcion                           STRING,
        fecha_publicacion_texto               STRING,
        fecha_publicacion_aprox               STRING,
        fecha_publicacion_precision           STRING,
        superficie_total_m2                   STRING,
        superficie_util_m2                    STRING,
        dormitorios                           STRING,
        banos                                 STRING,
        estacionamientos                      STRING,
        antiguedad_anos                       STRING,
        amoblado                              STRING,
        admite_mascotas                       STRING,
        condominio_cerrado                    STRING,
        bodegas                               STRING,
        gastos_comunes                        STRING,
        estacionamiento_visitas               STRING,
        solo_familias                         STRING,
        max_habitantes                        STRING,
        piscina                               STRING,
        quincho                               STRING,
        conserjeria                           STRING,
        ascensor                              STRING,
        piso_unidad                           STRING,
        deptos_por_piso                       STRING,
        barrio                                STRING,
        latitud                               STRING,
        longitud                              STRING,
        cantidad_paraderos                    STRING,
        distancia_min_m_paraderos             STRING,
        cantidad_estaciones_metro             STRING,
        distancia_min_m_estaciones_metro      STRING,
        cantidad_jardines_infantiles          STRING,
        distancia_min_m_jardines_infantiles   STRING,
        cantidad_colegios                     STRING,
        distancia_min_m_colegios              STRING,
        cantidad_universidades                STRING,
        distancia_min_m_universidades         STRING,
        cantidad_plazas                       STRING,
        distancia_min_m_plazas                STRING,
        cantidad_supermercados                STRING,
        distancia_min_m_supermercados         STRING,
        cantidad_farmacias                    STRING,
        distancia_min_m_farmacias             STRING,
        cantidad_centros_comerciales          STRING,
        distancia_min_m_centros_comerciales   STRING,
        cantidad_hospitales                   STRING,
        distancia_min_m_hospitales            STRING,
        cantidad_clinicas                     STRING,
        distancia_min_m_clinicas              STRING,
        estado_publicacion                    STRING,
        fecha_scrapeo                         STRING,
        _sistema_origen                       STRING,
        _id_corrida                           STRING
    )
    PARTITIONED BY (fecha_scrapeo)
""")
