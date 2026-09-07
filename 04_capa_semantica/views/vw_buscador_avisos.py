# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Vista — `gran_concepcion.04_capa_semantica.vw_buscador_avisos`
# MAGIC
# MAGIC **Creada por:** `03_oro/11_modelo_dimensional_oro_sql.py` (sección 10) —
# MAGIC referencia de solo lectura, el notebook fuente la crea inline con
# MAGIC `CREATE OR REPLACE VIEW`. Correr esta celda es idempotente y no destruye
# MAGIC datos (una vista no tiene datos propios); sirve para recrear la vista en
# MAGIC un workspace nuevo o inspeccionar su definición sin abrir el notebook
# MAGIC fuente.
# MAGIC
# MAGIC **Capa semántica del buscador de arriendos.** Una fila por aviso
# MAGIC publicado (`activo` o `pausado`) con coordenadas y predicción vigente,
# MAGIC uniendo `fact_aviso` con sus dimensiones satélite y las dos SCD2. El
# MAGIC dashboard AI/BI ("Buscador de Arriendos - Gran Concepcion", dataset
# MAGIC `ds_buscador`) lee `SELECT * FROM` esta vista en vez de repetir el join —
# MAGIC así la lógica de negocio (traducción de `etiqueta`/`nivel_confianza` a
# MAGIC texto legible, filtro de estados publicables) queda versionada acá y no
# MAGIC atrapada en el JSON del dashboard.
# MAGIC
# MAGIC Vista plana (no materializada): el join es liviano sobre el esquema
# MAGIC estrella y se recalcula en cada refresh del dashboard.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW gran_concepcion.04_capa_semantica.vw_buscador_avisos AS
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
