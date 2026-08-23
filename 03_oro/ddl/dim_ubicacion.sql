-- Tabla: gran_concepcion.03_oro.dim_ubicacion
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Dimensión satélite 1:1 con el aviso — comuna, coordenadas, distancias y
-- vulnerabilidad socioterritorial. `barrio_id` es FK a dim_barrio (Snowflake
-- deliberado: barrio se comparte entre muchos avisos). Se refresca (MERGE,
-- SCD1) desde stg_avisos_features en cada corrida — `barrio_id` puede quedar
-- NULL si el barrio del aviso no está en dim_barrio.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_ubicacion (
    ubicacion_id                   BIGINT GENERATED ALWAYS AS IDENTITY,
    id_aviso                       STRING NOT NULL,
    barrio_id                      BIGINT,
    comuna                         STRING,
    latitud                        DOUBLE,
    longitud                       DOUBLE,
    distancia_centro_comuna_m      DOUBLE,
    distancia_centro_concepcion_m  DOUBLE,
    rank_nac                       DOUBLE,
    pob_rsh_uv                     DOUBLE,
    p_urbano                       DOUBLE,
    c_ig_com                       DOUBLE,
    hog_uv                         DOUBLE,
    uv_rsh                         STRING
);
