-- Tabla: gran_concepcion.03_oro.dim_amenidades
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Dimensión satélite 1:1 con el aviso — puntos de interés cercanos (radio
-- 500m), 11 categorías, cada una con cantidad + distancia al más cercano.
-- Se refresca (MERGE, SCD1) desde stg_avisos_features en cada corrida.

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
);
