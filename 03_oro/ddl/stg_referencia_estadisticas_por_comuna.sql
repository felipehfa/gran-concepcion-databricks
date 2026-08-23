-- Tabla: gran_concepcion.03_oro.stg_referencia_estadisticas_por_comuna
-- Creada por: 03_oro/00_carga_manual_poblacion_referencia_oro_python.py (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Mismos respaldos que stg_referencia_estadisticas, agrupados por comuna —
-- segundo nivel de la cascada de fallback antes de caer a la media global.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_referencia_estadisticas_por_comuna (
    comuna              STRING NOT NULL,
    mediana_antiguedad  DOUBLE,
    media_rank_nac      DOUBLE,
    media_pob_rsh_uv    DOUBLE,
    media_p_urbano      DOUBLE,
    media_c_ig_com      DOUBLE,
    media_hog_uv        DOUBLE
);
