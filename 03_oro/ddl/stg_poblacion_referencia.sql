-- Tabla: gran_concepcion.03_oro.stg_poblacion_referencia
-- Creada por: 03_oro/00_carga_manual_poblacion_referencia_oro_python.py (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Población de entrenamiento congelada del modelo vigente — 06_features_oro_sql.py
-- puntúa cada aviso nuevo contra ESTA tabla (nunca contra el resto del
-- catálogo), para que reciba exactamente las mismas features que en el
-- proyecto original. Carga manual, una sola vez (o al reentrenar con un
-- dataset de referencia distinto) — se sobreescribe completa cada vez.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_poblacion_referencia (
    id_aviso          STRING NOT NULL,
    comuna            STRING,
    latitud           DOUBLE,
    longitud          DOUBLE,
    antiguedad_anos   DOUBLE,
    piso_unidad       DOUBLE,
    precio_m2         DOUBLE,
    precio_m2_valido  BOOLEAN,
    rank_nac          DOUBLE,
    pob_rsh_uv        DOUBLE,
    p_urbano          DOUBLE,
    c_ig_com          DOUBLE,
    hog_uv            DOUBLE
);
