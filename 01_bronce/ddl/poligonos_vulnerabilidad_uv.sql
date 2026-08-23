-- Tabla: gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv
-- Creada por: 01_bronce/00_carga_manual_poligonos_vulnerabilidad_bronce_python.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Referencia estática (shapefile IGVUST recortado a las 10 comunas del Gran
-- Concepción, reproyectado a WGS84 y guardado como WKT). Carga manual, una
-- sola vez o al actualizar el shapefile — no particionada, no incremental.
-- Única tabla de Bronce que NO es append-only por fila individual: el
-- `MERGE` de este notebook actualiza una Unidad Vecinal si ya existía.

CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.poligonos_vulnerabilidad_uv (
    uv_rsh          STRING NOT NULL,
    comuna          STRING NOT NULL,
    rank_nac        DOUBLE,
    pob_rsh_uv      DOUBLE,
    p_urbano        DOUBLE,
    c_ig_com        DOUBLE,
    hog_uv          DOUBLE,
    geometria_wkt   STRING NOT NULL,
    _sistema_origen STRING,
    _id_corrida     STRING
);
