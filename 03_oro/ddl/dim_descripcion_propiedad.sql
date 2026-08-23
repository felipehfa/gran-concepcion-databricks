-- Tabla: gran_concepcion.03_oro.dim_descripcion_propiedad
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Dimensión satélite 1:1 con el aviso — descripción física de la propiedad.
-- Se refresca (MERGE, SCD1) desde stg_avisos_features en cada corrida.
-- `amoblado`/`ascensor`/etc. quedan INT (0/1), no BOOLEAN: así vienen
-- tipados desde 04_limpieza_plata_sql.py, y castear acá metería una
-- conversión de tipo innecesaria al MERGE.

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
);
