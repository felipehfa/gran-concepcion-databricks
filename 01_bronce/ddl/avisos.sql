-- Tabla: gran_concepcion.01_bronce.avisos
-- Creada por: 01_bronce/01_scraper_manual_grilla_bronce_python.ipynb (sección 3)
-- El notebook la crea inline con `CREATE TABLE IF NOT EXISTS` — este archivo
-- es la referencia de solo lectura del esquema, no se ejecuta como parte del
-- pipeline.
--
-- Append-only: nunca se actualiza una fila insertada. `estado_publicacion`
-- NO vive acá (vive en `avisos_detalle`, ver metadata-trazabilidad.md).

CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.avisos (
    id_aviso        STRING NOT NULL,
    comuna          STRING,
    tipo_propiedad  STRING,
    operacion       STRING,
    titulo          STRING,
    precio          STRING,
    moneda          STRING,
    ubicacion       STRING,
    dormitorios     STRING,
    banos           STRING,
    superficie_m2   STRING,
    url             STRING,
    first_seen      STRING,
    _sistema_origen STRING,
    _id_corrida     STRING
)
PARTITIONED BY (first_seen);
