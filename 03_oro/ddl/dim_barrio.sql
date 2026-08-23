-- Tabla: gran_concepcion.03_oro.dim_barrio
-- Creada por: 03_oro/00_carga_manual_poblacion_referencia_oro_python.py (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Única dimensión que no lleva prefijo `stg_`: se consume directo desde
-- Power BI. Mapa barrio -> nivel de precio (1-5), congelado al entrenar el
-- modelo. `barrio_id` es surrogate key para el modelo dimensional — no
-- admite `CREATE TABLE ... AS SELECT` con IDENTITY, así que el notebook la
-- crea vacía acá y la puebla con `INSERT INTO (barrio, nivel_barrio)` en la
-- sección 7 (nunca con overwrite de DataFrame). Carga manual, se sobreescribe
-- completa (`DELETE` + `INSERT`) cada vez que corre el notebook.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_barrio (
    barrio_id    BIGINT GENERATED ALWAYS AS IDENTITY,
    barrio       STRING NOT NULL,
    nivel_barrio INT NOT NULL
);
