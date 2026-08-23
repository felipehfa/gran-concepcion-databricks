-- Tabla: gran_concepcion.01_bronce.intentos_scraping_detalle
-- Creada por: 01_bronce/02_scraper_manual_detalle_bronce_python.ipynb (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Log append-only: una fila nueva por cada intento de scrapeo de detalle
-- (nuevo o reintento entre corridas), nunca se actualiza una fila existente.
-- Reemplaza los contadores mutables que en el proyecto original vivían como
-- columnas en `avisos` — la cola de "pendientes" (qué reintentar, qué ya se
-- sabe inalcanzable) se deriva con una query contra este log, ver
-- .claude/rules/metadata-trazabilidad.md.

CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.intentos_scraping_detalle (
    id_aviso      STRING NOT NULL,
    resultado     STRING NOT NULL,
    motivo        STRING,
    fecha_intento TIMESTAMP NOT NULL
);
