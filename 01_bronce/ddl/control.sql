-- Tabla: gran_concepcion.01_bronce.control
-- Creada por: 01_bronce/01_scraper_manual_grilla_bronce_python.ipynb (sección 3)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Clave/valor genérica para estado interno de los scrapers de Bronce (ej.
-- cooldown tras CAPTCHA en 02_scraper_manual_detalle_bronce). Infraestructura
-- operativa del pipeline, no una entidad de negocio — queda fuera de la
-- convención de inmutabilidad append-only del resto de Bronce.

CREATE TABLE IF NOT EXISTS gran_concepcion.01_bronce.control (
    clave STRING NOT NULL,
    valor STRING
);
