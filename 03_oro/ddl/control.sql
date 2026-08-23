-- Tabla: gran_concepcion.03_oro.control
-- Creada por: 03_oro/09_actualizacion_estado_avisos_oro_python.py (sección 3)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Clave/valor genérica para estado interno de Oro (cooldown tras CAPTCHA o
-- muro de verificación durante el re-chequeo de estado). Propia de Oro — no
-- se lee ni se escribe la tabla `control` de Bronce, cada capa mantiene la
-- suya. Fuera del esquema dimensional (stg_/dim_/fact_): es metadata
-- operativa del pipeline, no un dato de negocio.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.control (
    clave STRING NOT NULL,
    valor STRING
);
