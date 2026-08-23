-- Tabla: gran_concepcion.03_oro.stg_referencia_estadisticas
-- Creada por: 03_oro/00_carga_manual_poblacion_referencia_oro_python.py (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Respaldos globales (clave/valor: medianas, medias, límites IQR, nivel de
-- barrio por defecto) usados por 06_features_oro_sql.py cuando un aviso
-- nuevo no tiene vecinos válidos en stg_poblacion_referencia.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_referencia_estadisticas (
    clave STRING NOT NULL,
    valor DOUBLE
);
