-- Tabla: gran_concepcion.03_oro.stg_features_seleccionadas
-- Creada por: 03_oro/00_carga_manual_poblacion_referencia_oro_python.py (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Lista de las features que espera el modelo vigente, en el orden exacto
-- que usa 10_prediccion_oro_python.py al armar la matriz de entrada.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_features_seleccionadas (
    feature STRING NOT NULL
);
