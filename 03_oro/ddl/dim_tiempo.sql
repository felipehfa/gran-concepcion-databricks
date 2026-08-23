-- Tabla: gran_concepcion.03_oro.dim_tiempo
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Dimensión de tiempo estándar. `fecha_id = yyyyMMdd` derivado de
-- `fecha_prediccion` (no de `fecha_publicacion`, que no tiene rol de tiempo
-- propio en este modelo — queda como atributo directo en fact_aviso). Se
-- puebla incrementalmente (INSERT, NOT EXISTS) con las fechas nuevas que
-- aparezcan en stg_predicciones.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_tiempo (
    fecha_id     BIGINT NOT NULL,
    fecha        DATE NOT NULL,
    anio         INT,
    trimestre    INT,
    mes          INT,
    nombre_mes   STRING,
    dia          INT
);
