-- Tabla: gran_concepcion.03_oro.stg_predicciones
-- Creada por: 03_oro/10_prediccion_oro_python.py (sección 2)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Una fila por (id_aviso, version_modelo) — histórico completo, nunca se
-- pisa entre reentrenamientos. Fuente de `dim_prediccion_scd2` y de las
-- medidas reales/predichas de `fact_aviso` (11_modelo_dimensional_oro_sql.py).
-- Particionada por version_modelo: la dimensión que más se filtra en las
-- queries de este notebook y del visualizador.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.stg_predicciones (
    id_aviso              STRING NOT NULL,
    version_modelo        STRING NOT NULL,
    fecha_prediccion      TIMESTAMP NOT NULL,
    costo_total_predicho  DOUBLE NOT NULL,
    z_robusto             DOUBLE,
    decil_precio          INT,
    etiqueta              STRING,
    nivel_confianza       STRING,
    cv_ensamble           DOUBLE
)
PARTITIONED BY (version_modelo);
