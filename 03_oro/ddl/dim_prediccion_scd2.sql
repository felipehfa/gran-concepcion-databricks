-- Tabla: gran_concepcion.03_oro.dim_prediccion_scd2
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- SCD2: histórico de cómo cambió la evaluación del modelo para un aviso
-- (típicamente entre reentrenamientos) — costo predicho, z-score, etiqueta,
-- confianza. Guarda measures (no solo atributos categóricos) a propósito:
-- son necesarias para reconstruir "qué decía el modelo en el momento X",
-- algo que stg_predicciones no responde directo sin filtrar por versión.
-- Mismo patrón cerrar-antes-de-insertar que dim_estado_aviso_scd2.
-- `fact_aviso.prediccion_scd_id` apunta siempre a la fila `is_current = true`.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_prediccion_scd2 (
    prediccion_scd_id     BIGINT GENERATED ALWAYS AS IDENTITY,
    id_aviso              STRING NOT NULL,
    version_modelo        STRING NOT NULL,
    costo_total_predicho  DOUBLE NOT NULL,
    z_robusto             DOUBLE,
    cv_ensamble           DOUBLE,
    decil_precio          INT,
    etiqueta              STRING,
    nivel_confianza       STRING,
    fecha_prediccion      TIMESTAMP,
    valid_from            TIMESTAMP NOT NULL,
    valid_to              TIMESTAMP,
    is_current            BOOLEAN NOT NULL
);
