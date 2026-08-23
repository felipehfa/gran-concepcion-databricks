-- Tabla: gran_concepcion.03_oro.dim_estado_aviso_scd2
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- SCD2: histórico de cómo cambió `estado_publicacion` de un aviso en el
-- tiempo (activo -> pausado -> finalizado/no_disponible). El notebook cierra
-- (`valid_to`, `is_current = false`) la fila vigente ANTES de insertar la
-- nueva — nunca al revés. `fact_aviso.estado_aviso_scd_id` apunta siempre a
-- la fila `is_current = true`.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.dim_estado_aviso_scd2 (
    estado_aviso_scd_id BIGINT GENERATED ALWAYS AS IDENTITY,
    id_aviso            STRING NOT NULL,
    estado_publicacion  STRING NOT NULL,
    valid_from          TIMESTAMP NOT NULL,
    valid_to            TIMESTAMP,
    is_current          BOOLEAN NOT NULL
);
