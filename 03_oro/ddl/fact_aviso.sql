-- Tabla: gran_concepcion.03_oro.fact_aviso
-- Creada por: 03_oro/11_modelo_dimensional_oro_sql.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Hecho snapshot — grano 1 fila por aviso, se sobreescribe con el estado
-- vigente (no acumula una fila por versión de modelo, a diferencia de
-- stg_predicciones/dim_prediccion_scd2). Concentra las 6 FK directo, sin
-- hub intermedio: `dim_propiedad` (solo llaves, sin atributos propios) se
-- descartó del diseño por no cumplir función en Kimball.
--
-- Solo incluye avisos que ya tienen al menos una predicción vigente en
-- dim_prediccion_scd2 (JOIN, no LEFT JOIN, en el MERGE que puebla esta
-- tabla) — sin eso no hay nada que comparar.

CREATE TABLE IF NOT EXISTS gran_concepcion.03_oro.fact_aviso (
    id_aviso                     STRING NOT NULL,
    descripcion_id               BIGINT,
    amenidades_id                BIGINT,
    ubicacion_id                 BIGINT,
    fecha_id                     BIGINT,
    estado_aviso_scd_id          BIGINT,
    prediccion_scd_id            BIGINT,
    url                          STRING,
    fecha_publicacion_precision  STRING,
    precio_clp_real              DOUBLE,
    gastos_comunes_real          DOUBLE,
    costo_total_real             DOUBLE
);
