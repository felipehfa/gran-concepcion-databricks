-- Tabla: gran_concepcion.02_plata.valores_pesos
-- Creada por: 02_plata/03_tasas_historicas_plata_python.py (sección 1)
-- Referencia de solo lectura — el notebook la crea inline.
--
-- Caché de valores de UF/USD por fecha (mindicador.cl), para convertir
-- precios a CLP con la tasa vigente el día exacto de publicación del aviso,
-- no la más reciente disponible. Se cachea solo lo que hace falta, nunca se
-- vuelve a consultar una fecha ya guardada.

CREATE TABLE IF NOT EXISTS gran_concepcion.02_plata.valores_pesos (
    fecha_valor     DATE,
    valor_uf_clp    DOUBLE,
    valor_dolar_clp DOUBLE,
    fecha_consulta  DATE
);
